use super::{BcCamera, Error, Result};
use crate::{bc::model::*, bc::xml::*, bcmedia::model::*};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver;
use tokio::time::timeout;

/// What a talk does when the camera answers its talk request with 422.
///
/// The camera answers 422 when another client is talking, or when an earlier
/// talk ended without sending message 11.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TalkOnBusy {
    /// Return [`Error::TalkBusy`] and send nothing more.
    Fail,
    /// Send message 11 to reset the camera's talk, then send the same talk
    /// request again. The official client does this.
    #[default]
    Preempt,
}

/// How long `talk_stream` waits for the camera to answer one block of audio.
const TALK_REPLY_TIMEOUT: Duration = Duration::from_secs(2);

/// How far `talk_stream` may run ahead of the audio the camera has played,
/// once the camera has stopped answering blocks and so gives no back pressure.
const TALK_SILENT_LEAD: Duration = Duration::from_millis(500);

impl BcCamera {
    ///
    /// Finish Talk
    ///
    /// The send the talk finish to the camera.
    ///
    /// It is also sent when the request for talk config returns status code 422
    ///
    pub async fn talk_stop(&self) -> Result<()> {
        let connection = self.get_connection();

        let msg_num = self.new_message_num();
        let mut sub = connection.subscribe(MSG_ID_TALKRESET, msg_num).await?;

        let msg = Bc {
            meta: BcMeta {
                msg_id: MSG_ID_TALKRESET,
                channel_id: self.channel_id,
                msg_num,
                stream_type: 0,
                response_code: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    channel_id: Some(self.channel_id),
                    ..Default::default()
                }),
                payload: None,
            }),
        };

        sub.send(msg).await?;
        let msg = sub.recv().await?;

        if let BcMeta {
            response_code: 200, ..
        } = msg.meta
        {
        } else {
            return Err(Error::UnintelligibleReply {
                reply: std::sync::Arc::new(Box::new(msg)),
                why: "The camera did not accept the talk stop command.",
            });
        }

        Ok(())
    }

    ///
    /// Requests the [`TalkAbility`] xml
    ///
    pub async fn talk_ability(&self) -> Result<TalkAbility> {
        let connection = self.get_connection();
        let msg_num = self.new_message_num();
        let mut sub_get = connection.subscribe(MSG_ID_TALKABILITY, msg_num).await?;
        let get = Bc {
            meta: BcMeta {
                msg_id: MSG_ID_TALKABILITY,
                channel_id: self.channel_id,
                msg_num,
                response_code: 0,
                stream_type: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    channel_id: Some(self.channel_id),
                    ..Default::default()
                }),
                payload: None,
            }),
        };

        sub_get.send(get).await?;
        let msg = sub_get.recv().await?;

        if let BcBody::ModernMsg(ModernMsg {
            payload:
                Some(BcPayloads::BcXml(BcXml {
                    talk_ability: Some(talk_ability),
                    ..
                })),
            ..
        }) = msg.body
        {
            Ok(talk_ability)
        } else {
            Err(Error::UnintelligibleReply {
                reply: std::sync::Arc::new(Box::new(msg)),
                why: "Expected TalkAbility xml but it was not recieved",
            })
        }
    }

    /// The message 201 that asks the camera to take talk in `talk_config`.
    fn talk_config_msg(&self, msg_num: u16, talk_config: &TalkConfig) -> Bc {
        Bc {
            meta: BcMeta {
                msg_id: MSG_ID_TALKCONFIG,
                channel_id: self.channel_id,
                msg_num,
                stream_type: 0,
                response_code: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    channel_id: Some(self.channel_id),
                    ..Default::default()
                }),
                payload: Some(BcPayloads::BcXml(BcXml {
                    talk_config: Some(talk_config.clone()),
                    ..Default::default()
                })),
            }),
        }
    }

    /// The message 202 that carries one payload of ADPCM blocks.
    fn talk_data_msg(&self, msg_num: u16, payload: Vec<u8>) -> Bc {
        Bc {
            meta: BcMeta {
                msg_id: MSG_ID_TALK,
                channel_id: self.channel_id,
                msg_num,
                stream_type: 0,
                response_code: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    channel_id: Some(self.channel_id),
                    binary_data: Some(1),
                    ..Default::default()
                }),
                payload: Some(BcPayloads::Binary(payload)),
            }),
        }
    }

    /// Sends the talk request and applies `on_busy` to a 422.
    ///
    /// On a 422 with [`TalkOnBusy::Preempt`] it sends message 11 and then the
    /// same request again. The camera's own reply is never sent back.
    async fn talk_open(&self, talk_config: &TalkConfig, on_busy: TalkOnBusy) -> Result<()> {
        if talk_config.audio_config.audio_type != "adpcm" {
            return Err(Error::UnknownTalkEncoding);
        }

        let connection = self.get_connection();
        let msg_num = self.new_message_num();
        let mut sub = connection.subscribe(MSG_ID_TALKCONFIG, msg_num).await?;

        sub.send(self.talk_config_msg(msg_num, talk_config)).await?;
        let mut msg = sub.recv().await?;

        // If another client is already talking OR if we crashed before sending
        // msgid 11 the camera will reply 422
        //
        // The official client in this case just sends msgid 11 and retries
        if msg.meta.response_code == 422 {
            match on_busy {
                TalkOnBusy::Fail => return Err(Error::TalkBusy),
                TalkOnBusy::Preempt => {
                    self.talk_stop().await?;
                    sub.send(self.talk_config_msg(msg_num, talk_config)).await?;
                    msg = sub.recv().await?;
                }
            }
        }

        if msg.meta.response_code != 200 {
            return Err(Error::UnintelligibleReply {
                reply: std::sync::Arc::new(Box::new(msg)),
                why:
                    "The camera did not accept the TalkConfig xml. Audio format is likely incorrect",
            });
        }
        Ok(())
    }

    ///
    /// Send sound to the camera
    ///
    /// The data should be in the format as described in `<TalkAbility>` xml
    /// This method assumes that you have set up the data in the desired format
    /// in the `<TalkAbility>` xml
    ///
    /// It also checks that it is ADPCM as the code is written to accept only that
    ///
    /// It paces the data with a tokio timer, so it never blocks the runtime.
    ///
    /// # Parameters
    ///
    /// * `adpcm` - Data must be adpcm in DVI-4 format
    ///
    /// * `talk_config` - The talk config that describes the adpcm data
    ///
    /// * `on_busy` - What to do when the camera answers that it is busy
    ///
    pub async fn talk(
        &self,
        adpcm: &[u8],
        talk_config: TalkConfig,
        on_busy: TalkOnBusy,
    ) -> Result<()> {
        let block_size = talk_config.audio_config.length_per_encoder / 2;
        let sample_rate = talk_config.audio_config.sample_rate;

        self.talk_open(&talk_config, on_busy).await?;

        let connection = self.get_connection();
        let full_block_size = block_size + 4; // Block size + predictor state
        let msg_num = self.new_message_num();
        let sub = connection.subscribe(MSG_ID_TALK, msg_num).await?;

        const BLOCK_PER_PAYLOAD: usize = 4;
        const BLOCK_HEADER_SIZE: usize = 4;
        const SAMPLES_PER_BYTE: usize = 2;

        for payload_bytes in adpcm.chunks(full_block_size as usize * BLOCK_PER_PAYLOAD) {
            let mut payload = vec![];
            for bytes in payload_bytes.chunks(full_block_size as usize) {
                let bcmedia_adpcm = BcMedia::Adpcm(BcMediaAdpcm {
                    data: bytes.to_vec(),
                });
                payload = bcmedia_adpcm.serialize(payload)?;
            }

            sub.send(self.talk_data_msg(msg_num, payload)).await?;

            let adpcm_len = payload_bytes.len();
            // There are two samples per byte
            //
            // To calculate the bytes we subtract the block headers from the len
            //
            // There is 1 initial sample stored in the block header so we add that in the end
            //
            let samples_sent = adpcm_len.saturating_sub(BLOCK_HEADER_SIZE * BLOCK_PER_PAYLOAD)
                * SAMPLES_PER_BYTE
                + BLOCK_PER_PAYLOAD;

            // Time to play the sample in seconds
            let play_length = samples_sent as f32 / sample_rate as f32;
            tokio::time::sleep(Duration::from_secs_f32(play_length)).await;
        }

        self.talk_stop().await?;

        Ok(())
    }

    ///
    /// Send sound to the camera through a channel
    ///
    /// This is similar to [`talk`] except it uses a channel to receive data.
    /// It reads the channel with `.recv().await` and ends when every sender
    /// has gone and the channel is empty.
    ///
    /// It waits up to two seconds for the camera to answer each block. After
    /// the first block with no answer it logs once, stops waiting and paces
    /// the blocks by their play time instead.
    ///
    /// The data should be in the format as described in `<TalkAbility>` xml
    /// This method assumes that you have set up the data in the desired format
    /// in the `<TalkAbility>` xml
    ///
    /// It also checks that it is ADPCM as the code is written to accept only that
    ///
    /// # Parameters
    ///
    /// * `rx` - Data must be adpcm in DVI-4 format
    ///
    /// * `talk_config` - The talk config that describes the adpcm data
    ///
    /// * `on_busy` - What to do when the camera answers that it is busy
    ///
    pub async fn talk_stream(
        &self,
        rx: &mut Receiver<Vec<u8>>,
        talk_config: TalkConfig,
        on_busy: TalkOnBusy,
    ) -> Result<()> {
        let block_size = talk_config.audio_config.length_per_encoder / 2;
        let sample_rate = talk_config.audio_config.sample_rate;

        self.talk_open(&talk_config, on_busy).await?;

        let connection = self.get_connection();
        let full_block_size = block_size + 4; // Block size + predictor state
        let msg_num = self.new_message_num();
        let mut sub = connection.subscribe(MSG_ID_TALK, msg_num).await?;

        const BLOCK_PER_PAYLOAD: usize = 1;
        const BLOCK_HEADER_SIZE: usize = 4;
        const SAMPLES_PER_BYTE: usize = 2;

        let target_chunks = full_block_size as usize * BLOCK_PER_PAYLOAD;

        let mut pending: Vec<u8> = vec![];
        let mut end_of_stream = false;
        let mut camera_answers = true;
        let mut expected_stream_end = Instant::now();
        while !end_of_stream {
            while pending.len() < target_chunks {
                match rx.recv().await {
                    Some(data) => pending.extend(data),
                    None => {
                        // Every sender has gone and the channel is empty
                        end_of_stream = true;
                        break;
                    }
                }
            }

            let take = std::cmp::min(target_chunks, pending.len());
            let payload_bytes: Vec<u8> = pending.drain(..take).collect();

            let mut payload = vec![];
            for block_bytes in payload_bytes.chunks(full_block_size as usize) {
                let bytes: Vec<u8> = block_bytes.to_vec();
                let bcmedia_adpcm = BcMedia::Adpcm(BcMediaAdpcm { data: bytes });
                payload = bcmedia_adpcm.serialize(payload)?;
            }

            let adpcm_len = payload_bytes.len();

            // There are two samples per byte
            //
            // To calculate the bytes we subtract the block headers from the len
            //
            // There is 1 initial sample stored in the block header so we add that in the end
            //
            let samples_sent = if adpcm_len >= BLOCK_HEADER_SIZE * BLOCK_PER_PAYLOAD {
                (adpcm_len - BLOCK_HEADER_SIZE * BLOCK_PER_PAYLOAD) * SAMPLES_PER_BYTE
                    + BLOCK_PER_PAYLOAD
            } else {
                // Zero samples in this block
                break;
            };

            // Time to play the sample in seconds
            let play_length = Duration::from_secs_f32(samples_sent as f32 / sample_rate as f32);

            if !camera_answers {
                // No answers means no back pressure, so keep at most
                // TALK_SILENT_LEAD ahead of what the camera has played.
                let ahead = expected_stream_end.saturating_duration_since(Instant::now());
                if ahead > TALK_SILENT_LEAD {
                    tokio::time::sleep(ahead - TALK_SILENT_LEAD).await;
                }
            }

            let time_sent = Instant::now();
            sub.send(self.talk_data_msg(msg_num, payload)).await?;
            if time_sent > expected_stream_end {
                expected_stream_end = time_sent + play_length;
            } else {
                expected_stream_end += play_length;
            }

            if camera_answers {
                match timeout(TALK_REPLY_TIMEOUT, sub.recv()).await {
                    Ok(reply) => {
                        let _ = reply?;
                    }
                    Err(_) => {
                        log::warn!(
                            "The camera did not answer a block of talk audio within {:?}. \
                            Talk goes on without waiting for answers.",
                            TALK_REPLY_TIMEOUT
                        );
                        camera_answers = false;
                    }
                }
            } else {
                // Take any late answers so they do not fill the subscription
                while let Ok(reply) = timeout(Duration::ZERO, sub.recv()).await {
                    let _ = reply?;
                }
            }
        }

        // Chunks are still being played, while talk_stop will interrupt them. Wait until we expect
        // the stream to end (+ and extra 100ms) before issuing talk_stop.
        let remaining_stream_duration =
            expected_stream_end.saturating_duration_since(Instant::now());
        tokio::time::sleep(remaining_stream_duration + Duration::from_millis(100)).await;

        self.talk_stop().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! An in-process fake camera: no network and no real device. Every talk
    //! config and every audio byte here is SYNTHETIC.
    use super::*;
    use crate::bc_protocol::{BcConnSink, BcConnSource, BcConnection, Credentials, ReadKind};
    use futures::{channel::mpsc::unbounded, SinkExt, StreamExt};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU16};
    use std::sync::{Arc, Mutex};
    use tokio::sync::RwLock;
    use tokio_util::sync::CancellationToken;

    /// A made-up talk config: ADPCM at 16000 Hz with 512 byte blocks.
    fn talk_config() -> TalkConfig {
        TalkConfig {
            channel_id: 0,
            duplex: "FDX".to_string(),
            audio_stream_mode: "followVideoStream".to_string(),
            audio_config: AudioConfig {
                audio_type: "adpcm".to_string(),
                sample_rate: 16000,
                sample_precision: 16,
                length_per_encoder: 1024,
                sound_track: "mono".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// One ADPCM block of [`talk_config`]: 512 data bytes and a 4 byte header.
    const FULL_BLOCK: usize = 516;

    fn reply(request: &Bc, code: u16) -> Bc {
        Bc {
            meta: BcMeta {
                response_code: code,
                ..request.meta
            },
            body: BcBody::ModernMsg(ModernMsg::default()),
        }
    }

    /// A fake camera that keeps every message it receives. `respond` returns
    /// the reply codes for each message, in order.
    struct Fake {
        camera: BcCamera,
        received: Arc<Mutex<Vec<Bc>>>,
    }

    impl Fake {
        async fn new<F>(mut respond: F) -> Self
        where
            F: FnMut(&Bc) -> Vec<u16> + Send + 'static,
        {
            let received = Arc::new(Mutex::new(Vec::new()));
            let log = received.clone();
            let (to_cam, mut cam_rx) = unbounded::<Bc>();
            let (cam_tx, from_cam) = unbounded::<Result<Bc>>();
            tokio::spawn(async move {
                while let Some(request) = cam_rx.next().await {
                    let replies: Vec<Bc> = respond(&request)
                        .into_iter()
                        .map(|code| reply(&request, code))
                        .collect();
                    // Log before replying, so a caller that has its answer
                    // always finds its message in the log.
                    log.lock().unwrap().push(request);
                    for reply in replies {
                        let _ = cam_tx.unbounded_send(Ok(reply));
                    }
                }
            });
            let sink: BcConnSink =
                Box::new(to_cam.sink_map_err(|_| Error::Other("fake camera is gone")));
            let source: BcConnSource = Box::new(from_cam);
            let connection = BcConnection::new(sink, source)
                .await
                .expect("in-memory connection");
            let mut abilities = HashMap::new();
            abilities.insert("talk".to_string(), ReadKind::ReadWrite);
            Fake {
                camera: BcCamera {
                    channel_id: 0,
                    connection: Arc::new(connection),
                    logged_in: AtomicBool::new(true),
                    message_num: AtomicU16::new(0),
                    credentials: Credentials::new("fake", None::<String>),
                    abilities: RwLock::new(abilities),
                    cancel: CancellationToken::new(),
                },
                received,
            }
        }

        fn ids(&self) -> Vec<u32> {
            self.received
                .lock()
                .unwrap()
                .iter()
                .map(|bc| bc.meta.msg_id)
                .collect()
        }
    }

    /// A camera that answers every message with 200.
    fn answers_all(_: &Bc) -> Vec<u16> {
        vec![200]
    }

    /// The ADPCM payload `talk_stream` puts in a 202 for one block.
    fn payload_of(block: &[u8]) -> Vec<u8> {
        BcMedia::Adpcm(BcMediaAdpcm {
            data: block.to_vec(),
        })
        .serialize(vec![])
        .expect("serialise a synthetic block")
    }

    fn binary(bc: &Bc) -> Vec<u8> {
        match &bc.body {
            BcBody::ModernMsg(ModernMsg {
                payload: Some(BcPayloads::Binary(data)),
                ..
            }) => data.clone(),
            other => panic!("expected a binary payload, got {:?}", other),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn talk_does_not_block_the_runtime() {
        let fake = Fake::new(answers_all).await;
        // Eight payloads of four blocks: about 2 seconds of audio at 16000 Hz.
        let adpcm = vec![0x11u8; FULL_BLOCK * 4 * 8];

        // The ticker records the time of each tick. Skip keeps a starved
        // ticker from firing its missed ticks in a burst, so a blocked runtime
        // shows as one long gap between two ticks.
        let ticks = Arc::new(Mutex::new(Vec::<Instant>::new()));
        let log = ticks.clone();
        let ticker = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                log.lock().unwrap().push(Instant::now());
            }
        });
        // Let the ticker take its first tick before the talk starts.
        tokio::task::yield_now().await;

        let started = Instant::now();
        fake.camera
            .talk(&adpcm, talk_config(), TalkOnBusy::Preempt)
            .await
            .expect("talk");
        let elapsed = started.elapsed();
        ticker.abort();
        let mut times = ticks.lock().unwrap().clone();
        times.push(Instant::now());

        assert!(
            elapsed >= Duration::from_millis(1800),
            "talk took {:?}",
            elapsed
        );
        // One payload of four blocks plays for 256 milliseconds. A blocking
        // sleep of one payload leaves a gap at least that long. A tokio sleep
        // leaves gaps near 50 milliseconds. The bar leaves room for a loaded
        // host and stays under one payload.
        let largest_gap = times
            .windows(2)
            .map(|pair| pair[1].duration_since(pair[0]))
            .max()
            .expect("at least two ticks");
        assert!(
            largest_gap < Duration::from_millis(200),
            "the timer stalled for {:?} during a talk of {:?}, {} ticks",
            largest_gap,
            elapsed,
            times.len()
        );
    }

    #[tokio::test]
    async fn talk_422_resends_the_request() {
        let mut configs = 0;
        let fake = Fake::new(move |bc| {
            if bc.meta.msg_id == MSG_ID_TALKCONFIG {
                configs += 1;
                if configs == 1 {
                    return vec![422];
                }
            }
            vec![200]
        })
        .await;

        fake.camera
            .talk(&[0x22u8; FULL_BLOCK], talk_config(), TalkOnBusy::Preempt)
            .await
            .expect("talk after a 422");

        let received = fake.received.lock().unwrap();
        let ids: Vec<u32> = received.iter().map(|bc| bc.meta.msg_id).collect();
        assert_eq!(
            ids,
            vec![
                MSG_ID_TALKCONFIG,
                MSG_ID_TALKRESET,
                MSG_ID_TALKCONFIG,
                MSG_ID_TALK,
                MSG_ID_TALKRESET
            ]
        );
        assert_eq!(
            received[0], received[2],
            "the retry is not the first request"
        );
        assert_eq!(received[0].meta.response_code, 0);
        match &received[2].body {
            BcBody::ModernMsg(ModernMsg {
                payload:
                    Some(BcPayloads::BcXml(BcXml {
                        talk_config: Some(sent),
                        ..
                    })),
                ..
            }) => assert_eq!(sent, &talk_config()),
            other => panic!("the retry carries no talk config: {:?}", other),
        }
    }

    #[tokio::test]
    async fn talk_busy_fails_when_asked() {
        let fake = Fake::new(|bc| {
            if bc.meta.msg_id == MSG_ID_TALKCONFIG {
                vec![422]
            } else {
                vec![200]
            }
        })
        .await;

        let result = fake
            .camera
            .talk(&[0x33u8; FULL_BLOCK], talk_config(), TalkOnBusy::Fail)
            .await;
        assert!(matches!(result, Err(Error::TalkBusy)), "got {:?}", result);

        let (_tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4);
        let result = fake
            .camera
            .talk_stream(&mut rx, talk_config(), TalkOnBusy::Fail)
            .await;
        assert!(matches!(result, Err(Error::TalkBusy)), "got {:?}", result);

        // Let any stray message reach the fake before reading its log.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(fake.ids(), vec![MSG_ID_TALKCONFIG, MSG_ID_TALKCONFIG]);
    }

    /// Three distinct synthetic blocks.
    fn three_blocks() -> Vec<Vec<u8>> {
        (1..=3u8).map(|n| vec![n; FULL_BLOCK]).collect()
    }

    /// Sends the blocks into a channel in frames that do not line up with the
    /// block edges, then closes it.
    fn send_in_odd_frames(blocks: &[Vec<u8>]) -> tokio::sync::mpsc::Receiver<Vec<u8>> {
        let all: Vec<u8> = blocks.concat();
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
        tokio::spawn(async move {
            for frame in all.chunks(300) {
                tx.send(frame.to_vec()).await.expect("talk_stream reads");
                tokio::task::yield_now().await;
            }
        });
        rx
    }

    #[tokio::test]
    async fn talk_stream_reads_async() {
        let fake = Fake::new(answers_all).await;
        let blocks = three_blocks();
        let mut rx = send_in_odd_frames(&blocks);

        fake.camera
            .talk_stream(&mut rx, talk_config(), TalkOnBusy::Preempt)
            .await
            .expect("talk_stream");

        let received = fake.received.lock().unwrap();
        let ids: Vec<u32> = received.iter().map(|bc| bc.meta.msg_id).collect();
        assert_eq!(
            ids,
            vec![
                MSG_ID_TALKCONFIG,
                MSG_ID_TALK,
                MSG_ID_TALK,
                MSG_ID_TALK,
                MSG_ID_TALKRESET
            ]
        );
        for (i, block) in blocks.iter().enumerate() {
            assert_eq!(binary(&received[i + 1]), payload_of(block), "block {i}");
        }
    }

    #[tokio::test]
    async fn talk_stream_survives_a_silent_camera() {
        let fake = Fake::new(|bc| {
            if bc.meta.msg_id == MSG_ID_TALK {
                vec![]
            } else {
                vec![200]
            }
        })
        .await;
        let blocks = three_blocks();
        let mut rx = send_in_odd_frames(&blocks);

        timeout(
            Duration::from_secs(10),
            fake.camera
                .talk_stream(&mut rx, talk_config(), TalkOnBusy::Preempt),
        )
        .await
        .expect("talk_stream hung on a silent camera")
        .expect("talk_stream");

        let received = fake.received.lock().unwrap();
        let ids: Vec<u32> = received.iter().map(|bc| bc.meta.msg_id).collect();
        assert_eq!(
            ids,
            vec![
                MSG_ID_TALKCONFIG,
                MSG_ID_TALK,
                MSG_ID_TALK,
                MSG_ID_TALK,
                MSG_ID_TALKRESET
            ]
        );
        for (i, block) in blocks.iter().enumerate() {
            assert_eq!(binary(&received[i + 1]), payload_of(block), "block {i}");
        }
    }
}
