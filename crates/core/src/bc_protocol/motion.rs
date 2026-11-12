use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, error::TryRecvError, Receiver};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Motion Status that the callback can send
#[derive(Clone, Copy, Debug)]
pub enum MotionStatus {
    /// Sent when motion is first detected
    Start(Instant),
    /// Sent when motion stops
    Stop(Instant),
    /// Sent when an Alarm about something other than motion was received
    NoChange(Instant),
}

/// A handle on current motion related events comming from the camera
///
/// When this object is dropped the motion events are stopped
pub struct MotionData {
    handle: JoinSet<Result<()>>,
    cancel: CancellationToken,
    rx: Receiver<Result<MotionStatus>>,
    last_update: MotionStatus,
}

impl MotionData {
    /// Get if motion has been detected. Returns None if
    /// no motion data has yet been recieved from the camera
    ///
    /// An error is raised if the motion connection to the camera is dropped
    pub fn motion_detected(&mut self) -> Result<Option<bool>> {
        self.consume_motion_events()?;
        Ok(match &self.last_update {
            MotionStatus::Start(_) => Some(true),
            MotionStatus::Stop(_) => Some(false),
            MotionStatus::NoChange(_) => None,
        })
    }

    /// Get if motion has been detected within given duration. Returns None if
    /// no motion data has yet been recieved from the camera
    ///
    /// An error is raised if the motion connection to the camera is dropped
    pub fn motion_detected_within(&mut self, duration: Duration) -> Result<Option<bool>> {
        self.consume_motion_events()?;
        Ok(match &self.last_update {
            MotionStatus::Start(_) => Some(true),
            MotionStatus::Stop(time) => Some((Instant::now() - *time) < duration),
            MotionStatus::NoChange(_) => None,
        })
    }

    /// Consume the motion events diretly
    ///
    /// An error is raised if the motion connection to the camera is dropped
    pub fn consume_motion_events(&mut self) -> Result<Vec<MotionStatus>> {
        let mut results: Vec<MotionStatus> = vec![];
        loop {
            match self.rx.try_recv() {
                Ok(motion) => results.push(motion?),
                Err(TryRecvError::Empty) => break,
                Err(e) => return Err(Error::from(e)),
            }
        }
        if let Some(last) = results.last() {
            self.last_update = *last;
        }
        Ok(results)
    }

    /// Await a new motion event
    ///
    ///
    pub async fn next_motion(&mut self) -> Result<MotionStatus> {
        let motions = self.consume_motion_events()?;
        if let Some(last) = motions.last() {
            Ok(*last)
        } else if let Some(moition) = self.rx.recv().await {
            let moition = moition?;
            self.last_update = moition;
            Ok(moition)
        } else {
            Err(Error::Other("Motion dropped"))
        }
    }

    /// Wait for the motion to stop
    ///
    /// It must be stopped for at least the given duration
    pub async fn await_stop(&mut self, duration: Duration) -> Result<()> {
        let motions = self.consume_motion_events()?;
        let mut last_motion = motions.last().copied();
        loop {
            if let Some(MotionStatus::Stop(time)) = last_motion {
                // In stop state
                if duration.is_zero() || (Instant::now() - time) > duration {
                    return Ok(());
                } else {
                    // Schedule a sleep or wait for motion to start
                    let remaining_sleep = duration - (Instant::now() - time);
                    let result = tokio::select! {
                        _ = tokio::time::sleep(remaining_sleep) => {None},
                        v = async {
                            loop {
                                match self.next_motion().await {
                                    n @ Ok(MotionStatus::Start(_)) => {return n;},
                                    n @ Err(_) => {return n;},
                                    _ => {continue;}
                                }
                            }
                        } => {Some(v)}
                    };
                    if let Some(v) = result {
                        v?;
                    } else {
                        return Ok(());
                    }
                }
            }
            last_motion = Some(self.next_motion().await?);
        }
    }

    /// Wait for the motion to start
    ///
    /// The motion must have a minimum duration as given
    pub async fn await_start(&mut self, duration: Duration) -> Result<()> {
        let motions = self.consume_motion_events()?;
        let mut last_motion = motions.last().copied();
        loop {
            if let Some(MotionStatus::Start(time)) = last_motion {
                // In start state
                if duration.is_zero() || (Instant::now() - time) > duration {
                    return Ok(());
                } else {
                    // Schedule a sleep or wait for motion to stop
                    let result = tokio::select! {
                        _ = tokio::time::sleep(duration - (Instant::now() - time)) => {None},
                        v = async {
                            loop {
                                match self.next_motion().await {
                                    n @ Ok(MotionStatus::Stop(_)) => {return n;},
                                    n @ Err(_) => {return n;},
                                    _ => {continue;}
                                }
                            }
                        } => {Some(v)}
                    };
                    if let Some(v) = result {
                        v?;
                    } else {
                        return Ok(());
                    }
                }
            }
            last_motion = Some(self.next_motion().await?);
        }
    }
}

impl BcCamera {
    /// This message tells the camera to send the motion events to us
    /// Which are the recieved on msgid 33
    pub(super) async fn start_motion_query(&self) -> Result<u16> {
        self.has_ability_rw("motion").await?;
        let connection = self.get_connection();

        let msg_num = self.new_message_num();
        let mut sub = connection.subscribe(MSG_ID_MOTION_REQUEST, msg_num).await?;
        let msg = Bc {
            meta: BcMeta {
                msg_id: MSG_ID_MOTION_REQUEST,
                channel_id: self.channel_id,
                msg_num,
                stream_type: 0,
                response_code: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                ..Default::default()
            }),
        };

        sub.send(msg).await?;

        let msg = sub.recv().await?;

        if let BcMeta {
            response_code: 200, ..
        } = msg.meta
        {
            Ok(msg_num)
        } else {
            Err(Error::UnintelligibleReply {
                reply: std::sync::Arc::new(Box::new(msg)),
                why: "The camera did not accept the request to start motion",
            })
        }
    }

    /// This returns a data structure which can be used to
    /// query motion events
    pub async fn listen_on_motion(&self) -> Result<MotionData> {
        let connection = self.get_connection();

        // After start_motion_query (MSG_ID 31) the camera sends motion messages
        // when whenever motion is detected.
        let (tx, rx) = channel(20);

        let mut set = JoinSet::new();
        let channel_id = self.channel_id;
        let cancel = CancellationToken::new();
        let thread_cancel = cancel.clone();
        let (subscribed_tx, subscribed_rx) = oneshot::channel();
        set.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    let mut sub = connection.subscribe_to_id(MSG_ID_MOTION).await?;
                    let _ = subscribed_tx.send(());

                    loop {
                        tokio::task::yield_now().await;
                        let msg = sub.recv().await;
                        let status = match msg {
                            Ok(motion_msg) => {
                                if let BcBody::ModernMsg(ModernMsg {
                                    payload:
                                        Some(BcPayloads::BcXml(BcXml {
                                            alarm_event_list: Some(alarm_event_list),
                                            ..
                                        })),
                                    ..
                                }) = motion_msg.body
                                {
                                    let mut result = MotionStatus::NoChange(Instant::now());
                                    for alarm_event in &alarm_event_list.alarm_events {
                                        if alarm_event.channel_id == channel_id {
                                            if alarm_event.status != "none"
                                                || alarm_event
                                                    .ai_type
                                                    .as_ref()
                                                    .map(|ai_type| ai_type != "none")
                                                    .unwrap_or(false)
                                            {
                                                result = MotionStatus::Start(Instant::now());
                                                break;
                                            } else {
                                                result = MotionStatus::Stop(Instant::now());
                                                break;
                                            }
                                        }
                                    }
                                    Ok(result)
                                } else {
                                    Ok(MotionStatus::NoChange(Instant::now()))
                                }
                            }
                            // On connection drop we stop
                            Err(e) => Err(e),
                        };

                        if tx.send(status).await.is_err() {
                            // Motion reciever has been dropped
                            break;
                        }
                    }
                    Ok(())
                } => v,
            }
        });

        // The message 33 subscriber must exist before message 31 is sent.
        // A message 33 that reaches the connection with no subscriber is
        // dropped, and the camera may send one right behind its reply.
        if subscribed_rx.await.is_err() {
            return Err(match set.join_next().await {
                Some(Ok(Err(e))) => e,
                _ => Error::DroppedSubscriber,
            });
        }
        self.start_motion_query().await?;

        Ok(MotionData {
            handle: set,
            cancel,
            rx,
            last_update: MotionStatus::NoChange(Instant::now()),
        })
    }
}

impl Drop for MotionData {
    fn drop(&mut self) {
        log::trace!("Drop MotionData");
        self.cancel.cancel();
        let mut handle = std::mem::take(&mut self.handle);
        let _gt = tokio::runtime::Handle::current().enter();
        tokio::task::spawn(async move {
            while handle.join_next().await.is_some() {}
            log::trace!("Dropped MotionData");
        });
    }
}

#[cfg(test)]
mod tests {
    //! An in-process fake camera: no network and no real device. The alarm
    //! XML is SYNTHETIC, written by hand to the field names of [`AlarmEvent`].
    use super::*;
    use crate::bc_protocol::{BcConnSink, BcConnSource, BcConnection, Credentials, ReadKind};
    use futures::{channel::mpsc::unbounded, SinkExt, StreamExt};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU16};
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tokio::time::timeout;

    const CHANNEL: u8 = 0;

    /// A message 33 that reports motion and a person on [`CHANNEL`].
    fn alarm_frame() -> Bc {
        Bc::new_from_xml(
            BcMeta {
                msg_id: MSG_ID_MOTION,
                channel_id: CHANNEL,
                msg_num: 0,
                stream_type: 0,
                response_code: 0,
                class: 0x0000,
            },
            BcXml {
                alarm_event_list: Some(AlarmEventList {
                    version: "1.1".to_string(),
                    alarm_events: vec![AlarmEvent {
                        version: "1.1".to_string(),
                        channel_id: CHANNEL,
                        status: "MD".to_string(),
                        ai_type: Some("people".to_string()),
                        recording: 0,
                        timeStamp: 0,
                    }],
                }),
                ..Default::default()
            },
        )
    }

    /// A camera that answers message 31 with 200 and sends one alarm right
    /// behind the reply, in the same burst. A real camera may do this when
    /// motion is already under way as the request lands.
    async fn camera_with_early_alarm() -> BcCamera {
        let (to_cam, mut cam_rx) = unbounded::<Bc>();
        let (cam_tx, from_cam) = unbounded::<Result<Bc>>();
        tokio::spawn(async move {
            while let Some(request) = cam_rx.next().await {
                if request.meta.msg_id == MSG_ID_MOTION_REQUEST {
                    let reply = Bc {
                        meta: BcMeta {
                            response_code: 200,
                            ..request.meta
                        },
                        body: BcBody::ModernMsg(ModernMsg::default()),
                    };
                    let _ = cam_tx.unbounded_send(Ok(reply));
                    let _ = cam_tx.unbounded_send(Ok(alarm_frame()));
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
        abilities.insert("motion".to_string(), ReadKind::ReadWrite);
        BcCamera {
            channel_id: CHANNEL,
            connection: Arc::new(connection),
            logged_in: AtomicBool::new(true),
            message_num: AtomicU16::new(0),
            credentials: Credentials::new("fake", None::<String>),
            abilities: RwLock::new(abilities),
            cancel: CancellationToken::new(),
        }
    }

    /// TIN-8227: an alarm that arrives right after the message 31 reply
    /// reaches `listen_on_motion`.
    #[tokio::test]
    async fn motion_keeps_alarm_sent_with_the_request_reply() {
        let camera = camera_with_early_alarm().await;
        let mut motion = camera.listen_on_motion().await.expect("motion started");

        let first = timeout(Duration::from_secs(2), motion.next_motion()).await;

        assert!(
            matches!(first, Ok(Ok(MotionStatus::Start(_)))),
            "the alarm sent right after the message 31 reply was lost: {:?}",
            first
        );
    }

    /// TIN-8227: the same early alarm reaches `listen_on_alarms` unchanged.
    #[tokio::test]
    async fn alarms_keep_alarm_sent_with_the_request_reply() {
        let camera = camera_with_early_alarm().await;
        let mut alarms = camera.listen_on_alarms().await.expect("alarms started");

        let first = timeout(Duration::from_secs(2), alarms.next_alarm()).await;

        match first {
            Ok(Ok(event)) => {
                assert_eq!(event.status, "MD");
                assert_eq!(event.ai_type.as_deref(), Some("people"));
            }
            other => panic!(
                "the alarm sent right after the message 31 reply was lost: {:?}",
                other
            ),
        }
    }
}
