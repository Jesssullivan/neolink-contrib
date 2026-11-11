//! Pipeline tests for the RTSP audio track. Each test builds the audio branch
//! the RTSP factory builds for a client, feeds it frames made by a GStreamer
//! pipeline in the test, and reads the payloader output from an appsink. The
//! payloader caps are what gst-rtsp-server writes into the SDP.

use std::time::{Duration, Instant};

use gstreamer::{prelude::*, Bin, Buffer, ClockTime, Element, ElementFactory, Pipeline, Sample};
use gstreamer_app::{AppSink, AppSrc};

use super::factory::{
    build_audio, build_audio_with, installed_aac_encoder, pick_aac_encoder, AudioType,
    StreamConfig, AAC_ENCODERS,
};
use crate::config::AudioCodec;

/// One AAC frame holds 1024 samples.
const AAC_SAMPLES: u64 = 1024;
/// Rate of the AAC the test camera sends.
const AAC_RATE: u64 = 16000;
/// A DVI block of 516 bytes: the 4 byte header and 512 bytes of samples, so
/// the factory sees a block size of 512, as `BcMediaAdpcm::block_size` gives.
const DVI_BLOCK_ALIGN: u32 = 516;
/// Samples in one such block: one in the header, two per data byte.
const DVI_SAMPLES: u64 = (DVI_BLOCK_ALIGN as u64 - 4) * 2 + 1;

fn init() {
    gstreamer::init().expect("GStreamer must initialise for the RTSP audio tests");
}

/// Runs `launch`, whose last element is `appsink name=out`, to its end and
/// returns every buffer. Inputs are made this way so no fixture enters git.
fn generate(launch: &str) -> Vec<Buffer> {
    init();
    let pipeline = gstreamer::parse::launch(launch)
        .unwrap_or_else(|e| panic!("cannot build `{}`: {}", launch, e))
        .downcast::<Pipeline>()
        .unwrap();
    let sink = pipeline
        .by_name("out")
        .unwrap()
        .downcast::<AppSink>()
        .unwrap();
    pipeline.set_state(gstreamer::State::Playing).unwrap();
    let mut out = vec![];
    while let Some(sample) = sink.try_pull_sample(ClockTime::from_seconds(10)) {
        out.push(sample.buffer_owned().unwrap());
    }
    pipeline.set_state(gstreamer::State::Null).unwrap();
    assert!(!out.is_empty(), "`{}` made no buffers", launch);
    out
}

/// ADTS AAC frames, mono at `AAC_RATE`, as a Reolink camera sends them.
fn camera_aac(frames: u32) -> Vec<Buffer> {
    generate(&format!(
        "audiotestsrc wave=sine num-buffers={frames} samplesperbuffer={AAC_SAMPLES} \
         ! audio/x-raw,format=S16LE,rate={AAC_RATE},channels=1 \
         ! voaacenc ! aacparse ! audio/mpeg,mpegversion=4,stream-format=adts \
         ! appsink name=out sync=false"
    ))
}

/// DVI ADPCM blocks, mono at 8000 Hz, one block per buffer, as a Reolink
/// camera sends them.
fn camera_dvi(blocks: u32) -> Vec<Buffer> {
    let out = generate(&format!(
        "audiotestsrc wave=sine num-buffers={blocks} samplesperbuffer={DVI_SAMPLES} \
         ! audio/x-raw,format=S16LE,rate=8000,channels=1 \
         ! adpcmenc blockalign={DVI_BLOCK_ALIGN} layout=dvi \
         ! appsink name=out sync=false"
    ));
    for buffer in &out {
        assert_eq!(
            buffer.size(),
            DVI_BLOCK_ALIGN as usize,
            "one block per buffer"
        );
    }
    out
}

/// The audio branch of one client pipeline, ended in an appsink after `pay1`.
struct Branch {
    pipeline: Pipeline,
    bin: Bin,
    src: Option<AppSrc>,
    sink: Option<AppSink>,
}

impl Branch {
    fn new(aud: AudioType, codec: AudioCodec) -> Self {
        init();
        Self::with(aud, codec, installed_aac_encoder())
    }

    fn with(aud: AudioType, codec: AudioCodec, aac_encoder: Option<&'static str>) -> Self {
        init();
        let pipeline = Pipeline::new();
        let bin = Bin::new();
        pipeline.add(&bin).unwrap();
        let src = build_audio_with(
            bin.upcast_ref(),
            Some(&aud),
            codec,
            &StreamConfig::for_test(15),
            aac_encoder,
        )
        .expect("the audio branch builds");
        let sink = bin.by_name("pay1").map(|pay| {
            let sink = AppSink::builder().name("rtpsink").sync(false).build();
            bin.add(&sink).unwrap();
            pay.link(&sink).unwrap();
            sink
        });
        Branch {
            pipeline,
            bin,
            src,
            sink,
        }
    }

    /// Factory names of every element in the branch, sorted.
    fn kinds(&self) -> Vec<String> {
        let mut kinds: Vec<String> = self
            .bin
            .iterate_elements()
            .into_iter()
            .flatten()
            .filter(|e| e.name() != "rtpsink")
            .map(|e| e.factory().unwrap().name().to_string())
            .collect();
        kinds.sort();
        kinds
    }

    fn element(&self, name: &str) -> Option<Element> {
        self.bin.by_name(name)
    }

    /// Starts the pipeline and pushes `frames`, each lasting `frame_ns`.
    fn play(&self, frames: Vec<Buffer>, frame_ns: u64) {
        self.pipeline.set_state(gstreamer::State::Playing).unwrap();
        let src = self.src.as_ref().expect("an audio source");
        for (i, mut frame) in frames.into_iter().enumerate() {
            {
                let frame = frame.make_mut();
                frame.set_pts(ClockTime::from_nseconds(i as u64 * frame_ns));
                frame.set_duration(ClockTime::from_nseconds(frame_ns));
            }
            src.push_buffer(frame).unwrap();
        }
    }

    /// Up to `count` RTP packets, waiting at most `within` in all.
    fn packets(&self, count: usize, within: Duration) -> Vec<Sample> {
        let sink = self.sink.as_ref().expect("a payloader");
        let deadline = Instant::now() + within;
        let mut out = vec![];
        while out.len() < count {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            match sink.try_pull_sample(ClockTime::from_nseconds(left.as_nanos() as u64)) {
                Some(sample) => out.push(sample),
                None => break,
            }
        }
        out
    }
}

impl Drop for Branch {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gstreamer::State::Null);
    }
}

/// The RTP caps of a packet as (encoding-name, clock-rate, payload).
fn rtp_caps(sample: &Sample) -> (String, i32, i32) {
    let caps = sample.caps().expect("RTP packets carry caps");
    let s = caps.structure(0).unwrap();
    assert_eq!(s.name(), "application/x-rtp");
    assert_eq!(s.get::<&str>("media").unwrap(), "audio");
    (
        s.get::<&str>("encoding-name").unwrap().to_string(),
        s.get::<i32>("clock-rate").unwrap(),
        s.get::<i32>("payload").unwrap(),
    )
}

const AAC_FRAME_NS: u64 = AAC_SAMPLES * 1_000_000_000 / AAC_RATE;
const DVI_FRAME_NS: u64 = DVI_SAMPLES * 1_000_000_000 / 8000;

#[test]
fn aac_mode_payloads_mpeg4_generic() {
    let branch = Branch::new(AudioType::Aac, AudioCodec::Aac);
    branch.play(camera_aac(40), AAC_FRAME_NS);
    let packets = branch.packets(10, Duration::from_secs(10));
    assert_eq!(packets.len(), 10, "AAC packets from AAC input");
    for packet in &packets {
        let (name, rate, pt) = rtp_caps(packet);
        assert_eq!(name, "MPEG4-GENERIC");
        assert_eq!(rate, AAC_RATE as i32, "the camera rate is kept");
        assert_eq!(pt, 97);
    }
    assert!(!branch.kinds().contains(&"rtpL16pay".to_string()));
}

#[test]
fn adpcm_mode_encodes_aac() {
    let branch = Branch::new(AudioType::Adpcm(DVI_BLOCK_ALIGN - 4), AudioCodec::Aac);
    branch.play(camera_dvi(40), DVI_FRAME_NS);
    let packets = branch.packets(10, Duration::from_secs(10));
    assert_eq!(packets.len(), 10, "AAC packets from DVI ADPCM input");
    for packet in &packets {
        let (name, rate, _) = rtp_caps(packet);
        assert_eq!(name, "MPEG4-GENERIC");
        assert_eq!(rate, 8000);
    }
}

#[test]
fn pcmu_mode_payloads_pcmu() {
    let branch = Branch::new(AudioType::Aac, AudioCodec::Pcmu);
    branch.play(camera_aac(40), AAC_FRAME_NS);
    let packets = branch.packets(10, Duration::from_secs(10));
    assert_eq!(packets.len(), 10, "PCMU packets from 16 kHz AAC input");
    for packet in &packets {
        let (name, rate, pt) = rtp_caps(packet);
        assert_eq!(name, "PCMU");
        assert_eq!(rate, 8000);
        assert_eq!(pt, 0);
    }
}

#[test]
fn pcmu_mode_payloads_pcmu_from_adpcm() {
    let branch = Branch::new(AudioType::Adpcm(DVI_BLOCK_ALIGN - 4), AudioCodec::Pcmu);
    branch.play(camera_dvi(40), DVI_FRAME_NS);
    let packets = branch.packets(5, Duration::from_secs(10));
    assert_eq!(packets.len(), 5, "PCMU packets from DVI ADPCM input");
    for packet in &packets {
        assert_eq!(rtp_caps(packet).0, "PCMU");
    }
}

#[test]
fn l16_mode_unchanged() {
    let branch = Branch::new(AudioType::Aac, AudioCodec::L16);
    // The chain earlier releases built for every AAC camera.
    let decoder = branch
        .element("auddecoder_faad")
        .or_else(|| branch.element("auddecoder_avdec_aac"))
        .expect("an AAC decoder");
    let mut expected = vec![
        "aacparse".to_string(),
        "appsrc".to_string(),
        "audioconvert".to_string(),
        decoder.factory().unwrap().name().to_string(),
        "queue".to_string(),
        "rtpL16pay".to_string(),
    ];
    if branch.element("audfallbackswitch").is_some() {
        expected.push("audiotestsrc".to_string());
        expected.push("fallbackswitch".to_string());
    }
    expected.sort();
    assert_eq!(branch.kinds(), expected);

    branch.play(camera_aac(40), AAC_FRAME_NS);
    let packets = branch.packets(5, Duration::from_secs(10));
    assert_eq!(packets.len(), 5);
    let (name, rate, _) = rtp_caps(&packets[0]);
    assert_eq!(name, "L16");
    assert_eq!(rate, AAC_RATE as i32);
}

#[test]
fn none_mode_has_no_audio_pad() {
    for aud in [AudioType::Aac, AudioType::Adpcm(DVI_BLOCK_ALIGN - 4)] {
        let branch = Branch::new(aud, AudioCodec::None);
        assert!(branch.src.is_none(), "no audio source");
        assert!(branch.element("pay1").is_none(), "no pay1 element");
        assert!(branch.kinds().is_empty(), "nothing added to the bin");
    }
}

#[test]
fn no_camera_audio_has_no_audio_pad() {
    init();
    for codec in [
        AudioCodec::Aac,
        AudioCodec::Pcmu,
        AudioCodec::L16,
        AudioCodec::None,
    ] {
        let bin = Bin::new();
        let src = build_audio(bin.upcast_ref(), None, codec, &StreamConfig::for_test(15)).unwrap();
        assert!(src.is_none());
        assert_eq!(bin.children().len(), 0);
    }
}

#[test]
fn aac_without_encoder_serves_no_audio() {
    let branch = Branch::with(AudioType::Aac, AudioCodec::Aac, None);
    assert!(branch.src.is_none(), "no audio source");
    assert!(
        branch.element("pay1").is_none(),
        "never an L16 track instead"
    );
    assert!(branch.kinds().is_empty(), "nothing half built in the bin");
}

#[test]
fn aac_encoder_prefers_voaacenc() {
    assert_eq!(AAC_ENCODERS[0], "voaacenc");
    assert_eq!(pick_aac_encoder(|_| true), Some("voaacenc"));
    assert_eq!(pick_aac_encoder(|k| k == "avenc_aac"), Some("avenc_aac"));
    assert_eq!(pick_aac_encoder(|k| k == "voaacenc"), Some("voaacenc"));
    assert_eq!(pick_aac_encoder(|_| false), None);

    // The branch uses the encoder the registry offers first.
    init();
    if ElementFactory::find("voaacenc").is_some() {
        let branch = Branch::new(AudioType::Aac, AudioCodec::Aac);
        let encoder = branch.element("audaacenc").expect("an AAC encoder");
        assert_eq!(encoder.factory().unwrap().name(), "voaacenc");
    } else {
        // The estate-test job checks voaacenc is installed, so this branch
        // runs only on a host without it.
        eprintln!("voaacenc not installed: registry half not run");
    }
}

#[test]
fn silence_fallback_stays_aac() {
    let branch = Branch::new(AudioType::Aac, AudioCodec::Aac);
    if branch.element("audfallbackswitch").is_none() {
        // fallbackswitch comes from gst-plugins-rs. Without it the factory
        // builds no silence branch, so there is nothing to test here.
        eprintln!("fallbackswitch (gst-plugins-rs) not installed: silence fallback not built");
        return;
    }
    branch.play(camera_aac(5), AAC_FRAME_NS);
    let stopped = Instant::now();
    // The camera sends nothing more. The switch falls back to silence after
    // its 3 s timeout, and the track keeps going as AAC.
    let mut late = 0;
    let deadline = stopped + Duration::from_secs(6);
    while Instant::now() < deadline && late < 5 {
        for packet in branch.packets(1, Duration::from_secs(1)) {
            assert_eq!(rtp_caps(&packet).0, "MPEG4-GENERIC");
            if stopped.elapsed() >= Duration::from_secs(2) {
                late += 1;
            }
        }
    }
    assert!(
        late >= 5,
        "only {} AAC packets 2 s after the camera went quiet",
        late
    );
}
