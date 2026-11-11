//! Attempts to subclass GstMediaFactory
//!
//! We are now messing with gstreamer glib objects
//! expect issues

use super::AnyResult;
use gstreamer::glib::object_subclass;
use gstreamer::Element;
use gstreamer::{
    glib::{self, Object},
    Structure,
};
use gstreamer_rtsp::RTSPUrl;
use gstreamer_rtsp_server::prelude::*;
use gstreamer_rtsp_server::subclass::prelude::*;
use gstreamer_rtsp_server::RTSPMediaFactory;
use gstreamer_rtsp_server::RTSPTransportMode;
use gstreamer_rtsp_server::{RTSP_PERM_MEDIA_FACTORY_ACCESS, RTSP_PERM_MEDIA_FACTORY_CONSTRUCT};
use log::*;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

/// The pipeline builder a factory calls for each new media. It gets the bin
/// to fill and the cache key the media will be stored under.
pub(crate) type BuildCallback =
    dyn Fn(Element, MediaKey) -> AnyResult<Option<Element>> + Send + Sync;

/// Generation counters for the shared medias of one factory, one per cache
/// key (one per mount path).
///
/// gst-rtsp-server caches a shared media under the key from `gen_key` and
/// hands the cached media to every later client with that key. Up to 1.22
/// it does so without checking whether the media can still be shared, so
/// `set_shared(false)` does not evict a media there (1.24 added that check
/// through `gst_rtsp_media_can_be_shared`). Instead the generation is part of
/// the key: retiring a generation makes the next client miss the cache and
/// build a fresh media, on every version. The dead media keeps its stale
/// entry until its last client leaves, when the factory's own `unprepared`
/// handler removes it.
#[derive(Default)]
pub(crate) struct MediaGenerations {
    current: std::sync::Mutex<HashMap<String, u64>>,
}

impl MediaGenerations {
    /// The cache key to use now for `base`, and the generation in it.
    pub(crate) fn key(&self, base: &str) -> (String, u64) {
        let generation = self
            .current
            .lock()
            .map(|map| map.get(base).copied().unwrap_or(0))
            .unwrap_or(0);
        (generation_key(base, generation), generation)
    }

    /// Retires the media built under generation `built` of `base`, so that
    /// new clients get a fresh media. Returns false, and changes nothing,
    /// when a newer media has already replaced it: retiring a stale media
    /// must not orphan a healthy one.
    pub(crate) fn retire(&self, base: &str, built: u64) -> bool {
        let Ok(mut map) = self.current.lock() else {
            return false;
        };
        let current = map.entry(base.to_string()).or_insert(0);
        if *current == built {
            *current += 1;
            true
        } else {
            false
        }
    }
}

/// The key gst-rtsp-server caches a media under: the default key with the
/// generation appended.
pub(crate) fn generation_key(base: &str, generation: u64) -> String {
    format!("{base}#generation={generation}")
}

/// The cache key a media was built under. Retire it when nothing will feed
/// that media again.
#[derive(Clone)]
pub(crate) struct MediaKey {
    generations: Arc<MediaGenerations>,
    base: String,
    built: u64,
}

impl MediaKey {
    /// See [`MediaGenerations::retire`].
    pub(crate) fn retire(&self) -> bool {
        self.generations.retire(&self.base, self.built)
    }
}

thread_local! {
    /// The key `gen_key` produced last on this thread. gst-rtsp-server calls
    /// `gen_key` and then, on a cache miss, `create_element` on the same
    /// thread within one construct call, so this pairs a new media with the
    /// exact key it will be cached under, even if the generation moves in
    /// between.
    static LAST_KEY: RefCell<Option<(String, u64)>> = const { RefCell::new(None) };
}

glib::wrapper! {
    /// The wrapped RTSPMediaFactory
    pub(crate) struct NeoMediaFactory(ObjectSubclass<NeoMediaFactoryImpl>) @extends RTSPMediaFactory;
}

impl Default for NeoMediaFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl NeoMediaFactory {
    fn new() -> Self {
        let factory = Object::new::<NeoMediaFactory>();
        // Share one media - and therefore one camera stream - between every
        // client on this path, instead of giving each client its own.
        //
        // With per-client medias, two clients on the same stream each call
        // start_video for the same stream code (Main => 0). The camera then
        // interleaves two subscriptions of the same stream over one Baichuan
        // connection, which makes audio arrive in bursts relative to video
        // and thrashes the audio anchor in MediaClock. Worse, StreamData's
        // Drop sends VIDEO_STOP for that stream code, so whichever client
        // leaves first stops the stream for the one still watching.
        //
        // Sharing is keyed per mount path, so clients must use the same path
        // string (/AltasFixed2/main and /AltasFixed2/mainStream are different
        // keys even though both are aliases of the same stream).
        factory.set_shared(true);
        factory.set_eos_shutdown(false);
        factory.set_stop_on_disconnect(false);
        // factory.set_publish_clock_mode(gstreamer_rtsp_server::RTSPPublishClockMode::Clock);
        // Reset suspends a prepared media to NULL until PLAY, which flushes
        // the frames already pushed, including the first keyframe.
        factory.set_suspend_mode(gstreamer_rtsp_server::RTSPSuspendMode::None);
        factory.set_launch("videotestsrc pattern=\"snow\" ! video/x-raw,width=896,height=512,framerate=25/1 ! textoverlay name=\"inittextoverlay\" text=\"Stream not Ready\" valignment=top halignment=left font-desc=\"Sans, 32\" ! jpegenc ! rtpjpegpay name=pay0");
        factory.set_transport_mode(RTSPTransportMode::PLAY);
        factory
    }

    pub(crate) async fn new_with_callback<F>(callback: F) -> AnyResult<Self>
    where
        F: Fn(Element, MediaKey) -> AnyResult<Option<Element>> + Send + Sync + 'static,
    {
        let factory = Self::new();
        factory.imp().set_callback(callback).await;
        Ok(factory)
    }

    pub(crate) fn add_permitted_roles<T: AsRef<str>>(&self, permitted_roles: &HashSet<T>) {
        for permitted_role in permitted_roles {
            let s = permitted_role.as_ref();
            log::debug!("Adding {} as permitted user", s);
            self.add_role_from_structure(
                &Structure::builder(s)
                    .field(RTSP_PERM_MEDIA_FACTORY_ACCESS, true)
                    .field(RTSP_PERM_MEDIA_FACTORY_CONSTRUCT, true)
                    .build(),
            );
        }
        // During auth, first it binds anonymously. At this point it checks
        // RTSP_PERM_MEDIA_FACTORY_ACCESS to see if anyone can connect
        // This is done before the auth token is loaded, possibliy an upstream bug there
        // After checking RTSP_PERM_MEDIA_FACTORY_ACCESS anonymously
        // It loads the auth token of the user and checks that users
        // RTSP_PERM_MEDIA_FACTORY_CONSTRUCT allowing them to play
        // As a result of this we must ensure that if anonymous is not granted RTSP_PERM_MEDIA_FACTORY_ACCESS
        // As a part of permitted users then we must allow it to access
        // at least RTSP_PERM_MEDIA_FACTORY_ACCESS but not RTSP_PERM_MEDIA_FACTORY_CONSTRUCT
        // Watching Actually happens during RTSP_PERM_MEDIA_FACTORY_CONSTRUCT
        // So this should be OK to do.
        // FYI: If no RTSP_PERM_MEDIA_FACTORY_ACCESS then server returns 404 not found
        //      If yes RTSP_PERM_MEDIA_FACTORY_ACCESS but no RTSP_PERM_MEDIA_FACTORY_CONSTRUCT
        //        server returns 401 not authourised
        if !permitted_roles
            .iter()
            .map(|i| i.as_ref())
            .collect::<HashSet<&str>>()
            .contains(&"anonymous")
        {
            self.add_role_from_structure(
                &Structure::builder("anonymous")
                    .field(RTSP_PERM_MEDIA_FACTORY_ACCESS, true)
                    .build(),
            );
        }
    }
}

unsafe impl Send for NeoMediaFactory {}
unsafe impl Sync for NeoMediaFactory {}

pub(crate) struct NeoMediaFactoryImpl {
    call_back: Arc<Mutex<Option<Arc<BuildCallback>>>>,
    generations: Arc<MediaGenerations>,
}

impl Default for NeoMediaFactoryImpl {
    fn default() -> Self {
        debug!("Constructing Factor Impl");
        // Prepare thread that sends data into the appsrcs
        Self {
            call_back: Arc::new(Mutex::new(None)),
            generations: Default::default(),
        }
    }
}

impl NeoMediaFactoryImpl {
    async fn set_callback<F>(&self, callback: F)
    where
        F: Fn(Element, MediaKey) -> AnyResult<Option<Element>> + Send + Sync + 'static,
    {
        self.call_back.lock().await.replace(Arc::new(callback));
    }
    fn build_pipeline(&self, media: Element, key: MediaKey) -> AnyResult<Option<Element>> {
        match self.call_back.blocking_lock().as_ref() {
            Some(call) => {
                let new_media = call(media, key);
                match new_media {
                    Ok(new_media) => Ok(new_media),
                    Err(e) => {
                        log::debug!("Media source is currently restarting: {e:?}");
                        Ok(None)
                    }
                }
            }
            None => Ok(None),
        }
    }
}

impl ObjectImpl for NeoMediaFactoryImpl {}
impl RTSPMediaFactoryImpl for NeoMediaFactoryImpl {
    fn gen_key(&self, url: &RTSPUrl) -> Option<glib::GString> {
        let base = self.parent_gen_key(url)?;
        let (key, generation) = self.generations.key(base.as_str());
        LAST_KEY.with(|last| last.replace(Some((base.to_string(), generation))));
        Some(key.into())
    }

    fn create_element(&self, url: &RTSPUrl) -> Option<Element> {
        let (base, built) = LAST_KEY
            .with(|last| last.take())
            .or_else(|| {
                let base = self.parent_gen_key(url)?.to_string();
                let (_, generation) = self.generations.key(&base);
                Some((base, generation))
            })
            .unwrap_or_default();
        let key = MediaKey {
            generations: self.generations.clone(),
            base,
            built,
        };
        self.parent_create_element(url).and_then(|orig| {
            self.build_pipeline(orig, key)
                .expect("Could not build pipeline")
        })
    }
}

#[object_subclass]
impl ObjectSubclass for NeoMediaFactoryImpl {
    const NAME: &'static str = "NeoMediaFactory";
    type Type = super::NeoMediaFactory;
    type ParentType = RTSPMediaFactory;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_differ_per_generation() {
        assert_ne!(
            generation_key("8554/cam/main", 0),
            generation_key("8554/cam/main", 1)
        );
        assert_ne!(
            generation_key("8554/cam/main", 0),
            generation_key("8554/cam/sub", 0)
        );
    }

    #[test]
    fn retire_gives_the_next_client_a_new_key() {
        let gens = MediaGenerations::default();
        let (dead_key, built) = gens.key("8554/cam/main");
        // Clients that join while the media is healthy share its key.
        assert_eq!(gens.key("8554/cam/main").0, dead_key);
        assert!(gens.retire("8554/cam/main", built));
        let (fresh_key, _) = gens.key("8554/cam/main");
        assert_ne!(fresh_key, dead_key);
    }

    #[test]
    fn retire_is_per_path() {
        let gens = MediaGenerations::default();
        let (main_key, main_built) = gens.key("8554/cam/main");
        let (sub_key, _) = gens.key("8554/cam/sub");
        assert!(gens.retire("8554/cam/main", main_built));
        assert_ne!(gens.key("8554/cam/main").0, main_key);
        assert_eq!(gens.key("8554/cam/sub").0, sub_key);
    }

    #[test]
    fn stale_retire_keeps_the_healthy_media() {
        let gens = MediaGenerations::default();
        let (_, old) = gens.key("8554/cam/main");
        assert!(gens.retire("8554/cam/main", old));
        let (healthy_key, _) = gens.key("8554/cam/main");
        // A late exit from the old media's pusher moves nothing.
        assert!(!gens.retire("8554/cam/main", old));
        assert_eq!(gens.key("8554/cam/main").0, healthy_key);
    }

    /// Drives a real factory, so the cache behaviour is that of the
    /// GStreamer this test runs against, not a model of it.
    #[test]
    fn retired_media_is_not_handed_to_new_clients() {
        gstreamer::init().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let keys = Arc::new(std::sync::Mutex::new(Vec::<MediaKey>::new()));
        let built = keys.clone();
        let factory = rt
            .block_on(NeoMediaFactory::new_with_callback(move |element, key| {
                built.lock().unwrap().push(key);
                Ok(Some(element))
            }))
            .unwrap();
        // Core elements only, so the test needs no plugins.
        factory.set_launch("( fakesrc )");
        drop(rt);

        // From 1.24 construct returns the media locked and the RTSP client
        // unlocks it, and finalizing a locked media aborts. The binding
        // exposes that unlock only with its v1_24 feature, which this crate
        // does not enable, so the test runs only below 1.24. That is where
        // the shared flag is not consulted and the key is what evicts.
        if gstreamer::version() >= (1, 24, 0, 0) {
            eprintln!("skipped: needs GStreamer below 1.24");
            return;
        }

        let (_, url) = RTSPUrl::parse("rtsp://127.0.0.1:8554/cam/main");
        let url = url.unwrap();
        let first = factory.construct(&url).unwrap();
        let joined = factory.construct(&url).unwrap();
        assert_eq!(first, joined, "a shared media is reused while healthy");
        assert_eq!(keys.lock().unwrap().len(), 1);

        assert!(keys.lock().unwrap()[0].retire());
        let fresh = factory.construct(&url).unwrap();
        assert_ne!(first, fresh, "a retired media is not reused");
        assert_eq!(factory.construct(&url).unwrap(), fresh);
        assert_eq!(keys.lock().unwrap().len(), 2);
    }
}
