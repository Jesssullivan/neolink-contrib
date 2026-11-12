//! Test support for crates that use `neolink_core`, built only with the
//! `test-util` feature. [`FakeCamera`] holds a logged in [`BcCamera`] on an
//! in-memory connection, so a test needs no camera and no network.

use super::{BcCamera, BcConnSink, BcConnSource, BcConnection, Credentials, ReadKind};
use crate::{bc::model::Bc, Error, Result};
use futures::{channel::mpsc::unbounded, SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU16};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// A camera on channel 0 that answers each message with a closure and keeps
/// the id of every message it receives.
pub struct FakeCamera {
    /// The handle to pass to the code under test.
    pub camera: BcCamera,
    received: Arc<Mutex<Vec<u32>>>,
}

impl FakeCamera {
    /// Builds the camera. It grants read and write on each ability in
    /// `abilities`, as a camera does for an admin login. `respond` gets every
    /// message the camera receives and returns the replies to send back.
    ///
    /// Must be called inside a tokio runtime.
    pub async fn new<F>(abilities: &[&str], mut respond: F) -> Result<Self>
    where
        F: FnMut(&Bc) -> Vec<Bc> + Send + 'static,
    {
        let received = Arc::new(Mutex::new(Vec::new()));
        let log = received.clone();
        let (to_cam, mut cam_rx) = unbounded::<Bc>();
        let (cam_tx, from_cam) = unbounded::<Result<Bc>>();
        tokio::spawn(async move {
            while let Some(request) = cam_rx.next().await {
                log.lock().unwrap().push(request.meta.msg_id);
                for reply in respond(&request) {
                    let _ = cam_tx.unbounded_send(Ok(reply));
                }
            }
        });
        let sink: BcConnSink =
            Box::new(to_cam.sink_map_err(|_| Error::Other("fake camera is gone")));
        let source: BcConnSource = Box::new(from_cam);
        let connection = BcConnection::new(sink, source).await?;
        let abilities = abilities
            .iter()
            .map(|name| (name.to_string(), ReadKind::ReadWrite))
            .collect::<HashMap<_, _>>();
        Ok(Self {
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
        })
    }

    /// The id of every message the camera has received, in order.
    pub fn received_ids(&self) -> Vec<u32> {
        self.received.lock().unwrap().clone()
    }
}
