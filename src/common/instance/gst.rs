use super::*;

use crate::common::UseCounter;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use neolink_core::{bc_protocol::StreamKind, bcmedia::model::BcMedia};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    sync::mpsc::Receiver as MpscReceiver,
    task::{JoinError, JoinHandle},
};

/// A task handle that aborts its task when dropped.
///
/// Dropping a plain `JoinHandle` detaches the task. The motion and push
/// notification watchers loop until their channel closes, so a detached
/// watcher keeps running, and keeps its permit, after the future that
/// spawned it was dropped, for example by a caller's timeout before the
/// camera task took ownership of it, or after the camera task ended.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

#[cfg(feature = "pushnoti")]
use crate::common::PushNoti;

impl NeoInstance {
    /// Streams a camera source while not paused
    pub(crate) async fn stream_while_live(
        &self,
        stream: StreamKind,
    ) -> AnyResult<MpscReceiver<BcMedia>> {
        let config = self.config().await?.borrow().clone();
        let name = config.name.clone();

        let media_rx = if config.pause.on_motion {
            let (media_tx, media_rx) = tokio::sync::mpsc::channel(100);
            let counter = UseCounter::new().await;

            let mut md = self.motion().await?;
            let mut tasks = FuturesUnordered::new();
            // Stream for 5s on a new client always
            // This lets us negotiate the camera stream type
            let init_permit = counter.create_activated().await?;
            tokio::spawn(
                async {
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    drop(init_permit);
                }
                .map(|e| {
                    log::debug!("Init permit thread stopped {e:?}");
                    e
                }),
            );

            // Create the permit for controlling the motion
            let mut md_permit = {
                let md_state = md.borrow_and_update().clone();
                match md_state {
                    MdState::Start(_) => {
                        log::info!("{name}::{stream:?}: Starting with Motion");
                        counter.create_activated().await?
                    }
                    MdState::Stop(_) | MdState::Unknown => {
                        log::info!("{name}::{stream:?}: Waiting with Motion");
                        counter.create_deactivated().await?
                    }
                }
            };
            // Now listen to the motion
            let thread_name = name.clone();
            tasks.push(AbortOnDrop(tokio::spawn(
                async move {
                    loop {
                        match md.changed().await {
                            Ok(_) => {
                                let md_state: MdState = md.borrow_and_update().clone();
                                match md_state {
                                    MdState::Start(_) => {
                                        log::info!("{thread_name}::{stream:?}: Motion Started");
                                        md_permit.activate().await?;
                                    }
                                    MdState::Stop(_) => {
                                        log::info!("{thread_name}::{stream:?}: Motion Stopped");
                                        md_permit.deactivate().await?;
                                    }
                                    MdState::Unknown => {}
                                }
                            }
                            Err(e) => {
                                // Use break here so we can define the full type on the async closure
                                break AnyResult::Err(e.into());
                            }
                        }
                    }?;
                    AnyResult::Ok(())
                }
                .map(|e| {
                    log::debug!("Motion thread stopped {e:?}");
                    e
                }),
            )));

            #[cfg(feature = "pushnoti")]
            {
                // Creates a permit for controlling based on the PN
                let pn_permit = counter.create_deactivated().await?;
                let mut pn = self.push_notifications().await?;
                pn.borrow_and_update(); // Ignore any PNs that have already been sent before this
                let thread_name = name.clone();
                tasks.push(AbortOnDrop(tokio::spawn(
                    async move {
                        loop {
                            let noti: Option<PushNoti> = pn.borrow_and_update().clone();
                            if let Some(noti) = noti {
                                if noti.message.contains("Motion Alert from") {
                                    log::info!(
                                        "{thread_name}::{stream:?}: Push Notification Recieved"
                                    );
                                    let mut new_pn_permit = pn_permit.subscribe();
                                    new_pn_permit.activate().await?;
                                    tokio::spawn(async move {
                                        tokio::time::sleep(tokio::time::Duration::from_secs(30))
                                            .await;
                                        drop(new_pn_permit);
                                    });
                                }
                            }
                            if let Err(e) = pn.changed().await {
                                break Err(e);
                            }
                        }?;
                        AnyResult::Ok(())
                    }
                    .map(|e| {
                        log::debug!("PN thread stopped {e:?}");
                        e
                    }),
                )));
            }

            // Send the camera when the pemit is active
            let camera_permit = counter.create_deactivated().await?;
            let thread_camera = self.clone();
            tokio::spawn(
                async move {
                    loop {
                        if let Err(e) = camera_permit.aquired_users().await {
                            break AnyResult::Err(e);
                        }
                        log::debug!("Starting stream");
                        tokio::select! {
                            v = camera_permit.dropped_users() => {
                                log::debug!("Dropped users: {v:?}");
                                v
                            },
                            v = async {
                                log::debug!("Getting stream");
                                let mut stream = thread_camera.stream(stream).await?;
                                log::debug!("Got stream");
                                while let Some(media) = stream.recv().await {
                                    media_tx.send(media).await?;
                                }
                                AnyResult::Ok(())
                            } => {
                                log::debug!("Stopped stream: {v:?}");
                                v
                            },
                            v = tasks.next() => {
                                log::debug!("Task failed: {v:?}");
                                Err(anyhow!("Task ended prematurly: {v:?}"))
                            }
                        }?;
                        log::debug!("Pausing stream");
                    }?;
                    drop(counter); // Make sure counter is owned by this thread
                    AnyResult::Ok(())
                }
                .map(|e| {
                    log::debug!("Stream thread stopped {e:?}");
                    e
                }),
            );

            Ok(media_rx)
        } else {
            self.stream(stream).await
        }?;

        Ok(media_rx)
    }

    /// Streams a camera source
    pub(crate) async fn stream(&self, stream: StreamKind) -> AnyResult<MpscReceiver<BcMedia>> {
        let (media_tx, media_rx) = tokio::sync::mpsc::channel(100);
        let config = self.config().await?.borrow().clone();
        let strict = config.strict;
        let thread_camera = self.clone();
        tokio::task::spawn(
            tokio::task::spawn(async move {
                thread_camera
                    .run_task(move |cam| {
                        let media_tx = media_tx.clone();
                        Box::pin(async move {
                            let mut media_stream = cam.start_video(stream, 0, strict).await?;
                            log::trace!("Camera started");
                            while let Ok(media) = media_stream.get_data().await? {
                                media_tx.send(media).await?;
                            }
                            AnyResult::Ok(())
                        })
                    })
                    .await
            })
            .and_then(|res| async move {
                log::debug!("Camera finished streaming: {res:?}");
                Ok(())
            }),
        );

        Ok(media_rx)
    }
}

#[cfg(test)]
mod tests {
    use super::AbortOnDrop;
    use futures::stream::FuturesUnordered;
    use tokio::sync::oneshot;

    /// A task that never ends on its own, like a watcher whose channel stays
    /// open. Its sender is dropped only when the task is dropped.
    fn watcher() -> (AbortOnDrop<()>, oneshot::Receiver<()>) {
        let (alive, gone) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _alive = alive;
            std::future::pending::<()>().await
        });
        (AbortOnDrop(task), gone)
    }

    #[tokio::test]
    async fn dropping_the_handle_ends_the_task() {
        let (task, gone) = watcher();
        drop(task);
        assert!(gone.await.is_err(), "the task ended and dropped its sender");
    }

    #[tokio::test]
    async fn dropping_the_task_set_ends_every_task() {
        let tasks = FuturesUnordered::new();
        let mut gone = vec![];
        for _ in 0..2 {
            let (task, rx) = watcher();
            tasks.push(task);
            gone.push(rx);
        }
        drop(tasks);
        for rx in gone {
            assert!(rx.await.is_err());
        }
    }
}
