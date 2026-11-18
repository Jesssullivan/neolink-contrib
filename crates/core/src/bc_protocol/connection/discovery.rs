//! This module handles discovery
//!
//! Given a UID find the associated IP
//!
use super::DiscoveryResult;
use crate::bc::model::*;
use crate::bc_protocol::{md5_string, DiscoveryScope, Md5Trunc, TcpSource};
use crate::bcudp::codex::BcUdpCodex;
use crate::bcudp::model::*;
use crate::bcudp::xml::*;
use crate::{Error, Result};
use futures::{
    sink::SinkExt,
    stream::{FuturesUnordered, Stream, StreamExt},
};
use lazy_static::lazy_static;
use log::*;
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::collections::{btree_map::Entry, BTreeMap, HashSet};
use std::convert::TryInto;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::MissedTickBehavior;
use tokio::{
    net::UdpSocket,
    sync::{
        mpsc::{channel, Receiver, Sender},
        RwLock, Semaphore,
    },
    task::JoinSet,
    time::{interval, timeout, Duration},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::udp::UdpFramed;

#[derive(Debug, Clone)]
pub(crate) struct RegisterResult {
    reg: SocketAddr,
    dev: Option<SocketAddr>,
    dmap: Option<SocketAddr>,
    relay: Option<SocketAddr>,
    client_id: i32,
    sid: u32,
}

#[derive(Debug, Clone)]
struct ConnectResult {
    addr: SocketAddr,
    client_id: i32,
    camera_id: i32,
    sid: u32,
}

#[derive(Debug, Clone)]
struct UidLookupResults {
    reg: SocketAddr,
    relay: SocketAddr,
}

const MTU: u32 = 1350;
/// The tids a request of ours may carry: 8 bits, as neolink has always sent,
/// without 0, which `subscribe` keeps for replies to no request of ours.
const TID_RANGE: std::ops::RangeInclusive<u32> = 1..=255;
lazy_static! {
    static ref P2P_RELAY_HOSTNAMES: [&'static str; 12] = [
        "p2p.reolink.com",
        "p2p1.reolink.com",
        "p2p2.reolink.com",
        "p2p3.reolink.com",
        "p2p4.reolink.com",
        "p2p5.reolink.com",
        "p2p6.reolink.com",
        "p2p7.reolink.com",
        "p2p8.reolink.com",
        "p2p9.reolink.com",
        "p2p10.reolink.com",
        "p2p11.reolink.com",
        // These following are all currently set to 127.0.0.1
        // probably reserved for future use
        // "p2p12.reolink.com",
        // "p2p13.reolink.com",
        // "p2p14.reolink.com",
        // "p2p15.reolink.com",
        // "p2p16.reolink.com",
    ];
    /// Maximum wait for a reply
    static ref MAXIMUM_WAIT: Duration = Duration::from_secs(15);
    /// Wait for tcp connections
    static ref TCP_WAIT: Duration = Duration::from_secs(4);
    /// How long to wait before resending
    static ref RESEND_WAIT: Duration = Duration::from_millis(500);

}

type Subscriber = Arc<RwLock<BTreeMap<u32, Sender<Result<(UdpDiscovery, SocketAddr)>>>>>;
type Handlers = Arc<RwLock<Vec<Sender<Result<(UdpDiscovery, SocketAddr)>>>>>;
type ArcFramedSocket = UdpFramed<BcUdpCodex, Arc<UdpSocket>>;
pub(crate) struct Discoverer {
    semaphore: Arc<Semaphore>,
    socket: Arc<UdpSocket>,
    handle: RwLock<JoinSet<Result<()>>>,
    writer: Sender<Result<(BcUdp, SocketAddr)>>,
    subsribers: Subscriber,
    handlers: Handlers,
    local_addr: SocketAddr,
    cancel: CancellationToken,
    guard: DestinationGuard,
}

/// The check at the one writer of a `Discoverer`.
///
/// With no allowed set, which is the default and the `All` scope, every
/// destination passes. Once a scoped discovery sets the allowed set, a packet
/// to any other destination is dropped, counted and logged at error with the
/// destination only. It covers only packets that pass through this writer.
/// The keep-alive senders and the session after discovery write to the same
/// socket without the writer, so the guard does not cover them. The
/// keep-alive senders write to the address in the connect result.
#[derive(Clone, Default)]
struct DestinationGuard {
    allowed: Arc<std::sync::RwLock<Option<HashSet<SocketAddr>>>>,
    refused: Arc<AtomicUsize>,
    /// Every destination the writer handed to the socket, in order. Tests
    /// read it to see what left the host.
    #[cfg(test)]
    sent: Arc<std::sync::Mutex<Vec<SocketAddr>>>,
}

impl DestinationGuard {
    fn limit_to(&self, dests: &[SocketAddr]) {
        let mut allowed = self
            .allowed
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *allowed = Some(dests.iter().copied().collect());
    }

    fn admits(&self, dest: &SocketAddr) -> bool {
        let allowed = self
            .allowed
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match allowed.as_ref() {
            None => true,
            Some(allowed) => allowed.contains(dest),
        }
    }

    /// Notes that the writer handed a packet for `dest` to the socket. Only
    /// a test build keeps the record.
    fn note_sent(&self, dest: &SocketAddr) {
        #[cfg(test)]
        self.sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(*dest);
        #[cfg(not(test))]
        let _ = dest;
    }

    fn refuse(&self, dest: &SocketAddr) {
        self.refused.fetch_add(1, Ordering::SeqCst);
        log::error!(
            "Discovery: refused a packet to {}, which is outside the discovery scope",
            dest
        );
    }
}

fn valid_ip(ip: &str) -> bool {
    !ip.is_empty() && matches!(ip.parse::<Ipv4Addr>(), Ok(addr) if addr.octets()[3] != 0)
}

fn valid_port(port: u16) -> bool {
    port != 0
}

fn valid_addr(ip_port: &IpPort) -> bool {
    valid_ip(&ip_port.ip) && valid_port(ip_port.port)
}

impl Discoverer {
    async fn new() -> Result<Discoverer> {
        Self::new_on(Ipv4Addr::UNSPECIFIED).await
    }

    /// As `new`, with the socket bound to `bind` in place of 0.0.0.0.
    async fn new_on(bind: Ipv4Addr) -> Result<Discoverer> {
        let socket = Arc::new(connect(bind).await?);
        let local_addr = socket.local_addr()?;
        let inner: ArcFramedSocket = UdpFramed::new(socket.clone(), BcUdpCodex::new());
        let cancel = CancellationToken::new();

        let (mut writer, mut reader) = inner.split();
        let mut set = JoinSet::new();
        let subsribers: Subscriber = Default::default();
        let handlers: Handlers = Default::default();

        let thread_subscriber = subsribers.clone();
        let thread_handlers = handlers.clone();
        let thread_cancel = cancel.clone();
        set.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    loop {
                        tokio::task::yield_now().await;
                        match reader.next().await {
                            Some(Ok((BcUdp::Discovery(bcudp), addr))) => {
                                log::trace!("Got discovery {:?} for {}", bcudp, addr);
                                let tid = bcudp.tid;
                                let mut needs_removal = false;
                                if let (Some(sender), true) =
                                    (thread_subscriber.read().await.get(&tid), tid > 0)
                                {
                                    if sender.send(Ok((bcudp, addr))).await.is_err() {
                                        needs_removal = true;
                                    }
                                } else {
                                    for sender in thread_handlers.write().await.iter_mut() {
                                        let _ = sender.send(Ok((bcudp.clone(), addr))).await;
                                    }
                                }
                                if needs_removal {
                                    thread_subscriber.write().await.remove(&tid);
                                }
                            }
                            Some(Ok(bcudp)) => {
                                // Only discovery packets should be possible atm
                                log::debug!("Got non Discovery during discovery: {:?}", bcudp);
                            }
                            Some(Err(e)) => {
                                log::error!("Error on discovery socket: {:?}", e);
                                let mut locked_sub = thread_subscriber.write().await;
                                for (_, sub) in locked_sub.iter() {
                                    let _ = sub.send(Err(e.clone())).await;
                                }
                                locked_sub.clear();
                                break Result::Ok(());
                            }
                            None => break Result::Ok(()),
                        }
                    }
                }=> v,
            }
        });

        let (sinker, sinker_rx) = channel::<Result<(BcUdp, SocketAddr)>>(100);
        let thread_cancel = cancel.clone();
        let guard = DestinationGuard::default();
        let thread_guard = guard.clone();
        set.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    let mut sinker_rx = ReceiverStream::new(sinker_rx);
                    while let Some(Ok(packet)) = sinker_rx.next().await {
                        if thread_guard.admits(&packet.1) {
                            let dest = packet.1;
                            let _ = writer.send(packet).await;
                            thread_guard.note_sent(&dest);
                        } else {
                            thread_guard.refuse(&packet.1);
                        }
                    }
                    Result::Ok(())
                } => v,
            }
        });

        Ok(Discoverer {
            semaphore: Arc::new(Semaphore::new(1)),
            socket,
            handle: RwLock::new(set),
            writer: sinker,
            subsribers,
            handlers,
            local_addr,
            cancel,
            guard,
        })
    }

    /// From now on the writer sends only to `dests`.
    fn limit_to(&self, dests: &[SocketAddr]) {
        self.guard.limit_to(dests);
    }

    /// How many packets the writer refused because their destination lay
    /// outside the allowed set.
    #[cfg(test)]
    fn refused_destinations(&self) -> usize {
        self.guard.refused.load(Ordering::SeqCst)
    }

    /// The set the writer is limited to, or `None` when it is not limited.
    #[cfg(test)]
    fn allowed_destinations(&self) -> Option<HashSet<SocketAddr>> {
        self.guard
            .allowed
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Every destination the writer has sent to so far, in order.
    #[cfg(test)]
    fn sent_destinations(&self) -> Vec<SocketAddr> {
        self.guard
            .sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    async fn get_socket(&self) -> Arc<UdpSocket> {
        self.socket.clone()
    }

    async fn subscribe(&self, tid: u32) -> Result<Receiver<Result<(UdpDiscovery, SocketAddr)>>> {
        if tid > 0 {
            let mut subs = self.subsribers.write().await;
            match subs.entry(tid) {
                Entry::Vacant(vacant) => {
                    let (tx, rx) = channel(10);
                    vacant.insert(tx);
                    Ok(rx)
                }
                Entry::Occupied(mut occ) => {
                    if occ.get().is_closed() {
                        let (tx, rx) = channel(10);
                        occ.insert(tx);
                        Ok(rx)
                    } else {
                        // log::error!("Failed to subscribe in discovery to {:?}", tid);
                        Err(Error::SimultaneousSubscription {
                            msg_num: Some(tid as u16),
                        })
                    }
                }
            }
        } else {
            // If tid is zero we listen to all!
            let mut handlers = self.handlers.write().await;
            let (tx, rx) = channel(10);
            handlers.push(tx);
            Ok(rx)
        }
    }

    /// Reserves a tid that no open request of this discoverer holds, and
    /// subscribes to its replies. The camera echoes the tid of a request and
    /// replies are routed by it, so two open requests must never share one.
    /// The tid is drawn at random among the free tids of `TID_RANGE` under the
    /// same lock that inserts it, so concurrent requests, such as one per
    /// destination of local discovery, each get their own. A tid whose
    /// receiver was dropped is free again. With every tid held, which takes
    /// more than 255 open requests, this request fails rather than share a
    /// tid, and the requests already open go on.
    async fn subscribe_new(&self) -> Result<(u32, Receiver<Result<(UdpDiscovery, SocketAddr)>>)> {
        let mut subs = self.subsribers.write().await;
        let held = |tid: &u32| subs.get(tid).is_some_and(|sub| !sub.is_closed());
        let free: Vec<u32> = TID_RANGE.filter(|tid| !held(tid)).collect();
        let Some(&tid) = free.choose(&mut thread_rng()) else {
            log::warn!(
                "Discovery: all {} tids are held by open requests, so this destination is skipped",
                TID_RANGE.count()
            );
            return Err(Error::Other("No free discovery tid"));
        };
        let (tx, rx) = channel(10);
        subs.insert(tid, tx);
        Ok((tid, rx))
    }

    /// Subsciber others is for messages that we did not initiate and are therefore
    /// using an unknown tid
    /// In this case we subscribe to tid 0
    async fn handle_incoming<T, F>(&self, map: F) -> Result<T>
    where
        F: Fn(UdpDiscovery, SocketAddr) -> Option<T>,
    {
        let mut reply = ReceiverStream::new(self.subscribe(0).await?);
        tokio::select! {
            v = async {
                loop {
                    let (reply, addr) = reply.next().await.ok_or(Error::ConnectionUnavailable)??;
                    if let Some(result) = map(reply, addr) {
                        return Ok(result);
                    }
                }
            } => v,
            _ = tokio::time::sleep(*MAXIMUM_WAIT) => Err::<T, Error>(Error::DiscoveryTimeout),
        }
    }

    async fn send(&self, disc: BcUdp, addr: SocketAddr) -> Result<()> {
        self.writer
            .send(Ok((disc, addr)))
            .await
            .map_err(Error::from)
    }

    fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    async fn retry_send<F, T>(&self, mut disc: UdpDiscovery, dest: SocketAddr, map: F) -> Result<T>
    where
        F: Fn(UdpDiscovery, SocketAddr) -> Option<T>,
    {
        let reply = if disc.tid == 0 {
            // If 0 take a free one
            let (tid, reply) = self.subscribe_new().await?;
            disc.tid = tid;
            reply
        } else {
            self.subscribe(disc.tid).await?
        };
        self.retry_send_on(reply, disc, dest, map).await
    }

    /// As `retry_send`, for a request whose tid is already subscribed.
    async fn retry_send_on<F, T>(
        &self,
        reply: Receiver<Result<(UdpDiscovery, SocketAddr)>>,
        disc: UdpDiscovery,
        dest: SocketAddr,
        map: F,
    ) -> Result<T>
    where
        F: Fn(UdpDiscovery, SocketAddr) -> Option<T>,
    {
        let mut reply = ReceiverStream::new(reply);
        let msg = BcUdp::Discovery(disc);

        let mut inter = interval(*RESEND_WAIT);
        inter.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let result = tokio::select! {
            v = async {
                // Recv while channel is viable
                while let Some(msg) = reply.next().await {
                    if let Ok((reply, addr)) = msg {
                        if let Some(result) = map(reply, addr) {
                            return Ok(result);
                        }
                    }
                }
                Err(Error::DiscoveryIgnored)
            } => {v},
            v = async {
                // Send every inter for ever or until channel is no longer viable
                loop {
                    inter.tick().await;
                    if let Err(e) = self.send(msg.clone(), dest).await {
                        return e;
                    }
                }
            } => {Err::<T, Error>(v)},
            _ = {
                // Sleep then emit Timeout
                tokio::time::sleep(*MAXIMUM_WAIT)
            } => {
                Err::<T, Error>(Error::DiscoveryTimeout)
            }
        };

        result
    }

    async fn send_and_forget(&self, mut disc: UdpDiscovery, dest: SocketAddr) -> Result<()> {
        if disc.tid == 0 {
            // If 0 make a random one
            let target_tid = generate_tid();
            disc.tid = target_tid;
        }
        let mut inter = interval(*RESEND_WAIT);
        inter.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let msg = BcUdp::Discovery(disc);

        for _i in 0..5 {
            inter.tick().await;

            self.send(msg.clone(), dest).await?;
        }

        Ok(())
    }

    async fn client_initiated_direct(
        &self,
        uid: &str,
        client_id: i32,
        addr: SocketAddr,
    ) -> Result<ConnectResult> {
        let (tid, reply) = self.subscribe_new().await?;

        let port = self.local_addr().port();
        let msg = UdpDiscovery {
            tid,
            payload: UdpXml::C2dC(C2dC {
                uid: uid.to_string(),
                cli: ClientList { port: port as u32 },
                cid: client_id,
                mtu: MTU,
                debug: false,
                os: "MAC".to_string(),
            }),
        };

        log::debug!(
            "Trying a direct connect to: {:?} with tid: {}",
            addr,
            msg.tid
        );

        let (camera_address, camera_id) = self
            .retry_send_on(reply, msg, addr, |bc, addr| match bc {
                UdpDiscovery {
                    tid: _,
                    payload: UdpXml::D2cCr(D2cCr { did, cid, .. }),
                } if cid == client_id => {
                    log::trace!("Got expected reply");
                    Some((addr, did))
                }
                n => {
                    log::debug!("Got unexpected reply: {:?}", n);
                    None
                }
            })
            .await?;

        log::debug!(
            "Direct connect success at {:?} client: {}, device: {}",
            addr,
            client_id,
            camera_id
        );

        let result = ConnectResult {
            addr: camera_address,
            client_id,
            camera_id,
            sid: 0,
        };
        self.keep_alive_device(tid, &result).await;

        log::debug!("Returning direct connect: {:?}", result);
        Ok(result)
    }

    async fn uid_lookup_all<'a>(
        &'a self,
        uid: &'a str,
    ) -> Result<impl Stream<Item = UidLookupResults> + 'a> {
        let task = tokio::task::spawn_blocking(move || {
            let mut addrs = vec![];
            for p2p_relay in P2P_RELAY_HOSTNAMES.iter() {
                addrs.append(
                    &mut format!("{}:9999", p2p_relay)
                        .to_socket_addrs()
                        .map(|i| i.collect::<Vec<SocketAddr>>())
                        .unwrap_or_else(|_| vec![]),
                );
            }
            addrs
        });
        let mut addrs = timeout(*MAXIMUM_WAIT, task).await??;
        trace!("Uid lookup to: {:?}", addrs);

        Ok(addrs
            .drain(..)
            .map(|addr| self.uid_lookup(uid, addr))
            .collect::<FuturesUnordered<_>>()
            .filter_map(|f| async { f.ok() }))
    }
    /// This function will contact the p2p relay servers
    ///
    /// It will ask each of the servers for details on a specific UID
    ///
    /// On success it will returns an async iter that yields the M2cQr that the p2p relay
    /// server has about the UID
    async fn uid_lookup(&self, uid: &str, addr: SocketAddr) -> Result<UidLookupResults> {
        let msg = UdpDiscovery {
            tid: 0,
            payload: UdpXml::C2mQ(C2mQ {
                uid: uid.to_string(),
                os: "MAC".to_string(),
            }),
        };
        trace!("Sending look up {:?}", msg);
        let (reg, relay, _) = self
            .retry_send(msg, addr, |bc, addr| match bc {
                UdpDiscovery {
                    tid: _,
                    payload:
                        UdpXml::M2cQr(M2cQr {
                            reg: Some(reg),
                            relay: Some(relay),
                            ..
                        }),
                } if valid_addr(&reg) && valid_addr(&relay) => Some((reg, relay, addr)),
                _ => None,
            })
            .await?;
        trace!("Look up complete");
        Ok(UidLookupResults {
            reg: SocketAddr::new(reg.ip.parse()?, reg.port),
            relay: SocketAddr::new(relay.ip.parse()?, relay.port),
        })
    }

    /// Register our local ip address with the reolink servers
    /// This will be used for the device to contact us
    async fn register_address(
        &self,
        uid: &str,
        client_id: i32,
        lookup: &UidLookupResults,
    ) -> Result<RegisterResult> {
        let local_ip = get_local_ip()?;
        let local_addr = SocketAddr::new(local_ip, self.local_addr().port());
        log::debug!("Registering {:?} to reolink", local_addr);
        let local_ip = local_addr.ip();
        let local_port = local_addr.port();
        let local_family = if local_addr.ip().is_ipv4() { 4 } else { 6 };

        let msg = UdpDiscovery {
            // retry_send takes a free tid
            tid: 0,
            payload: UdpXml::C2rC(C2rC {
                uid: uid.to_string(),
                cli: IpPort {
                    ip: local_ip.to_string(),
                    port: local_port,
                },
                relay: IpPort {
                    ip: lookup.relay.ip().to_string(),
                    port: lookup.relay.port(),
                },
                cid: client_id,
                family: local_family,
                debug: false,
                os: "MAC".to_string(),
                revision: Some(3),
            }),
        };

        if let UdpXml::C2rC(ref c2r_c) = msg.payload {
            trace!("Registering: {:?}", c2r_c);
        }

        // Send and await acceptance
        let (sid, dev, dmap, relay) = self
            .retry_send(msg, lookup.reg, |bc, socket| {
                trace!("{}", socket.ip());
                match bc {
                    UdpDiscovery {
                        tid: _,
                        payload:
                            UdpXml::R2cCr(R2cCr {
                                dmap,
                                dev,
                                relay,
                                sid: Some(sid),
                                rsp,
                                ..
                            }),
                    } if (dev.as_ref().map(valid_addr).unwrap_or(false)
                        || dmap.as_ref().map(valid_addr).unwrap_or(false)
                        || relay.as_ref().map(valid_addr).unwrap_or(false))
                        && rsp != -1
                        && rsp != -3 =>
                    {
                        Some(Ok((sid, dev, dmap, relay)))
                    }
                    // UdpDiscovery {
                    //     tid: _,
                    //     payload:
                    //         UdpXml {
                    //             r2c_c_r:
                    //                 Some(R2cCr {
                    //                     dmap,
                    //                     dev,
                    //                     relay: Some(mut relay),
                    //                     sid,
                    //                     rsp,
                    //                     ..
                    //                 }),
                    //             ..
                    //         },
                    // } if (dev
                    //     .as_ref()
                    //     .map(valid_addr)
                    //     .unwrap_or(false)
                    //     || dmap
                    //         .as_ref()
                    //         .map(valid_addr)
                    //         .unwrap_or(false)
                    //     || (relay.ip == format!("{}", socket.ip()) && relay.port == 0))
                    //     && rsp != -1 =>
                    // {
                    //     // For a relay connection if port is 0 and the ip is the current socker addr
                    //     // we use the current port
                    //     relay.port = socket.port();
                    //     Some(Ok((sid, dev, dmap, Some(relay))))
                    // }
                    UdpDiscovery {
                        tid: _,
                        payload:
                            UdpXml::R2cCr(R2cCr {
                                dev,
                                dmap,
                                relay,
                                rsp,
                                ..
                            }),
                    } if (dev.as_ref().map(valid_addr).unwrap_or(false)
                        || dmap.as_ref().map(valid_addr).unwrap_or(false)
                        || relay.as_ref().map(valid_addr).unwrap_or(false))
                        && (rsp == -1 || rsp == -3) =>
                    {
                        Some(Err(Error::RegisterError))
                    }
                    _ => None,
                }
            })
            .await??;

        Ok(RegisterResult {
            reg: lookup.reg,
            sid,
            client_id,
            dev: dev.and_then(|d| d.try_into().ok()),
            dmap: dmap.and_then(|d| d.try_into().ok()),
            relay: relay.and_then(|d| d.try_into().ok()),
        })
    }

    async fn device_initiated_dev(
        &self,
        register_result: &RegisterResult,
    ) -> Result<ConnectResult> {
        let (addr, local_tid, local_did) = self
            .handle_incoming(|bc, addr| {
                trace!("bc: {:?}", bc);
                match (bc, register_result) {
                    (
                        UdpDiscovery {
                            tid,
                            payload:
                                UdpXml::D2cT(D2cT {
                                    sid,
                                    cid,
                                    did,
                                    conn,
                                    ..
                                }),
                        },
                        RegisterResult {
                            dmap: register_dmap,
                            sid: register_sid,
                            ..
                        },
                    ) if cid == register_result.client_id
                        && &sid == register_sid
                        && register_dmap
                            .as_ref()
                            .map(|dmap| &addr == dmap)
                            .unwrap_or(false)
                        && &conn == "local" =>
                    {
                        Some((addr, tid, did))
                    }
                    _ => None,
                }
            })
            .await?;

        let msg = UdpDiscovery {
            tid: local_tid,
            payload: UdpXml::C2dA(C2dA {
                sid: register_result.sid,
                conn: "local".to_string(),
                cid: register_result.client_id,
                did: local_did,
                mtu: MTU,
            }),
        };

        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Other("Discovery already complete"))?;
        // Send and await confirm
        self.retry_send(msg, addr, |bc, _| {
            trace!("msg: {:?}", &bc);
            match bc {
                UdpDiscovery {
                    tid: _,
                    payload:
                        UdpXml::D2cCfm(D2cCfm {
                            sid,
                            cid,
                            did,
                            conn,
                            ..
                        }),
                } if sid == register_result.sid
                    && did == local_did
                    && cid == register_result.client_id
                    && &conn == "local" =>
                {
                    Some(())
                }
                _ => None,
            }
        })
        .await?;

        let result = ConnectResult {
            addr,
            client_id: register_result.client_id,
            sid: register_result.sid,
            camera_id: local_did,
        };

        // Confirm local to register
        let msg = UdpDiscovery {
            tid: 0,
            payload: UdpXml::C2rCfm(C2rCfm {
                sid: result.sid,
                cid: result.client_id,
                did: result.camera_id,
                rsp: 0,
                conn: "local".to_string(),
            }),
        };

        self.send_and_forget(msg, register_result.reg).await?;

        self.keep_alive_device(local_tid, &result).await;

        self.semaphore.close();
        drop(permit);
        Ok(result)
    }

    async fn device_initiated_map(
        &self,
        register_result: &RegisterResult,
    ) -> Result<ConnectResult> {
        let (addr, local_tid, local_did) = self
            .handle_incoming(|bc, addr| {
                trace!("bc: {:?}", bc);
                match (bc, register_result) {
                    (
                        UdpDiscovery {
                            tid,
                            payload:
                                UdpXml::D2cT(D2cT {
                                    sid,
                                    cid,
                                    did,
                                    conn,
                                    ..
                                }),
                        },
                        RegisterResult {
                            dmap: register_dmap,
                            sid: register_sid,
                            ..
                        },
                    ) if cid == register_result.client_id
                        && &sid == register_sid
                        && register_dmap
                            .as_ref()
                            .map(|dmap| &addr == dmap)
                            .unwrap_or(false)
                        && &conn == "map" =>
                    {
                        Some((addr, tid, did))
                    }
                    _ => None,
                }
            })
            .await?;

        let msg = UdpDiscovery {
            tid: local_tid,
            payload: UdpXml::C2dA(C2dA {
                sid: register_result.sid,
                conn: "map".to_string(),
                cid: register_result.client_id,
                did: local_did,
                mtu: MTU,
            }),
        };

        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Other("Discovery already complete"))?;
        // Send and await confirm
        self.retry_send(msg, addr, |bc, _| {
            trace!("msg: {:?}", &bc);
            match bc {
                UdpDiscovery {
                    tid: _,
                    payload:
                        UdpXml::D2cCfm(D2cCfm {
                            sid,
                            cid,
                            did,
                            conn,
                            ..
                        }),
                } if sid == register_result.sid
                    && did == local_did
                    && cid == register_result.client_id
                    && &conn == "map" =>
                {
                    Some(())
                }
                _ => None,
            }
        })
        .await?;

        let result = ConnectResult {
            addr,
            client_id: register_result.client_id,
            sid: register_result.sid,
            camera_id: local_did,
        };

        // Confirm map to register
        let msg = UdpDiscovery {
            tid: 0,
            payload: UdpXml::C2rCfm(C2rCfm {
                sid: result.sid,
                cid: result.client_id,
                did: result.camera_id,
                rsp: 0,
                conn: "map".to_string(),
            }),
        };

        self.send_and_forget(msg, register_result.reg).await?;

        self.keep_alive_device(local_tid, &result).await;

        self.semaphore.close();
        drop(permit);
        Ok(result)
    }

    async fn client_initiated_dev(
        &self,
        register_result: &RegisterResult,
    ) -> Result<ConnectResult> {
        let dev_addr = register_result.dev.ok_or(Error::NoDev)?;
        let (tid, reply) = self.subscribe_new().await?;
        let msg = UdpDiscovery {
            tid,
            payload: UdpXml::C2dT(C2dT {
                sid: register_result.sid,
                cid: register_result.client_id,
                mtu: MTU,
                conn: "local".to_string(),
            }),
        };

        let (final_addr, local_did) = self
            .retry_send_on(reply, msg, dev_addr, |bc, addr| match bc {
                UdpDiscovery {
                    tid: _,
                    payload: UdpXml::D2cCfm(D2cCfm { cid, did, sid, .. }),
                } if cid == register_result.client_id && sid == register_result.sid => {
                    Some((addr, did))
                }
                _ => None,
            })
            .await?;

        let result = ConnectResult {
            addr: final_addr,
            client_id: register_result.client_id,
            sid: register_result.sid,
            camera_id: local_did,
        };

        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Other("Discovery already complete"))?;
        // Confirm local to register
        let msg = UdpDiscovery {
            tid: 0,
            payload: UdpXml::C2rCfm(C2rCfm {
                sid: result.sid,
                cid: result.client_id,
                did: result.camera_id,
                conn: "local".to_string(),
                rsp: 0,
            }),
        };

        self.send_and_forget(msg, register_result.reg).await?;

        self.keep_alive_device(tid, &result).await;

        self.semaphore.close();
        drop(permit);
        Ok(result)
    }

    async fn client_initiated_relay(
        &self,
        register_result: &RegisterResult,
    ) -> Result<ConnectResult> {
        let relay_addr = register_result.relay.ok_or(Error::NoDev)?;
        let (tid, reply) = self.subscribe_new().await?;
        let msg = UdpDiscovery {
            tid,
            payload: UdpXml::C2dT(C2dT {
                sid: register_result.sid,
                cid: register_result.client_id,
                mtu: MTU,
                conn: "relay".to_string(),
            }),
        };

        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Other("Discovery already complete"))?;
        let (final_addr, local_did) = self
            .retry_send_on(reply, msg, relay_addr, |bc, addr| match bc {
                UdpDiscovery {
                    tid: _,
                    payload:
                        UdpXml::D2cCfm(D2cCfm {
                            cid,
                            did,
                            sid,
                            conn,
                            ..
                        }),
                } if cid == register_result.client_id
                    && sid == register_result.sid
                    && &conn == "relay" =>
                {
                    Some((addr, did))
                }
                _ => None,
            })
            .await?;

        let result = ConnectResult {
            addr: final_addr,
            client_id: register_result.client_id,
            sid: register_result.sid,
            camera_id: local_did,
        };

        // Confirm relay to register
        let msg = UdpDiscovery {
            tid: 0,
            payload: UdpXml::C2rCfm(C2rCfm {
                sid: result.sid,
                cid: result.client_id,
                did: result.camera_id,
                conn: "relay".to_string(),
                rsp: 0,
            }),
        };

        self.send_and_forget(msg, register_result.reg).await?;

        self.keep_alive_device(tid, &result).await;
        // self.keep_alive_relay(tid, &result).await;

        self.semaphore.close();
        drop(permit);
        Ok(result)
    }

    async fn keep_alive_device(&self, tid: u32, connect_result: &ConnectResult) {
        let client_id = connect_result.client_id;
        let camera_id = connect_result.camera_id;
        let addr = connect_result.addr;
        let mut sender = ArcFramedSocket::new(self.socket.clone(), BcUdpCodex::new());
        let mut interval = interval(Duration::from_secs(1));
        let thread_cancel = self.cancel.clone();
        self.handle.write().await.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    loop {
                        tokio::task::yield_now().await;
                        interval.tick().await;
                        let msg = BcUdp::Discovery(UdpDiscovery {
                            tid,
                            payload: UdpXml::C2dHb(C2dHb {
                                    cid: client_id,
                                    did: camera_id,
                                }),
                        });
                        if sender.send((msg, addr)).await.is_err() {
                            break Result::Ok(());
                        }
                    }
                } => v,
            }
        });
    }

    #[allow(dead_code)] // Haven't seen this in the wild yet it is just speculation
    async fn keep_alive_relay(&self, tid: u32, connect_result: &ConnectResult) {
        let client_id = connect_result.client_id;
        let camera_id = connect_result.camera_id;
        let sid = connect_result.sid;
        let addr = connect_result.addr;
        let mut sender = ArcFramedSocket::new(self.socket.clone(), BcUdpCodex::new());
        let mut interval = interval(Duration::from_secs(1));
        let thread_cancel = self.cancel.clone();
        self.handle.write().await.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    loop {
                        tokio::task::yield_now().await;
                        interval.tick().await;
                        let msg = BcUdp::Discovery(UdpDiscovery {
                            tid,
                            payload: UdpXml::C2rHb(C2rHb {
                                    sid,
                                    cid: client_id,
                                    did: camera_id,
                                }),
                        });
                        if sender.send((msg, addr)).await.is_err() {
                            break Result::Ok(());
                        }
                    }
                } => v,
            }
        });
    }
}

impl Drop for Discoverer {
    fn drop(&mut self) {
        log::trace!("Drop Discoverer");
        self.cancel.cancel();
        let _gt = tokio::runtime::Handle::current().enter();
        let mut handle = std::mem::take(&mut self.handle);
        tokio::task::spawn(async move { while handle.get_mut().join_next().await.is_some() {} });
        log::trace!("Dropped Discoverer");
    }
}

pub(crate) struct Discovery {
    discoverer: Discoverer,
    client_id: i32,
}

impl Discovery {
    pub(crate) async fn new() -> Result<Self> {
        Ok(Self {
            discoverer: Discoverer::new().await?,
            client_id: generate_cid(),
        })
    }

    /// A discovery whose socket suits `scope`. For `Interface` the socket
    /// binds that interface's IPv4 address, and a missing interface is an
    /// error before any socket is made. Every other scope binds 0.0.0.0.
    pub(crate) async fn for_scope(scope: &DiscoveryScope) -> Result<Self> {
        let bind = match scope {
            DiscoveryScope::Interface(name) => find_interface(name, &host_interfaces()?)?.ipv4,
            DiscoveryScope::All | DiscoveryScope::Targets => Ipv4Addr::UNSPECIFIED,
        };
        Ok(Self {
            discoverer: Discoverer::new_on(bind).await?,
            client_id: generate_cid(),
        })
    }

    pub(crate) async fn get_registration(&self, uid: &str) -> Result<RegisterResult> {
        let lookups = self.discoverer.uid_lookup_all(uid).await?;

        let checked_reg = Arc::new(RwLock::new(HashSet::new()));
        let reg_result = Box::pin(
            lookups
                .then(|lookup| {
                    let discoverer = &self.discoverer;
                    let client_id = self.client_id;

                    let thread_checked_reg = checked_reg.clone();
                    async move {
                        let mut locked_checked_reg = thread_checked_reg.write().await;
                        if locked_checked_reg.contains(&lookup.reg) {
                            return Result::Err(Error::Other("Already checked."));
                        }
                        locked_checked_reg.insert(lookup.reg);
                        drop(locked_checked_reg);
                        trace!("lookup: {:?}", lookup);
                        let reg_result = discoverer.register_address(uid, client_id, &lookup).await;
                        trace!("reg_result: {:?}", reg_result);
                        reg_result
                    }
                })
                .filter_map(|f| async { f.ok() }),
        )
        .next()
        .await
        .ok_or(Error::Other(
            "No reolink registers returned valid device data.",
        ))?;

        Ok(reg_result)
    }

    // Check if TCP is possible
    //
    // To do this we send a dummy login  and see if it replies with any BC packet
    pub(crate) async fn check_tcp(&self, addr: SocketAddr, channel_id: u8) -> Result<()> {
        let username = "admin";
        let password = Some("123456");
        let mut tcp_source =
            timeout(*TCP_WAIT, TcpSource::new(addr, username, password, false)).await??;

        let md5_username = md5_string(username, Md5Trunc::ZeroLast);
        let md5_password = password
            .map(|p| md5_string(p, Md5Trunc::ZeroLast))
            .unwrap_or_else(|| EMPTY_LEGACY_PASSWORD.to_owned());

        tcp_source
            .send(Bc {
                meta: BcMeta {
                    msg_id: MSG_ID_LOGIN,
                    channel_id,
                    msg_num: 0,
                    stream_type: 0,
                    response_code: 0x00,
                    class: 0x6514,
                },
                body: BcBody::LegacyMsg(LegacyMsg::LoginMsg {
                    username: md5_username,
                    password: md5_password,
                }),
            })
            .await?;

        let _bc: Bc = timeout(*TCP_WAIT, tcp_source.next())
            .await?
            .ok_or(Error::CannotInitCamera)??; // Successful recv should mean a Bc packet if not then deser will fail
        Ok(())
    }

    // Perform UDP broadcast lookup and connection
    pub(crate) async fn local(
        &self,
        uid: &str,
        mut optional_addrs: Option<Vec<SocketAddr>>,
    ) -> Result<DiscoveryResult> {
        let mut dests = get_broadcasts(&[2015, 2018])?;
        if let Some(mut optional_addrs) = optional_addrs.take() {
            debug!("Also sending to {:?}", optional_addrs);
            dests.append(&mut optional_addrs);
        }
        self.connect_first(uid, &dests).await
    }

    /// Local discovery limited to `scope`, with the host's own interfaces.
    /// The writer is limited to the plan before the first packet, so nothing
    /// outside it can leave. A plan error fails discovery with no packet sent.
    pub(crate) async fn local_in_scope(
        &self,
        uid: &str,
        given: &[SocketAddr],
        scope: &DiscoveryScope,
    ) -> Result<DiscoveryResult> {
        self.local_with_interfaces(uid, given, scope, &host_interfaces()?)
            .await
    }

    async fn local_with_interfaces(
        &self,
        uid: &str,
        given: &[SocketAddr],
        scope: &DiscoveryScope,
        interfaces: &[InterfaceV4],
    ) -> Result<DiscoveryResult> {
        let dests = plan_destinations(scope, interfaces, given, &[2015, 2018])?;
        debug!("Discovery in its scope sends to {:?}", dests);
        self.discoverer.limit_to(&dests);
        self.connect_first(uid, &dests).await
    }

    /// Sends a direct connect to every destination at once and returns the
    /// first camera that answers.
    async fn connect_first(&self, uid: &str, dests: &[SocketAddr]) -> Result<DiscoveryResult> {
        let discoverer_ref = &self.discoverer;
        let client_id = self.client_id;
        let mut futures = FuturesUnordered::new();
        for addr in dests.iter().copied() {
            futures.push(async move {
                discoverer_ref
                    .client_initiated_direct(uid, client_id, addr)
                    .await
            })
        }

        let connect_result;
        loop {
            match futures.next().await {
                Some(Ok(good_result)) => {
                    connect_result = good_result;
                    break;
                }
                Some(Err(_)) => {
                    continue;
                }
                None => {
                    return Err(Error::DiscoveryTimeout);
                }
            }
        }
        drop(futures);
        // drop(discoverer_ref);

        let socket = self.discoverer.get_socket().await;
        Ok(DiscoveryResult {
            socket,
            addr: connect_result.addr,
            camera_id: connect_result.camera_id,
            client_id: connect_result.client_id,
        })
    }

    // This will start remote discovery against the reolink p2p servers
    //
    // This works by registering our ip and intent to connect with the reolink
    // servers
    //
    // We will then try to connect to the camera local ip address while the camera
    // will also attempt to connec to ours
    //
    // This method is best when broadcasts are not possible but we can contact the camera
    // directly
    #[allow(unused)]
    pub(crate) async fn remote(
        &self,
        uid: &str,
        reg_result: &RegisterResult,
    ) -> Result<DiscoveryResult> {
        trace!("Start remote");
        let connect_result = tokio::select! {
            v = self.discoverer.client_initiated_dev(reg_result) => {v},
            v = self.discoverer.device_initiated_dev(reg_result) => {v},
        }?;
        trace!("connect_result: {:?}", connect_result);
        let socket = self.discoverer.get_socket().await;
        Ok(DiscoveryResult {
            socket,
            addr: connect_result.addr,
            client_id: self.client_id,
            camera_id: connect_result.camera_id,
        })
    }

    // This is similar to remote, except that it allows the camera to connect to us
    // over it's dmap (public) ip address that it has registered with reolink servers.
    //
    // This works by registering our ip address and the desire to connect with the
    // reolink servers. Data however should go to the camera's public ip address
    //
    // This method should be used when the camera is behind a NAT or firewall but we are
    // reachable
    pub(crate) async fn map(&self, reg_result: &RegisterResult) -> Result<DiscoveryResult> {
        let connect_result = self.discoverer.device_initiated_map(reg_result).await?;
        trace!("connect_result: {:?}", connect_result);

        let socket = self.discoverer.get_socket().await;
        Ok(DiscoveryResult {
            socket,
            addr: connect_result.addr,
            client_id: self.client_id,
            camera_id: connect_result.camera_id,
        })
    }

    // This will forward all connections via the reolinks servers
    //
    // This method should work if all else fails but it will require
    // us to trust reolink with our data once more...
    //
    pub(crate) async fn relay(&self, reg_result: &RegisterResult) -> Result<DiscoveryResult> {
        let connect_result = self.discoverer.client_initiated_relay(reg_result).await?;
        trace!("connect_result: {:?}", connect_result);

        let socket = self.discoverer.get_socket().await;
        Ok(DiscoveryResult {
            socket,
            addr: connect_result.addr,
            client_id: self.client_id,
            camera_id: connect_result.camera_id,
        })
    }
}

fn get_local_ip() -> Result<std::net::IpAddr> {
    get_if_addrs::get_if_addrs()?
        .iter()
        .find(|i| !i.is_loopback() && matches!(i.addr, get_if_addrs::IfAddr::V4(_)))
        .map(|iface| Ok(iface.ip()))
        .unwrap_or_else(|| Err(Error::Other("No Local Ip Address Found")))
}

/// An IPv4 interface address of this host, as the planner sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InterfaceV4 {
    pub(crate) name: String,
    pub(crate) ipv4: Ipv4Addr,
    pub(crate) netmask: Ipv4Addr,
    pub(crate) broadcast: Option<Ipv4Addr>,
}

impl InterfaceV4 {
    fn contains(&self, ip: Ipv4Addr) -> bool {
        let mask = u32::from(self.netmask);
        u32::from(ip) & mask == u32::from(self.ipv4) & mask
    }
}

/// Every IPv4 interface address of this host, in the order the system
/// lists them.
fn host_interfaces() -> Result<Vec<InterfaceV4>> {
    Ok(get_if_addrs::get_if_addrs()?
        .into_iter()
        .filter_map(|iface| match iface.addr {
            get_if_addrs::IfAddr::V4(v4) => Some(InterfaceV4 {
                name: iface.name,
                ipv4: v4.ip,
                netmask: v4.netmask,
                broadcast: v4.broadcast,
            }),
            get_if_addrs::IfAddr::V6(_) => None,
        })
        .collect())
}

/// The first address of the interface `name` that has an IPv4 broadcast.
fn find_interface<'a>(name: &str, interfaces: &'a [InterfaceV4]) -> Result<&'a InterfaceV4> {
    interfaces
        .iter()
        .find(|iface| iface.name == name && iface.broadcast.is_some())
        .ok_or(Error::Other(
            "The discovery interface is missing or has no IPv4 broadcast",
        ))
}

/// Whether a packet to `ip` would reach more than one host: the limited
/// broadcast, the broadcast of any interface, an unspecified or a multicast
/// address.
fn is_broadcast_like(ip: Ipv4Addr, interfaces: &[InterfaceV4]) -> bool {
    ip.is_broadcast()
        || ip.is_unspecified()
        || ip.is_multicast()
        || interfaces.iter().any(|iface| iface.broadcast == Some(ip))
}

/// The destinations local discovery sends to under `scope`.
///
/// - `All`: 255.255.255.255 and the broadcast of every interface on each
///   port, then every given address, as neolink has always sent.
/// - `Targets`: each given IPv4 host on each port, in the order given,
///   without repeats. The port of a given address is not used. A broadcast,
///   unspecified or multicast address is dropped and logged. With none left
///   this is an error.
/// - `Interface(name)`: that interface's broadcast on each port, then each
///   given IPv4 host inside its subnet on each port. A given address outside
///   it is dropped and logged. A missing interface is an error. So is a
///   reported broadcast that is 255.255.255.255, unspecified, multicast or
///   outside the interface's subnet.
///
/// An error means discovery sends nothing.
pub(crate) fn plan_destinations(
    scope: &DiscoveryScope,
    interfaces: &[InterfaceV4],
    given: &[SocketAddr],
    ports: &[u16],
) -> Result<Vec<SocketAddr>> {
    fn push_unique(dests: &mut Vec<SocketAddr>, dest: SocketAddr) {
        if !dests.contains(&dest) {
            dests.push(dest);
        }
    }
    match scope {
        DiscoveryScope::All => {
            let mut dests = broadcasts(interfaces, ports);
            dests.extend_from_slice(given);
            Ok(dests)
        }
        DiscoveryScope::Targets => {
            let mut dests = vec![];
            for dest in given {
                match dest.ip() {
                    IpAddr::V4(ip) if !is_broadcast_like(ip, interfaces) => {
                        for &port in ports {
                            push_unique(&mut dests, SocketAddr::new(ip.into(), port));
                        }
                    }
                    _ => warn!(
                        "Discovery: {} is not a single IPv4 host, so it is not sent to",
                        dest
                    ),
                }
            }
            if dests.is_empty() {
                return Err(Error::Other("The discovery targets hold no address"));
            }
            Ok(dests)
        }
        DiscoveryScope::Interface(name) => {
            let iface = find_interface(name, interfaces)?;
            let Some(broadcast) = iface.broadcast else {
                return Err(Error::Other(
                    "The discovery interface is missing or has no IPv4 broadcast",
                ));
            };
            // The interface's own directed broadcast is the only broadcast
            // this scope sends to. A reported broadcast that is the limited
            // broadcast, unspecified, multicast or outside the interface's
            // subnet is refused, and discovery sends nothing.
            if broadcast.is_broadcast()
                || broadcast.is_unspecified()
                || broadcast.is_multicast()
                || !iface.contains(broadcast)
            {
                warn!(
                    "Discovery: interface {} reports the broadcast {}, which is not its own subnet broadcast, so discovery sends nothing",
                    name, broadcast
                );
                return Err(Error::Other(
                    "The discovery interface reports a broadcast outside its own subnet",
                ));
            }
            let mut dests = vec![];
            for &port in ports {
                push_unique(&mut dests, SocketAddr::new(broadcast.into(), port));
            }
            for dest in given {
                match dest.ip() {
                    IpAddr::V4(ip) if iface.contains(ip) && !is_broadcast_like(ip, interfaces) => {
                        for &port in ports {
                            push_unique(&mut dests, SocketAddr::new(ip.into(), port));
                        }
                    }
                    _ => warn!(
                        "Discovery: {} lies outside interface {}, so it is not sent to",
                        dest, name
                    ),
                }
            }
            Ok(dests)
        }
    }
}

fn get_broadcasts(ports: &[u16]) -> Result<Vec<SocketAddr>> {
    Ok(broadcasts(&host_interfaces()?, ports))
}

/// 255.255.255.255 and the broadcast of every interface that has one, each
/// on every port.
fn broadcasts(interfaces: &[InterfaceV4], ports: &[u16]) -> Vec<SocketAddr> {
    let mut broadcasts = vec![Ipv4Addr::BROADCAST];
    for iface in interfaces.iter() {
        if let Some(broadcast) = iface.broadcast.as_ref() {
            broadcasts.push(*broadcast);
        }
    }
    let mut destinations: Vec<(Ipv4Addr, u16)> = broadcasts
        .iter()
        .flat_map(|&addr| {
            ports
                .iter()
                .map(|&port| (addr, port))
                .collect::<Vec<(Ipv4Addr, u16)>>()
        })
        .collect();
    debug!("Broadcasting to: {:?}", destinations);
    destinations
        .drain(..)
        .map(|(addr, port)| SocketAddr::new(addr.into(), port))
        .collect()
}

fn generate_tid() -> u32 {
    let mut rng = thread_rng();
    (rng.gen::<u8>()) as u32
}

fn generate_cid() -> i32 {
    let mut rng = thread_rng();
    rng.gen()
}

async fn connect(bind: Ipv4Addr) -> Result<UdpSocket> {
    let mut ports: Vec<u16> = (53500..54000).collect();
    {
        let mut rng = thread_rng();
        ports.shuffle(&mut rng);
    }

    let addrs: Vec<_> = ports
        .iter()
        .map(|&port| SocketAddr::from((bind, port)))
        .collect();
    let socket = UdpSocket::bind(&addrs[..]).await?;
    socket.set_broadcast(true)?;

    Ok(socket)
}

/*
    # Discovery Methods

    # Register

    This is the inital query to known hosts with a known UID

    - C->R Port 9999
    ```xml
    <P2P>
    <C2M_Q>
    <uid>95270000YGAKNWKJ</uid>
    <p>MAC</p>
    </C2M_Q>
    </P2P>
    ```

    Replies with details of the camera we want to connect to

    - R->C
    ```xml
    <P2P>
        <M2C_Q_R>
            <reg>
                <ip>18.162.200.47</ip>
                <port>58200</port>
            </reg>
            <relay>
                <ip>18.162.200.47</ip>
                <port>58100</port>
            </relay>
            <log>
                <ip>18.162.200.47</ip>
                <port>57850</port>
            </log>
            <t>
                <ip>18.162.200.47</ip>
                <port>9996</port>
            </t>
            <timer/>
            <retry/>
            <mtu>1350</mtu>
            <debug>251658240</debug>
            <ac>-1700607721</ac>
            <rsp>0</rsp>
        </M2C_Q_R>
    </P2P>
    ```

    ## Thread 1: Observed during relay

    - D->C 59733
    ```xml
    <P2P>
    <D2C_T>
    <sid>495151439</sid>
    <conn>map</conn>
    <cid>254000</cid>
    <did>735</did>
    </D2C_T>
    </P2P>
    ```

    - C->D
    ```xml
    <P2P>
    <C2D_A>
    <sid>495151439</sid>
    <conn>map</conn>
    <cid>254000</cid>
    <did>735</did>
    <mtu>1350</mtu>
    </C2D_A>
    </P2P>
    ```

    - D->C
    ```xml
    <P2P>
    <D2C_CFM>
    <sid>495151439</sid>
    <conn>map</conn>
    <rsp>0</rsp>
    <cid>254000</cid>
    <did>735</did>
    <time_r>55607</time_r>
    </D2C_CFM>
    </P2P>
     ```

    ## Thread 2: Observed during relay

    - C->R: 58200
    ```xml
    <P2P>
    <C2R_C>
    <uid>95270000YGAKNWKJ</uid>
    <cli>
    <ip>10.202.133.237</ip>
    <port>12254</port>
    </cli>
    <relay>
    <ip>18.162.200.47</ip>
    <port>58100</port>
    </relay>
    <cid>254000</cid>
    <debug>251658240</debug>
    <family>4</family>
    <p>MAC</p>
    <r>3</r>
    </C2R_C>
    </P2P>
    ```

    - R->C
    ```xml
    <P2P><R2C_T><dev><ip>192.168.1.100</ip><port>57933</port></dev><dmap><ip>184.22.90.67</ip><port>57933</port></dmap><sid>495151439</sid><cid>254000</cid><rsp>0</rsp></R2C_T></P2P>
    ```

    - R->C
    ```xml
    <P2P>
        <R2C_C_R>
            <dmap>
                <ip>184.22.90.67</ip>
                <port>57933</port>
            </dmap>
            <dev>
                <ip>192.168.1.100</ip>
                <port>57933</port>
            </dev>
            <relay>
                <ip>18.162.200.47</ip>
                <port>51134</port>
            </relay>
            <relayt>
                <ip>18.162.200.47</ip>
                <port>9997</port>
            </relayt>
            <nat>NULL</nat>
            <sid>495151439</sid>
            <rsp>0</rsp>
            <ac>495151439</ac>
        </R2C_C_R>
    </P2P>
    ```

    - R->C
    ```xml
    <P2P>
        <R2C_T>
            <dev>
                <ip>192.168.1.100</ip>
                <port>57933</port>
            </dev>
            <dmap>
                <ip>184.22.90.67</ip>
                <port>57933</port>
            </dmap>
            <sid>495151439</sid>
            <cid>254000</cid>
            <rsp>0</rsp>
        </R2C_T>
    </P2P>
    ```

    - R->C Repeats later so possibly was not responded to by client
    ```xml
    <P2P>
        <R2C_C_R>
            <dmap>
                <ip>184.22.90.67</ip>
                <port>57933</port>
            </dmap>
            <dev>
                <ip>192.168.1.100</ip>
                <port>57933</port>
            </dev>
            <relay>
                <ip>18.162.200.47</ip>
                <port>51134</port>
            </relay>
            <relayt>
                <ip>18.162.200.47</ip>
                <port>9997</port>
            </relayt>
            <nat>NULL</nat>
            <sid>495151439</sid>
            <rsp>0</rsp>
            <ac>495151439</ac>
        </R2C_C_R>
    </P2P>
    ```

    - C->R
    ```xml
    <P2P>
    <C2R_CFM>
    <sid>495151439</sid>
    <conn>map</conn>
    <rsp>0</rsp>
    <cid>254000</cid>
    <did>735</did>
    </C2R_CFM>
    </P2P>
    ```

    - R->C
    ```xml
    <P2P>
        <R2C_T>
            <dev>
                <ip>192.168.1.100</ip>
                <port>57933</port>
            </dev>
            <dmap>
                <ip>184.22.90.67</ip>
                <port>57933</port>
            </dmap>
            <sid>495151439</sid>
            <cid>254000</cid>
            <rsp>0</rsp>
        </R2C_T>
    </P2P>
    ```

    # Thread 3: Observed during relay
    After connection. No response

    - C->R
    ```xml
    <P2P>
    <C2R_CFM>
    <sid>495151439</sid>
    <conn>map</conn>
    <rsp>0</rsp>
    <cid>254000</cid>
    <did>735</did>
    </C2R_CFM>
    </P2P>
    ```

    # Thread 4: Observed when behind a NAT on both ends of the connection

    - C->R
    ```
    <P2P>
    <C2D_T>
    <sid>526020041</sid>
    <conn>relay</conn>
    <cid>38000</cid>
    <mtu>1350</mtu>
    </C2D_T>
    </P2P>
    ```

    - R->C
    ```xml
    <P2P>
    <D2C_CFM>
    <sid>526020041</sid>
    <conn>relay</conn>
    <rsp>0</rsp>
    <cid>38000</cid>
    <did>32</did>
    <time_r>0</time_r>
    </D2C_CFM>
    </P2P>
    ```

*/

#[cfg(test)]
mod tests {
    use super::*;

    const UID: &str = "TESTUID000000000";

    /// Local discovery sends one request per destination, all open at once,
    /// from one `Discoverer`. Here every destination is a loopback socket and
    /// there are as many as there are tids, so a tid drawn without regard to
    /// the open ones repeats with near certainty. A repeat is refused by
    /// `subscribe`, and that destination never gets its request.
    ///
    /// Once every request is open, each camera answers as a real one does,
    /// with a D2C_C_R that echoes the tid and the cid of the request, and
    /// again for each resend. Every request must then succeed. A reply that
    /// carries tid 0 is not routed to its request, so a request that drew
    /// tid 0 would time out here.
    #[tokio::test]
    async fn every_open_destination_gets_its_own_tid() -> Result<()> {
        fn answer(tid: u32, cid: i32, did: i32) -> BcUdp {
            BcUdp::Discovery(UdpDiscovery {
                tid,
                payload: UdpXml::D2cCr(D2cCr {
                    timer: Default::default(),
                    rsp: 0,
                    cid,
                    did,
                }),
            })
        }

        let discoverer = Discoverer::new().await?;
        let mut cameras = vec![];
        for _ in TID_RANGE {
            let socket = UdpSocket::bind("127.0.0.1:0").await?;
            cameras.push(UdpFramed::new(socket, BcUdpCodex::new()));
        }
        let dests = cameras
            .iter()
            .map(|camera| camera.get_ref().local_addr())
            .collect::<std::io::Result<Vec<_>>>()?;

        let requests = dests
            .iter()
            .map(|&dest| discoverer.client_initiated_direct(UID, 7, dest))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>();

        let cameras_answer = async {
            let mut tids = BTreeMap::new();
            let mut asked = vec![];
            for (n, camera) in cameras.iter_mut().enumerate() {
                let (msg, from) = timeout(Duration::from_secs(5), camera.next())
                    .await
                    .unwrap_or_else(|_| {
                        panic!("destination {} of {} got no request", n, dests.len())
                    })
                    .expect("socket open")
                    .expect("a discovery packet");
                match msg {
                    BcUdp::Discovery(UdpDiscovery {
                        tid,
                        payload: UdpXml::C2dC(C2dC { uid, cid, .. }),
                    }) => {
                        assert_eq!(uid, UID);
                        assert!((1..=255).contains(&tid), "tid {} outside 1..=255", tid);
                        if let Some(earlier) = tids.insert(tid, n) {
                            panic!("destinations {} and {} share tid {}", earlier, n, tid);
                        }
                        asked.push((tid, cid, from));
                    }
                    other => panic!("destination {} got {:?}", n, other),
                }
            }
            cameras
                .iter_mut()
                .zip(asked)
                .enumerate()
                .map(|(n, (camera, (tid, cid, from)))| async move {
                    let did = n as i32;
                    camera
                        .send((answer(tid, cid, did), from))
                        .await
                        .expect("the reply is sent");
                    while let Some(Ok((msg, from))) = camera.next().await {
                        if let BcUdp::Discovery(UdpDiscovery {
                            tid,
                            payload: UdpXml::C2dC(C2dC { cid, .. }),
                        }) = msg
                        {
                            camera
                                .send((answer(tid, cid, did), from))
                                .await
                                .expect("the reply is sent");
                        }
                    }
                })
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await;
        };

        let results = timeout(Duration::from_secs(30), async {
            tokio::select! {
                _ = cameras_answer => panic!("the cameras stopped answering"),
                results = requests => results,
            }
        })
        .await
        .expect("every request ends");
        assert_eq!(results.len(), dests.len());
        for result in results {
            assert!(result.is_ok(), "a request failed: {:?}", result.err());
        }
        Ok(())
    }

    /// With every tid held, a new request fails rather than share a tid,
    /// and a tid comes free again once its receiver is dropped.
    #[tokio::test]
    async fn new_tid_is_refused_while_all_are_held() -> Result<()> {
        let discoverer = Discoverer::new().await?;
        let mut held = BTreeMap::new();
        for _ in TID_RANGE {
            let (tid, rx) = discoverer.subscribe_new().await?;
            assert!((1..=255).contains(&tid), "tid {} outside 1..=255", tid);
            assert!(
                held.insert(tid, rx).is_none(),
                "tid {} handed out twice",
                tid
            );
        }
        assert_eq!(held.len(), 255);
        assert!(
            discoverer.subscribe_new().await.is_err(),
            "a tid was handed out while all {} were held",
            held.len()
        );

        let freed = 42;
        drop(held.remove(&freed));
        let (tid, _rx) = discoverer.subscribe_new().await?;
        assert_eq!(tid, freed);
        Ok(())
    }

    /// A D2C_C_R that answers a C2D_C, as a camera sends it.
    fn connect_reply(tid: u32, cid: i32, did: i32) -> BcUdp {
        BcUdp::Discovery(UdpDiscovery {
            tid,
            payload: UdpXml::D2cCr(D2cCr {
                timer: Default::default(),
                rsp: 0,
                cid,
                did,
            }),
        })
    }

    /// A C2D_C as local discovery sends it.
    fn connect_request(uid: &str, tid: u32) -> BcUdp {
        BcUdp::Discovery(UdpDiscovery {
            tid,
            payload: UdpXml::C2dC(C2dC {
                uid: uid.to_string(),
                cli: ClientList { port: 53500 },
                cid: 7,
                mtu: MTU,
                debug: false,
                os: "MAC".to_string(),
            }),
        })
    }

    /// Answers every C2D_C that reaches `camera`, until its socket closes.
    async fn answer_connects(mut camera: UdpFramed<BcUdpCodex>, did: i32) {
        while let Some(Ok((msg, from))) = camera.next().await {
            if let BcUdp::Discovery(UdpDiscovery {
                tid,
                payload: UdpXml::C2dC(C2dC { cid, .. }),
            }) = msg
            {
                let _ = camera.send((connect_reply(tid, cid, did), from)).await;
            }
        }
    }

    async fn loopback_camera(addr: &str) -> Result<UdpFramed<BcUdpCodex>> {
        Ok(UdpFramed::new(
            UdpSocket::bind(addr).await?,
            BcUdpCodex::new(),
        ))
    }

    /// Fails when `socket` receives any datagram within one second.
    async fn assert_silent(socket: &UdpSocket) {
        let mut buf = [0u8; 2048];
        let got = timeout(Duration::from_secs(1), socket.recv_from(&mut buf)).await;
        if let Ok(Ok((len, from))) = got {
            panic!(
                "{:?} received {} bytes from {}",
                socket.local_addr(),
                len,
                from
            );
        }
    }

    fn sa(text: &str) -> SocketAddr {
        text.parse().expect("a socket address")
    }

    fn iface(name: &str, ip: &str, mask: &str, broadcast: Option<&str>) -> InterfaceV4 {
        InterfaceV4 {
            name: name.to_string(),
            ipv4: ip.parse().expect("an address"),
            netmask: mask.parse().expect("a netmask"),
            broadcast: broadcast.map(|b| b.parse().expect("a broadcast")),
        }
    }

    /// A host in the shape of sting: loopback, four networks and 100 pod
    /// interfaces. Every address is from a documentation or benchmark
    /// range and is only data here.
    fn synthetic_host() -> Vec<InterfaceV4> {
        let mut host = vec![
            iface("lo", "127.0.0.1", "255.0.0.0", None),
            iface("eno1", "192.0.2.10", "255.255.255.0", Some("192.0.2.255")),
            iface(
                "bench0",
                "198.51.100.20",
                "255.255.255.0",
                Some("198.51.100.255"),
            ),
            iface(
                "wlan0",
                "203.0.113.5",
                "255.255.255.128",
                Some("203.0.113.127"),
            ),
            iface("wg0", "203.0.113.200", "255.255.255.255", None),
        ];
        for n in 0..100u32 {
            let base = u32::from(Ipv4Addr::new(198, 18, 0, 0)) + n * 4;
            host.push(InterfaceV4 {
                name: format!("veth{}", n),
                ipv4: Ipv4Addr::from(base + 1),
                netmask: Ipv4Addr::new(255, 255, 255, 252),
                broadcast: Some(Ipv4Addr::from(base + 3)),
            });
        }
        host
    }

    const PORTS: [u16; 2] = [2015, 2018];

    #[test]
    fn plan_all_matches_today() -> Result<()> {
        let host = synthetic_host();
        let given = [sa("192.0.2.50:2018"), sa("192.0.2.50:2015")];
        let plan = plan_destinations(&DiscoveryScope::All, &host, &given, &PORTS)?;

        // Today's list, built here by hand: the limited broadcast first, then
        // each interface broadcast in the order the host lists them, each on
        // 2015 then 2018, then the given addresses as they came.
        let mut want = vec![sa("255.255.255.255:2015"), sa("255.255.255.255:2018")];
        for b in ["192.0.2.255", "198.51.100.255", "203.0.113.127"] {
            want.push(sa(&format!("{}:2015", b)));
            want.push(sa(&format!("{}:2018", b)));
        }
        for n in 0..100u32 {
            let b = Ipv4Addr::from(u32::from(Ipv4Addr::new(198, 18, 0, 0)) + n * 4 + 3);
            want.push(SocketAddr::new(b.into(), 2015));
            want.push(SocketAddr::new(b.into(), 2018));
        }
        want.extend_from_slice(&given);
        assert_eq!(plan.len(), (1 + 3 + 100) * 2 + 2);
        assert_eq!(plan, want);
        Ok(())
    }

    #[test]
    fn plan_targets_has_no_broadcast() -> Result<()> {
        let host = synthetic_host();
        let given = [
            sa("192.0.2.50:2018"),
            sa("192.0.2.50:2015"),
            sa("192.0.2.50:9000"),
            sa("255.255.255.255:2015"),
            sa("192.0.2.255:2018"),
            sa("198.18.0.3:2015"),
            sa("0.0.0.0:2015"),
            sa("224.0.0.1:2018"),
            sa("192.0.2.50:2018"),
        ];
        let plan = plan_destinations(&DiscoveryScope::Targets, &host, &given, &PORTS)?;
        assert_eq!(plan, vec![sa("192.0.2.50:2015"), sa("192.0.2.50:2018")]);
        for dest in &plan {
            assert!(
                PORTS.contains(&dest.port()),
                "{} is off 2015 and 2018",
                dest
            );
            let IpAddr::V4(ip) = dest.ip() else {
                panic!("{} is not IPv4", dest)
            };
            assert!(!ip.is_broadcast(), "{} is the limited broadcast", dest);
            assert!(
                host.iter().all(|i| i.broadcast != Some(ip)),
                "{} is an interface broadcast",
                dest
            );
        }

        let only_broadcasts = [sa("255.255.255.255:2015"), sa("192.0.2.255:2018")];
        assert!(
            plan_destinations(&DiscoveryScope::Targets, &host, &only_broadcasts, &PORTS).is_err()
        );
        assert!(plan_destinations(&DiscoveryScope::Targets, &host, &[], &PORTS).is_err());
        Ok(())
    }

    #[test]
    fn plan_interface_is_one_subnet() -> Result<()> {
        let host = synthetic_host();
        let given = [
            sa("198.51.100.77:2018"),
            sa("198.51.100.77:2015"),
            sa("192.0.2.50:2018"),
            sa("198.18.0.1:2015"),
        ];
        let scope = DiscoveryScope::Interface("bench0".to_string());
        let plan = plan_destinations(&scope, &host, &given, &PORTS)?;
        assert_eq!(
            plan,
            vec![
                sa("198.51.100.255:2015"),
                sa("198.51.100.255:2018"),
                sa("198.51.100.77:2015"),
                sa("198.51.100.77:2018"),
            ]
        );
        assert!(plan
            .iter()
            .all(|d| d.ip() != IpAddr::V4(Ipv4Addr::BROADCAST)));
        Ok(())
    }

    #[tokio::test]
    async fn plan_missing_interface_fails_closed() -> Result<()> {
        let host = synthetic_host();
        for name in ["nosuch0", "lo", "wg0"] {
            let scope = DiscoveryScope::Interface(name.to_string());
            assert!(
                plan_destinations(&scope, &host, &[], &PORTS).is_err(),
                "interface {} gave a plan",
                name
            );
        }

        // Interfaces that report a broadcast other than their own subnet
        // broadcast: the limited broadcast, unspecified, multicast and the
        // broadcast of another subnet. Each fails with no plan.
        let mut odd = host.clone();
        odd.push(iface(
            "limbr0",
            "192.0.2.130",
            "255.255.255.128",
            Some("255.255.255.255"),
        ));
        odd.push(iface(
            "zerobr0",
            "198.51.100.130",
            "255.255.255.128",
            Some("0.0.0.0"),
        ));
        odd.push(iface(
            "mcastbr0",
            "203.0.113.140",
            "255.255.255.192",
            Some("224.0.0.1"),
        ));
        odd.push(iface(
            "farbr0",
            "198.18.200.1",
            "255.255.255.0",
            Some("192.0.2.255"),
        ));
        // A netmask of 0.0.0.0 puts every address inside the subnet, so the
        // subnet check passes. Each of these is caught by its own check.
        odd.push(iface(
            "widelim0",
            "198.18.7.1",
            "0.0.0.0",
            Some("255.255.255.255"),
        ));
        odd.push(iface("widezero0", "198.18.8.1", "0.0.0.0", Some("0.0.0.0")));
        odd.push(iface(
            "widemcast0",
            "198.18.9.1",
            "0.0.0.0",
            Some("224.0.0.1"),
        ));
        for name in [
            "limbr0",
            "zerobr0",
            "mcastbr0",
            "farbr0",
            "widelim0",
            "widezero0",
            "widemcast0",
        ] {
            let scope = DiscoveryScope::Interface(name.to_string());
            let given = [sa("198.18.200.9:2015")];
            assert!(
                plan_destinations(&scope, &odd, &given, &PORTS).is_err(),
                "interface {} with an odd broadcast gave a plan",
                name
            );
        }

        let silent = UdpSocket::bind("127.0.0.6:0").await?;
        let discovery = Discovery::new().await?;
        let scope = DiscoveryScope::Interface("nosuch0".to_string());
        let result = discovery
            .local_with_interfaces(FAKE_UID, &[silent.local_addr()?], &scope, &host)
            .await;
        assert!(
            result.is_err(),
            "discovery with a missing interface went on"
        );
        assert_silent(&silent).await;
        assert_eq!(discovery.discoverer.refused_destinations(), 0);
        assert!(discovery.discoverer.sent_destinations().is_empty());

        // An interface with an odd broadcast fails discovery the same way,
        // with no packet handed to the socket.
        for name in [
            "limbr0",
            "zerobr0",
            "mcastbr0",
            "farbr0",
            "widelim0",
            "widezero0",
            "widemcast0",
        ] {
            let discovery = Discovery::new().await?;
            let scope = DiscoveryScope::Interface(name.to_string());
            let result = discovery
                .local_with_interfaces(FAKE_UID, &[silent.local_addr()?], &scope, &odd)
                .await;
            assert!(result.is_err(), "discovery on interface {} went on", name);
            assert_eq!(discovery.discoverer.refused_destinations(), 0);
            assert!(
                discovery.discoverer.sent_destinations().is_empty(),
                "discovery on interface {} sent a packet",
                name
            );
        }
        assert_silent(&silent).await;
        Ok(())
    }

    #[tokio::test]
    async fn guard_drops_an_unplanned_destination() -> Result<()> {
        let discoverer = Discoverer::new().await?;
        let planned = UdpSocket::bind("127.0.0.7:0").await?;
        let unplanned = UdpSocket::bind("127.0.0.3:0").await?;
        discoverer.limit_to(&[planned.local_addr()?]);

        discoverer
            .send(connect_request(FAKE_UID, 9), unplanned.local_addr()?)
            .await?;
        discoverer
            .send(connect_request(FAKE_UID, 10), planned.local_addr()?)
            .await?;

        let mut buf = [0u8; 2048];
        timeout(Duration::from_secs(5), planned.recv_from(&mut buf))
            .await
            .expect("the planned destination gets its packet")?;
        assert_silent(&unplanned).await;
        assert_eq!(discoverer.refused_destinations(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn targets_scope_finds_a_loopback_camera() -> Result<()> {
        let cameras = [
            loopback_camera("127.0.0.2:2015").await?,
            loopback_camera("127.0.0.2:2018").await?,
        ];
        let mut silent = vec![];
        for addr in [
            "127.0.0.3:2015",
            "127.0.0.3:2018",
            "127.0.0.4:2015",
            "127.0.0.4:2018",
        ] {
            silent.push(UdpSocket::bind(addr).await?);
        }

        let scope = DiscoveryScope::Targets;
        let discovery = Discovery::for_scope(&scope).await?;
        let given = [sa("127.0.0.2:2018"), sa("127.0.0.2:2015")];
        let [first, second] = cameras;
        let found = timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = discovery.local_in_scope(FAKE_UID, &given, &scope) => result,
                _ = futures::future::join(answer_connects(first, 1), answer_connects(second, 2)) => {
                    panic!("the cameras stopped answering")
                }
            }
        })
        .await
        .expect("discovery ends")?;
        assert_eq!(
            found.get_addr().ip(),
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))
        );
        assert!(PORTS.contains(&found.get_addr().port()));

        futures::future::join_all(silent.iter().map(assert_silent)).await;
        assert_eq!(discovery.discoverer.refused_destinations(), 0);

        // What the writer was allowed is exactly the two camera ports. A plan
        // that grew a broadcast fails here, even though the loopback sockets
        // above would never see one.
        let want: HashSet<SocketAddr> = [sa("127.0.0.2:2015"), sa("127.0.0.2:2018")]
            .iter()
            .copied()
            .collect();
        assert_eq!(
            discovery.discoverer.allowed_destinations(),
            Some(want.clone())
        );
        // What it sent holds the port that answered and nothing outside the
        // two. Discovery returns on the first answer, so the send to the
        // other port may not have happened yet.
        let sent = discovery.discoverer.sent_destinations();
        let sent_set: HashSet<SocketAddr> = sent.iter().copied().collect();
        assert!(sent_set.is_subset(&want), "the writer sent to {:?}", sent);
        assert!(
            sent_set.contains(&found.get_addr()),
            "the writer sent to {:?}",
            sent
        );
        Ok(())
    }

    /// A made-up UID, not in the shape of a real one, for the scope tests.
    const FAKE_UID: &str = "not-a-real-uid-5e1d";
    const LOG_CAMERA: &str = "lane-test-cam";
    const LOG_CHILD_ENV: &str = "NEOLINK_CORE_UID_LOG_CHILD";

    static LINES: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    static CAPTURE: Capture = Capture;

    /// Keeps every log record of the process it is installed in.
    struct Capture;

    impl log::Log for Capture {
        fn enabled(&self, _: &log::Metadata) -> bool {
            true
        }
        fn log(&self, record: &log::Record) {
            LINES
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(record.args().to_string());
        }
        fn flush(&self) {}
    }

    fn log_test_options(scope: DiscoveryScope) -> crate::bc_protocol::BcCameraOpt {
        crate::bc_protocol::BcCameraOpt {
            name: LOG_CAMERA.to_string(),
            channel_id: 0,
            addrs: vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 5))],
            uid: Some(FAKE_UID.to_string()),
            port: None,
            protocol: crate::bc_protocol::ConnectionProtocol::Udp,
            discovery: crate::bc_protocol::DiscoveryMethods::Local,
            discovery_scope: scope,
            max_discovery_retries: 1,
            credentials: crate::bc_protocol::Credentials {
                username: "admin".to_string(),
                password: None,
            },
            debug: false,
        }
    }

    /// The body of `no_uid_in_discovery_logs`. Other tests of this binary
    /// install their own logger, so it runs in a process of its own, where
    /// it is the only test and its logger is the only one.
    #[tokio::test]
    #[ignore = "run by no_uid_in_discovery_logs in a process of its own"]
    async fn uid_log_child() -> Result<()> {
        if std::env::var_os(LOG_CHILD_ENV).is_none() {
            return Ok(());
        }
        log::set_logger(&CAPTURE).expect("this test runs alone in its process");
        log::set_max_level(log::LevelFilter::Trace);

        let first = loopback_camera("127.0.0.5:2015").await?;
        let second = loopback_camera("127.0.0.5:2018").await?;
        let found_options = log_test_options(DiscoveryScope::Targets);
        let found = timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = crate::bc_protocol::BcCamera::find_camera(&found_options) => result,
                _ = futures::future::join(answer_connects(first, 1), answer_connects(second, 2)) => {
                    panic!("the camera stopped answering")
                }
            }
        })
        .await
        .expect("the found discovery ends");
        assert!(
            matches!(found, Ok(crate::bc_protocol::CameraLocation::Udp(_))),
            "the loopback camera was not found"
        );

        let failed_options = log_test_options(DiscoveryScope::Interface("nosuch0".to_string()));
        let failed = timeout(
            Duration::from_secs(10),
            crate::bc_protocol::BcCamera::find_camera(&failed_options),
        )
        .await
        .expect("the failed discovery ends");
        assert!(
            failed.is_err(),
            "discovery on a missing interface succeeded"
        );

        let lines = LINES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let naming = |text: &str| {
            lines
                .iter()
                .any(|line| line.contains(LOG_CAMERA) && line.contains(text))
        };
        assert!(
            naming("Local discovery success"),
            "no found line names the camera"
        );
        assert!(naming("failed"), "no failed line names the camera");
        // Each offending line is shown with the UID masked.
        let with_uid: Vec<String> = lines
            .iter()
            .filter(|line| line.contains(FAKE_UID))
            .map(|line| line.replace(FAKE_UID, "<UID>"))
            .collect();
        assert!(
            with_uid.is_empty(),
            "{} of {} log lines hold the UID:\n{}",
            with_uid.len(),
            lines.len(),
            with_uid.join("\n")
        );
        println!("uid_log_child: checked {} lines", lines.len());
        Ok(())
    }

    #[test]
    fn no_uid_in_discovery_logs() {
        let exe = std::env::current_exe().expect("the test binary");
        let out = std::process::Command::new(exe)
            .args([
                "bc_protocol::connection::discovery::tests::uid_log_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(LOG_CHILD_ENV, "1")
            .output()
            .expect("the child test runs");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success(),
            "the child test failed\n{}\n{}",
            stdout,
            stderr
        );
        assert!(
            stdout.contains("uid_log_child: checked"),
            "the child test did not run its checks\n{}",
            stdout
        );
    }
}
