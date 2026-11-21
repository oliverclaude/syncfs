//! Peer membership: one TCP connection per pair of nodes, heartbeats, peer
//! state tracking and reconnects.
//!
//! Threads, not async: with at most 98 peers a node runs one reader and one
//! writer thread per connection plus one dialer thread per higher-id peer
//! (about 250 threads in a 99-node full mesh), all blocking on sockets or
//! channels. The FUSE side is synchronous too, so nothing needs an async
//! runtime.
//!
//! - The lower node id dials, the higher accepts ([`Identity::dials`]).
//! - Both sides send a heartbeat every `heartbeat_interval`. Any frame
//!   counts as a sign of life; a connection that is silent for
//!   [`DOWN_AFTER_HEARTBEATS`] intervals is closed and the peer is `down`.
//! - The dialer reconnects with exponential backoff (from
//!   `heartbeat_interval / 2`, doubling, capped at 5 x `heartbeat_interval`
//!   but at least 1 s and at most 30 s, with +-20% jitter) and resets it
//!   after a successful handshake.
//! - A new connection from a peer replaces an existing one (the peer
//!   restarted before we noticed the old connection died).
//! - Two data nodes also open a bulk lane: a second connection, dialed the
//!   same way right after the main one is up, that carries journal entries,
//!   blocks and hash replies ([`Message::is_bulk`]). It belongs to the main
//!   connection and fails with it (either one closing closes both), so a
//!   session number covers both. Until it is up, bulk messages queue
//!   behind control messages on the main connection.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::json;

use super::handshake::{self, HandshakeError, Identity};
use super::wire::{self, Heartbeat, Lane, Message, Role, Welcome, MAX_FRAME_LEN};
use crate::config::{Config, LiveDuration, NodeId};
use crate::status::{FnProvider, Provider};

/// A peer silent for this many heartbeat intervals is considered down.
pub const DOWN_AFTER_HEARTBEATS: u32 = 3;
/// Stack size of network threads: they only shuffle frames, and a 99-node
/// mesh runs about 250 of them.
const NET_THREAD_STACK: usize = 256 << 10;
/// Time allowed for connecting and for the handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerState {
    /// Never connected since this node started.
    Unknown,
    /// First connection attempt in progress (dialing side only).
    Connecting,
    Up,
    /// Was up, or a connection attempt failed.
    Down,
    /// The handshake was refused because the configurations disagree.
    Rejected,
}

/// One peer as seen by this node.
#[derive(Clone, Debug, Serialize)]
pub struct PeerStatus {
    pub id: NodeId,
    pub address: String,
    #[serde(rename = "self")]
    pub is_self: bool,
    pub state: PeerState,
    /// Milliseconds since `state` last changed.
    pub state_age_ms: u64,
    /// Milliseconds since the last frame from the peer.
    pub last_seen_ms_ago: Option<u64>,
    /// `outbound` if this node dials the peer, else `inbound`.
    pub direction: &'static str,
    /// Successful handshakes so far.
    pub connects: u64,
    /// The peer's process incarnation (changes when it restarts).
    pub incarnation: Option<u64>,
    pub last_error: Option<String>,
    /// Nodes the peer reported up in its last heartbeat.
    pub sees: Vec<NodeId>,
    /// The peer's member ids, when they differ from ours (accepted, with a
    /// warning).
    pub members_mismatch: Option<Vec<NodeId>>,
    /// What the peer stores, from its last handshake.
    pub role: Option<Role>,
    /// The bulk lane to the peer is open.
    pub bulk_lane: bool,
}

/// Membership at one instant.
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub node_id: NodeId,
    pub cluster_size: usize,
    pub quorum: usize,
    /// Up peers plus this node.
    pub reachable: usize,
    pub have_quorum: bool,
    pub peers: Vec<PeerStatus>,
    /// Configuration problems worth an operator's attention.
    pub warnings: Vec<String>,
}

impl Snapshot {
    pub fn up_ids(&self) -> Vec<NodeId> {
        self.peers
            .iter()
            .filter(|p| p.is_self || p.state == PeerState::Up)
            .map(|p| p.id)
            .collect()
    }

    pub fn state_of(&self, id: NodeId) -> Option<PeerState> {
        self.peers.iter().find(|p| p.id == id).map(|p| p.state)
    }
}

/// To the writer thread: a control message, or "a bulk message is
/// queued". Control messages (heartbeats, locks) go out before queued bulk
/// ones (journal entries, blocks), so transfers cannot delay them much.
enum Out {
    Msg(Message),
    Bulk,
}

struct Conn {
    stream: TcpStream,
    tx: Sender<Out>,
    bulk: Arc<Mutex<std::collections::VecDeque<Message>>>,
}

struct PeerInfo {
    state: PeerState,
    since: Instant,
    last_seen: Option<Instant>,
    incarnation: Option<u64>,
    connects: u64,
    last_error: Option<String>,
    sees: Vec<NodeId>,
    members_mismatch: Option<Vec<NodeId>>,
    warning: Option<String>,
    role: Option<Role>,
    session: u64,
    conn: Option<Conn>,
    /// The bulk lane of the current session, and its number.
    lane: Option<Conn>,
    lane_id: u64,
    /// Why the bulk lane closed the session, if it did.
    lane_failed: Option<String>,
}

struct Slot {
    id: NodeId,
    address: String,
    info: Mutex<PeerInfo>,
}

struct Shared {
    me: Identity,
    self_address: String,
    /// `heartbeat_interval`; a reload can change it
    /// ([`Network::set_heartbeat`]).
    heartbeat: LiveDuration,
    quorum: usize,
    cluster_size: usize,
    /// Other members, sorted by id.
    slots: Vec<Slot>,
    stopping: AtomicBool,
    next_session: AtomicU64,
    /// Bumped on every state change.
    epoch: Mutex<u64>,
    changed: Condvar,
    /// Last time each refusal message was logged at warn level.
    logged: Mutex<HashMap<String, Instant>>,
    acceptor: Mutex<Option<std::thread::JoinHandle<()>>>,
    handlers: std::sync::RwLock<Vec<(Filter, Handler)>>,
}

/// Receives the messages its filter accepts (never handshake, heartbeat
/// and goodbye). Runs on the connection's reader thread, so it must not
/// block.
pub type Handler = Arc<dyn Fn(NodeId, Message) + Send + Sync>;
pub type Filter = fn(&Message) -> bool;

/// The running membership service. Cheap to clone.
#[derive(Clone)]
pub struct Network {
    shared: Arc<Shared>,
    listen: Option<SocketAddr>,
}

fn random_u64() -> u64 {
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Network {
    /// Binds `cfg.listen` and starts connecting to the peers. A standalone
    /// node (no peers) starts nothing.
    pub fn start(cfg: &Config) -> io::Result<Network> {
        Self::start_with_disk(cfg, 0)
    }

    /// Like [`start`](Self::start), announcing the backing directory's
    /// disk id to peers.
    pub fn start_with_disk(cfg: &Config, disk: u64) -> io::Result<Network> {
        let mut me = Identity::from_config(cfg, random_u64());
        me.role.disk = disk;
        let heartbeat = cfg.heartbeat_interval.get();
        let now = Instant::now();
        let slots = cfg
            .peers
            .iter()
            .filter(|p| p.id != cfg.node_id)
            .map(|p| Slot {
                id: p.id,
                address: p.address.clone(),
                info: Mutex::new(PeerInfo {
                    state: PeerState::Unknown,
                    since: now,
                    last_seen: None,
                    incarnation: None,
                    connects: 0,
                    last_error: None,
                    sees: Vec::new(),
                    members_mismatch: None,
                    warning: None,
                    role: None,
                    session: 0,
                    conn: None,
                    lane: None,
                    lane_id: 0,
                    lane_failed: None,
                }),
            })
            .collect();
        let self_address = cfg
            .peers
            .iter()
            .find(|p| p.id == cfg.node_id)
            .map_or_else(|| cfg.listen.to_string(), |p| p.address.clone());
        let shared = Arc::new(Shared {
            me,
            self_address,
            heartbeat: LiveDuration::new(heartbeat),
            quorum: cfg.quorum(),
            cluster_size: cfg.cluster_size(),
            slots,
            stopping: AtomicBool::new(false),
            next_session: AtomicU64::new(1),
            epoch: Mutex::new(0),
            changed: Condvar::new(),
            logged: Mutex::new(HashMap::new()),
            acceptor: Mutex::new(None),
            handlers: std::sync::RwLock::new(Vec::new()),
        });
        if shared.slots.is_empty() {
            return Ok(Network {
                shared,
                listen: None,
            });
        }
        let listener = TcpListener::bind(cfg.listen)?;
        let listen = listener.local_addr()?;
        log::info!(
            "node {} of cluster {:?} listening on {listen}; {} peers, quorum {}",
            shared.me.node_id,
            shared.me.cluster_name,
            shared.slots.len(),
            shared.quorum
        );
        let sh = Arc::clone(&shared);
        let acceptor = std::thread::Builder::new()
            .stack_size(NET_THREAD_STACK)
            .name("syncfs-accept".into())
            .spawn(move || accept_loop(sh, listener))?;
        *lock(&shared.acceptor) = Some(acceptor);
        for i in 0..shared.slots.len() {
            if shared.me.dials(shared.slots[i].id) {
                let sh = Arc::clone(&shared);
                std::thread::Builder::new()
                    .stack_size(NET_THREAD_STACK)
                    .name(format!("syncfs-dial-{}", shared.slots[i].id))
                    .spawn(move || dial_loop(sh, i))?;
            }
        }
        Ok(Network {
            shared,
            listen: Some(listen),
        })
    }

    /// Adds a receiver for the application messages `filter` accepts (the
    /// first matching handler gets each message). Messages nobody handles
    /// are dropped.
    pub fn add_handler(&self, filter: Filter, h: Handler) {
        self.shared
            .handlers
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .push((filter, h));
    }

    /// This node's role as announced to peers.
    pub fn role(&self) -> Role {
        self.shared.me.role
    }

    /// The members other than this node.
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.shared.slots.iter().map(|s| s.id).collect()
    }

    /// The bound listen address (`None` for a standalone node).
    pub fn listen_addr(&self) -> Option<SocketAddr> {
        self.listen
    }

    pub fn snapshot(&self) -> Snapshot {
        self.shared.snapshot()
    }

    /// Sends `msg` to peer `id` if it is connected. Bulk messages
    /// ([`Message::is_bulk`]) go on the bulk lane, or queue behind control
    /// messages while there is none.
    pub fn send(&self, id: NodeId, msg: Message) -> bool {
        self.shared
            .slots
            .iter()
            .find(|s| s.id == id)
            .and_then(|s| {
                let i = lock(&s.info);
                let main = i.conn.as_ref()?;
                Some(match (&i.lane, msg.is_bulk()) {
                    (Some(lane), true) => lane.tx.send(Out::Msg(msg)).is_ok(),
                    (None, true) => {
                        lock(&main.bulk).push_back(msg);
                        main.tx.send(Out::Bulk).is_ok()
                    }
                    (_, false) => main.tx.send(Out::Msg(msg)).is_ok(),
                })
            })
            .unwrap_or(false)
    }

    /// The current connection's session number for peer `id` (changes on
    /// every reconnect; 0 when not connected).
    pub fn session_of(&self, id: NodeId) -> u64 {
        self.shared
            .slots
            .iter()
            .find(|s| s.id == id)
            .map_or(0, |s| {
                let i = lock(&s.info);
                if i.conn.is_some() {
                    i.session
                } else {
                    0
                }
            })
    }

    /// Waits until `pred` holds for a snapshot or `timeout` passes.
    pub fn wait_until(&self, timeout: Duration, pred: impl Fn(&Snapshot) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        let mut epoch = lock(&self.shared.epoch);
        loop {
            drop(epoch);
            if pred(&self.snapshot()) {
                return true;
            }
            epoch = lock(&self.shared.epoch);
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            // Wake on changes, and at least every heartbeat (ages move).
            let wait = (deadline - now).min(self.shared.heartbeat.get());
            epoch = self
                .shared
                .changed
                .wait_timeout(epoch, wait)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    /// Waits for the next state change, up to `timeout`.
    pub fn wait_change(&self, timeout: Duration) {
        let epoch = lock(&self.shared.epoch);
        let _ = self.shared.changed.wait_timeout(epoch, timeout);
    }

    /// Says goodbye to every peer and stops reconnecting.
    /// The heartbeat interval in effect.
    pub fn heartbeat(&self) -> Duration {
        self.shared.heartbeat.get()
    }

    /// Applies a new `heartbeat_interval` from a configuration reload.
    /// Connections stay up: every writer sends its next heartbeat on the
    /// new schedule, and the read and write timeouts of open connections
    /// (the down detection) move to the new value at once.
    pub fn set_heartbeat(&self, heartbeat: Duration) {
        let sh = &self.shared;
        sh.heartbeat.set(heartbeat);
        let down_after = Some(sh.down_after());
        for slot in &sh.slots {
            let i = lock(&slot.info);
            for c in [&i.conn, &i.lane].into_iter().flatten() {
                // The reader and writer share the socket, and with it its
                // timeouts.
                let _ = c.stream.set_read_timeout(down_after);
                let _ = c.stream.set_write_timeout(down_after);
            }
        }
        sh.notify();
    }

    pub fn shutdown(&self) {
        let sh = &self.shared;
        if sh.stopping.swap(true, Ordering::SeqCst) {
            return;
        }
        for s in &sh.slots {
            if let Some(c) = &lock(&s.info).conn {
                let _ = c.tx.send(Out::Msg(Message::Goodbye {
                    reason: "shutting down".into(),
                }));
            }
        }
        // Give the writers a moment to flush the goodbyes.
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline && sh.slots.iter().any(|s| lock(&s.info).conn.is_some()) {
            std::thread::sleep(Duration::from_millis(10));
        }
        for s in &sh.slots {
            let mut i = lock(&s.info);
            for c in [i.conn.take(), i.lane.take()].into_iter().flatten() {
                let _ = c.stream.shutdown(Shutdown::Both);
            }
        }
        if let Some(addr) = self.listen {
            // Wake the accept loop so it sees `stopping`.
            let ip = if addr.ip().is_unspecified() {
                match addr {
                    SocketAddr::V4(_) => [127, 0, 0, 1].into(),
                    SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
                }
            } else {
                addr.ip()
            };
            let woke = TcpStream::connect_timeout(
                &SocketAddr::new(ip, addr.port()),
                Duration::from_secs(1),
            );
            // Wait for the listener to close, so the port can be reused.
            if let (Ok(_), Some(t)) = (woke, lock(&sh.acceptor).take()) {
                let _ = t.join();
            }
        }
        sh.notify();
    }

    /// The `.syncfs/peers` provider.
    pub fn provider(&self) -> Arc<dyn Provider> {
        let net = self.clone();
        Arc::new(FnProvider::new("peers", move || {
            let snap = net.snapshot();
            let sh = &net.shared;
            json!({
                "implemented": true,
                "cluster_name": sh.me.cluster_name,
                "node_id": sh.me.node_id,
                "protocol_version": wire::PROTOCOL_VERSION,
                "listen": net.listen.map(|a| a.to_string()),
                "heartbeat_ms": sh.heartbeat.get().as_millis() as u64,
                "down_after_ms": sh.down_after().as_millis() as u64,
                "cluster_size": snap.cluster_size,
                "quorum": snap.quorum,
                "reachable": snap.reachable,
                "have_quorum": snap.have_quorum,
                "up": snap.up_ids(),
                "warnings": snap.warnings,
                "peers": snap.peers,
            })
        }))
    }
}

impl Shared {
    /// Silence after which a peer counts as down (the read timeout of
    /// every connection).
    fn down_after(&self) -> Duration {
        self.heartbeat.get() * DOWN_AFTER_HEARTBEATS
    }

    fn notify(&self) {
        *lock(&self.epoch) += 1;
        self.changed.notify_all();
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn snapshot(&self) -> Snapshot {
        let now = Instant::now();
        let ms = |d: Duration| d.as_millis() as u64;
        let mut peers: Vec<PeerStatus> = self
            .slots
            .iter()
            .map(|s| {
                let i = lock(&s.info);
                PeerStatus {
                    id: s.id,
                    address: s.address.clone(),
                    is_self: false,
                    state: i.state,
                    state_age_ms: ms(now - i.since),
                    last_seen_ms_ago: i.last_seen.map(|t| ms(now - t)),
                    direction: if self.me.dials(s.id) {
                        "outbound"
                    } else {
                        "inbound"
                    },
                    connects: i.connects,
                    incarnation: i.incarnation,
                    last_error: i.last_error.clone(),
                    sees: i.sees.clone(),
                    members_mismatch: i.members_mismatch.clone(),
                    role: i.role,
                    bulk_lane: i.lane.is_some(),
                }
            })
            .collect();
        let warnings: Vec<String> = self
            .slots
            .iter()
            .filter_map(|s| lock(&s.info).warning.clone())
            .collect();
        let up = peers.iter().filter(|p| p.state == PeerState::Up).count();
        let mut sees: Vec<NodeId> = peers
            .iter()
            .filter(|p| p.state == PeerState::Up)
            .map(|p| p.id)
            .chain([self.me.node_id])
            .collect();
        sees.sort_unstable();
        peers.push(PeerStatus {
            id: self.me.node_id,
            address: self.self_address.clone(),
            is_self: true,
            state: PeerState::Up,
            state_age_ms: 0,
            last_seen_ms_ago: Some(0),
            direction: "self",
            connects: 0,
            incarnation: Some(self.me.incarnation),
            last_error: None,
            sees,
            members_mismatch: None,
            role: Some(self.me.role),
            bulk_lane: false,
        });
        peers.sort_by_key(|p| p.id);
        let reachable = up + 1;
        Snapshot {
            node_id: self.me.node_id,
            cluster_size: self.cluster_size,
            quorum: self.quorum,
            reachable,
            have_quorum: reachable >= self.quorum,
            peers,
            warnings,
        }
    }

    fn up_ids(&self) -> Vec<NodeId> {
        let mut up: Vec<NodeId> = self
            .slots
            .iter()
            .filter(|s| lock(&s.info).state == PeerState::Up)
            .map(|s| s.id)
            .collect();
        up.push(self.me.node_id);
        up.sort_unstable();
        up
    }

    fn set_state(&self, slot: &Slot, state: PeerState, error: Option<String>) {
        let mut i = lock(&slot.info);
        if i.state != state {
            i.state = state;
            i.since = Instant::now();
        }
        if error.is_some() {
            i.last_error = error;
        }
        drop(i);
        self.notify();
    }

    /// Logs at warn, or at debug when the same text was warned about in
    /// the last minute (a misconfigured peer retries forever).
    fn warn_limited(&self, text: String) {
        let mut l = lock(&self.logged);
        let now = Instant::now();
        let recent = l
            .get(&text)
            .is_some_and(|t| now.duration_since(*t) < Duration::from_secs(60));
        if recent {
            log::debug!("{text}");
        } else {
            log::warn!("{text}");
            l.insert(text, now);
        }
    }
}

fn accept_loop(sh: Arc<Shared>, listener: TcpListener) {
    for stream in listener.incoming() {
        if sh.stopping() {
            return;
        }
        let Ok(stream) = stream else { continue };
        let sh = Arc::clone(&sh);
        let _ = std::thread::Builder::new()
            .stack_size(NET_THREAD_STACK)
            .name("syncfs-conn".into())
            .spawn(move || accept_one(sh, stream));
    }
}

fn accept_one(sh: Arc<Shared>, mut stream: TcpStream) {
    let peer_addr = stream
        .peer_addr()
        .map_or_else(|_| "?".into(), |a| a.to_string());
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT));
    let _ = stream.set_write_timeout(Some(HANDSHAKE_TIMEOUT));
    match handshake::accept(&mut stream, &sh.me) {
        Ok(hello) => {
            if sh.stopping() {
                return;
            }
            let Some(slot) = sh.slots.iter().find(|s| s.id == hello.node_id) else {
                return; // check_hello guarantees membership
            };
            if hello.lane == Lane::Bulk {
                accept_lane(&sh, slot, stream, hello.incarnation);
                return;
            }
            run_session(
                &sh,
                slot,
                stream,
                hello.incarnation,
                &hello.members,
                hello.role,
            );
        }
        Err(HandshakeError::Refused(r)) => {
            sh.warn_limited(format!(
                "refused connection from {peer_addr} ({:?}): {}",
                r.reason, r.detail
            ));
        }
        Err(HandshakeError::Wire(e)) if e.is_eof() => {} // e.g. a port probe
        Err(e) => sh.warn_limited(format!("handshake with {peer_addr} failed: {e}")),
    }
}

fn backoff_bounds(heartbeat: Duration) -> (Duration, Duration) {
    let cap = (heartbeat * 5).clamp(Duration::from_secs(1), Duration::from_secs(30));
    let initial = (heartbeat / 2).clamp(Duration::from_millis(10), cap);
    (initial, cap)
}

fn connect(sh: &Shared, slot: &Slot) -> Result<(TcpStream, Welcome), HandshakeError> {
    connect_lane(sh, slot, Lane::Main)
}

fn connect_lane(
    sh: &Shared,
    slot: &Slot,
    lane: Lane,
) -> Result<(TcpStream, Welcome), HandshakeError> {
    let addrs: Vec<SocketAddr> = slot.address.to_socket_addrs()?.collect();
    let mut last = io::Error::new(io::ErrorKind::NotFound, "address resolved to nothing");
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, HANDSHAKE_TIMEOUT) {
            Ok(mut s) => {
                let _ = s.set_nodelay(true);
                s.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
                s.set_write_timeout(Some(HANDSHAKE_TIMEOUT))?;
                let w = handshake::dial_lane(&mut s, &sh.me, slot.id, lane)?;
                return Ok((s, w));
            }
            Err(e) => last = e,
        }
    }
    Err(last.into())
}

fn dial_loop(sh: Arc<Shared>, index: usize) {
    let slot = &sh.slots[index];
    let mut backoff = backoff_bounds(sh.heartbeat.get()).0;
    let mut rng = random_u64() | 1;
    while !sh.stopping() {
        // Recomputed every round: a reload may change the heartbeat.
        let (initial, cap) = backoff_bounds(sh.heartbeat.get());
        // `connecting` until the first attempt ends; afterwards retries keep
        // the down/rejected state (and its age) until one succeeds.
        if lock(&slot.info).state == PeerState::Unknown {
            sh.set_state(slot, PeerState::Connecting, None);
        }
        match connect(&sh, slot) {
            Ok((stream, w)) => {
                backoff = initial;
                run_session(&sh, slot, stream, w.incarnation, &w.members, w.role);
            }
            Err(e) => {
                let state = match e {
                    HandshakeError::Rejected(_)
                    | HandshakeError::Version { .. }
                    | HandshakeError::WrongNode { .. } => {
                        sh.warn_limited(format!("node {} at {}: {e}", slot.id, slot.address));
                        PeerState::Rejected
                    }
                    _ => {
                        log::debug!("node {} at {}: {e}", slot.id, slot.address);
                        PeerState::Down
                    }
                };
                sh.set_state(slot, state, Some(e.to_string()));
            }
        }
        if sh.stopping() {
            break;
        }
        // xorshift for +-20% jitter.
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let jitter = 0.8 + (rng % 1000) as f64 / 2500.0;
        let wait = backoff.mul_f64(jitter);
        let epoch = lock(&sh.epoch);
        // Sleep, but wake early on shutdown.
        let deadline = Instant::now() + wait;
        let mut epoch = epoch;
        while !sh.stopping() && Instant::now() < deadline {
            epoch = sh
                .changed
                .wait_timeout(epoch, deadline - Instant::now())
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        drop(epoch);
        backoff = (backoff * 2).min(cap);
    }
}

/// Runs an established connection until it fails or the peer leaves.
fn run_session(
    sh: &Arc<Shared>,
    slot: &Slot,
    stream: TcpStream,
    incarnation: u64,
    members: &[NodeId],
    role: Role,
) {
    let warning = sh.me.membership_warning(slot.id, members);
    if let Some(w) = &warning {
        sh.warn_limited(w.clone());
    }
    let session = sh.next_session.fetch_add(1, Ordering::Relaxed);
    let (Ok(writer), Ok(handle)) = (stream.try_clone(), stream.try_clone()) else {
        return;
    };
    let _ = stream.set_read_timeout(Some(sh.down_after()));
    let _ = writer.set_write_timeout(Some(sh.down_after()));
    let (tx, rx) = mpsc::channel();
    let bulk = Arc::new(Mutex::new(std::collections::VecDeque::new()));
    let wbulk = Arc::clone(&bulk);
    {
        let mut i = lock(&slot.info);
        if let Some(old) = i.conn.take() {
            let _ = old.stream.shutdown(Shutdown::Both);
        }
        if i.incarnation.is_some_and(|old| old != incarnation) {
            log::info!("node {} restarted", slot.id);
        }
        i.state = PeerState::Up;
        i.since = Instant::now();
        i.last_seen = Some(Instant::now());
        i.incarnation = Some(incarnation);
        i.connects += 1;
        i.last_error = None;
        i.members_mismatch = warning.as_ref().map(|_| {
            let mut m = members.to_vec();
            m.sort_unstable();
            m.dedup();
            m
        });
        i.warning = warning;
        i.role = Some(role);
        i.session = session;
        i.conn = Some(Conn {
            stream: handle,
            tx,
            bulk: Arc::clone(&bulk),
        });
        if let Some(old) = i.lane.take() {
            let _ = old.stream.shutdown(Shutdown::Both);
        }
        i.lane_failed = None;
    }
    sh.notify();
    log::info!("node {} at {} is up", slot.id, slot.address);
    if sh.me.dials(slot.id) && sh.me.role.data && role.data {
        let sh2 = Arc::clone(sh);
        let id = slot.id;
        let _ = std::thread::Builder::new()
            .stack_size(NET_THREAD_STACK)
            .name(format!("syncfs-lane-{id}"))
            .spawn(move || dial_lane(&sh2, id, session, incarnation));
    }

    let wsh = Arc::clone(sh);
    let spawned = std::thread::Builder::new()
        .stack_size(NET_THREAD_STACK)
        .name(format!("syncfs-tx-{}", slot.id))
        .spawn(move || write_loop(&wsh, writer, &rx, &wbulk));
    let reason = if spawned.is_err() {
        "cannot start writer thread".to_string()
    } else {
        read_loop(sh, slot, &stream)
    };
    let _ = stream.shutdown(Shutdown::Both);
    let mut i = lock(&slot.info);
    if i.session == session {
        i.conn = None;
        if let Some(lane) = i.lane.take() {
            let _ = lane.stream.shutdown(Shutdown::Both);
        }
        let reason = i.lane_failed.take().unwrap_or(reason);
        i.state = PeerState::Down;
        i.since = Instant::now();
        i.last_error = Some(reason.clone());
        drop(i);
        sh.notify();
        log::info!("node {} is down: {reason}", slot.id);
    }
}

/// Dialer side of the bulk lane of main session `session` with `id` (whose
/// process is `incarnation`). A few attempts; without a lane, bulk
/// messages keep using the main connection.
fn dial_lane(sh: &Arc<Shared>, id: NodeId, session: u64, incarnation: u64) {
    let Some(slot) = sh.slots.iter().find(|s| s.id == id) else {
        return;
    };
    let current = || lock(&slot.info).session == session && !sh.stopping();
    for attempt in 0..5u32 {
        if !current() {
            return;
        }
        match connect_lane(sh, slot, Lane::Bulk) {
            Ok((stream, w)) if w.incarnation == incarnation => {
                run_lane(sh, slot, stream, session);
                return;
            }
            Ok(_) => return, // the peer restarted: a new session follows
            Err(e) => log::debug!("node {id}: bulk lane attempt {attempt}: {e}"),
        }
        std::thread::sleep(Duration::from_millis(100 << attempt));
    }
    log::warn!("node {id}: no bulk lane; transfers share the main connection");
}

/// Acceptor side: attaches a bulk lane from the process `incarnation` to
/// its main connection (which may still be registering).
fn accept_lane(sh: &Arc<Shared>, slot: &Slot, stream: TcpStream, incarnation: u64) {
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    loop {
        let session = {
            let i = lock(&slot.info);
            (i.conn.is_some() && i.incarnation == Some(incarnation)).then_some(i.session)
        };
        if let Some(session) = session {
            run_lane(sh, slot, stream, session);
            return;
        }
        if Instant::now() >= deadline || sh.stopping() {
            log::debug!("node {}: bulk lane without a main connection", slot.id);
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Runs the bulk lane of main session `session` until either fails.
fn run_lane(sh: &Arc<Shared>, slot: &Slot, stream: TcpStream, session: u64) {
    let (Ok(writer), Ok(handle)) = (stream.try_clone(), stream.try_clone()) else {
        return;
    };
    let _ = stream.set_read_timeout(Some(sh.down_after()));
    let _ = writer.set_write_timeout(Some(sh.down_after()));
    let (tx, rx) = mpsc::channel();
    let bulk = Arc::new(Mutex::new(std::collections::VecDeque::new()));
    let lane_id = sh.next_session.fetch_add(1, Ordering::Relaxed);
    {
        let mut i = lock(&slot.info);
        if i.session != session || i.conn.is_none() {
            return;
        }
        if let Some(old) = i.lane.take() {
            let _ = old.stream.shutdown(Shutdown::Both);
        }
        i.lane = Some(Conn {
            stream: handle,
            tx,
            bulk: Arc::clone(&bulk),
        });
        i.lane_id = lane_id;
    }
    sh.notify();
    log::debug!("node {}: bulk lane up", slot.id);
    let wsh = Arc::clone(sh);
    let spawned = std::thread::Builder::new()
        .stack_size(NET_THREAD_STACK)
        .name(format!("syncfs-ltx-{}", slot.id))
        .spawn(move || write_loop(&wsh, writer, &rx, &bulk));
    let reason = if spawned.is_err() {
        "cannot start bulk lane writer".to_string()
    } else {
        read_loop(sh, slot, &stream)
    };
    let _ = stream.shutdown(Shutdown::Both);
    let mut i = lock(&slot.info);
    if i.lane_id == lane_id && i.lane.is_some() {
        i.lane = None;
        // The main connection goes too: what was in flight on the lane is
        // lost, and a new session tells everyone to resend.
        if i.session == session {
            if i.conn.is_some() {
                i.lane_failed = Some(format!("bulk lane: {reason}"));
            }
            if let Some(c) = &i.conn {
                let _ = c.stream.shutdown(Shutdown::Both);
            }
        }
        drop(i);
        sh.notify();
    }
}

fn read_loop(sh: &Shared, slot: &Slot, mut stream: &TcpStream) -> String {
    loop {
        let msg = match wire::read_frame(&mut stream, MAX_FRAME_LEN) {
            Ok(m) => m,
            Err(e) if e.is_eof() => return "connection closed".into(),
            Err(wire::WireError::Io(e))
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                return "heartbeat timeout".into()
            }
            Err(e) => return e.to_string(),
        };
        let mut i = lock(&slot.info);
        i.last_seen = Some(Instant::now());
        match msg {
            Message::Heartbeat(h) => i.sees = h.up,
            Message::Goodbye { reason } => return format!("left: {reason}"),
            m @ (Message::Hello(_) | Message::Welcome(_) | Message::Reject(_)) => {
                return format!("protocol error: {} after handshake", m.kind())
            }
            other => {
                drop(i);
                let h = sh
                    .handlers
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .iter()
                    .find(|(f, _)| f(&other))
                    .map(|(_, h)| Arc::clone(h));
                match h {
                    Some(h) => h(slot.id, other),
                    None => log::debug!("node {}: dropping {} (no handler)", slot.id, other.kind()),
                }
            }
        }
    }
}

fn write_loop(
    sh: &Shared,
    mut stream: TcpStream,
    rx: &mpsc::Receiver<Out>,
    bulk: &Mutex<std::collections::VecDeque<Message>>,
) {
    let mut seq = 0u64;
    let mut next_beat = Instant::now();
    // Bulk messages announced but not sent yet.
    let mut queued = 0usize;
    loop {
        let now = Instant::now();
        let out = if now >= next_beat {
            next_beat = now + sh.heartbeat.get();
            seq += 1;
            Out::Msg(Message::Heartbeat(Heartbeat {
                seq,
                up: sh.up_ids(),
            }))
        } else if queued > 0 {
            // Control messages first, then one bulk message.
            match rx.try_recv() {
                Ok(o) => o,
                Err(mpsc::TryRecvError::Disconnected) => break,
                Err(mpsc::TryRecvError::Empty) => {
                    queued -= 1;
                    match lock(bulk).pop_front() {
                        Some(m) => Out::Msg(m),
                        None => continue,
                    }
                }
            }
        } else {
            match rx.recv_timeout(next_beat - now) {
                Ok(o) => o,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        };
        let msg = match out {
            Out::Bulk => {
                queued += 1;
                continue;
            }
            Out::Msg(m) => m,
        };
        let bye = matches!(msg, Message::Goodbye { .. });
        if wire::write_frame(&mut stream, &msg).is_err() || bye {
            break;
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_bounded() {
        let (i, c) = backoff_bounds(Duration::from_secs(1));
        assert_eq!((i, c), (Duration::from_millis(500), Duration::from_secs(5)));
        let (i, c) = backoff_bounds(Duration::from_millis(100));
        assert_eq!((i, c), (Duration::from_millis(50), Duration::from_secs(1)));
        let (_, c) = backoff_bounds(Duration::from_secs(60));
        assert_eq!(c, Duration::from_secs(30));
    }
}
