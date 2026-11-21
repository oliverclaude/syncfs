//! The lock manager: this node's voter table plus the locks this node
//! holds, driven by network messages. One per node, in mount and daemon
//! mode alike (a daemon only votes).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use fuser::Errno;
use serde_json::json;

use crate::config::{Config, LiveDuration, NodeId};
use crate::net::wire::{Ballot, DenyReason, FileId, Message, Outcome, Version, VersionInfo};
use crate::net::{Network, PeerState};
use crate::repl::version::{self, VersionTable};
use crate::status::{FnProvider, Provider};

use super::attempt::{Attempt, Phase, Step};
use super::audit::{self, Event};
use super::handoff::Handoff;
use super::key::Key;
use super::voter::{Reply, Voter};

/// Timing and membership, derived from the config. Clones share the
/// timings, which a reload can change (see [`LockManager::set_timings`]).
#[derive(Clone, Debug)]
pub struct Settings {
    pub me: NodeId,
    /// Every node, voters included, sorted.
    pub members: Vec<NodeId>,
    timing: Arc<Timing>,
    /// This node's disk id, stamped into the versions it makes.
    pub disk: u64,
}

#[derive(Debug)]
struct Timing {
    lease: LiveDuration,
    wait: LiveDuration,
    heartbeat: LiveDuration,
}

/// How long before its lease could end anywhere a holder of a `lease`
/// stops relying on it: covers clock rate differences and a write that
/// started just before the deadline.
pub fn margin_for(lease: Duration) -> Duration {
    (lease / 10).max(Duration::from_millis(50))
}

impl Settings {
    pub fn from_config(cfg: &Config) -> Settings {
        let mut members: Vec<NodeId> = cfg.peers.iter().map(|p| p.id).collect();
        if members.is_empty() {
            members.push(cfg.node_id);
        }
        members.sort_unstable();
        Settings {
            me: cfg.node_id,
            members,
            timing: Arc::new(Timing {
                lease: LiveDuration::new(cfg.lock_lease_timeout.get()),
                wait: LiveDuration::new(cfg.lock_wait.get()),
                heartbeat: LiveDuration::new(cfg.heartbeat_interval.get()),
            }),
            disk: 0,
        }
    }

    /// `lock_lease_timeout`: the lease this node asks for from now on.
    pub fn lease(&self) -> Duration {
        self.timing.lease.get()
    }

    /// `lock_wait`.
    pub fn wait(&self) -> Duration {
        self.timing.wait.get()
    }

    /// `heartbeat_interval`.
    pub fn heartbeat(&self) -> Duration {
        self.timing.heartbeat.get()
    }

    pub fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }

    /// A holder stops writing this long before its lease could end
    /// anywhere: covers clock rate differences and a write that started
    /// just before the deadline.
    pub fn margin(&self) -> Duration {
        margin_for(self.lease())
    }

    /// How long one attempt waits for votes.
    pub fn request_timeout(&self) -> Duration {
        (self.heartbeat() * 2).clamp(Duration::from_millis(200), Duration::from_secs(2))
    }

    pub fn provisional_ttl(&self) -> Duration {
        self.request_timeout() * 4
    }

    pub fn renew_every(&self) -> Duration {
        self.lease() / 3
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LockError {
    #[error("no quorum")]
    NoQuorum,
    #[error("locked by node {0:?}")]
    Busy(Option<NodeId>),
    #[error("shutting down")]
    Shutdown,
    /// The previous holders' changes were not applied here within
    /// `freshness_timeout` (see [`super::handoff`]).
    #[error("the previous holder's changes did not arrive in time")]
    HandoffTimeout,
}

impl LockError {
    pub fn errno(self) -> Errno {
        match self {
            LockError::NoQuorum | LockError::Shutdown => Errno::EROFS,
            LockError::Busy(_) | LockError::HandoffTimeout => Errno::EBUSY,
        }
    }
}

struct Renewal {
    req: u64,
    sent: Instant,
    /// The lease asked for in this round: voters extend by this much, so
    /// the holder must not count on more, even if a reload changed
    /// `lock_lease_timeout` since.
    lease: Duration,
    acks: BTreeSet<NodeId>,
}

/// The file a lock is for, so that it takes part in versioning.
#[derive(Clone, Copy, Debug)]
pub struct FileCtx {
    pub file: FileId,
    /// The local copy's version.
    pub local: Option<Version>,
}

/// What acquiring a lock on a file found out.
#[derive(Clone, Debug, Default)]
pub struct Acquired {
    /// The granting voters' entries for the file.
    pub known: Vec<VersionInfo>,
    /// The version this session writes (announced to the voters unless
    /// the lock was already held without a file).
    pub version: Option<Version>,
    /// A new lock session (as opposed to another reference to a held
    /// lock): the caller must check freshness, then call
    /// [`LockManager::ready`] (other references wait until then).
    pub new_session: bool,
}

struct Held {
    ballot: Ballot,
    file: Option<FileId>,
    version: Option<Version>,
    written: bool,
    known: Vec<VersionInfo>,
    /// The attempt that acquired it: grants arriving after the quorum get
    /// a confirm too, so every reachable voter carries the lease.
    req: u64,
    refs: u32,
    valid_until: Instant,
    since: Instant,
    renewal: Option<Renewal>,
    lapsed_logged: bool,
    /// The session is set up (the journal handoff applied and, for a new
    /// file session, the caller's freshness check done; see
    /// [`LockManager::ready`]). Other references wait for it, so that
    /// nothing on this node writes to a copy that is not caught up yet.
    ready: bool,
}

struct Pending {
    key: Key,
    attempt: Attempt,
    confirm_sent: Option<Instant>,
    /// The lease asked for in every confirm of this attempt (fixed when it
    /// starts, so a reload cannot change it halfway).
    lease: Duration,
    file: Option<FileCtx>,
    /// Versions reported by granting voters.
    known: Vec<VersionInfo>,
    /// Journal heads to apply first, from granting voters (see
    /// [`super::handoff`]).
    after: BTreeMap<NodeId, u64>,
    /// Version announced with the confirms; preset when re-acquiring.
    announce: Option<Version>,
}

#[derive(Default, serde::Serialize)]
struct Counters {
    acquired: u64,
    busy: u64,
    no_quorum: u64,
    /// Acquired, but given back: the previous holders' changes did not
    /// arrive within `freshness_timeout` (`EBUSY`).
    handoff_refused: u64,
    retries: u64,
    renewed: u64,
    lapsed: u64,
    votes_granted: u64,
    votes_denied: u64,
    preempted: u64,
}

struct State {
    clock: u64,
    next_req: u64,
    voter: Voter,
    pending: HashMap<u64, Pending>,
    held: BTreeMap<Key, Held>,
    /// Keys a local thread is acquiring right now; others wait for it.
    acquiring: HashSet<Key>,
    counters: Counters,
    /// Since this process started: acquired a lock / granted a vote.
    acquired_since_boot: bool,
    granted_since_boot: bool,
    /// Cold-start probe: current round and the nodes that answered fresh.
    fresh_round: u64,
    fresh_acks: BTreeSet<NodeId>,
    grace_skipped: bool,
    handoff: Handoff,
}

impl State {
    /// Whether this node can be relying on a lease that `asker` granted
    /// before it restarted: it has acquired a lock since it started, or an
    /// attempt in progress already counts a vote from `asker` (which,
    /// being in its start-up grace, cannot have voted since restarting).
    fn fresh_for(&self, asker: NodeId) -> bool {
        !self.acquired_since_boot
            && !self
                .pending
                .values()
                .any(|p| p.attempt.granted().contains(&asker))
    }

    fn observe(&mut self, b: Ballot) {
        self.clock = self.clock.max(b.counter);
    }

    fn req(&mut self) -> u64 {
        self.next_req += 1;
        self.next_req
    }
}

struct Inner {
    s: Settings,
    net: Network,
    versions: Arc<Mutex<VersionTable>>,
    epoch: Instant,
    st: Mutex<State>,
    cv: Condvar,
    stop: AtomicBool,
    /// Set by a reload: the renewer recomputes its schedule.
    retime: AtomicBool,
    renewer: Mutex<Option<std::thread::JoinHandle<()>>>,
    chain: std::sync::OnceLock<Chain>,
}

/// How a node takes part in lock handoff (see [`super::handoff`]): its
/// journal head, sent with every release, and a wait until it has applied
/// other nodes' journals up to given heads.
pub struct Chain {
    pub head: Box<dyn Fn() -> u64 + Send + Sync>,
    pub wait: Box<WaitFn>,
}

/// Waits until this node applied each `(origin, head)`'s journal up to it.
/// False: they did not arrive within `freshness_timeout`; the lock must
/// not be used.
pub type WaitFn = dyn Fn(&[(NodeId, u64)]) -> bool + Send + Sync;

/// (from, to, message); `to == me` is handled in-process.
type Queue = VecDeque<(NodeId, NodeId, Message)>;

#[derive(Clone)]
pub struct LockManager {
    inner: Arc<Inner>,
}

impl LockManager {
    /// Starts voting (and, in mount mode, serving local acquisitions) on
    /// `net`.
    pub fn start(s: Settings, net: Network) -> LockManager {
        let versions = Arc::new(Mutex::new(VersionTable::new(s.quorum())));
        Self::start_with_versions(s, net, versions)
    }

    /// [`start`](Self::start) with a (persisted) version table.
    pub fn start_with_versions(
        s: Settings,
        net: Network,
        versions: Arc<Mutex<VersionTable>>,
    ) -> LockManager {
        let epoch = Instant::now();
        let lease_ms = s.lease().as_millis() as u64;
        let voter = Voter::new(
            // Grants nothing for one lease after start: see the README.
            lease_ms,
            s.provisional_ttl().as_millis() as u64,
        );
        let inner = Arc::new(Inner {
            s,
            net: net.clone(),
            versions,
            epoch,
            st: Mutex::new(State {
                clock: 0,
                next_req: 0,
                voter,
                pending: HashMap::new(),
                held: BTreeMap::new(),
                acquiring: HashSet::new(),
                counters: Counters::default(),
                acquired_since_boot: false,
                granted_since_boot: false,
                fresh_round: 0,
                fresh_acks: BTreeSet::new(),
                grace_skipped: false,
                handoff: Handoff::default(),
            }),
            cv: Condvar::new(),
            stop: AtomicBool::new(false),
            retime: AtomicBool::new(false),
            renewer: Mutex::new(None),
            chain: std::sync::OnceLock::new(),
        });
        let weak: Weak<Inner> = Arc::downgrade(&inner);
        net.add_handler(
            Message::is_lock,
            Arc::new(move |from, msg| {
                if let Some(inner) = weak.upgrade() {
                    inner.receive(from, msg);
                }
            }),
        );
        let weak = Arc::downgrade(&inner);
        let h = std::thread::Builder::new()
            .name("syncfs-locks".into())
            .stack_size(256 << 10)
            .spawn(move || renew_loop(weak))
            .expect("spawning the lock renewer");
        *lock(&inner.renewer) = Some(h);
        log::info!(
            "locks: {} members, quorum {}, lease {:?}, granting from {:?} after start",
            inner.s.members.len(),
            inner.s.quorum(),
            inner.s.lease(),
            inner.s.lease()
        );
        LockManager { inner }
    }

    pub fn settings(&self) -> &Settings {
        &self.inner.s
    }

    /// Applies new timings from a configuration reload. Nothing in flight
    /// is disturbed: held leases keep their deadlines, attempts and
    /// renewal rounds already sent keep the lease they asked for, and the
    /// next request, renewal and vote use the new values.
    pub fn set_timings(&self, lease: Duration, wait: Duration, heartbeat: Duration) {
        let inner = &self.inner;
        inner.s.timing.lease.set(lease);
        inner.s.timing.wait.set(wait);
        inner.s.timing.heartbeat.set(heartbeat);
        let mut st = lock(&inner.st);
        st.voter
            .set_provisional_ttl(inner.s.provisional_ttl().as_millis() as u64);
        inner.retime.store(true, Ordering::SeqCst);
        drop(st);
        inner.cv.notify_all();
    }

    /// Turns on lock handoff (mount nodes, once replication runs).
    pub fn set_chain(&self, chain: Chain) {
        let _ = self.inner.chain.set(chain);
    }

    /// Takes (another reference to) the write lock on `key`, waiting up to
    /// `lock_wait` while another node holds a conflicting lock.
    pub fn acquire(&self, key: &Key) -> Result<(), LockError> {
        self.inner.acquire(key, true, false, None).map(|_| ())
    }

    /// [`acquire`](Self::acquire) for a file that takes part in versioning:
    /// a new session announces the version it will write.
    pub fn acquire_file(
        &self,
        key: &Key,
        file: Option<FileCtx>,
        fail_on_held: bool,
    ) -> Result<Acquired, LockError> {
        self.inner.acquire(key, true, fail_on_held, file)
    }

    /// The new session on `key` is set up (its freshness check is done):
    /// other references to it no longer wait.
    pub fn ready(&self, key: &Key) {
        let mut st = lock(&self.inner.st);
        if let Some(h) = st.held.get_mut(key) {
            h.ready = true;
        }
        drop(st);
        self.inner.cv.notify_all();
    }

    /// The session on `key` changed its file: its version is written.
    pub fn mark_written(&self, key: &Key) -> Option<(FileId, Version)> {
        let mut st = lock(&self.inner.st);
        let h = st.held.get_mut(key)?;
        h.written = true;
        Some((h.file?, h.version?))
    }

    /// The version the session on `key` writes.
    pub fn session(&self, key: &Key) -> Option<(FileId, Version)> {
        let st = lock(&self.inner.st);
        let h = st.held.get(key)?;
        Some((h.file?, h.version?))
    }

    /// The file of the session on `key` if only one reference is left
    /// (the next [`release`](Self::release) ends the session).
    pub fn last_reference(&self, key: &Key) -> Option<FileId> {
        let st = lock(&self.inner.st);
        let h = st.held.get(key)?;
        (h.refs <= 1).then_some(h.file).flatten()
    }

    /// After a rename, the session held under `from` continues under `to`
    /// (both held).
    pub fn move_session(&self, from: &Key, to: &Key) {
        let mut st = lock(&self.inner.st);
        let Some(old) = st.held.get_mut(from) else {
            return;
        };
        let (file, version, written) = (old.file, old.version, old.written);
        if file.is_none() {
            return;
        }
        let Some(new) = st.held.get_mut(to) else {
            return;
        };
        if new.file.is_some() {
            return;
        }
        new.file = file;
        new.version = version;
        new.written = written;
        let old = st.held.get_mut(from).expect("checked");
        old.file = None;
        old.version = None;
        old.written = false;
    }

    /// This node's voter table of file versions.
    pub fn versions(&self) -> Arc<Mutex<VersionTable>> {
        Arc::clone(&self.inner.versions)
    }

    /// Sends `msg` to every member (this node included).
    pub fn broadcast_versions(&self, msg: Message) {
        let mut st = lock(&self.inner.st);
        let mut q = Queue::new();
        self.inner.broadcast(&mut q, |_| msg.clone());
        let out = self.inner.pump(&mut st, q);
        drop(st);
        self.inner.send(out);
    }

    /// Like [`acquire`](Self::acquire), but fails with `Busy` at once,
    /// without waiting, if another node holds a conflicting lease (used by
    /// rename). Transient conflicts (competing attempts) are still retried
    /// within `lock_wait`.
    pub fn acquire_unless_held(&self, key: &Key) -> Result<(), LockError> {
        self.inner.acquire(key, true, true, None).map(|_| ())
    }

    /// Ok if this node holds `key` with a valid lease, re-acquiring it
    /// (without taking a reference) if it lapsed or is not held.
    pub fn ensure(&self, key: &Key) -> Result<(), LockError> {
        self.inner.acquire(key, false, false, None).map(|_| ())
    }

    /// Drops one reference; the last one releases the lock everywhere.
    pub fn release(&self, key: &Key) {
        if self.release_ref(key).is_some() {
            self.finish_release(key);
        }
    }

    /// Drops one reference. For the last one, returns the session's file
    /// (if any): the lock then stays held, with other references waiting
    /// (as for a session being set up), until
    /// [`finish_release`](Self::finish_release), so the caller can end
    /// the session first without another reference joining it.
    pub fn release_ref(&self, key: &Key) -> Option<Option<FileId>> {
        let mut st = lock(&self.inner.st);
        let Some(h) = st.held.get_mut(key) else {
            log::warn!("locks: releasing {key}, which is not held");
            return None;
        };
        h.refs = h.refs.saturating_sub(1);
        if h.refs > 0 {
            return None;
        }
        h.ready = false;
        Some(h.file)
    }

    /// Releases the lock everywhere after the last
    /// [`release_ref`](Self::release_ref) (unless it was taken again).
    pub fn finish_release(&self, key: &Key) {
        self.inner.release(key);
    }

    /// Whether this node holds `key` with a valid lease.
    pub fn is_valid(&self, key: &Key) -> bool {
        let st = lock(&self.inner.st);
        st.held
            .get(key)
            .is_some_and(|h| h.valid_until > Instant::now())
    }

    /// Releases everything and stops renewing.
    pub fn shutdown(&self) {
        let inner = &self.inner;
        inner.stop.store(true, Ordering::SeqCst);
        inner.cv.notify_all();
        if let Some(h) = lock(&inner.renewer).take() {
            let _ = h.join();
        }
        let head = inner.head();
        let mut st = lock(&inner.st);
        let keys: Vec<(Key, Ballot, Option<Outcome>)> = st
            .held
            .iter()
            .map(|(k, h)| (k.clone(), h.ballot, h.outcome()))
            .collect();
        st.held.clear();
        let mut q = Queue::new();
        for (k, b, outcome) in keys {
            audit::record(Event::End, inner.s.me, &k, b, None);
            inner.broadcast(&mut q, |_| Message::LockRelease {
                key: k.as_bytes().to_vec(),
                ballot: b,
                outcome,
                head,
            });
        }
        let out = inner.pump(&mut st, q);
        drop(st);
        inner.send(out);
    }

    pub fn provider(&self) -> Arc<dyn Provider> {
        let weak = Arc::downgrade(&self.inner);
        Arc::new(FnProvider::new("locks", move || match weak.upgrade() {
            Some(inner) => inner.render(),
            None => json!({"implemented": true, "running": false}),
        }))
    }
}

impl Held {
    fn outcome(&self) -> Option<Outcome> {
        Some(Outcome {
            file: self.file?,
            version: self.version?,
            written: self.written,
        })
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn ms(d: Duration) -> u64 {
    d.as_millis() as u64
}

impl Inner {
    fn now_ms(&self) -> u64 {
        ms(self.epoch.elapsed())
    }

    fn lease_ms(&self) -> u64 {
        ms(self.s.lease())
    }

    fn broadcast(&self, q: &mut Queue, mut f: impl FnMut(NodeId) -> Message) {
        for &m in &self.s.members {
            q.push_back((self.s.me, m, f(m)));
        }
    }

    /// Delivers local messages until none are left; returns the remote
    /// ones, to be sent once the state lock is dropped.
    fn pump(&self, st: &mut State, mut q: Queue) -> Vec<(NodeId, Message)> {
        let mut remote = Vec::new();
        while let Some((from, to, msg)) = q.pop_front() {
            if to == self.s.me {
                self.handle(st, from, msg, &mut q);
            } else {
                remote.push((to, msg));
            }
        }
        remote
    }

    fn send(&self, out: Vec<(NodeId, Message)>) {
        for (to, msg) in out {
            if !self.net.send(to, msg) {
                log::trace!("locks: node {to} unreachable");
            }
        }
    }

    fn receive(&self, from: NodeId, msg: Message) {
        let mut st = lock(&self.st);
        let mut q = Queue::new();
        self.handle(&mut st, from, msg, &mut q);
        let out = self.pump(&mut st, q);
        drop(st);
        self.cv.notify_all();
        self.send(out);
    }

    fn reply(&self, q: &mut Queue, to: NodeId, req: u64, ballot: Ballot, r: Reply, ok: Message) {
        let msg = match r {
            Reply::Grant => ok,
            Reply::Deny(reason, holder) => Message::LockDeny {
                req,
                ballot,
                reason,
                holder,
            },
        };
        q.push_back((self.s.me, to, msg));
    }

    fn handle(&self, st: &mut State, from: NodeId, msg: Message, q: &mut Queue) {
        if !self.s.members.contains(&from) {
            return;
        }
        let now = self.now_ms();
        match msg {
            // Voter side.
            Message::LockRequest {
                req,
                key,
                ballot,
                file,
            } => {
                st.observe(ballot);
                let (r, pre) = st.voter.request(now, from, req, &Key::new(&key), ballot);
                match r {
                    Reply::Grant => {
                        st.counters.votes_granted += 1;
                        st.granted_since_boot = true;
                    }
                    Reply::Deny(..) => st.counters.votes_denied += 1,
                }
                for p in pre {
                    st.counters.preempted += 1;
                    q.push_back((
                        self.s.me,
                        p.node,
                        Message::LockDeny {
                            req: p.req,
                            ballot: p.ballot,
                            reason: DenyReason::Preempted,
                            holder: Some(from),
                        },
                    ));
                }
                let known = file.and_then(|f| lock(&self.versions).get(&f).cloned());
                let after = match r {
                    Reply::Grant => st.handoff.after(&Key::new(&key)),
                    Reply::Deny(..) => Vec::new(),
                };
                self.reply(
                    q,
                    from,
                    req,
                    ballot,
                    r,
                    Message::LockGrant {
                        req,
                        ballot,
                        known,
                        after,
                    },
                );
            }
            Message::LockConfirm {
                req,
                key,
                ballot,
                lease_ms,
                version,
            } => {
                st.observe(ballot);
                let r = if lease_ms > self.lease_ms() {
                    log::warn!(
                        "locks: node {from} asks for a {lease_ms} ms lease, longer than ours; \
                         check lock_lease_timeout on every node"
                    );
                    Reply::Deny(DenyReason::Lease, None)
                } else {
                    st.voter
                        .confirm(now, from, &Key::new(&key), ballot, lease_ms)
                };
                if let (Reply::Grant, Some((file, v))) = (r, version) {
                    lock(&self.versions).on_confirm(file, v);
                }
                self.reply(
                    q,
                    from,
                    req,
                    ballot,
                    r,
                    Message::LockConfirmed { req, ballot },
                );
            }
            Message::LockRenew {
                req,
                key,
                ballot,
                lease_ms,
            } => {
                st.observe(ballot);
                let r = if lease_ms > self.lease_ms() {
                    Reply::Deny(DenyReason::Lease, None)
                } else {
                    st.voter.renew(now, from, &Key::new(&key), ballot, lease_ms)
                };
                self.reply(
                    q,
                    from,
                    req,
                    ballot,
                    r,
                    Message::LockRenewed { req, ballot },
                );
            }
            Message::LockRelease {
                key,
                ballot,
                outcome,
                head,
            } => {
                st.observe(ballot);
                let key = Key::new(&key);
                st.voter.release(from, &key, ballot);
                st.handoff.record(&key, from, head);
                if let Some(o) = outcome.filter(|o| o.version.node == from) {
                    lock(&self.versions).on_release(o);
                }
            }
            Message::VersionHolders {
                file,
                version,
                holders,
            } => lock(&self.versions).on_holders(file, version, &holders),
            // Requester side.
            Message::LockGrant {
                req,
                ballot,
                known,
                after,
            } => {
                let Some(p) = st.pending.get_mut(&req) else {
                    let late = st
                        .held
                        .iter()
                        .find(|(_, h)| h.req == req && h.ballot == ballot);
                    if let Some((key, h)) = late {
                        q.push_back((
                            self.s.me,
                            from,
                            Message::LockConfirm {
                                req,
                                key: key.as_bytes().to_vec(),
                                ballot,
                                lease_ms: self.lease_ms(),
                                version: h.file.zip(h.version),
                            },
                        ));
                    }
                    return;
                };
                if let Some(k) = known {
                    p.known.push(k);
                }
                for (n, h) in after {
                    let e = p.after.entry(n).or_default();
                    *e = (*e).max(h);
                }
                let step = p.attempt.on_grant(from);
                self.step(st, req, step, q);
            }
            Message::LockConfirmed { req, .. } => {
                let Some(p) = st.pending.get_mut(&req) else {
                    return;
                };
                let step = p.attempt.on_confirmed(from);
                self.step(st, req, step, q);
            }
            Message::LockDeny {
                req,
                reason,
                holder,
                ..
            } => {
                if let Some(p) = st.pending.get_mut(&req) {
                    let step = p.attempt.on_deny(from, reason, holder);
                    self.step(st, req, step, q);
                }
                // A refused renewal needs nothing: the lease just runs out.
            }
            Message::LockRenewed { req, .. } => {
                let quorum = self.s.quorum();
                for (key, h) in st.held.iter_mut() {
                    let Some(r) = h.renewal.as_mut().filter(|r| r.req == req) else {
                        continue;
                    };
                    r.acks.insert(from);
                    if r.acks.len() >= quorum {
                        let extend = r.lease.saturating_sub(margin_for(r.lease));
                        h.valid_until = h.valid_until.max(r.sent + extend);
                        audit::record(Event::Renew, self.s.me, key, h.ballot, Some(h.valid_until));
                        h.renewal = None;
                        h.lapsed_logged = false;
                        st.counters.renewed += 1;
                    }
                    break;
                }
            }
            Message::LockFreshQuery { round } => {
                let fresh = st.fresh_for(from);
                q.push_back((self.s.me, from, Message::LockFreshReply { round, fresh }));
            }
            Message::LockFreshReply { round, fresh } => {
                if round != st.fresh_round || !st.voter.quarantined(now) {
                    return;
                }
                if !fresh {
                    log::debug!("locks: node {from} is not fresh; keeping the start-up grace");
                    return;
                }
                st.fresh_acks.insert(from);
                let all = self
                    .s
                    .members
                    .iter()
                    .all(|m| *m == self.s.me || st.fresh_acks.contains(m));
                if all {
                    st.voter.lift_quarantine(now);
                    st.grace_skipped = true;
                    log::info!(
                        "locks: cold start (all {} nodes fresh): voting without the start-up grace",
                        self.s.members.len()
                    );
                }
            }
            other => log::debug!("locks: unexpected {} from node {from}", other.kind()),
        }
    }

    fn step(&self, st: &mut State, req: u64, step: Step, q: &mut Queue) {
        let Step::Confirm(to) = step else {
            return;
        };
        let (me, disk) = (self.s.me, self.s.disk);
        let p = st.pending.get_mut(&req).expect("pending attempt");
        // Leases start no earlier than this at any voter.
        p.confirm_sent.get_or_insert_with(Instant::now);
        if let (None, Some(ctx)) = (p.announce, p.file) {
            p.announce = Some(version::next(ctx.local, &p.known, me, disk));
        }
        let version = p.file.map(|c| c.file).zip(p.announce);
        for n in to {
            q.push_back((
                self.s.me,
                n,
                Message::LockConfirm {
                    req,
                    key: p.key.as_bytes().to_vec(),
                    ballot: p.attempt.ballot,
                    lease_ms: ms(p.lease),
                    version,
                },
            ));
        }
    }

    fn have_quorum(&self) -> bool {
        self.net.snapshot().have_quorum
    }

    fn acquire(
        &self,
        key: &Key,
        add_ref: bool,
        fail_on_held: bool,
        mut file: Option<FileCtx>,
    ) -> Result<Acquired, LockError> {
        let deadline = Instant::now() + self.s.wait();
        let mut st = lock(&self.st);
        // Re-acquiring a lapsed lock continues its session and version.
        let mut reuse = None;
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return Err(LockError::Shutdown);
            }
            if st.acquiring.contains(key) {
                st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
                continue;
            }
            let (me, disk) = (self.s.me, self.s.disk);
            if let Some(h) = st.held.get_mut(key) {
                if !h.ready {
                    // Another reference is still setting the session up.
                    st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
                    continue;
                }
                if h.valid_until > Instant::now() {
                    if add_ref {
                        h.refs += 1;
                    }
                    let mut new_session = false;
                    if let (None, Some(ctx)) = (h.file, file) {
                        // Held without a file so far (e.g. it was just
                        // created): a session starts, its version
                        // unannounced. The caller checks freshness.
                        h.file = Some(ctx.file);
                        h.version = Some(version::next(ctx.local, &h.known, me, disk));
                        h.ready = false;
                        new_session = true;
                    }
                    return Ok(Acquired {
                        known: h.known.clone(),
                        version: h.version,
                        new_session,
                    });
                }
                if let (Some(f), v) = (h.file, h.version) {
                    file = Some(FileCtx {
                        file: f,
                        local: file.and_then(|c| c.local),
                    });
                    reuse = v;
                }
            }
            break;
        }
        st.clock += 1;
        let ballot = Ballot {
            counter: st.clock,
            node: self.s.me,
        };
        st.acquiring.insert(key.clone());
        let (mut st, res) = self.attempts(st, key, ballot, deadline, fail_on_held, file, reuse);
        st.acquiring.remove(key);
        let mut acquired = Acquired::default();
        let mut handoff = Vec::new();
        match &res {
            Ok((req, valid_until, known, announced, after)) => {
                handoff = after.clone();
                let (req, valid_until) = (*req, *valid_until);
                st.counters.acquired += 1;
                st.acquired_since_boot = true;
                acquired = Acquired {
                    known: known.clone(),
                    version: *announced,
                    new_session: reuse.is_none(),
                };
                let h = st.held.entry(key.clone()).or_insert(Held {
                    ballot,
                    file: file.map(|c| c.file),
                    version: *announced,
                    written: false,
                    known: known.clone(),
                    req,
                    refs: 0,
                    valid_until,
                    since: Instant::now(),
                    renewal: None,
                    lapsed_logged: false,
                    ready: false,
                });
                // Replaces a lapsed lock's ballot (its voters dropped it
                // when we asked again).
                h.ballot = ballot;
                h.req = req;
                h.valid_until = valid_until;
                audit::record(Event::Hold, self.s.me, key, ballot, Some(valid_until));
                h.renewal = None;
                h.lapsed_logged = false;
                if add_ref {
                    h.refs += 1;
                }
                if h.refs == 0 {
                    // ensure() on a key nobody references: don't keep it.
                    let out = self.drop_held(&mut st, key, 0);
                    drop(st);
                    self.cv.notify_all();
                    self.send(out);
                    return Ok(acquired);
                }
            }
            Err(LockError::Busy(_)) => st.counters.busy += 1,
            Err(_) => st.counters.no_quorum += 1,
        }
        drop(st);
        self.cv.notify_all();
        handoff.retain(|(n, _)| *n != self.s.me);
        if let (Ok(_), Some(c)) = (&res, self.chain.get()) {
            if !handoff.is_empty() && !(c.wait)(&handoff) {
                // Acting on a namespace without the previous holder's
                // changes could diverge: give the lock back.
                let mut st = lock(&self.st);
                st.counters.handoff_refused += 1;
                let mut out = Vec::new();
                if let Some(h) = st.held.get_mut(key) {
                    if add_ref {
                        h.refs = h.refs.saturating_sub(1);
                    }
                    if h.refs == 0 {
                        out = self.drop_held(&mut st, key, 0);
                    } else {
                        h.ready = true;
                    }
                }
                drop(st);
                self.cv.notify_all();
                self.send(out);
                return Err(LockError::HandoffTimeout);
            }
        }
        if res.is_ok() && !(acquired.new_session && file.is_some()) {
            // Nothing for the caller to check: the session is set up.
            let mut st = lock(&self.st);
            if let Some(h) = st.held.get_mut(key) {
                h.ready = true;
            }
            drop(st);
            self.cv.notify_all();
        }
        res.map(|_| acquired)
    }

    /// Runs attempts with one ballot until one succeeds or the deadline
    /// passes. Returns the successful request, until when the lock is
    /// valid, the voters' version entries and the announced version.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn attempts<'a>(
        &'a self,
        mut st: MutexGuard<'a, State>,
        key: &Key,
        ballot: Ballot,
        deadline: Instant,
        fail_on_held: bool,
        file: Option<FileCtx>,
        reuse: Option<Version>,
    ) -> (
        MutexGuard<'a, State>,
        Result<
            (
                u64,
                Instant,
                Vec<VersionInfo>,
                Option<Version>,
                Vec<(NodeId, u64)>,
            ),
            LockError,
        >,
    ) {
        let mut backoff = Duration::from_millis(20);
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return (st, Err(LockError::Shutdown));
            }
            if !self.have_quorum() {
                return (st, Err(LockError::NoQuorum));
            }
            let req = st.req();
            let mut attempt = Attempt::new(ballot, req, &self.s.members, self.s.quorum());
            let snap = self.net.snapshot();
            for &m in &self.s.members {
                if m != self.s.me && snap.state_of(m) != Some(PeerState::Up) {
                    attempt.on_unreachable(m);
                }
            }
            st.pending.insert(
                req,
                Pending {
                    key: key.clone(),
                    attempt,
                    confirm_sent: None,
                    lease: self.s.lease(),
                    file,
                    known: Vec::new(),
                    after: BTreeMap::new(),
                    announce: reuse,
                },
            );
            let mut q = Queue::new();
            self.broadcast(&mut q, |_| Message::LockRequest {
                req,
                key: key.as_bytes().to_vec(),
                ballot,
                file: file.map(|c| c.file),
            });
            let out = self.pump(&mut st, q);
            drop(st);
            self.send(out);
            st = lock(&self.st);

            let until = Instant::now() + self.s.request_timeout() * 2;
            loop {
                let p = st.pending.get_mut(&req).expect("pending attempt");
                if matches!(p.attempt.phase, Phase::Acquired | Phase::Failed) {
                    break;
                }
                let now = Instant::now();
                if now >= until || self.stop.load(Ordering::SeqCst) {
                    p.attempt.timeout();
                    break;
                }
                let snap = self.net.snapshot();
                for &m in &self.s.members {
                    if m != self.s.me && snap.state_of(m) != Some(PeerState::Up) {
                        p.attempt.on_unreachable(m);
                    }
                }
                if matches!(p.attempt.phase, Phase::Failed) {
                    break;
                }
                let wait = (until - now).min(Duration::from_millis(50));
                st = self
                    .cv
                    .wait_timeout(st, wait)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            let p = st.pending.remove(&req).expect("pending attempt");
            if p.attempt.phase == Phase::Acquired {
                let sent = p.confirm_sent.expect("confirmed attempts sent confirms");
                let valid = sent + p.lease.saturating_sub(margin_for(p.lease));
                let after = p.after.into_iter().collect();
                return (st, Ok((req, valid, p.known, p.announce, after)));
            }
            // Withdraw every vote this attempt may have got (and the
            // announced version, if confirms went out).
            let outcome = file.zip(p.announce).map(|(c, v)| Outcome {
                file: c.file,
                version: v,
                written: false,
            });
            let outcome = outcome.filter(|_| reuse.is_none());
            let mut q = Queue::new();
            self.broadcast(&mut q, |_| Message::LockRelease {
                key: key.as_bytes().to_vec(),
                ballot,
                outcome,
                head: 0,
            });
            let out = self.pump(&mut st, q);
            drop(st);
            self.send(out);
            let now = Instant::now();
            if now >= deadline || (fail_on_held && p.attempt.held_seen) {
                let err = if p.attempt.conflict_seen {
                    LockError::Busy(p.attempt.conflict)
                } else {
                    LockError::NoQuorum
                };
                return (lock(&self.st), Err(err));
            }
            // Jittered exponential backoff; the ballot stays, so it only
            // gets older relative to new rivals.
            let jitter = 0.75 + 0.5 * (rand_u32() as f64 / u32::MAX as f64);
            let pause = backoff.mul_f64(jitter).min(deadline - now);
            std::thread::sleep(pause);
            backoff = (backoff * 2).min(Duration::from_millis(500));
            st = lock(&self.st);
            st.counters.retries += 1;
        }
    }

    fn drop_held(&self, st: &mut State, key: &Key, head: u64) -> Vec<(NodeId, Message)> {
        let Some(h) = st.held.remove(key) else {
            return Vec::new();
        };
        audit::record(Event::End, self.s.me, key, h.ballot, None);
        let mut q = Queue::new();
        let outcome = h.outcome();
        self.broadcast(&mut q, |_| Message::LockRelease {
            key: key.as_bytes().to_vec(),
            ballot: h.ballot,
            outcome,
            head,
        });
        self.pump(st, q)
    }

    fn release(&self, key: &Key) {
        // Before taking the state lock: the head comes from replication.
        let head = self.head();
        let mut st = lock(&self.st);
        let Some(h) = st.held.get_mut(key) else {
            return;
        };
        if h.refs > 0 {
            // Taken again meanwhile (e.g. `ensure` re-acquired it).
            h.ready = true;
            drop(st);
            self.cv.notify_all();
            return;
        }
        let out = self.drop_held(&mut st, key, head);
        drop(st);
        self.cv.notify_all();
        self.send(out);
    }

    fn head(&self) -> u64 {
        self.chain.get().map_or(0, |c| (c.head)())
    }

    /// While in the start-up grace, asks every other node whether it is
    /// fresh; once all of them say so in one round, the grace ends. Returns
    /// whether this node is still in the grace.
    fn probe_fresh(&self) -> bool {
        let mut st = lock(&self.st);
        if !st.voter.quarantined(self.now_ms()) {
            return false;
        }
        let snap = self.net.snapshot();
        let others: Vec<NodeId> = self
            .s
            .members
            .iter()
            .copied()
            .filter(|&m| m != self.s.me)
            .collect();
        if others.is_empty() {
            let now = self.now_ms();
            st.voter.lift_quarantine(now);
            st.grace_skipped = true;
            return false;
        }
        if others
            .iter()
            .any(|&m| snap.state_of(m) != Some(PeerState::Up))
        {
            return true; // an unreachable node could be a live holder
        }
        st.fresh_round += 1;
        st.fresh_acks.clear();
        let round = st.fresh_round;
        drop(st);
        self.send(
            others
                .into_iter()
                .map(|m| (m, Message::LockFreshQuery { round }))
                .collect(),
        );
        true
    }

    /// Starts a renewal round for every held lock.
    fn renew_all(&self) {
        let mut st = lock(&self.st);
        let now = Instant::now();
        let keys: Vec<Key> = st.held.keys().cloned().collect();
        let mut q = Queue::new();
        for key in keys {
            let req = st.req();
            let h = st.held.get_mut(&key).expect("held");
            if h.valid_until <= now && !h.lapsed_logged {
                h.lapsed_logged = true;
                log::warn!("locks: lease on {key} lapsed (no quorum renewed it)");
                st.counters.lapsed += 1;
            }
            let h = st.held.get_mut(&key).expect("held");
            let lease = self.s.lease();
            h.renewal = Some(Renewal {
                req,
                sent: now,
                lease,
                acks: BTreeSet::new(),
            });
            let ballot = h.ballot;
            self.broadcast(&mut q, |_| Message::LockRenew {
                req,
                key: key.as_bytes().to_vec(),
                ballot,
                lease_ms: ms(lease),
            });
        }
        let out = self.pump(&mut st, q);
        drop(st);
        self.send(out);
    }

    fn render(&self) -> serde_json::Value {
        let st = lock(&self.st);
        let now = Instant::now();
        let now_ms = self.now_ms();
        let held: Vec<_> = st
            .held
            .iter()
            .map(|(k, h)| {
                json!({
                    "path": k.to_string(),
                    "ballot": h.ballot.to_string(),
                    "refs": h.refs,
                    "valid": h.valid_until > now,
                    "valid_for_ms": ms(h.valid_until.saturating_duration_since(now)),
                    "held_for_ms": ms(now.saturating_duration_since(h.since)),
                })
            })
            .collect();
        let votes: Vec<_> = st
            .voter
            .grants()
            .iter()
            .filter(|g| g.until > now_ms)
            .map(|g| {
                json!({
                    "path": g.key.to_string(),
                    "holder": g.holder,
                    "ballot": g.ballot.to_string(),
                    "state": if g.confirmed { "leased" } else { "provisional" },
                    "expires_in_ms": g.until - now_ms,
                })
            })
            .collect();
        json!({
            "implemented": true,
            "lease_timeout": humantime::format_duration(self.s.lease()).to_string(),
            "lock_wait": humantime::format_duration(self.s.wait()).to_string(),
            "quorum": self.s.quorum(),
            "members": self.s.members,
            "grants_after_ms": st.voter.quarantine_until().saturating_sub(now_ms),
            "grace_skipped": st.grace_skipped,
            "acquired_since_start": st.acquired_since_boot,
            "granted_since_start": st.granted_since_boot,
            "held": held.len(),
            "locks": held,
            "votes": votes,
            "counters": serde_json::to_value(&st.counters).unwrap_or_default(),
        })
    }
}

fn renew_loop(weak: Weak<Inner>) {
    let mut next_renew = Instant::now();
    loop {
        let Some(inner) = weak.upgrade() else { return };
        if inner.stop.load(Ordering::SeqCst) {
            return;
        }
        if inner.retime.swap(false, Ordering::SeqCst) {
            // A reload changed the lease: renew no later than the new
            // schedule asks for.
            next_renew = next_renew.min(Instant::now() + inner.s.renew_every());
        }
        if Instant::now() >= next_renew {
            inner.renew_all();
            next_renew = Instant::now() + inner.s.renew_every();
        }
        // During the start-up grace, probe for a cold start every heartbeat.
        let quarantined = inner.probe_fresh();
        let every = if quarantined {
            inner.s.heartbeat().min(inner.s.renew_every())
        } else {
            next_renew.saturating_duration_since(Instant::now())
        };
        // Wait for the next round, waking early on shutdown.
        let st = lock(&inner.st);
        let _ = inner.cv.wait_timeout_while(st, every, |_| {
            !inner.stop.load(Ordering::SeqCst) && !inner.retime.load(Ordering::SeqCst)
        });
    }
}

/// Small non-cryptographic randomness for backoff jitter.
fn rand_u32() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.finish() as u32
}
