//! The lock manager: this node's voter table plus the locks this node
//! holds, driven by network messages. One per node, in mount and daemon
//! mode alike (a daemon only votes).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use fuser::Errno;
use serde_json::json;

use crate::config::{Config, NodeId};
use crate::net::wire::{Ballot, DenyReason, Message};
use crate::net::{Network, PeerState};
use crate::status::{FnProvider, Provider};

use super::attempt::{Attempt, Phase, Step};
use super::key::Key;
use super::voter::{Reply, Voter};

/// Timing and membership, derived from the config.
#[derive(Clone, Debug)]
pub struct Settings {
    pub me: NodeId,
    /// Every node, voters included, sorted.
    pub members: Vec<NodeId>,
    pub lease: Duration,
    pub wait: Duration,
    pub heartbeat: Duration,
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
            lease: cfg.lock_lease_timeout.get(),
            wait: cfg.lock_wait.get(),
            heartbeat: cfg.heartbeat_interval.get(),
        }
    }

    pub fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }

    /// A holder stops writing this long before its lease could end
    /// anywhere: covers clock rate differences and a write that started
    /// just before the deadline.
    pub fn margin(&self) -> Duration {
        (self.lease / 10).max(Duration::from_millis(50))
    }

    /// How long one attempt waits for votes.
    pub fn request_timeout(&self) -> Duration {
        (self.heartbeat * 2).clamp(Duration::from_millis(200), Duration::from_secs(2))
    }

    pub fn provisional_ttl(&self) -> Duration {
        self.request_timeout() * 4
    }

    pub fn renew_every(&self) -> Duration {
        self.lease / 3
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
}

impl LockError {
    pub fn errno(self) -> Errno {
        match self {
            LockError::NoQuorum | LockError::Shutdown => Errno::EROFS,
            LockError::Busy(_) => Errno::EBUSY,
        }
    }
}

struct Renewal {
    req: u64,
    sent: Instant,
    acks: BTreeSet<NodeId>,
}

struct Held {
    ballot: Ballot,
    refs: u32,
    valid_until: Instant,
    since: Instant,
    renewal: Option<Renewal>,
    lapsed_logged: bool,
}

struct Pending {
    key: Key,
    attempt: Attempt,
    confirm_sent: Option<Instant>,
}

#[derive(Default, serde::Serialize)]
struct Counters {
    acquired: u64,
    busy: u64,
    no_quorum: u64,
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
}

impl State {
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
    epoch: Instant,
    st: Mutex<State>,
    cv: Condvar,
    stop: AtomicBool,
    renewer: Mutex<Option<std::thread::JoinHandle<()>>>,
}

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
        let epoch = Instant::now();
        let lease_ms = s.lease.as_millis() as u64;
        let voter = Voter::new(
            // Grants nothing for one lease after start: see the README.
            lease_ms,
            s.provisional_ttl().as_millis() as u64,
        );
        let inner = Arc::new(Inner {
            s,
            net: net.clone(),
            epoch,
            st: Mutex::new(State {
                clock: 0,
                next_req: 0,
                voter,
                pending: HashMap::new(),
                held: BTreeMap::new(),
                acquiring: HashSet::new(),
                counters: Counters::default(),
            }),
            cv: Condvar::new(),
            stop: AtomicBool::new(false),
            renewer: Mutex::new(None),
        });
        let weak: Weak<Inner> = Arc::downgrade(&inner);
        net.set_handler(Arc::new(move |from, msg| {
            if let Some(inner) = weak.upgrade() {
                inner.receive(from, msg);
            }
        }));
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
            inner.s.lease,
            inner.s.lease
        );
        LockManager { inner }
    }

    pub fn settings(&self) -> &Settings {
        &self.inner.s
    }

    /// Takes (another reference to) the write lock on `key`, waiting up to
    /// `lock_wait` while another node holds a conflicting lock.
    pub fn acquire(&self, key: &Key) -> Result<(), LockError> {
        self.inner.acquire(key, true)
    }

    /// Ok if this node holds `key` with a valid lease, re-acquiring it
    /// (without taking a reference) if it lapsed or is not held.
    pub fn ensure(&self, key: &Key) -> Result<(), LockError> {
        self.inner.acquire(key, false)
    }

    /// Drops one reference; the last one releases the lock everywhere.
    pub fn release(&self, key: &Key) {
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
        let mut st = lock(&inner.st);
        let keys: Vec<(Key, Ballot)> = st.held.iter().map(|(k, h)| (k.clone(), h.ballot)).collect();
        st.held.clear();
        let mut q = Queue::new();
        for (k, b) in keys {
            inner.broadcast(&mut q, |_| Message::LockRelease {
                key: k.as_bytes().to_vec(),
                ballot: b,
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
        ms(self.s.lease)
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
            Message::LockRequest { req, key, ballot } => {
                st.observe(ballot);
                let (r, pre) = st.voter.request(now, from, req, &Key::new(&key), ballot);
                match r {
                    Reply::Grant => st.counters.votes_granted += 1,
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
                self.reply(q, from, req, ballot, r, Message::LockGrant { req, ballot });
            }
            Message::LockConfirm {
                req,
                key,
                ballot,
                lease_ms,
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
            Message::LockRelease { key, ballot } => {
                st.observe(ballot);
                st.voter.release(from, &Key::new(&key), ballot);
            }
            // Requester side.
            Message::LockGrant { req, .. } => {
                let Some(p) = st.pending.get_mut(&req) else {
                    return;
                };
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
                let extend = self.s.lease.saturating_sub(self.s.margin());
                for h in st.held.values_mut() {
                    let Some(r) = h.renewal.as_mut().filter(|r| r.req == req) else {
                        continue;
                    };
                    r.acks.insert(from);
                    if r.acks.len() >= quorum {
                        h.valid_until = h.valid_until.max(r.sent + extend);
                        h.renewal = None;
                        h.lapsed_logged = false;
                        st.counters.renewed += 1;
                    }
                    break;
                }
            }
            other => log::debug!("locks: unexpected {} from node {from}", other.kind()),
        }
    }

    fn step(&self, st: &mut State, req: u64, step: Step, q: &mut Queue) {
        let Step::Confirm(to) = step else {
            return;
        };
        let p = st.pending.get_mut(&req).expect("pending attempt");
        // Leases start no earlier than this at any voter.
        p.confirm_sent.get_or_insert_with(Instant::now);
        for n in to {
            q.push_back((
                self.s.me,
                n,
                Message::LockConfirm {
                    req,
                    key: p.key.as_bytes().to_vec(),
                    ballot: p.attempt.ballot,
                    lease_ms: self.lease_ms(),
                },
            ));
        }
    }

    fn have_quorum(&self) -> bool {
        self.net.snapshot().have_quorum
    }

    fn acquire(&self, key: &Key, add_ref: bool) -> Result<(), LockError> {
        let deadline = Instant::now() + self.s.wait;
        let mut st = lock(&self.st);
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return Err(LockError::Shutdown);
            }
            if st.acquiring.contains(key) {
                st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
                continue;
            }
            if let Some(h) = st.held.get_mut(key) {
                if h.valid_until > Instant::now() {
                    if add_ref {
                        h.refs += 1;
                    }
                    return Ok(());
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
        let (mut st, res) = self.attempts(st, key, ballot, deadline);
        st.acquiring.remove(key);
        match res {
            Ok(valid_until) => {
                st.counters.acquired += 1;
                let h = st.held.entry(key.clone()).or_insert(Held {
                    ballot,
                    refs: 0,
                    valid_until,
                    since: Instant::now(),
                    renewal: None,
                    lapsed_logged: false,
                });
                // Replaces a lapsed lock's ballot (its voters dropped it
                // when we asked again).
                h.ballot = ballot;
                h.valid_until = valid_until;
                h.renewal = None;
                h.lapsed_logged = false;
                if add_ref {
                    h.refs += 1;
                }
                if h.refs == 0 {
                    // ensure() on a key nobody references: don't keep it.
                    let out = self.drop_held(&mut st, key);
                    drop(st);
                    self.cv.notify_all();
                    self.send(out);
                    return Ok(());
                }
            }
            Err(LockError::Busy(_)) => st.counters.busy += 1,
            Err(_) => st.counters.no_quorum += 1,
        }
        drop(st);
        self.cv.notify_all();
        res.map(|_| ())
    }

    /// Runs attempts with one ballot until one succeeds or the deadline
    /// passes. Returns until when the lock is valid.
    fn attempts<'a>(
        &'a self,
        mut st: MutexGuard<'a, State>,
        key: &Key,
        ballot: Ballot,
        deadline: Instant,
    ) -> (MutexGuard<'a, State>, Result<Instant, LockError>) {
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
                },
            );
            let mut q = Queue::new();
            self.broadcast(&mut q, |_| Message::LockRequest {
                req,
                key: key.as_bytes().to_vec(),
                ballot,
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
                let valid = sent + self.s.lease.saturating_sub(self.s.margin());
                return (st, Ok(valid));
            }
            // Withdraw every vote this attempt may have got.
            let mut q = Queue::new();
            self.broadcast(&mut q, |_| Message::LockRelease {
                key: key.as_bytes().to_vec(),
                ballot,
            });
            let out = self.pump(&mut st, q);
            drop(st);
            self.send(out);
            let now = Instant::now();
            if now >= deadline {
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

    fn drop_held(&self, st: &mut State, key: &Key) -> Vec<(NodeId, Message)> {
        let Some(h) = st.held.remove(key) else {
            return Vec::new();
        };
        let mut q = Queue::new();
        self.broadcast(&mut q, |_| Message::LockRelease {
            key: key.as_bytes().to_vec(),
            ballot: h.ballot,
        });
        self.pump(st, q)
    }

    fn release(&self, key: &Key) {
        let mut st = lock(&self.st);
        let Some(h) = st.held.get_mut(key) else {
            log::warn!("locks: releasing {key}, which is not held");
            return;
        };
        h.refs = h.refs.saturating_sub(1);
        if h.refs > 0 {
            return;
        }
        let out = self.drop_held(&mut st, key);
        drop(st);
        self.cv.notify_all();
        self.send(out);
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
            h.renewal = Some(Renewal {
                req,
                sent: now,
                acks: BTreeSet::new(),
            });
            let ballot = h.ballot;
            self.broadcast(&mut q, |_| Message::LockRenew {
                req,
                key: key.as_bytes().to_vec(),
                ballot,
                lease_ms: ms(self.s.lease),
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
            "lease_timeout": humantime::format_duration(self.s.lease).to_string(),
            "lock_wait": humantime::format_duration(self.s.wait).to_string(),
            "quorum": self.s.quorum(),
            "members": self.s.members,
            "grants_after_ms": st.voter.quarantine_until().saturating_sub(now_ms),
            "held": held.len(),
            "locks": held,
            "votes": votes,
            "counters": serde_json::to_value(&st.counters).unwrap_or_default(),
        })
    }
}

fn renew_loop(weak: Weak<Inner>) {
    loop {
        let Some(inner) = weak.upgrade() else { return };
        if inner.stop.load(Ordering::SeqCst) {
            return;
        }
        inner.renew_all();
        let every = inner.s.renew_every();
        // Wait for the next round, waking early on shutdown.
        let st = lock(&inner.st);
        let _ = inner
            .cv
            .wait_timeout_while(st, every, |_| !inner.stop.load(Ordering::SeqCst));
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
