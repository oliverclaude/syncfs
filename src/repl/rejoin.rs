//! Rejoining: local changes wait until this node has applied everything
//! its connected data peers report applied (their `Positions`, sent on
//! each new connection), so a node that was away does not change a
//! namespace it has not caught up with. That holds
//!
//! - after the start, for at most `freshness_timeout`;
//! - after a new connection to a peer, and after this process was not
//!   scheduled for longer than the peers' down timeout (stopped, or the
//!   machine was suspended: the peers dropped it), for at most
//!   [`RECONNECT_WAIT`] (or `freshness_timeout` if shorter). After such a
//!   pause the positions must be newer than the pause.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Weak;
use std::time::{Duration, Instant};

use crate::config::NodeId;

use super::core::{lock, Repl};

/// The longest a change waits after a reconnect or a pause.
pub const RECONNECT_WAIT: Duration = Duration::from_secs(2);

const TICK: Duration = Duration::from_millis(20);

/// What the gate last passed with.
#[derive(Debug)]
pub(crate) struct Gate {
    /// Per connected peer: its connection then.
    passed: BTreeMap<NodeId, u64>,
    passed_at: Instant,
    /// Since when changes wait (a new connection or a pause was seen).
    noticed: Option<Instant>,
    /// The ticker's last tick, and the end of the last pause it saw.
    tick: Instant,
    paused: Option<Instant>,
    /// Per peer: when its last `Positions` arrived, on which connection.
    positions: HashMap<NodeId, (Instant, u64)>,
}

impl Default for Gate {
    fn default() -> Gate {
        let now = Instant::now();
        Gate {
            passed: BTreeMap::new(),
            passed_at: now,
            noticed: None,
            tick: now,
            paused: None,
            positions: HashMap::new(),
        }
    }
}

impl Gate {
    /// Changes are waiting for a reconnected peer or after a pause.
    pub(crate) fn waiting(&self) -> bool {
        self.noticed.is_some()
    }
}

impl Repl {
    /// `peer`'s `Positions` arrived on connection `session`.
    pub(crate) fn positions_heard(&self, peer: NodeId, session: u64) {
        lock(&self.gate)
            .positions
            .insert(peer, (Instant::now(), session));
    }

    /// Whether some connected data peer reported its positions on its
    /// current connection (after `since`, if given) and this node has
    /// applied at least as much of every origin as each such peer did.
    pub(crate) fn caught_up_since(&self, since: Option<Instant>) -> bool {
        let up = self.data_peers_up();
        if up.is_empty() {
            // Nobody to catch up with if every peer is connected and none
            // stores data (only voter daemons).
            let snap = self.net.snapshot();
            return self.peers.iter().all(|id| {
                snap.peers
                    .iter()
                    .any(|p| p.id == *id && p.role.is_some_and(|r| !r.data))
            });
        }
        let heard: Vec<NodeId> = {
            let g = lock(&self.gate);
            up.iter()
                .copied()
                .filter(|p| {
                    g.positions.get(p).is_some_and(|(at, s)| {
                        *s == self.net.session_of(*p) && since.is_none_or(|t| *at > t)
                    })
                })
                .collect()
        };
        if heard.is_empty() {
            return false;
        }
        let st = self.state();
        heard.iter().all(|&p| {
            self.peers
                .iter()
                .all(|&o| st.applied.get(&o).copied().unwrap_or(0) >= st.relay.position(p, o))
        })
    }

    pub(crate) fn caught_up(&self) -> bool {
        self.caught_up_since(None)
    }

    /// The connections to peers now (connected ones only).
    fn connections(&self) -> BTreeMap<NodeId, u64> {
        self.peers
            .iter()
            .map(|&p| (p, self.net.session_of(p)))
            .filter(|(_, s)| *s != 0)
            .collect()
    }

    /// The end of a pause since the gate last passed, if any (a tick
    /// overdue counts: the ticker has not run since the pause).
    fn pause_since_pass(&self) -> Option<Instant> {
        let g = lock(&self.gate);
        if g.tick.elapsed() > self.pause_limit.get() {
            return Some(Instant::now());
        }
        g.paused.filter(|p| *p > g.passed_at)
    }

    /// Before a local change: waits as the module docs say.
    pub fn rejoin_gate(&self) {
        if !self.rejoined.load(Ordering::Acquire) {
            if Instant::now() < self.started + self.freshness_timeout.get() {
                self.start_gate();
                return;
            }
            // The first change comes late: no start wait, but the
            // connections are new to the gate (below).
            let _g = lock(&self.reconcile_lock);
            if !self.reconciled.load(Ordering::Acquire) {
                log::warn!("rejoin: taking changes before the namespace was compared with a peer");
            }
            self.rejoined.store(true, Ordering::Release);
        }
        let now = self.connections();
        let pause = self.pause_since_pass();
        let deadline = {
            let mut g = lock(&self.gate);
            if pause.is_none() && now.iter().all(|(p, s)| g.passed.get(p) == Some(s)) {
                // Fewer connections (a peer left) need no catching up.
                g.passed = now;
                g.passed_at = Instant::now();
                g.noticed = None;
                return;
            }
            *g.noticed.get_or_insert_with(Instant::now)
                + RECONNECT_WAIT.min(self.freshness_timeout.get())
        };
        let mut waited = false;
        loop {
            if self.caught_up_since(pause) {
                if waited {
                    log::info!("rejoin: caught up after a reconnect; taking changes");
                }
                break;
            }
            if Instant::now() >= deadline || self.stopping() {
                log::warn!(
                    "rejoin: not caught up after a reconnect in time; taking changes anyway"
                );
                break;
            }
            if !waited {
                waited = true;
                log::info!(
                    "rejoin: {}; changes wait until caught up",
                    if pause.is_some() {
                        "this node was paused"
                    } else {
                        "a peer reconnected"
                    }
                );
            }
            let st = self.state();
            let _ = self.cv.wait_timeout(st, TICK);
        }
        self.passed();
    }

    fn passed(&self) {
        let now = self.connections();
        let mut g = lock(&self.gate);
        g.passed = now;
        g.passed_at = Instant::now();
        g.noticed = None;
    }

    /// The first change after the start (see the module docs).
    fn start_gate(&self) {
        let deadline = (self.started + self.freshness_timeout.get())
            .max(Instant::now() + RECONNECT_WAIT.min(self.freshness_timeout.get()));
        let mut waited = false;
        loop {
            let pause = self.pause_since_pass();
            if self.caught_up_since(pause) && self.reconciled.load(Ordering::Acquire) {
                if waited {
                    log::info!("rejoin: caught up with the peers; taking changes");
                }
                break;
            }
            if Instant::now() >= deadline || self.stopping() {
                log::warn!(
                    "rejoin: not caught up with any data peer within {}; taking changes anyway",
                    humantime::format_duration(self.freshness_timeout.get())
                );
                break;
            }
            if !waited {
                waited = true;
                log::info!("rejoin: changes wait until this node has caught up with its peers");
            }
            let st = self.state();
            let _ = self.cv.wait_timeout(st, Duration::from_millis(50));
        }
        let _g = lock(&self.reconcile_lock);
        self.passed();
        self.rejoined.store(true, Ordering::Release);
    }
}

/// Notices pauses of this process (see the module docs).
pub(crate) fn ticker(weak: Weak<Repl>) {
    loop {
        std::thread::sleep(TICK);
        let Some(r) = weak.upgrade() else { return };
        if r.stopping() {
            return;
        }
        let now = Instant::now();
        let mut g = lock(&r.gate);
        if now - g.tick > r.pause_limit.get() {
            log::info!(
                "rejoin: this process did not run for {} ms",
                (now - g.tick).as_millis()
            );
            g.paused = Some(now);
        }
        g.tick = now;
    }
}
