//! Rejoining: after a start, local changes wait until this node has
//! applied everything its connected data peers report applied (their
//! `Positions`), or `freshness_timeout` passed. So a node that was away
//! does not make changes on top of a namespace it has not caught up with.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::core::Repl;

impl Repl {
    /// Whether some connected data peer reported its positions and this
    /// node has applied at least as much of every origin as each did.
    pub(crate) fn caught_up(&self) -> bool {
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
        let st = self.state();
        let mut heard = false;
        for &p in &up {
            if !st.relay.heard_from(p) {
                continue;
            }
            heard = true;
            for &o in &self.peers {
                if st.applied.get(&o).copied().unwrap_or(0) < st.relay.position(p, o) {
                    return false;
                }
            }
        }
        heard
    }

    /// Before the first local change after start: waits until
    /// [`caught_up`](Self::caught_up), at most `freshness_timeout` from
    /// the start.
    pub fn rejoin_gate(&self) {
        if self.rejoined.load(Ordering::Acquire) {
            return;
        }
        let deadline = self.started + self.freshness_timeout;
        let mut waited = false;
        loop {
            if self.caught_up() && self.reconciled.load(Ordering::Acquire) {
                if waited {
                    log::info!("rejoin: caught up with the peers; taking changes");
                }
                break;
            }
            if Instant::now() >= deadline || self.stopping() {
                log::warn!(
                    "rejoin: not caught up with any data peer within {}; taking changes anyway",
                    humantime::format_duration(self.freshness_timeout)
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
        let _g = super::core::lock(&self.reconcile_lock);
        self.rejoined.store(true, Ordering::Release);
    }
}
