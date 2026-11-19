//! The replication side of lock handoff (see `crate::lock::handoff`): a new
//! lock holder first applies what the previous holders released.

use std::time::{Duration, Instant};

use super::core::Repl;
use crate::config::NodeId;

impl Repl {
    /// This node's journal head, sent with its lock releases.
    pub(crate) fn journal_head(&self) -> u64 {
        self.state().journal.last()
    }

    /// Waits until this node has applied each `(origin, head)` of `after`,
    /// at most `freshness_timeout`, and only while someone that can provide
    /// the entries is connected: the origin, or a data peer that has
    /// applied them. Returns false if they were reachable but did not
    /// arrive in time (the caller refuses the operation); true otherwise,
    /// also when nobody connected has them (the holder is gone; the lock
    /// goes ahead without them, as the freshness check's fallback does).
    pub(crate) fn wait_applied(&self, after: &[(NodeId, u64)]) -> bool {
        let deadline = Instant::now() + self.freshness_timeout;
        let mut waited = false;
        let mut st = self.state();
        loop {
            let missing: Vec<(NodeId, u64)> = after
                .iter()
                .copied()
                .filter(|(o, h)| st.applied.get(o).copied().unwrap_or(0) < *h)
                .collect();
            if missing.is_empty() {
                return true;
            }
            drop(st);
            let peers = self.data_peers_up();
            st = self.state();
            let available = missing.iter().any(|&(o, h)| {
                self.net.session_of(o) != 0 || peers.iter().any(|&p| st.relay.position(p, o) >= h)
            });
            let now = Instant::now();
            if available && now >= deadline && !self.stopping() {
                drop(st);
                log::warn!(
                    "lock handoff: the entries {missing:?} (origin, journal head) of the \
                     previous holders were not applied within freshness_timeout; refusing \
                     the operation (EBUSY)"
                );
                return false;
            }
            if !available || self.stopping() {
                st.counters.handoff_gaps += 1;
                drop(st);
                log::warn!(
                    "lock handoff: continuing without the entries {missing:?} (origin, journal \
                     head) of the previous holders (nobody connected has them)"
                );
                return true;
            }
            if !waited {
                waited = true;
                st.counters.handoff_waits += 1;
            }
            st = self
                .cv
                .wait_timeout(st, (deadline - now).min(Duration::from_millis(50)))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}
