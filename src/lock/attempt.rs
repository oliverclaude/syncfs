//! The requester side: one attempt to collect a quorum of votes for one
//! ballot. Pure, like the voter table.
//!
//! Phase 1 (`Asking`): wait for provisional grants from a quorum. Phase 2
//! (`Confirming`): ask the granters to confirm; the lock is held once a
//! quorum confirmed. The attempt fails as soon as a quorum is out of
//! reach.

use std::collections::BTreeSet;

use crate::config::NodeId;
use crate::net::wire::{Ballot, DenyReason};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    Asking,
    Confirming,
    Acquired,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Nothing to do yet.
    Wait,
    /// Send `LockConfirm` to these nodes.
    Confirm(Vec<NodeId>),
    Acquired,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Attempt {
    pub ballot: Ballot,
    pub req: u64,
    pub phase: Phase,
    members: BTreeSet<NodeId>,
    quorum: usize,
    granted: BTreeSet<NodeId>,
    confirmed: BTreeSet<NodeId>,
    /// Nodes that will not (or no longer) support this attempt.
    lost: BTreeSet<NodeId>,
    /// Some refusal said another node holds or is about to hold the lock.
    pub conflict: Option<NodeId>,
    pub conflict_seen: bool,
    /// Some voter said another node holds a confirmed lease.
    pub held_seen: bool,
}

impl Attempt {
    pub fn new(ballot: Ballot, req: u64, members: &[NodeId], quorum: usize) -> Attempt {
        Attempt {
            ballot,
            req,
            phase: Phase::Asking,
            members: members.iter().copied().collect(),
            quorum,
            granted: BTreeSet::new(),
            confirmed: BTreeSet::new(),
            lost: BTreeSet::new(),
            conflict: None,
            conflict_seen: false,
            held_seen: false,
        }
    }

    pub fn granted(&self) -> &BTreeSet<NodeId> {
        &self.granted
    }

    fn check(&mut self) -> Step {
        let possible = match self.phase {
            Phase::Asking => self
                .members
                .iter()
                .filter(|n| !self.lost.contains(n))
                .count(),
            Phase::Confirming => self
                .granted
                .iter()
                .filter(|n| !self.lost.contains(n))
                .count(),
            _ => return Step::Wait,
        };
        if possible < self.quorum {
            self.phase = Phase::Failed;
            return Step::Failed;
        }
        Step::Wait
    }

    pub fn on_grant(&mut self, from: NodeId) -> Step {
        if !self.members.contains(&from) || self.lost.contains(&from) {
            return Step::Wait;
        }
        match self.phase {
            Phase::Asking => {
                self.granted.insert(from);
                let live = self
                    .granted
                    .iter()
                    .filter(|n| !self.lost.contains(n))
                    .count();
                if live >= self.quorum {
                    self.phase = Phase::Confirming;
                    let live = self.granted.difference(&self.lost).copied().collect();
                    return Step::Confirm(live);
                }
                Step::Wait
            }
            // A late grant: ask it to confirm too.
            Phase::Confirming if self.granted.insert(from) => Step::Confirm(vec![from]),
            _ => Step::Wait,
        }
    }

    pub fn on_confirmed(&mut self, from: NodeId) -> Step {
        if self.phase != Phase::Confirming || !self.granted.contains(&from) {
            return Step::Wait;
        }
        self.confirmed.insert(from);
        if self.confirmed.len() >= self.quorum {
            self.phase = Phase::Acquired;
            return Step::Acquired;
        }
        Step::Wait
    }

    pub fn on_deny(&mut self, from: NodeId, reason: DenyReason, holder: Option<NodeId>) -> Step {
        if !self.members.contains(&from) || self.confirmed.contains(&from) {
            return Step::Wait;
        }
        if matches!(
            reason,
            DenyReason::Held | DenyReason::Older | DenyReason::Preempted
        ) {
            self.conflict_seen = true;
            if holder.is_some() {
                self.conflict = holder;
            }
            if reason == DenyReason::Held {
                self.held_seen = true;
            }
        }
        self.lost.insert(from);
        self.check()
    }

    /// `from` cannot answer (disconnected).
    pub fn on_unreachable(&mut self, from: NodeId) -> Step {
        if self.confirmed.contains(&from) {
            return Step::Wait;
        }
        self.lost.insert(from);
        self.check()
    }

    pub fn timeout(&mut self) -> Step {
        if self.phase == Phase::Acquired {
            return Step::Acquired;
        }
        self.phase = Phase::Failed;
        Step::Failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a() -> Attempt {
        Attempt::new(
            Ballot {
                counter: 1,
                node: 1,
            },
            1,
            &[1, 2, 3],
            2,
        )
    }

    #[test]
    fn two_phases() {
        let mut t = a();
        assert_eq!(t.on_grant(1), Step::Wait);
        assert_eq!(t.on_grant(2), Step::Confirm(vec![1, 2]));
        assert_eq!(t.on_grant(3), Step::Confirm(vec![3]));
        assert_eq!(t.on_confirmed(2), Step::Wait);
        assert_eq!(t.on_confirmed(3), Step::Acquired);
    }

    #[test]
    fn fails_once_quorum_is_out_of_reach() {
        let mut t = a();
        t.on_grant(1);
        assert_eq!(t.on_deny(2, DenyReason::Held, Some(2)), Step::Wait);
        assert_eq!(t.on_deny(3, DenyReason::Older, Some(3)), Step::Failed);
        assert!(t.conflict_seen);
        let mut t = a();
        t.on_grant(1);
        t.on_grant(2);
        t.on_grant(3);
        // Preempted during phase 2.
        assert_eq!(t.on_deny(2, DenyReason::Preempted, Some(3)), Step::Wait);
        assert_eq!(t.on_deny(1, DenyReason::NotHeld, None), Step::Failed);
        let mut t = a();
        t.on_unreachable(2);
        assert_eq!(t.on_deny(3, DenyReason::Quarantine, None), Step::Failed);
        assert!(!t.conflict_seen);
    }

    #[test]
    fn strangers_are_ignored() {
        let mut t = a();
        assert_eq!(t.on_grant(9), Step::Wait);
        assert_eq!(t.on_deny(9, DenyReason::Held, None), Step::Wait);
        assert_eq!(t.phase, Phase::Asking);
    }
}
