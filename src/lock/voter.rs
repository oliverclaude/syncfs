//! The voter side of the lock protocol: one table per node, deciding which
//! attempts get this node's vote. Pure (time is passed in, in ms) so it can
//! be unit-tested and model-checked.
//!
//! An entry is a *provisional* grant (phase 1, short-lived, can be taken
//! over by an older ballot) or a *confirmed* lease (phase 2, exclusive until
//! it expires or is released). Entries of the requesting node itself never
//! conflict with its new requests: a node coordinates its own locks.

use crate::config::NodeId;
use crate::net::wire::{Ballot, DenyReason};

use super::key::Key;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Grant {
    pub key: Key,
    pub holder: NodeId,
    pub ballot: Ballot,
    pub req: u64,
    pub confirmed: bool,
    /// Expiry, in the table's ms clock.
    pub until: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    Grant,
    Deny(DenyReason, Option<NodeId>),
}

/// A provisional grant taken away from a younger attempt; its requester is
/// told with `LockDeny { reason: Preempted }`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Preemption {
    pub node: NodeId,
    pub req: u64,
    pub ballot: Ballot,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Voter {
    grants: Vec<Grant>,
    /// No grants before this time: a restarted voter forgot what it
    /// granted, so it waits out every lease it may have given.
    grant_after: u64,
    /// Lifetime of a provisional grant.
    prov_ttl: u64,
}

impl Voter {
    pub fn new(grant_after: u64, prov_ttl: u64) -> Voter {
        Voter {
            grants: Vec::new(),
            grant_after,
            prov_ttl,
        }
    }

    /// Changes the lifetime of provisional grants made from now on.
    pub fn set_provisional_ttl(&mut self, prov_ttl: u64) {
        self.prov_ttl = prov_ttl;
    }

    pub fn grants(&self) -> &[Grant] {
        &self.grants
    }

    pub fn quarantined(&self, now: u64) -> bool {
        now < self.grant_after
    }

    /// Ends the start-up grace now (cold start: no live lease can exist).
    pub fn lift_quarantine(&mut self, now: u64) {
        self.grant_after = self.grant_after.min(now);
    }

    pub fn quarantine_until(&self) -> u64 {
        self.grant_after
    }

    pub fn expire(&mut self, now: u64) {
        self.grants.retain(|g| g.until > now);
    }

    /// Phase 1.
    pub fn request(
        &mut self,
        now: u64,
        from: NodeId,
        req: u64,
        key: &Key,
        ballot: Ballot,
    ) -> (Reply, Vec<Preemption>) {
        self.expire(now);
        if self.quarantined(now) {
            return (Reply::Deny(DenyReason::Quarantine, None), Vec::new());
        }
        // Leftovers of this node's earlier attempts on the same key.
        self.grants.retain(|g| !(g.holder == from && g.key == *key));
        let conflicting = |g: &Grant| g.holder != from && g.key.conflicts(key);
        if let Some(g) = self.grants.iter().find(|g| conflicting(g) && g.confirmed) {
            return (Reply::Deny(DenyReason::Held, Some(g.holder)), Vec::new());
        }
        if let Some(g) = self
            .grants
            .iter()
            .filter(|g| conflicting(g) && g.ballot < ballot)
            .min_by_key(|g| g.ballot)
        {
            return (Reply::Deny(DenyReason::Older, Some(g.holder)), Vec::new());
        }
        let mut preempted = Vec::new();
        self.grants.retain(|g| {
            let take = conflicting(g);
            if take {
                preempted.push(Preemption {
                    node: g.holder,
                    req: g.req,
                    ballot: g.ballot,
                });
            }
            !take
        });
        self.grants.push(Grant {
            key: key.clone(),
            holder: from,
            ballot,
            req,
            confirmed: false,
            until: now + self.prov_ttl,
        });
        (Reply::Grant, preempted)
    }

    fn find(&mut self, from: NodeId, key: &Key, ballot: Ballot) -> Option<&mut Grant> {
        self.grants
            .iter_mut()
            .find(|g| g.holder == from && g.key == *key && g.ballot == ballot)
    }

    /// Phase 2: the lease runs `lease` ms from now.
    pub fn confirm(
        &mut self,
        now: u64,
        from: NodeId,
        key: &Key,
        ballot: Ballot,
        lease: u64,
    ) -> Reply {
        self.expire(now);
        let rival = self
            .grants
            .iter()
            .find(|g| g.holder != from && g.confirmed && g.key.conflicts(key))
            .map(|g| g.holder);
        if let Some(h) = rival {
            // Cannot happen (a provisional grant excludes rivals), but a
            // voter must never confirm two conflicting leases.
            return Reply::Deny(DenyReason::Held, Some(h));
        }
        match self.find(from, key, ballot) {
            Some(g) => {
                g.confirmed = true;
                g.until = now + lease;
                Reply::Grant
            }
            None => Reply::Deny(DenyReason::NotHeld, None),
        }
    }

    pub fn renew(
        &mut self,
        now: u64,
        from: NodeId,
        key: &Key,
        ballot: Ballot,
        lease: u64,
    ) -> Reply {
        self.expire(now);
        match self.find(from, key, ballot) {
            Some(g) if g.confirmed => {
                g.until = now + lease;
                Reply::Grant
            }
            _ => Reply::Deny(DenyReason::NotHeld, None),
        }
    }

    pub fn release(&mut self, from: NodeId, key: &Key, ballot: Ballot) {
        self.grants
            .retain(|g| !(g.holder == from && g.key == *key && g.ballot == ballot));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Key {
        Key::new(s.as_bytes())
    }

    fn b(counter: u64, node: NodeId) -> Ballot {
        Ballot { counter, node }
    }

    const DENY_HELD: fn(NodeId) -> Reply = |n| Reply::Deny(DenyReason::Held, Some(n));

    #[test]
    fn grant_confirm_release() {
        let mut v = Voter::new(0, 100);
        assert_eq!(v.request(0, 1, 7, &k("f"), b(1, 1)).0, Reply::Grant);
        assert_eq!(v.confirm(1, 1, &k("f"), b(1, 1), 1000), Reply::Grant);
        assert_eq!(v.request(2, 2, 8, &k("f"), b(0, 2)).0, DENY_HELD(1));
        assert_eq!(v.request(2, 2, 8, &k("f/x"), b(0, 2)).0, DENY_HELD(1));
        assert_eq!(v.request(2, 2, 8, &k(""), b(0, 2)).0, DENY_HELD(1));
        assert_eq!(v.request(2, 2, 8, &k("g"), b(0, 2)).0, Reply::Grant);
        v.release(1, &k("f"), b(1, 1));
        assert_eq!(v.request(3, 2, 9, &k("f"), b(5, 2)).0, Reply::Grant);
    }

    #[test]
    fn older_ballot_preempts_provisional_grants() {
        let mut v = Voter::new(0, 100);
        assert_eq!(v.request(0, 2, 1, &k("f"), b(5, 2)).0, Reply::Grant);
        // Younger: refused in favour of the older provisional grant.
        assert_eq!(
            v.request(0, 3, 1, &k("f"), b(6, 3)).0,
            Reply::Deny(DenyReason::Older, Some(2))
        );
        // Older: takes over.
        let (r, pre) = v.request(0, 1, 4, &k("f"), b(5, 1));
        assert_eq!(r, Reply::Grant);
        assert_eq!(
            pre,
            [Preemption {
                node: 2,
                req: 1,
                ballot: b(5, 2)
            }]
        );
        assert_eq!(
            v.confirm(0, 2, &k("f"), b(5, 2), 1000),
            Reply::Deny(DenyReason::NotHeld, None)
        );
        assert_eq!(v.confirm(0, 1, &k("f"), b(5, 1), 1000), Reply::Grant);
        // Confirmed leases are never preempted.
        assert_eq!(v.request(0, 4, 1, &k("f"), b(0, 4)).0, DENY_HELD(1));
    }

    #[test]
    fn own_locks_do_not_conflict() {
        let mut v = Voter::new(0, 100);
        v.request(0, 1, 1, &k("d/f"), b(1, 1));
        v.confirm(0, 1, &k("d/f"), b(1, 1), 1000);
        assert_eq!(v.request(0, 1, 2, &k("d"), b(2, 1)).0, Reply::Grant);
        assert_eq!(v.confirm(0, 1, &k("d"), b(2, 1), 1000), Reply::Grant);
    }

    #[test]
    fn expiry_and_renewal() {
        let mut v = Voter::new(0, 50);
        v.request(0, 1, 1, &k("f"), b(1, 1));
        // Provisional grants lapse after prov_ttl.
        assert_eq!(
            v.confirm(60, 1, &k("f"), b(1, 1), 100),
            Reply::Deny(DenyReason::NotHeld, None)
        );
        v.request(100, 1, 2, &k("f"), b(2, 1));
        v.confirm(100, 1, &k("f"), b(2, 1), 100);
        assert_eq!(v.renew(150, 1, &k("f"), b(2, 1), 100), Reply::Grant);
        assert_eq!(v.request(240, 2, 1, &k("f"), b(0, 2)).0, DENY_HELD(1));
        // Renewed at 150 for 100 ms: free at 250.
        assert_eq!(v.request(250, 2, 1, &k("f"), b(0, 2)).0, Reply::Grant);
        assert_eq!(
            v.renew(250, 1, &k("f"), b(2, 1), 100),
            Reply::Deny(DenyReason::NotHeld, None)
        );
    }

    #[test]
    fn quarantine_after_start() {
        let mut v = Voter::new(1000, 50);
        assert_eq!(
            v.request(999, 1, 1, &k("f"), b(1, 1)).0,
            Reply::Deny(DenyReason::Quarantine, None)
        );
        assert_eq!(v.request(1000, 1, 1, &k("f"), b(1, 1)).0, Reply::Grant);
    }

    #[test]
    fn retry_replaces_own_leftovers() {
        let mut v = Voter::new(0, 100);
        v.request(0, 2, 1, &k("f"), b(3, 2));
        assert_eq!(v.request(0, 2, 2, &k("f"), b(3, 2)).0, Reply::Grant);
        assert_eq!(v.grants().len(), 1);
        assert_eq!(v.grants()[0].req, 2);
    }
}
