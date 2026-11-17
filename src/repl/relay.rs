//! Forwarding peers' journals: a node keeps the entries it applied from
//! other origins until every other data node has applied them, and serves
//! them to a node that cannot reach the origin. So nothing waits for a
//! node that is down: its entries (and the entries that depend on them)
//! travel through any node that has them.
//!
//! * Every data node advertises how far it has durably applied each
//!   origin's journal (`Positions`, about once a second).
//! * [`RelayLog`]: the entries this node applied from other origins, kept
//!   until every data node other than the origin advertised them applied.
//! * [`Inbox`]: per origin, the entries received (from the origin or a
//!   relay), applied strictly in sequence order, each only after its
//!   dependencies; duplicates are dropped by sequence number.
//!
//! Pure data structures, shared with the model check in
//! `crate::repl::relay_model`.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::NodeId;
use crate::net::wire::Entry;

use super::journal::Record;
use super::store::{decode, encode, Op, Table};

fn key(origin: NodeId, seq: u64) -> Vec<u8> {
    let mut k = origin.to_be_bytes().to_vec();
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

#[derive(Clone, Debug, Default)]
pub struct RelayLog {
    /// Data peers that may need entries (daemons excluded).
    peers: BTreeSet<NodeId>,
    entries: BTreeMap<(NodeId, u64), Record>,
    /// Per peer: what it advertised as durably applied, per origin.
    known: BTreeMap<NodeId, BTreeMap<NodeId, u64>>,
    new: BTreeSet<(NodeId, u64)>,
    dropped: BTreeSet<(NodeId, u64)>,
    /// Keep nothing beyond the entry being applied (the model check shows
    /// that this loses entries).
    #[cfg(test)]
    pub eager: bool,
}

impl RelayLog {
    pub fn load(
        peers: impl IntoIterator<Item = NodeId>,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> std::io::Result<RelayLog> {
        let mut r = RelayLog {
            peers: peers.into_iter().collect(),
            ..Default::default()
        };
        for (k, v) in rows {
            if k.len() != 10 {
                continue;
            }
            let origin = NodeId::from_be_bytes([k[0], k[1]]);
            let seq = u64::from_be_bytes(k[2..].try_into().expect("8 bytes"));
            r.entries.insert((origin, seq), decode(&v)?);
        }
        Ok(r)
    }

    /// `peer` stores data now.
    pub fn add_peer(&mut self, peer: NodeId) {
        self.peers.insert(peer);
    }

    /// `peer` stores no data: it needs no entries.
    pub fn drop_peer(&mut self, peer: NodeId) {
        if self.peers.remove(&peer) {
            self.known.remove(&peer);
            self.trim();
        }
    }

    /// Whether some data node other than `origin` may still need
    /// `(origin, seq)`.
    fn needed(&self, origin: NodeId, seq: u64) -> bool {
        #[cfg(test)]
        if self.eager {
            return false;
        }
        self.peers.iter().filter(|p| **p != origin).any(|p| {
            self.known
                .get(p)
                .and_then(|k| k.get(&origin))
                .copied()
                .unwrap_or(0)
                < seq
        })
    }

    /// This node applied (or gave up on) `origin`'s entry `seq`.
    pub fn keep(&mut self, origin: NodeId, seq: u64, rec: Record) {
        if self.needed(origin, seq) {
            self.entries.insert((origin, seq), rec);
            self.new.insert((origin, seq));
        }
    }

    /// `peer` advertised its durably applied positions (its own journal's
    /// position under its own id).
    pub fn learn(&mut self, peer: NodeId, positions: &[(NodeId, u64)]) {
        if !self.peers.contains(&peer) {
            return;
        }
        let k = self.known.entry(peer).or_default();
        let mut grew = false;
        for &(o, s) in positions {
            let e = k.entry(o).or_default();
            if s > *e {
                *e = s;
                grew = true;
            }
        }
        if grew {
            self.trim();
        }
    }

    /// What `peer` advertised for `origin`.
    /// Whether `peer` told its positions since start.
    pub fn heard_from(&self, peer: NodeId) -> bool {
        self.known.contains_key(&peer)
    }

    pub fn position(&self, peer: NodeId, origin: NodeId) -> u64 {
        self.known
            .get(&peer)
            .and_then(|k| k.get(&origin))
            .copied()
            .unwrap_or(0)
    }

    fn trim(&mut self) {
        let gone: Vec<(NodeId, u64)> = self
            .entries
            .keys()
            .filter(|(o, s)| !self.needed(*o, *s))
            .copied()
            .collect();
        for k in gone {
            self.entries.remove(&k);
            if !self.new.remove(&k) {
                self.dropped.insert(k);
            }
        }
    }

    /// Up to `max` kept entries of `origin` after `after`, in order.
    pub fn serve(&self, origin: NodeId, after: u64, max: usize) -> Vec<(u64, Record)> {
        self.entries
            .range((origin, after.saturating_add(1))..=(origin, u64::MAX))
            .take(max)
            .map(|((_, s), r)| (*s, r.clone()))
            .collect()
    }

    pub fn contains(&self, origin: NodeId, seq: u64) -> bool {
        self.entries.contains_key(&(origin, seq))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Kept entries and known positions, in a fixed order (for the model
    /// check's state digest).
    #[cfg(test)]
    pub fn dump(&self) -> String {
        format!(
            "{:?}{:?}",
            self.entries.keys().collect::<Vec<_>>(),
            self.known
        )
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn take_changed(&mut self) -> Vec<Op> {
        let mut ops = Vec::new();
        for k in std::mem::take(&mut self.new) {
            if let Some(r) = self.entries.get(&k) {
                ops.push((Table::Relay, key(k.0, k.1), Some(encode(r))));
            }
        }
        for k in std::mem::take(&mut self.dropped) {
            ops.push((Table::Relay, key(k.0, k.1), None));
        }
        ops
    }
}

/// An entry received and not applied yet; `via` is the node it came from
/// (the origin, or a relay).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Queued {
    pub deps: Vec<(NodeId, u64)>,
    pub entry: Entry,
    pub via: NodeId,
}

/// One origin's received entries.
#[derive(Clone, Debug, Default)]
pub struct Inbox {
    pub queue: BTreeMap<u64, Queued>,
    pub worker: bool,
}

impl Inbox {
    /// Adds `seq` (a copy that came from the origin itself replaces one
    /// that came through a relay; otherwise the first copy stays).
    pub fn insert(&mut self, origin: NodeId, seq: u64, q: Queued) {
        match self.queue.get(&seq) {
            Some(old) if old.via == origin || q.via != origin => {}
            _ => {
                self.queue.insert(seq, q);
            }
        }
    }

    /// The next entry to apply: `applied + 1` once every dependency is
    /// applied (`applied_all`; this node's own entries always are). Drops
    /// duplicates of applied entries.
    pub fn next(
        &mut self,
        me: NodeId,
        applied: u64,
        applied_all: &BTreeMap<NodeId, u64>,
    ) -> Option<(u64, Queued)> {
        while self
            .queue
            .first_key_value()
            .is_some_and(|(s, _)| *s <= applied)
        {
            self.queue.pop_first();
        }
        let (&s, q) = self.queue.first_key_value()?;
        if s != applied + 1 {
            return None;
        }
        let ready = q
            .deps
            .iter()
            .all(|(n, k)| *n == me || applied_all.get(n).copied().unwrap_or(0) >= *k);
        ready.then(|| self.queue.pop_first().expect("checked"))
    }

    /// Whether `seq` is queued.
    pub fn has(&self, seq: u64) -> bool {
        self.queue.contains_key(&seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(n: u8) -> Record {
        Record {
            deps: vec![],
            entry: Entry::Remove {
                id: None,
                path: vec![n],
                dir: false,
                version: None,
                lineage: Vec::new(),
            },
        }
    }

    #[test]
    fn kept_until_every_other_data_node_has_it() {
        // Node 2's view; data peers 1 (the origin) and 3.
        let mut r = RelayLog::load([1, 3], vec![]).unwrap();
        r.keep(1, 1, rec(1));
        r.keep(1, 2, rec(2));
        assert_eq!(r.serve(1, 0, 10).len(), 2);
        assert_eq!(r.serve(1, 1, 10)[0].0, 2);
        r.learn(1, &[(1, 9)]); // the origin's own position does not count
        assert_eq!(r.len(), 2);
        r.learn(3, &[(1, 1)]);
        assert_eq!(r.len(), 1);
        r.learn(3, &[(1, 0)]); // stale: ignored
        assert!(r.contains(1, 2));
        let ops = r.take_changed();
        let back = RelayLog::load(
            [1, 3],
            ops.into_iter()
                .filter_map(|(_, k, v)| v.map(|v| (k, v)))
                .collect(),
        )
        .unwrap();
        assert!(back.contains(1, 2) && !back.contains(1, 1));
        r.drop_peer(3);
        assert!(r.is_empty());
        // Two data nodes: nobody else needs anything.
        let mut two = RelayLog::load([1], vec![]).unwrap();
        two.keep(1, 1, rec(1));
        assert!(two.is_empty());
    }

    #[test]
    fn inbox_orders_dedups_and_waits_for_dependencies() {
        let q = |via, deps: Vec<(NodeId, u64)>| Queued {
            deps,
            entry: rec(0).entry,
            via,
        };
        let mut ib = Inbox::default();
        let mut applied = BTreeMap::new();
        ib.insert(1, 2, q(3, vec![]));
        assert!(ib.next(9, 0, &applied).is_none(), "1 is missing");
        ib.insert(1, 1, q(3, vec![(4, 5), (9, 100)]));
        ib.insert(1, 1, q(1, vec![(4, 5), (9, 100)])); // the origin's copy wins
        assert_eq!(ib.queue[&1].via, 1);
        ib.insert(1, 1, q(3, vec![]));
        assert_eq!(ib.queue[&1].via, 1);
        assert!(
            ib.next(9, 0, &applied).is_none(),
            "waits for node 4's entry 5"
        );
        applied.insert(4, 5);
        assert_eq!(ib.next(9, 0, &applied).unwrap().0, 1);
        ib.insert(1, 1, q(1, vec![])); // a resend
        assert_eq!(ib.next(9, 1, &applied).unwrap().0, 2);
        assert!(ib.queue.is_empty());
    }
}
