//! This node's metadata journal: an ordered, persisted list of the changes
//! it made, sent to every data peer in order. Each entry records the
//! journal positions of other origins this node had applied when it made
//! it (`deps`), so receivers apply causally related changes in order.
//!
//! Entries are sent only once durable (after the flush that stored them),
//! so a crash never makes a node reuse a sequence number its peers saw.
//! An entry is dropped once every data peer has acknowledged it.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::config::NodeId;
use crate::net::wire::Entry;

use super::store::{decode, encode, Op, Table};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub deps: Vec<(NodeId, u64)>,
    pub entry: Entry,
}

#[derive(Clone, Debug, Default)]
pub struct Journal {
    /// Next sequence number to assign.
    next: u64,
    entries: BTreeMap<u64, Record>,
    /// Highest sequence number known durable.
    durable: u64,
    /// Highest assigned when the last flush was prepared.
    staged: u64,
    /// Per data peer: highest acknowledged.
    acked: BTreeMap<NodeId, u64>,
    new: BTreeSet<u64>,
    dropped: BTreeSet<u64>,
    acks_changed: bool,
}

const NEXT_KEY: &[u8] = b"journal.next";

fn ack_key(peer: NodeId) -> Vec<u8> {
    format!("acked.{peer}").into_bytes()
}

impl Journal {
    pub fn load(
        peers: impl IntoIterator<Item = NodeId>,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        meta_next: Option<Vec<u8>>,
        positions: &[(Vec<u8>, Vec<u8>)],
    ) -> std::io::Result<Journal> {
        let mut j = Journal::default();
        for (k, v) in entries {
            let Ok(seq) = <[u8; 8]>::try_from(k.as_slice()) else {
                continue;
            };
            j.entries.insert(u64::from_be_bytes(seq), decode(&v)?);
        }
        let stored_next: u64 = meta_next.map(|b| decode(&b)).transpose()?.unwrap_or(1);
        let last = j.entries.keys().next_back().copied().unwrap_or(0);
        j.next = stored_next.max(last + 1).max(1);
        j.durable = j.next - 1;
        j.staged = j.durable;
        for p in peers {
            let acked = positions
                .iter()
                .find(|(k, _)| *k == ack_key(p))
                .map(|(_, v)| decode::<u64>(v))
                .transpose()?
                .unwrap_or(0);
            j.acked.insert(p, acked);
        }
        Ok(j)
    }

    pub fn append(&mut self, deps: Vec<(NodeId, u64)>, entry: Entry) -> u64 {
        let seq = self.next.max(1);
        self.next = seq + 1;
        self.entries.insert(seq, Record { deps, entry });
        self.new.insert(seq);
        seq
    }

    /// Durable entries after `after`, at most `max`.
    pub fn sendable(&self, after: u64, max: usize) -> Vec<(u64, Record)> {
        if after >= self.durable {
            return Vec::new();
        }
        self.entries
            .range(after + 1..=self.durable)
            .take(max)
            .map(|(s, r)| (*s, r.clone()))
            .collect()
    }

    /// Every kept entry, in order.
    pub fn entries(&self) -> impl Iterator<Item = (u64, &Record)> + '_ {
        self.entries.iter().map(|(s, r)| (*s, r))
    }

    pub fn durable(&self) -> u64 {
        self.durable
    }

    pub fn last(&self) -> u64 {
        self.next.saturating_sub(1)
    }

    pub fn acked(&self, peer: NodeId) -> u64 {
        self.acked.get(&peer).copied().unwrap_or(0)
    }

    /// Peers this journal is replicated to.
    pub fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.acked.keys().copied()
    }

    /// Starts replicating to `peer` (a node that now stores data), from
    /// what is still kept.
    pub fn add_peer(&mut self, peer: NodeId) {
        if let std::collections::btree_map::Entry::Vacant(e) = self.acked.entry(peer) {
            e.insert(0);
            self.acks_changed = true;
        }
    }

    pub fn drop_peer(&mut self, peer: NodeId) {
        if self.acked.remove(&peer).is_some() {
            self.acks_changed = true;
            self.trim();
        }
    }

    pub fn ack(&mut self, peer: NodeId, seq: u64) {
        let Some(a) = self.acked.get_mut(&peer) else {
            return;
        };
        let seq = seq.min(self.durable);
        if seq > *a {
            *a = seq;
            self.acks_changed = true;
            self.trim();
        }
    }

    fn trim(&mut self) {
        let low = self.acked.values().copied().min().unwrap_or(self.durable);
        let gone: Vec<u64> = self.entries.range(..=low).map(|(s, _)| *s).collect();
        for s in gone {
            self.entries.remove(&s);
            if !self.new.remove(&s) {
                self.dropped.insert(s);
            }
        }
    }

    /// Entries kept (not yet acknowledged by every peer).
    pub fn queued(&self) -> usize {
        self.entries.len()
    }

    /// Unacknowledged entries per peer.
    pub fn lag(&self) -> BTreeMap<NodeId, u64> {
        self.acked
            .iter()
            .map(|(p, a)| (*p, self.durable.saturating_sub(*a)))
            .collect()
    }

    /// Store operations for the next flush. Call [`committed`]
    /// (Self::committed) with the returned position once they are durable.
    pub fn take_changed(&mut self) -> (Vec<Op>, u64) {
        let mut ops = Vec::new();
        for s in std::mem::take(&mut self.new) {
            if let Some(r) = self.entries.get(&s) {
                ops.push((Table::Journal, s.to_be_bytes().to_vec(), Some(encode(r))));
            }
        }
        for s in std::mem::take(&mut self.dropped) {
            ops.push((Table::Journal, s.to_be_bytes().to_vec(), None));
        }
        if std::mem::take(&mut self.acks_changed) {
            for (p, a) in &self.acked {
                ops.push((Table::Positions, ack_key(*p), Some(encode(a))));
            }
        }
        let upto = self.last();
        if upto > self.staged {
            ops.push((Table::Meta, NEXT_KEY.to_vec(), Some(encode(&self.next))));
            self.staged = upto;
        }
        (ops, upto)
    }

    /// Everything up to `upto` is durable.
    pub fn committed(&mut self, upto: u64) {
        self.durable = self.durable.max(upto);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::wire::FileId;

    fn e(n: u8) -> Entry {
        Entry::Remove {
            id: Some(FileId([n; 16])),
            path: vec![n],
            dir: false,
            version: None,
            lineage: Vec::new(),
        }
    }

    fn rows(ops: &[Op], t: Table) -> Vec<(Vec<u8>, Vec<u8>)> {
        ops.iter()
            .filter(|(x, _, v)| *x == t && v.is_some())
            .map(|(_, k, v)| (k.clone(), v.clone().unwrap()))
            .collect()
    }

    #[test]
    fn only_durable_entries_are_sent_and_acks_trim() {
        let mut j = Journal::load([2, 3], vec![], None, &[]).unwrap();
        assert_eq!(j.append(vec![], e(1)), 1);
        assert_eq!(j.append(vec![(2, 5)], e(2)), 2);
        assert!(j.sendable(0, 10).is_empty());
        let (ops, upto) = j.take_changed();
        assert_eq!(upto, 2);
        j.committed(upto);
        assert_eq!(j.sendable(0, 10).len(), 2);
        assert_eq!(j.sendable(1, 10)[0].1.deps, [(2, 5)]);
        j.ack(2, 2);
        assert_eq!(j.queued(), 2, "node 3 has not acknowledged");
        j.ack(3, 1);
        assert_eq!(j.queued(), 1);
        assert_eq!(j.lag()[&3], 1);
        let (more, _) = j.take_changed();
        // Reload: sequence numbers continue, the unacknowledged entry stays.
        let mut all = ops.clone();
        all.extend(more);
        let journal: Vec<_> = rows(&all, Table::Journal)
            .into_iter()
            .filter(|(k, _)| {
                all.iter()
                    .all(|(t, k2, v)| !(*t == Table::Journal && k2 == k && v.is_none()))
            })
            .collect();
        let next = rows(&all, Table::Meta).pop().map(|(_, v)| v);
        let back = Journal::load([2, 3], journal, next, &rows(&all, Table::Positions)).unwrap();
        assert_eq!(back.last(), 2);
        assert_eq!(back.acked(3), 1);
        assert_eq!(back.sendable(1, 10).len(), 1);
    }
}
