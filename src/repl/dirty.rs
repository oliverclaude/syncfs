//! Dirty-block bitmaps: per file, per peer, the blocks that peer still
//! has to pull, tagged with the version whose session changed them.
//!
//! * A local write marks its blocks for every data peer.
//! * A peer applying a `Data` journal entry pulls "its" blocks; they move
//!   to *in flight* and are dropped once the peer acknowledges a later
//!   journal position (it applies entries in order and acknowledges only
//!   durable ones). If the connection drops first, they come back.
//! * A peer that already has a version (it caught up by hash comparison)
//!   discards the marks of that version and older.
//!
//! Persisted per file: pending and in-flight marks together, so nothing is
//! lost when the node restarts with transfers in flight.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::config::NodeId;
use crate::net::wire::{FileId, Version};

use super::ranges::Ranges;
use super::store::{decode, encode, Op, Table};

type ByVersion = BTreeMap<Version, Ranges>;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Persisted(Vec<(NodeId, Vec<(Version, Ranges)>)>);

#[derive(Clone, Debug, Default)]
pub struct Dirty {
    /// Peers that get marks.
    peers: BTreeSet<NodeId>,
    pending: HashMap<FileId, BTreeMap<NodeId, ByVersion>>,
    inflight: HashMap<(NodeId, FileId), ByVersion>,
    changed: BTreeSet<FileId>,
}

fn merge(into: &mut ByVersion, from: ByVersion) {
    for (v, r) in from {
        into.entry(v).or_default().extend(&r);
    }
}

impl Dirty {
    pub fn new(peers: impl IntoIterator<Item = NodeId>) -> Dirty {
        Dirty {
            peers: peers.into_iter().collect(),
            ..Default::default()
        }
    }

    pub fn load(&mut self, rows: Vec<(Vec<u8>, Vec<u8>)>) -> std::io::Result<()> {
        for (k, v) in rows {
            let Ok(id) = <[u8; 16]>::try_from(k.as_slice()) else {
                continue;
            };
            let Persisted(per_peer) = decode(&v)?;
            let e = self.pending.entry(FileId(id)).or_default();
            for (peer, vs) in per_peer {
                merge(e.entry(peer).or_default(), vs.into_iter().collect());
            }
        }
        Ok(())
    }

    /// Stops tracking `peer` (it stores no data).
    pub fn drop_peer(&mut self, peer: NodeId) {
        if !self.peers.remove(&peer) {
            return;
        }
        for (id, per) in self.pending.iter_mut() {
            if per.remove(&peer).is_some() {
                self.changed.insert(*id);
            }
        }
        self.pending.retain(|_, per| !per.is_empty());
        let gone: Vec<_> = self
            .inflight
            .keys()
            .filter(|(p, _)| *p == peer)
            .copied()
            .collect();
        for k in gone {
            self.inflight.remove(&k);
            self.changed.insert(k.1);
        }
    }

    pub fn peers(&self) -> &BTreeSet<NodeId> {
        &self.peers
    }

    /// Blocks `[a, b)` of `id` changed in `version`'s session.
    pub fn mark(&mut self, id: FileId, version: Version, a: u64, b: u64) {
        if a >= b || self.peers.is_empty() {
            return;
        }
        let per = self.pending.entry(id).or_default();
        for &p in &self.peers {
            per.entry(p)
                .or_default()
                .entry(version)
                .or_default()
                .insert(a, b);
        }
        self.changed.insert(id);
    }

    /// Marks every block below `end` (a whole file of unknown changes).
    pub fn mark_all(&mut self, id: FileId, version: Version, end: u64) {
        self.mark(id, version, 0, end.max(1));
    }

    /// The file now has `blocks` blocks: marks past the end go.
    pub fn truncate(&mut self, id: FileId, blocks: u64) {
        let Some(per) = self.pending.get_mut(&id) else {
            return;
        };
        for vs in per.values_mut() {
            for r in vs.values_mut() {
                r.truncate(blocks);
            }
            vs.retain(|_, r| !r.is_empty());
        }
        per.retain(|_, vs| !vs.is_empty());
        if per.is_empty() {
            self.pending.remove(&id);
        }
        self.changed.insert(id);
    }

    /// The file is gone: nothing to send.
    pub fn forget(&mut self, id: FileId) {
        if self.pending.remove(&id).is_some() {
            self.changed.insert(id);
        }
        let keys: Vec<_> = self
            .inflight
            .keys()
            .filter(|(_, f)| *f == id)
            .copied()
            .collect();
        for k in keys {
            self.inflight.remove(&k);
            self.changed.insert(id);
        }
    }

    /// Up to `max` blocks `peer` is missing of `id`, moved in flight.
    /// Returns them and whether more are left.
    pub fn take_for_pull(&mut self, peer: NodeId, id: FileId, max: u64) -> (Ranges, bool) {
        let Some(vs) = self.pending.get_mut(&id).and_then(|per| per.get_mut(&peer)) else {
            return (Ranges::default(), false);
        };
        // The lowest `max` blocks of the union over versions.
        let mut union = Ranges::default();
        for r in vs.values() {
            union.extend(r);
        }
        let out = union.take_front(max);
        let fl = self.inflight.entry((peer, id)).or_default();
        for (v, r) in vs.iter_mut() {
            let moved = r.subtract(&out);
            if !moved.is_empty() {
                fl.entry(*v).or_default().extend(&moved);
            }
        }
        vs.retain(|_, r| !r.is_empty());
        let more = !vs.is_empty();
        if let Some(per) = self.pending.get_mut(&id) {
            per.retain(|_, vs| !vs.is_empty());
            if per.is_empty() {
                self.pending.remove(&id);
            }
        }
        (out, more)
    }

    /// `peer` acknowledged a journal position after its pulls: what it
    /// pulled is durable there.
    pub fn acked(&mut self, peer: NodeId) {
        let keys: Vec<_> = self
            .inflight
            .keys()
            .filter(|(p, _)| *p == peer)
            .copied()
            .collect();
        for k in keys {
            self.inflight.remove(&k);
            self.changed.insert(k.1);
        }
    }

    /// `peer`'s connection dropped: in-flight blocks are pending again.
    pub fn restore(&mut self, peer: NodeId) {
        let keys: Vec<_> = self
            .inflight
            .keys()
            .filter(|(p, _)| *p == peer)
            .copied()
            .collect();
        for k in keys {
            if let Some(vs) = self.inflight.remove(&k) {
                merge(
                    self.pending
                        .entry(k.1)
                        .or_default()
                        .entry(peer)
                        .or_default(),
                    vs,
                );
            }
        }
    }

    /// `peer` has `version` of `id`: marks of that version and older go.
    pub fn discard(&mut self, peer: NodeId, id: FileId, version: Version) {
        let Some(per) = self.pending.get_mut(&id) else {
            return;
        };
        if let Some(vs) = per.get_mut(&peer) {
            vs.retain(|v, _| *v > version);
            if vs.is_empty() {
                per.remove(&peer);
            }
        }
        if per.is_empty() {
            self.pending.remove(&id);
        }
        self.changed.insert(id);
    }

    /// Whether `peer` has nothing pending or in flight for `id`.
    pub fn clean_for(&self, peer: NodeId, id: &FileId) -> bool {
        !self
            .pending
            .get(id)
            .is_some_and(|per| per.contains_key(&peer))
            && !self.inflight.contains_key(&(peer, *id))
    }

    pub fn files(&self) -> usize {
        let mut ids: BTreeSet<&FileId> = self.pending.keys().collect();
        ids.extend(self.inflight.keys().map(|(_, f)| f));
        ids.len()
    }

    /// Blocks pending per peer.
    pub fn blocks_per_peer(&self) -> BTreeMap<NodeId, u64> {
        let mut out: BTreeMap<NodeId, u64> = self.peers.iter().map(|p| (*p, 0)).collect();
        for per in self.pending.values() {
            for (p, vs) in per {
                let mut all = Ranges::default();
                for r in vs.values() {
                    all.extend(r);
                }
                *out.entry(*p).or_default() += all.len();
            }
        }
        out
    }

    /// Everything pending and in flight, in a fixed order (for the model
    /// check's state digest).
    #[cfg(test)]
    pub fn dump(&self) -> String {
        let mut pending: Vec<String> = self
            .pending
            .iter()
            .flat_map(|(f, per)| per.iter().map(move |(p, vs)| format!("{f}/{p}:{vs:?}")))
            .collect();
        pending.sort();
        let mut inflight: Vec<String> = self
            .inflight
            .iter()
            .map(|((p, f), vs)| format!("{f}/{p}:{vs:?}"))
            .collect();
        inflight.sort();
        format!("{pending:?}{inflight:?}")
    }

    pub fn inflight_blocks(&self) -> u64 {
        self.inflight
            .values()
            .flat_map(|vs| vs.values())
            .map(Ranges::len)
            .sum()
    }

    pub fn take_changed(&mut self) -> Vec<Op> {
        std::mem::take(&mut self.changed)
            .into_iter()
            .map(|id| {
                let mut all: BTreeMap<NodeId, ByVersion> =
                    self.pending.get(&id).cloned().unwrap_or_default();
                for ((p, f), vs) in &self.inflight {
                    if *f == id {
                        merge(all.entry(*p).or_default(), vs.clone());
                    }
                }
                all.retain(|_, vs| !vs.is_empty());
                let v = (!all.is_empty()).then(|| {
                    encode(&Persisted(
                        all.into_iter()
                            .map(|(p, vs)| (p, vs.into_iter().collect()))
                            .collect(),
                    ))
                });
                (Table::Dirty, id.0.to_vec(), v)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: FileId = FileId([3; 16]);
    fn v(seq: u64) -> Version {
        Version {
            seq,
            node: 1,
            disk: 1,
        }
    }
    fn blocks(r: &Ranges) -> Vec<(u64, u64)> {
        r.iter().collect()
    }

    #[test]
    fn pull_ack_restore_cycle() {
        let mut d = Dirty::new([2, 3]);
        d.mark(F, v(1), 0, 4);
        d.mark(F, v(2), 2, 6);
        let (got, more) = d.take_for_pull(2, F, 3);
        assert_eq!(blocks(&got), [(0, 3)]);
        assert!(more);
        let (got, more) = d.take_for_pull(2, F, 100);
        assert_eq!(blocks(&got), [(3, 6)]);
        assert!(!more);
        assert!(!d.clean_for(2, &F));
        // The connection drops: everything is pending again.
        d.restore(2);
        let (got, _) = d.take_for_pull(2, F, 100);
        assert_eq!(blocks(&got), [(0, 6)]);
        d.acked(2);
        assert!(d.clean_for(2, &F));
        assert!(!d.clean_for(3, &F));
        assert_eq!(d.blocks_per_peer()[&3], 6);
    }

    #[test]
    fn discard_keeps_newer_versions() {
        let mut d = Dirty::new([2]);
        d.mark(F, v(1), 0, 2);
        d.mark(F, v(3), 5, 6);
        d.discard(2, F, v(2));
        let (got, _) = d.take_for_pull(2, F, 100);
        assert_eq!(blocks(&got), [(5, 6)]);
    }

    #[test]
    fn persisted_with_inflight() {
        let mut d = Dirty::new([2]);
        d.mark(F, v(1), 0, 8);
        d.take_for_pull(2, F, 3);
        let rows: Vec<_> = d
            .take_changed()
            .into_iter()
            .filter_map(|(_, k, v)| v.map(|v| (k, v)))
            .collect();
        let mut back = Dirty::new([2]);
        back.load(rows).unwrap();
        let (got, _) = back.take_for_pull(2, F, 100);
        assert_eq!(blocks(&got), [(0, 8)]);
        back.truncate(F, 2);
        back.restore(2);
        assert_eq!(
            back.blocks_per_peer()[&2],
            8,
            "in-flight marks are not truncated"
        );
        back.drop_peer(2);
        assert_eq!(back.files(), 0);
    }
}
