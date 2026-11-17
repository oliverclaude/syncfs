//! Lock handoff: what a new holder must have applied first.
//!
//! Replication is asynchronous, so the next holder of a lock may not have
//! applied what the previous holder changed under it yet (a file it created,
//! removed or renamed). A release therefore carries the releasing node's
//! journal head; every voter remembers, per key, the newest head of each
//! node that released it, and a grant reports the heads recorded for every
//! conflicting key (the key, its ancestors and everything below it). The new
//! holder waits until it has applied those origins' journals that far
//! before it changes anything.
//!
//! Memory is bounded: past [`MAX_KEYS`] keys the oldest are folded into a
//! floor that applies to every key (more waiting, never less).

use std::collections::{BTreeMap, VecDeque};

use super::Key;
use crate::config::NodeId;

pub const MAX_KEYS: usize = 65_536;

#[derive(Default)]
pub struct Handoff {
    keys: BTreeMap<Key, BTreeMap<NodeId, u64>>,
    /// Keys in the order they were first recorded, for eviction.
    order: VecDeque<Key>,
    floor: BTreeMap<NodeId, u64>,
}

fn raise(m: &mut BTreeMap<NodeId, u64>, node: NodeId, head: u64) {
    let e = m.entry(node).or_default();
    *e = (*e).max(head);
}

impl Handoff {
    /// `node` released `key` with its journal at `head`.
    pub fn record(&mut self, key: &Key, node: NodeId, head: u64) {
        if head == 0 {
            return;
        }
        if !self.keys.contains_key(key) {
            self.order.push_back(key.clone());
        }
        raise(self.keys.entry(key.clone()).or_default(), node, head);
        while self.keys.len() > MAX_KEYS {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            for (n, h) in self.keys.remove(&old).unwrap_or_default() {
                raise(&mut self.floor, n, h);
            }
        }
    }

    /// The newest heads recorded for keys that conflict with `key`.
    pub fn after(&self, key: &Key) -> Vec<(NodeId, u64)> {
        let mut out = self.floor.clone();
        let mut add = |m: &BTreeMap<NodeId, u64>| {
            for (&n, &h) in m {
                raise(&mut out, n, h);
            }
        };
        // Ancestors (the root included) and the key itself.
        let bytes = key.as_bytes();
        let mut ends: Vec<usize> = bytes
            .iter()
            .enumerate()
            .filter(|(_, b)| **b == b'/')
            .map(|(i, _)| i)
            .collect();
        ends.insert(0, 0);
        ends.push(bytes.len());
        for end in ends {
            if let Some(m) = self.keys.get(&Key::new(&bytes[..end])) {
                add(m);
            }
        }
        // Below it: keys starting with `key/` (sorted between `key/` and
        // `key0`, `0` being the byte after `/`).
        let mut prefix = bytes.to_vec();
        if !prefix.is_empty() {
            prefix.push(b'/');
        }
        let mut upper = bytes.to_vec();
        upper.push(b'0');
        for (k, m) in self.keys.range(key.clone()..) {
            let kb = k.as_bytes();
            if !bytes.is_empty() && kb >= upper.as_slice() {
                break;
            }
            if kb.starts_with(&prefix) && kb != bytes {
                add(m);
            }
        }
        out.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Key {
        Key::new(s.as_bytes())
    }

    #[test]
    fn conflicting_keys_report_their_heads() {
        let mut h = Handoff::default();
        h.record(&k("a/f"), 2, 10);
        h.record(&k("a/f"), 2, 7);
        h.record(&k("a/g"), 3, 5);
        h.record(&k("a"), 1, 4);
        h.record(&k("b/f"), 3, 9);
        h.record(&k("a-x"), 5, 1);
        h.record(&k("a.b/c"), 5, 2);
        assert_eq!(h.after(&k("a/f")), vec![(1, 4), (2, 10)]);
        assert_eq!(h.after(&k("a")), vec![(1, 4), (2, 10), (3, 5)]);
        assert_eq!(h.after(&k("")), vec![(1, 4), (2, 10), (3, 9), (5, 2)]);
        assert_eq!(h.after(&k("a/fx")), vec![(1, 4)]);
        assert_eq!(h.after(&k("c")), vec![]);
        h.record(&k(""), 4, 1);
        assert_eq!(h.after(&k("c")), vec![(4, 1)]);
    }

    #[test]
    fn old_keys_fold_into_the_floor() {
        let mut h = Handoff::default();
        for i in 0..MAX_KEYS + 2 {
            h.record(&k(&format!("f{i}")), 1, i as u64 + 1);
        }
        assert_eq!(h.keys.len(), MAX_KEYS);
        // f0 and f1 were folded: their heads (1, 2) now apply everywhere.
        assert_eq!(h.after(&k("other")), vec![(1, 2)]);
    }
}
