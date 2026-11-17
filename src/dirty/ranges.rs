//! A set of `u64` as disjoint, non-adjacent half-open ranges.

use std::collections::BTreeMap;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Ranges(BTreeMap<u64, u64>);

impl Ranges {
    /// Adds `[a, b)`.
    pub fn insert(&mut self, mut a: u64, mut b: u64) {
        if a >= b {
            return;
        }
        // Merge with a range starting at or before `a` that reaches it.
        if let Some((&s, &e)) = self.0.range(..=a).next_back() {
            if e >= a {
                a = s;
                b = b.max(e);
            }
        }
        // Absorb every range starting inside [a, b].
        let inside: Vec<(u64, u64)> = self.0.range(a..=b).map(|(&s, &e)| (s, e)).collect();
        for (s, e) in inside {
            self.0.remove(&s);
            b = b.max(e);
        }
        self.0.insert(a, b);
    }

    /// Removes everything at or above `end`.
    pub fn truncate(&mut self, end: u64) {
        let above: Vec<u64> = self.0.range(end..).map(|(&s, _)| s).collect();
        for s in above {
            self.0.remove(&s);
        }
        if let Some((_, e)) = self.0.range_mut(..end).next_back() {
            *e = (*e).min(end);
        }
    }

    /// Number of elements.
    pub fn len(&self) -> u64 {
        self.0.iter().map(|(s, e)| e - s).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.0.iter().map(|(&s, &e)| (s, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn v(r: &Ranges) -> Vec<(u64, u64)> {
        r.iter().collect()
    }

    #[test]
    fn merges_overlapping_and_adjacent() {
        let mut r = Ranges::default();
        r.insert(5, 7);
        r.insert(1, 2);
        r.insert(2, 3);
        assert_eq!(v(&r), [(1, 3), (5, 7)]);
        r.insert(3, 5);
        assert_eq!(v(&r), [(1, 7)]);
        r.insert(0, 10);
        assert_eq!(v(&r), [(0, 10)]);
        r.insert(4, 4);
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn truncate_cuts_and_drops() {
        let mut r = Ranges::default();
        r.insert(0, 3);
        r.insert(5, 9);
        r.insert(12, 13);
        r.truncate(7);
        assert_eq!(v(&r), [(0, 3), (5, 7)]);
        r.truncate(5);
        assert_eq!(v(&r), [(0, 3)]);
        r.truncate(0);
        assert!(r.is_empty());
    }

    #[test]
    fn matches_a_plain_set() {
        // Deterministic pseudo-random operations against BTreeSet.
        let mut x: u64 = 0x9e3779b97f4a7c15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..200 {
            let mut r = Ranges::default();
            let mut s = BTreeSet::new();
            for _ in 0..30 {
                let a = next() % 64;
                let b = a + next() % 8;
                if next() % 5 == 0 {
                    r.truncate(a);
                    s.retain(|&i| i < a);
                } else {
                    r.insert(a, b);
                    s.extend(a..b);
                }
                let back: BTreeSet<u64> = r.iter().flat_map(|(a, b)| a..b).collect();
                assert_eq!(back, s);
                assert_eq!(r.len(), s.len() as u64);
                // Disjoint and non-adjacent.
                let w: Vec<_> = r.iter().collect();
                assert!(w.windows(2).all(|p| p[0].1 < p[1].0));
            }
        }
    }
}
