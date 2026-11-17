//! Exhaustive model check of file versions across lock sessions, using the
//! real [`VersionTable`], [`version::next`] and [`version::target`].
//!
//! One file. A lock session is one atomic step (mutual exclusion is the
//! lock protocol's job, checked in `crate::lock::model`): the writer
//! collects the entries of a majority of voters, runs the freshness check
//! against the nodes it can reach (catching up from a source, else from
//! the newest copy any reachable node has, refusing if that is older than
//! the newest committed version the voters report), announces its version to the granting
//! voters, maybe writes, and its release reaches any subset of voters (none:
//! it crashed). Replication copies a newer version between mount nodes and
//! its holders announcement reaches any subset of voters. Voter tables are
//! durable (see the README on the store).
//!
//! Invariants:
//! * every written version is newer than every version any voter holds
//!   committed, and
//! * the copy a session writes on is at least as new as every committed
//!   version (no committed version is lost by writing over an older copy).

use std::collections::{HashSet, VecDeque};

use crate::config::NodeId;
use crate::net::wire::{FileId, Outcome, Version};

use super::version::{self, VersionTable};

const F: FileId = FileId([7; 16]);

#[derive(Clone, Debug)]
struct State {
    voters: Vec<VersionTable>,
    /// Content version per node (daemons: always None).
    local: Vec<Option<Version>>,
    sessions: u8,
}

impl State {
    /// A digest of the state (keeps the visited set small).
    fn key(&self) -> [u8; 16] {
        let tables: Vec<_> = self.voters.iter().map(|v| v.record(&F)).collect();
        let s = format!("{tables:?}{:?}{}", self.local, self.sessions);
        let mut k = [0; 16];
        k.copy_from_slice(&blake3::hash(s.as_bytes()).as_bytes()[..16]);
        k
    }
}

struct Model {
    n: usize,
    mounts: Vec<usize>,
    quorum: usize,
    max_sessions: u8,
    /// The fallback asks every reachable node, not only the ones the voters
    /// name (turned off to show the check catches it).
    ask_everyone: bool,
}

fn id(i: usize) -> NodeId {
    i as NodeId + 1
}

fn subsets(n: usize) -> impl Iterator<Item = Vec<usize>> {
    (0u32..1 << n).map(move |m| (0..n).filter(|i| m & (1 << i) != 0).collect())
}

fn max_committed(s: &State) -> Option<Version> {
    s.voters.iter().filter_map(|v| v.get(&F)?.committed).max()
}

impl Model {
    fn check(&self) -> usize {
        let init = State {
            voters: (0..self.n)
                .map(|_| VersionTable::new(self.quorum))
                .collect(),
            local: vec![None; self.n],
            sessions: 0,
        };
        let mut seen = HashSet::new();
        seen.insert(init.key());
        let mut queue = VecDeque::from([init]);
        while let Some(s) = queue.pop_front() {
            for next in self.successors(&s) {
                if seen.insert(next.key()) {
                    queue.push_back(next);
                }
            }
        }
        seen.len()
    }

    fn successors(&self, s: &State) -> Vec<State> {
        let mut out = Vec::new();
        // Replication a -> b, holders announced to any subset of voters.
        for &a in &self.mounts {
            for &b in &self.mounts {
                let Some(v) = s.local[a] else { continue };
                if a == b || s.local[b] >= Some(v) {
                    continue;
                }
                for d in subsets(self.n) {
                    let mut t = s.clone();
                    t.local[b] = Some(v);
                    for &x in &d {
                        t.voters[x].on_holders(F, v, &[id(a), id(b)]);
                    }
                    out.push(t);
                }
            }
        }
        if s.sessions >= self.max_sessions {
            return out;
        }
        for &m in &self.mounts {
            for up in subsets(self.n).filter(|u| u.contains(&m)) {
                for q in subsets(self.n)
                    .filter(|q| q.len() >= self.quorum && q.iter().all(|x| up.contains(x)))
                {
                    self.session(s, m, &up, &q, &mut out);
                }
            }
        }
        out
    }

    fn session(&self, s: &State, m: usize, up: &[usize], q: &[usize], out: &mut Vec<State>) {
        let known: Vec<_> = q
            .iter()
            .filter_map(|&x| s.voters[x].get(&F).cloned())
            .collect();
        let t = version::target(&known);
        let mut st = s.clone();
        st.sessions += 1;
        let catch_up = |st: &mut State, from: usize| {
            if from != m && up.contains(&from) && st.local[from] > st.local[m] {
                st.local[m] = st.local[from];
            }
        };
        if !t.satisfied_by(st.local[m]) {
            for &src in &t.sources {
                catch_up(&mut st, src as usize - 1);
            }
            if !t.satisfied_by(st.local[m]) {
                // After freshness_timeout: the newest copy any reachable
                // node has.
                let ask: Vec<usize> = if self.ask_everyone {
                    up.to_vec()
                } else {
                    t.committed
                        .iter()
                        .flat_map(|(_, h)| h.iter().map(|&h| h as usize - 1))
                        .collect()
                };
                for x in ask {
                    catch_up(&mut st, x);
                }
                if let Some((cv, _)) = &t.committed {
                    if st.local[m] < Some(*cv) {
                        // Refused (EBUSY); the confirm is withdrawn.
                        out.push(st);
                        return;
                    }
                }
            }
        }
        let base = st.local[m];
        if let Some(c) = max_committed(&st) {
            assert!(
                base >= Some(c),
                "node {} writes on {base:?}, older than committed {c:?}\n{s:#?}\nquorum {q:?} up {up:?}",
                id(m)
            );
        }
        let v = version::next(base, &known, id(m), m as u64 + 1);
        if let Some(c) = max_committed(&st) {
            assert!(v > c, "version {v:?} not above committed {c:?}");
        }
        for &x in q {
            st.voters[x].on_confirm(F, v);
        }
        for written in [false, true] {
            let mut w = st.clone();
            if written {
                w.local[m] = Some(v);
            }
            for r in subsets(self.n) {
                let mut x = w.clone();
                for &voter in &r {
                    x.voters[voter].on_release(Outcome {
                        file: F,
                        version: v,
                        written,
                    });
                }
                out.push(x);
            }
        }
    }
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
fn two_mounts_and_a_daemon() {
    let states = Model {
        n: 3,
        mounts: vec![0, 1],
        quorum: 2,
        max_sessions: 3,
        ask_everyone: true,
    }
    .check();
    eprintln!("{states} states");
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
fn three_mounts() {
    let states = Model {
        n: 3,
        mounts: vec![0, 1, 2],
        quorum: 2,
        max_sessions: 2,
        ask_everyone: true,
    }
    .check();
    eprintln!("{states} states");
}

/// Falling back only to the holders the voters know of loses a committed
/// version: the voters a writer reaches may not have heard of the commit.
#[test]
#[should_panic(expected = "older than committed")]
fn broken_fallback_to_known_holders_is_caught() {
    Model {
        n: 3,
        mounts: vec![0, 1, 2],
        quorum: 2,
        max_sessions: 2,
        ask_everyone: false,
    }
    .check();
}
