//! Exhaustive model check of journal forwarding and dirty marks, using the
//! real [`Journal`], [`Dirty`], [`RelayLog`], [`Inbox`] and
//! [`on_journal_ack`].
//!
//! Three data nodes A, B, C. A runs one lock session on a file: write 1, an
//! interim `Data` entry, write 2, the final `Data` entry. B makes one entry
//! after applying A's first (so it depends on it). The link A-C may go down
//! and up, C may restart (losing what it received but not applied), and A
//! may die for good. Entries reach a node from their origin or, while the
//! origin is out of reach, from a relay, in any order and any number of
//! times; acknowledgements and positions arrive whenever the link is up.
//! A `Data` entry from the origin pulls its dirty marks; a forwarded one
//! copies the relay's content.
//!
//! Invariants:
//! * entries apply in sequence order per origin, after their
//!   dependencies;
//! * no entry is lost: whatever some node applied, every other node can
//!   still get (from the origin or a relay that kept it);
//! * a node that applied the final entry has both writes;
//! * once a peer acknowledged the final entry, the origin has no marks
//!   left for it (it is in sync).

use std::collections::{BTreeMap, HashSet};

use crate::config::NodeId;
use crate::net::wire::{Attrs, Entry, FileId, Version};

use super::core::on_journal_ack;
use super::dirty::Dirty;
use super::journal::{Journal, Record};
use super::relay::{Inbox, Queued, RelayLog};

const F: FileId = FileId([7; 16]);
const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
const V: Version = Version {
    seq: 1,
    node: 1,
    disk: 1,
};

fn id(i: usize) -> NodeId {
    i as NodeId + 1
}

fn data() -> Entry {
    Entry::Data {
        id: F,
        path: b"f".to_vec(),
        version: V,
        size: 0,
        attrs: Attrs::default(),
    }
}

#[derive(Clone, Debug)]
struct Node {
    journal: Journal,
    applied: BTreeMap<NodeId, u64>,
    inbox: BTreeMap<NodeId, Inbox>,
    relay: RelayLog,
    /// Writes of A's session this copy has (bit 0: write 1, bit 1: write 2).
    content: u8,
    version: Option<Version>,
}

#[derive(Clone, Debug)]
struct State {
    nodes: Vec<Node>,
    /// A's dirty marks for B and C, and its session-final entries.
    dirty: Dirty,
    awaiting: BTreeMap<u64, (FileId, Version)>,
    /// A's progress through its session (0..=4) and B's entry made.
    pc: u8,
    b_made: bool,
    final_seq: u64,
    ac_up: bool,
    toggles: u8,
    c_restarts: u8,
    a_dead: bool,
}

impl State {
    fn key(&self) -> [u8; 32] {
        let mut s = format!(
            "{}{:?}{}{}{}{}{}{}{}",
            self.dirty.dump(),
            self.awaiting,
            self.pc,
            self.b_made,
            self.final_seq,
            self.ac_up,
            self.toggles,
            self.c_restarts,
            self.a_dead
        );
        for n in &self.nodes {
            let inbox: Vec<_> = n
                .inbox
                .iter()
                .map(|(o, ib)| {
                    (
                        o,
                        ib.queue
                            .iter()
                            .map(|(s, q)| (*s, q.via))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect();
            let journal: Vec<_> = n.journal.entries().map(|(s, _)| s).collect();
            let acked: Vec<_> = [1, 2, 3].map(|p| n.journal.acked(p)).to_vec();
            s.push_str(&format!(
                "|{:?}{:?}{:?}{:?}{:?}{}{:?}",
                n.applied,
                inbox,
                n.relay.dump(),
                journal,
                acked,
                n.content,
                n.version
            ));
        }
        *blake3::hash(s.as_bytes()).as_bytes()
    }

    fn up(&self, a: usize, b: usize) -> bool {
        if self.a_dead && (a == A || b == A) {
            return false;
        }
        !((a, b) == (A, C) || (a, b) == (C, A)) || self.ac_up
    }

    fn applied(&self, n: usize, origin: NodeId) -> u64 {
        self.nodes[n].applied.get(&origin).copied().unwrap_or(0)
    }
}

/// Appends `entry` to `n`'s journal and makes it durable.
fn make(n: &mut Node, entry: Entry) -> u64 {
    let deps: Vec<(NodeId, u64)> = n.applied.iter().map(|(o, s)| (*o, *s)).collect();
    let seq = n.journal.append(deps, entry);
    let (_, upto) = n.journal.take_changed();
    n.journal.committed(upto);
    seq
}

struct Model {
    /// Discard marks on the acknowledgement of any entry, not only a
    /// session's final one (wrong: shown to be caught).
    discard_on_any_ack: bool,
    /// The relay log keeps nothing (wrong: shown to be caught).
    eager_relay: bool,
}

impl Model {
    fn init(&self) -> State {
        let node = |me: usize| {
            let peers: Vec<NodeId> = (0..3).filter(|&p| p != me).map(id).collect();
            let mut relay = RelayLog::load(peers.iter().copied(), vec![]).unwrap();
            relay.eager = self.eager_relay;
            Node {
                journal: Journal::load(peers.iter().copied(), vec![], None, &[]).unwrap(),
                applied: BTreeMap::new(),
                inbox: BTreeMap::new(),
                relay,
                content: 0,
                version: None,
            }
        };
        State {
            nodes: (0..3).map(node).collect(),
            dirty: Dirty::new([id(B), id(C)]),
            awaiting: BTreeMap::new(),
            pc: 0,
            b_made: false,
            final_seq: 0,
            ac_up: true,
            toggles: 0,
            c_restarts: 0,
            a_dead: false,
        }
    }

    fn check(&self) -> usize {
        let init = self.init();
        let mut seen = HashSet::new();
        seen.insert(init.key());
        let mut stack = vec![init];
        while let Some(s) = stack.pop() {
            if seen.len() % 1_000_000 == 0 {
                eprintln!("{} states, {} to explore", seen.len(), stack.len());
            }
            self.invariants(&s);
            for next in self.successors(&s) {
                if seen.insert(next.key()) {
                    stack.push(next);
                }
            }
        }
        seen.len()
    }

    fn invariants(&self, s: &State) {
        // No loss.
        for n in 0..3 {
            for o in 0..3 {
                if o == n {
                    continue;
                }
                let most = (0..3)
                    .filter(|&m| m != o)
                    .map(|m| s.applied(m, id(o)))
                    .max()
                    .unwrap_or(0);
                for seq in s.applied(n, id(o)) + 1..=most {
                    let at_origin = !(o == A && s.a_dead)
                        && s.nodes[o].journal.entries().any(|(x, _)| x == seq);
                    let relayed = (0..3).any(|p| {
                        p != n
                            && p != o
                            && !(p == A && s.a_dead)
                            && s.nodes[p].relay.contains(id(o), seq)
                    });
                    assert!(
                        at_origin || relayed,
                        "node {} can no longer get entry {seq} of node {}\n{s:#?}",
                        id(n),
                        id(o)
                    );
                }
            }
        }
        for n in [B, C] {
            if s.final_seq > 0 && s.applied(n, id(A)) >= s.final_seq {
                assert_eq!(
                    s.nodes[n].content,
                    3,
                    "node {} applied the final entry without both writes\n{s:#?}",
                    id(n)
                );
            }
            if s.final_seq > 0 && !s.a_dead && s.nodes[A].journal.acked(id(n)) >= s.final_seq {
                assert!(
                    s.dirty.clean_for(id(n), &F),
                    "node {} acknowledged everything but marks are left for it\n{s:#?}",
                    id(n)
                );
            }
        }
    }

    fn successors(&self, s: &State) -> Vec<State> {
        let mut out = Vec::new();
        // A's session.
        if !s.a_dead && s.pc < 4 {
            let mut t = s.clone();
            match t.pc {
                0 => {
                    t.nodes[A].content |= 1;
                    t.nodes[A].version = Some(V);
                    t.dirty.mark(F, V, 0, 1);
                }
                1 => {
                    make(&mut t.nodes[A], data());
                }
                2 => {
                    t.nodes[A].content |= 2;
                    t.dirty.mark(F, V, 1, 2);
                }
                _ => {
                    let seq = make(&mut t.nodes[A], data());
                    t.awaiting.insert(seq, (F, V));
                    t.final_seq = seq;
                }
            }
            t.pc += 1;
            out.push(t);
        }
        // B's entry, after it applied A's first.
        if !s.b_made && s.applied(B, id(A)) >= 1 {
            let mut t = s.clone();
            make(
                &mut t.nodes[B],
                Entry::Remove {
                    id: None,
                    path: b"x".to_vec(),
                    dir: false,
                },
            );
            t.b_made = true;
            out.push(t);
        }
        for n in 0..3 {
            for o in (0..3).filter(|&o| o != n) {
                let applied = s.applied(n, id(o));
                // From the origin: the next two entries (any order, again).
                if s.up(o, n) {
                    for (seq, rec) in s.nodes[o].journal.sendable(applied, 1) {
                        let mut t = s.clone();
                        t.nodes[n].inbox.entry(id(o)).or_default().insert(
                            id(o),
                            seq,
                            Queued {
                                deps: rec.deps,
                                entry: rec.entry,
                                via: id(o),
                            },
                        );
                        out.push(t);
                    }
                } else {
                    // From a relay, while the origin is out of reach.
                    for p in (0..3).filter(|&p| p != n && p != o && s.up(p, n)) {
                        for (seq, rec) in s.nodes[p].relay.serve(id(o), applied, 1) {
                            let mut t = s.clone();
                            t.nodes[n].inbox.entry(id(o)).or_default().insert(
                                id(o),
                                seq,
                                Queued {
                                    deps: rec.deps,
                                    entry: rec.entry,
                                    via: id(p),
                                },
                            );
                            out.push(t);
                        }
                    }
                }
                // Applying.
                if let Some(t) = self.apply(s, n, o) {
                    out.push(t);
                }
                // The origin hears the acknowledgement.
                if s.up(o, n) && s.nodes[o].journal.acked(id(n)) < applied {
                    let mut t = s.clone();
                    if o == A {
                        let st = &mut t;
                        let mut awaiting = st.awaiting.clone();
                        if self.discard_on_any_ack {
                            for (seq, _) in st.nodes[A].journal.entries() {
                                awaiting.insert(seq, (F, V));
                            }
                        }
                        on_journal_ack(
                            &mut st.nodes[A].journal,
                            &mut st.dirty,
                            &awaiting,
                            id(n),
                            applied,
                        );
                    } else {
                        t.nodes[o].journal.ack(id(n), applied);
                    }
                    out.push(t);
                }
            }
        }
        // A round of positions: every node learns what every node it can
        // reach has applied by now. (Learning later only keeps entries
        // longer.)
        {
            let mut t = s.clone();
            for n in 0..3 {
                let mut pos: Vec<(NodeId, u64)> =
                    s.nodes[n].applied.iter().map(|(o, s)| (*o, *s)).collect();
                pos.push((id(n), s.nodes[n].journal.durable()));
                for p in (0..3).filter(|&p| p != n && s.up(p, n)) {
                    t.nodes[p].relay.learn(id(n), &pos);
                }
            }
            if (0..3).any(|p| t.nodes[p].relay.dump() != s.nodes[p].relay.dump()) {
                out.push(t);
            }
        }
        // The link A-C.
        if s.toggles < 2 && !s.a_dead {
            let mut t = s.clone();
            t.ac_up = !t.ac_up;
            t.toggles += 1;
            if !t.ac_up {
                // Pulls in flight on the old connection are lost.
                t.dirty.restore(id(C));
            }
            out.push(t);
        }
        if s.c_restarts < 1 {
            let mut t = s.clone();
            t.nodes[C].inbox.clear();
            t.dirty.restore(id(C));
            t.c_restarts += 1;
            out.push(t);
        }
        if !s.a_dead && s.pc == 4 {
            let mut t = s.clone();
            t.a_dead = true;
            out.push(t);
        }
        out
    }

    /// `n` applies the next entry of origin `o`, if it can.
    fn apply(&self, s: &State, n: usize, o: usize) -> Option<State> {
        let mut t = s.clone();
        let applied = t.applied(n, id(o));
        let all = t.nodes[n].applied.clone();
        let (seq, q) = t.nodes[n]
            .inbox
            .get_mut(&id(o))?
            .next(id(n), applied, &all)?;
        assert_eq!(seq, applied + 1);
        for (d, k) in &q.deps {
            assert!(*d == id(n) || all.get(d).copied().unwrap_or(0) >= *k);
        }
        if matches!(q.entry, Entry::Data { .. }) {
            let via = q.via as usize - 1;
            if !t.up(via, n) {
                return None; // retried later
            }
            if t.nodes[n].version > Some(V) {
                // Superseded (not reached here: one version).
            } else if via == o {
                // Pull the marks (first restoring a failed attempt's).
                t.dirty.restore(id(n));
                let (blocks, _) = t.dirty.take_for_pull(id(n), F, 100);
                let mut mask = 0u8;
                for (a, b) in blocks.iter() {
                    for blk in a..b {
                        mask |= 1 << blk;
                    }
                }
                t.nodes[n].content = (t.nodes[n].content & !mask) | (t.nodes[o].content & mask);
                t.nodes[n].version = t.nodes[n].version.max(Some(V));
            } else {
                // Copy the relay's content (it applied the entry).
                let r = &t.nodes[via];
                if r.version < Some(V) {
                    return None;
                }
                let (c, v) = (r.content, r.version);
                t.nodes[n].content = c;
                t.nodes[n].version = v;
            }
        }
        let node = &mut t.nodes[n];
        node.applied.insert(id(o), seq);
        node.relay.keep(
            id(o),
            seq,
            Record {
                deps: q.deps,
                entry: q.entry,
            },
        );
        Some(t)
    }
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
fn forwarding_and_marks() {
    let states = Model {
        discard_on_any_ack: false,
        eager_relay: false,
    }
    .check();
    eprintln!("{states} states");
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
#[should_panic(expected = "without both writes")]
fn broken_discard_on_interim_ack_is_caught() {
    Model {
        discard_on_any_ack: true,
        eager_relay: false,
    }
    .check();
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
#[should_panic(expected = "can no longer get")]
fn broken_relay_that_keeps_nothing_is_caught() {
    Model {
        discard_on_any_ack: false,
        eager_relay: true,
    }
    .check();
}
