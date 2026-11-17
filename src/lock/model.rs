//! Exhaustive model check of the lock protocol: every interleaving of
//! message deliveries, attempt starts, releases and (optionally) timeouts
//! for a few voters and requesters, using the real [`Voter`] and
//! [`Attempt`] state machines. Channels are FIFO per ordered node pair, as
//! TCP gives us. Leases never expire here; the timing argument (holder
//! stops before any voter's lease ends) is separate and documented in the
//! README.
//!
//! The start-up grace is modelled too: a node can restart (losing its voter
//! table, its requester and its connections), and a restarted or freshly
//! started voter grants nothing until the cold-start probe says every
//! other node is fresh. Leases never expire, so a restart is the worst
//! case: forgotten leases stay live forever. The time-based end of the
//! grace (one lease) is not modelled.

use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};

use crate::config::NodeId;
use crate::net::wire::{Ballot, DenyReason};

use super::attempt::{Attempt, Step};
use super::key::Key;
use super::voter::{Reply, Voter};

const FOREVER: u64 = u64::MAX / 4;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Msg {
    Req { req: u64, ballot: Ballot },
    Confirm { req: u64, ballot: Ballot },
    Release { ballot: Ballot },
    Grant { req: u64 },
    Confirmed { req: u64 },
    Deny { req: u64, reason: DenyReason },
    FreshQuery { round: u8 },
    FreshReply { round: u8, fresh: bool },
}

/// What a node answers to the cold-start probe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FreshRule {
    /// The real rule: no lock acquired since start, no attempt pending
    /// that counts a vote from the asker.
    Full,
    /// Deliberately broken: only "no lock acquired".
    AcquiredOnly,
    /// Deliberately broken: no grace at all after a restart.
    NoGrace,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Boot {
    acquired: bool,
    /// Current probe round (0 = none) and the nodes that answered fresh.
    round: u8,
    acks: Vec<NodeId>,
    rounds_left: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RState {
    Idle,
    Trying,
    Holding,
    Done,
    Exhausted,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Requester {
    node: NodeId,
    key: Key,
    ballot: Ballot,
    state: RState,
    tries_left: u8,
    req: u64,
    attempt: Option<Attempt>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    voters: Vec<Voter>,
    /// `chans[from][to]`, nodes numbered from 1.
    chans: Vec<Vec<VecDeque<Msg>>>,
    reqs: Vec<Requester>,
    boot: Vec<Boot>,
    restarts_left: u8,
}

struct Model {
    n: usize,
    quorum: usize,
    timeouts: bool,
    /// Node that may restart (once).
    restart: Option<NodeId>,
    /// All voters start in the grace (a cold start).
    cold: bool,
    rule: FreshRule,
}

fn quarantined_voter() -> Voter {
    Voter::new(1, FOREVER) // the model's clock stays at 0
}

impl Model {
    fn members(&self) -> Vec<NodeId> {
        (1..=self.n as NodeId).collect()
    }

    fn send(s: &mut State, from: NodeId, to: NodeId, m: Msg) {
        s.chans[from as usize - 1][to as usize - 1].push_back(m);
    }

    fn fail(&self, s: &mut State, r: usize) {
        let rq = &mut s.reqs[r];
        rq.attempt = None;
        rq.tries_left -= 1;
        rq.state = if rq.tries_left == 0 {
            RState::Exhausted
        } else {
            RState::Idle
        };
        let (node, ballot) = (rq.node, rq.ballot);
        for v in self.members() {
            Self::send(s, node, v, Msg::Release { ballot });
        }
    }

    fn step(&self, s: &mut State, r: usize, step: Step) {
        let (node, req, ballot) = (s.reqs[r].node, s.reqs[r].req, s.reqs[r].ballot);
        match step {
            Step::Wait => {}
            Step::Confirm(to) => {
                for v in to {
                    Self::send(s, node, v, Msg::Confirm { req, ballot });
                }
            }
            Step::Acquired => {
                s.boot[node as usize - 1].acquired = true;
                s.reqs[r].attempt = None;
                s.reqs[r].state = RState::Holding;
            }
            Step::Failed => self.fail(s, r),
        }
    }

    fn successors(&self, s: &State) -> Vec<State> {
        let mut out = Vec::new();
        for r in 0..s.reqs.len() {
            let rq = &s.reqs[r];
            match rq.state {
                RState::Idle if rq.req == 0 || self.settled(s) => {
                    let mut t = s.clone();
                    let rq = &mut t.reqs[r];
                    rq.req += 1;
                    rq.state = RState::Trying;
                    rq.attempt = Some(Attempt::new(
                        rq.ballot,
                        rq.req,
                        &self.members(),
                        self.quorum,
                    ));
                    let (node, req, ballot) = (rq.node, rq.req, rq.ballot);
                    for v in self.members() {
                        Self::send(&mut t, node, v, Msg::Req { req, ballot });
                    }
                    out.push(t);
                }
                RState::Holding => {
                    let mut t = s.clone();
                    t.reqs[r].state = RState::Done;
                    let (node, ballot) = (rq.node, rq.ballot);
                    for v in self.members() {
                        Self::send(&mut t, node, v, Msg::Release { ballot });
                    }
                    out.push(t);
                }
                RState::Trying if self.timeouts => {
                    let mut t = s.clone();
                    self.fail(&mut t, r);
                    out.push(t);
                }
                _ => {}
            }
        }
        if let Some(r) = self.restart.filter(|_| s.restarts_left > 0) {
            let mut t = s.clone();
            self.do_restart(&mut t, r);
            out.push(t);
        }
        for n in 1..=self.n as NodeId {
            let b = &s.boot[n as usize - 1];
            if s.voters[n as usize - 1].quarantined(0) && b.rounds_left > 0 {
                let mut t = s.clone();
                let b = &mut t.boot[n as usize - 1];
                b.rounds_left -= 1;
                b.round += 1;
                b.acks.clear();
                let round = b.round;
                for m in self.members().into_iter().filter(|&m| m != n) {
                    Self::send(&mut t, n, m, Msg::FreshQuery { round });
                }
                out.push(t);
            }
        }
        for from in 1..=self.n as NodeId {
            for to in 1..=self.n as NodeId {
                if s.chans[from as usize - 1][to as usize - 1].is_empty() {
                    continue;
                }
                let mut t = s.clone();
                let m = t.chans[from as usize - 1][to as usize - 1]
                    .pop_front()
                    .unwrap();
                self.deliver(&mut t, from, to, m);
                out.push(t);
            }
        }
        out
    }

    /// Fairness for retries: a failed requester retries only once the
    /// network is quiet and nobody holds the lock (in the real system:
    /// retry backoff is longer than a round trip, and holders release).
    /// Only liveness depends on this; safety is checked for every
    /// interleaving.
    fn settled(&self, s: &State) -> bool {
        !s.reqs.iter().any(|r| r.state == RState::Holding)
            && s.chans.iter().flatten().all(|c| c.is_empty())
    }

    fn do_restart(&self, s: &mut State, r: NodeId) {
        s.restarts_left -= 1;
        let ri = r as usize - 1;
        s.voters[ri] = if self.rule == FreshRule::NoGrace {
            Voter::new(0, FOREVER)
        } else {
            quarantined_voter()
        };
        s.boot[ri] = Boot {
            rounds_left: 1,
            ..Boot::default()
        };
        // Connections drop: messages in flight either way are lost.
        for i in 0..self.n {
            s.chans[ri][i].clear();
            s.chans[i][ri].clear();
        }
        // Its requester dies (a holder stops holding).
        for rq in s.reqs.iter_mut().filter(|q| q.node == r) {
            rq.attempt = None;
            rq.state = RState::Done;
        }
        // Others notice it went away.
        for i in 0..s.reqs.len() {
            if let Some(a) = s.reqs[i].attempt.as_mut() {
                let step = a.on_unreachable(r);
                self.step(s, i, step);
            }
        }
    }

    fn fresh(&self, s: &State, n: NodeId, asker: NodeId) -> bool {
        let b = &s.boot[n as usize - 1];
        let relies = s.reqs.iter().any(|q| {
            q.node == n
                && q.attempt
                    .as_ref()
                    .is_some_and(|a| a.granted().contains(&asker))
        });
        match self.rule {
            FreshRule::Full => !b.acquired && !relies,
            FreshRule::AcquiredOnly | FreshRule::NoGrace => !b.acquired,
        }
    }

    fn key_of(s: &State, node: NodeId) -> Key {
        s.reqs.iter().find(|r| r.node == node).unwrap().key.clone()
    }

    fn deliver(&self, s: &mut State, from: NodeId, to: NodeId, m: Msg) {
        let vi = to as usize - 1;
        match m {
            Msg::Req { req, ballot } => {
                let key = Self::key_of(s, from);
                let (reply, pre) = s.voters[vi].request(0, from, req, &key, ballot);
                let reply = match reply {
                    Reply::Grant => Msg::Grant { req },
                    Reply::Deny(reason, _) => Msg::Deny { req, reason },
                };
                Self::send(s, to, from, reply);
                for p in pre {
                    Self::send(
                        s,
                        to,
                        p.node,
                        Msg::Deny {
                            req: p.req,
                            reason: DenyReason::Preempted,
                        },
                    );
                }
            }
            Msg::Confirm { req, ballot } => {
                let key = Self::key_of(s, from);
                let reply = match s.voters[vi].confirm(0, from, &key, ballot, FOREVER) {
                    Reply::Grant => Msg::Confirmed { req },
                    Reply::Deny(reason, _) => Msg::Deny { req, reason },
                };
                Self::send(s, to, from, reply);
            }
            Msg::Release { ballot } => {
                let key = Self::key_of(s, from);
                s.voters[vi].release(from, &key, ballot);
            }
            Msg::FreshQuery { round } => {
                let fresh = self.fresh(s, to, from);
                Self::send(s, to, from, Msg::FreshReply { round, fresh });
            }
            Msg::FreshReply { round, fresh } => {
                let b = &mut s.boot[vi];
                if !fresh || round != b.round || !s.voters[vi].quarantined(0) {
                    return;
                }
                if !b.acks.contains(&from) {
                    b.acks.push(from);
                    b.acks.sort_unstable();
                }
                if b.acks.len() == self.n - 1 {
                    s.voters[vi].lift_quarantine(0);
                }
            }
            Msg::Grant { req } | Msg::Confirmed { req } | Msg::Deny { req, .. } => {
                let Some(r) = s.reqs.iter().position(|r| r.node == to) else {
                    return;
                };
                let Some(a) = s.reqs[r].attempt.as_mut().filter(|a| a.req == req) else {
                    return;
                };
                let step = match m {
                    Msg::Grant { .. } => a.on_grant(from),
                    Msg::Confirmed { .. } => a.on_confirmed(from),
                    Msg::Deny { reason, .. } => a.on_deny(from, reason, None),
                    _ => unreachable!(),
                };
                self.step(s, r, step);
            }
        }
    }

    fn check(&self, s: &State) {
        let holders: Vec<_> = s
            .reqs
            .iter()
            .filter(|r| r.state == RState::Holding)
            .collect();
        for (i, a) in holders.iter().enumerate() {
            for b in &holders[i + 1..] {
                assert!(
                    !a.key.conflicts(&b.key),
                    "two holders of conflicting locks: {s:#?}"
                );
            }
        }
        for v in &s.voters {
            let c: Vec<_> = v.grants().iter().filter(|g| g.confirmed).collect();
            for (i, a) in c.iter().enumerate() {
                for b in &c[i + 1..] {
                    assert!(
                        a.holder == b.holder || !a.key.conflicts(&b.key),
                        "voter confirmed two conflicting leases: {v:?}"
                    );
                }
            }
        }
    }

    /// Explores everything reachable; returns (states, terminal states,
    /// terminal states in which every requester got the lock).
    fn explore(&self, reqs: Vec<Requester>) -> (usize, usize, usize) {
        let voter = if self.cold {
            quarantined_voter()
        } else {
            Voter::new(0, FOREVER)
        };
        let boot = Boot {
            rounds_left: u8::from(self.cold),
            ..Boot::default()
        };
        let init = State {
            voters: vec![voter; self.n],
            chans: vec![vec![VecDeque::new(); self.n]; self.n],
            reqs,
            boot: vec![boot; self.n],
            restarts_left: u8::from(self.restart.is_some()),
        };
        // Fingerprints instead of whole states keep memory small; a
        // 64-bit collision among ~10^6 states is vanishingly unlikely.
        let fp = |s: &State| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            s.hash(&mut h);
            h.finish()
        };
        let mut seen = HashSet::new();
        seen.insert(fp(&init));
        let mut todo = vec![init];
        let (mut terminal, mut all_done) = (0, 0);
        while let Some(s) = todo.pop() {
            self.check(&s);
            let next = self.successors(&s);
            if next.is_empty() {
                terminal += 1;
                // No deadlock: nobody is stuck mid-attempt, and someone
                // always gets the lock.
                assert!(
                    s.reqs
                        .iter()
                        .all(|r| matches!(r.state, RState::Done | RState::Exhausted)),
                    "stuck: {s:#?}"
                );
                if !self.timeouts && self.restart.is_none() && !self.cold {
                    assert!(
                        s.reqs.iter().any(|r| r.state == RState::Done),
                        "nobody acquired: {s:#?}"
                    );
                    // Given a retry, the oldest ballot is never starved.
                    let oldest = s.reqs.iter().min_by_key(|r| r.ballot).unwrap();
                    if oldest.tries_left as u64 + oldest.req >= 2 {
                        assert_eq!(oldest.state, RState::Done, "oldest starved: {s:#?}");
                    }
                }
                if s.reqs.iter().all(|r| r.state == RState::Done) {
                    all_done += 1;
                }
            }
            for t in next {
                if seen.insert(fp(&t)) {
                    todo.push(t);
                }
            }
        }
        (seen.len(), terminal, all_done)
    }
}

fn requester(node: NodeId, counter: u64, key: &str, tries: u8) -> Requester {
    Requester {
        node,
        key: Key::new(key.as_bytes()),
        ballot: Ballot { counter, node },
        state: RState::Idle,
        tries_left: tries,
        req: 0,
        attempt: None,
    }
}

fn run(n: usize, timeouts: bool, reqs: Vec<Requester>) {
    run_with(n, timeouts, None, false, FreshRule::Full, reqs);
}

fn run_with(
    n: usize,
    timeouts: bool,
    restart: Option<NodeId>,
    cold: bool,
    rule: FreshRule,
    reqs: Vec<Requester>,
) {
    let m = Model {
        n,
        quorum: n / 2 + 1,
        timeouts,
        restart,
        cold,
        rule,
    };
    let (states, terminal, all_done) = m.explore(reqs);
    eprintln!(
        "{n} voters, timeouts {timeouts}, restart {restart:?}, cold {cold}, {rule:?}: \
         {states} states, {terminal} terminal, {all_done} all acquired"
    );
    assert!(terminal > 0);
}

/// Every voter starts in its grace. About 24 million states, 3 minutes
/// optimized, so opt-in: `cargo test --release --lib cold_start_probe --
/// --ignored`.
#[test]
#[ignore = "large; run with --release -- --ignored"]
fn cold_start_probe() {
    run_with(
        3,
        false,
        None,
        true,
        FreshRule::Full,
        vec![requester(1, 1, "f", 1), requester(2, 1, "f", 1)],
    );
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
fn voter_only_node_restarts() {
    run_with(
        3,
        false,
        Some(3),
        false,
        FreshRule::Full,
        vec![requester(1, 1, "f", 1), requester(2, 1, "f", 1)],
    );
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
fn requester_node_restarts() {
    run_with(
        3,
        false,
        Some(2),
        false,
        FreshRule::Full,
        vec![requester(1, 1, "f", 1), requester(2, 1, "f", 1)],
    );
}

/// The checker must catch a broken grace: with no grace after a restart,
/// a forgotten lease lets a second holder in.
#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
#[should_panic(expected = "conflicting")]
fn broken_no_grace_is_caught() {
    run_with(
        3,
        false,
        Some(3),
        false,
        FreshRule::NoGrace,
        vec![requester(1, 1, "f", 1), requester(2, 1, "f", 1)],
    );
}

/// ...and a probe that only asks "did you acquire a lock?" (ignoring
/// attempts in progress that count the asker's old vote).
#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
#[should_panic(expected = "conflicting")]
fn broken_probe_is_caught() {
    run_with(
        3,
        false,
        Some(3),
        false,
        FreshRule::AcquiredOnly,
        vec![requester(1, 1, "f", 1), requester(2, 1, "f", 1)],
    );
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
fn two_requesters_same_key() {
    run(
        3,
        false,
        vec![requester(1, 1, "f", 2), requester(2, 1, "f", 2)],
    );
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
fn two_requesters_on_a_voter_only_and_ancestor_keys() {
    // Requester 3's ballot is older; requester 2 locks a parent directory.
    run(
        3,
        false,
        vec![requester(2, 2, "d", 2), requester(3, 1, "d/f", 2)],
    );
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "slow unoptimized; CI runs it with --release"
)]
fn two_requesters_with_timeouts() {
    run(
        3,
        true,
        vec![requester(1, 1, "f", 1), requester(3, 1, "f", 1)],
    );
}

#[test]
#[ignore = "large; run with --ignored"]
fn three_requesters() {
    run(
        3,
        false,
        vec![
            requester(1, 1, "f", 2),
            requester(2, 1, "f", 2),
            requester(3, 1, "f", 2),
        ],
    );
}
