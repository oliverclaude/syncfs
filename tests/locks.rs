//! The lock manager, in process over localhost TCP: mutual exclusion,
//! waiting, lease expiry, quorum loss and a randomized stress run.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use syncfs::config::{resolve, Config, Environment, Mode, NodeId, PartialConfig, Peer};
use syncfs::lock::{Key, LockError, LockManager, Settings};
use syncfs::net::Network;

const LEASE: Duration = Duration::from_secs(1);
const WAIT: Duration = Duration::from_secs(10);

fn config(id: NodeId, members: &[(NodeId, u16)], port: u16, lock_wait: &str) -> Config {
    let p = PartialConfig {
        node_id: Some(id),
        mode: Some(Mode::Daemon),
        listen: Some(format!("127.0.0.1:{port}").parse().unwrap()),
        peers: Some(
            members
                .iter()
                .map(|&(id, port)| Peer {
                    id,
                    address: format!("127.0.0.1:{port}"),
                })
                .collect(),
        ),
        state_dir: Some("/tmp/syncfs-lock-test".into()),
        heartbeat_interval: Some("100ms".parse().unwrap()),
        lock_lease_timeout: Some("1s".parse().unwrap()),
        lock_wait: Some(lock_wait.parse().unwrap()),
        ..Default::default()
    };
    let env = Environment {
        is_root: false,
        home: Some("/tmp".into()),
        xdg_state_home: None,
        cwd: "/".into(),
    };
    resolve(p, &env).unwrap().config
}

struct Node {
    net: Network,
    locks: LockManager,
}

fn start(cfg: &Config) -> Node {
    let net = Network::start(cfg).unwrap();
    let locks = LockManager::start(Settings::from_config(cfg), net.clone());
    Node { net, locks }
}

/// Nodes 1..=n, started together, connected.
fn fresh_cluster(n: usize, lock_wait: &str) -> (Vec<Node>, Vec<Config>) {
    let ports = common::listen_ports(n);
    let members: Vec<(NodeId, u16)> = (1..=n as NodeId).zip(ports.iter().copied()).collect();
    let cfgs: Vec<Config> = members
        .iter()
        .map(|&(id, port)| config(id, &members, port, lock_wait))
        .collect();
    let nodes: Vec<Node> = cfgs.iter().map(start).collect();
    for node in &nodes {
        assert!(node.net.wait_until(WAIT, |s| s.reachable == n));
    }
    (nodes, cfgs)
}

/// Nodes 1..=n, connected, voting.
fn cluster(n: usize, lock_wait: &str) -> Vec<Node> {
    let (nodes, _) = fresh_cluster(n, lock_wait);
    for node in &nodes {
        common::wait_until("grace over", || {
            node.locks.provider().render()["grants_after_ms"] == 0
        });
    }
    nodes
}

fn k(s: &str) -> Key {
    Key::new(s.as_bytes())
}

fn shutdown(nodes: &[Node]) {
    for n in nodes {
        n.locks.shutdown();
        n.net.shutdown();
    }
}

fn grace(n: &Node) -> (bool, u64) {
    let v = n.locks.provider().render();
    (
        v["grace_skipped"].as_bool().unwrap(),
        v["grants_after_ms"].as_u64().unwrap(),
    )
}

#[test]
fn cold_start_skips_the_grace() {
    let (nodes, _) = fresh_cluster(3, "300ms");
    // All three nodes started fresh: they skip the grace (1 s) and grant
    // as soon as everyone has answered the probe.
    // as soon as everyone has answered the probe.
    let t = Instant::now();
    common::wait_until("all skip the grace", || {
        nodes.iter().all(|n| grace(n) == (true, 0))
    });
    assert!(t.elapsed() < LEASE / 2, "{:?}", t.elapsed());
    assert_eq!(nodes[0].locks.acquire(&k("f")), Ok(()));
    shutdown(&nodes);
}

#[test]
fn restarted_voter_waits_while_others_hold_or_granted() {
    let (mut nodes, cfgs) = fresh_cluster(3, "300ms");
    common::wait_until("lock", || nodes[0].locks.acquire(&k("f")).is_ok());
    // Node 3 restarts while node 1 holds a lease (granted by 1 and 2,
    // maybe 3). Node 1 has acquired and node 2 has granted: not fresh.
    nodes[2].locks.shutdown();
    nodes[2].net.shutdown();
    nodes[2] = start(&cfgs[2]);
    assert!(nodes[2].net.wait_until(WAIT, |s| s.reachable == 3));
    let started = Instant::now();
    std::thread::sleep(Duration::from_millis(300));
    let (skipped, left) = grace(&nodes[2]);
    assert!(!skipped && left > 0, "restarted voter skipped its grace");
    // With node 2 gone, node 1's new locks need node 3's vote: refused
    // until node 3's grace is over.
    nodes[1].locks.shutdown();
    nodes[1].net.shutdown();
    assert!(nodes[0].net.wait_until(WAIT, |s| s.reachable == 2));
    assert_eq!(nodes[0].locks.acquire(&k("g")), Err(LockError::NoQuorum));
    common::wait_until("lock after node 3's grace", || {
        nodes[0].locks.acquire(&k("g")).is_ok()
    });
    assert!(
        started.elapsed() >= LEASE - Duration::from_millis(100),
        "{:?}",
        started.elapsed()
    );
    for n in [&nodes[0], &nodes[2]] {
        n.locks.shutdown();
        n.net.shutdown();
    }
}

#[test]
fn restart_with_no_lock_ever_taken_skips_the_grace() {
    let (mut nodes, cfgs) = fresh_cluster(3, "300ms");
    common::wait_until("grace over", || grace(&nodes[0]).0);
    nodes[2].locks.shutdown();
    nodes[2].net.shutdown();
    nodes[2] = start(&cfgs[2]);
    common::wait_until("restarted node skips", || grace(&nodes[2]).0);
    shutdown(&nodes);
}

#[test]
fn conflicting_requests_wait_or_fail_busy() {
    let nodes = cluster(3, "0s");
    let (a, b) = (&nodes[0].locks, &nodes[1].locks);
    a.acquire(&k("d/f")).unwrap();
    assert!(a.is_valid(&k("d/f")));
    // lock_wait 0: fail at once, naming the holder.
    assert_eq!(b.acquire(&k("d/f")), Err(LockError::Busy(Some(1))));
    // Ancestors and descendants conflict, siblings don't.
    assert_eq!(b.acquire(&k("d")), Err(LockError::Busy(Some(1))));
    assert_eq!(b.acquire(&k("")), Err(LockError::Busy(Some(1))));
    assert_eq!(b.acquire(&k("d/g")), Ok(()));
    // Same node, related keys: fine.
    assert_eq!(a.acquire(&k("d/f/x")), Ok(()));
    a.release(&k("d/f/x"));
    // References: released only by the last one.
    a.acquire(&k("d/f")).unwrap();
    a.release(&k("d/f"));
    assert_eq!(b.acquire(&k("d/f")), Err(LockError::Busy(Some(1))));
    a.release(&k("d/f"));
    // The release reaches the voters asynchronously.
    common::wait_until("b acquires d/f", || b.acquire(&k("d/f")).is_ok());
    shutdown(&nodes);
}

#[test]
fn rename_style_acquire_fails_at_once_when_held() {
    let nodes = cluster(3, "5s");
    nodes[0].locks.acquire(&k("d/f")).unwrap();
    for key in ["d/f", "d", "", "d/f/x"] {
        let t = Instant::now();
        assert_eq!(
            nodes[1].locks.acquire_unless_held(&k(key)),
            Err(LockError::Busy(Some(1))),
            "{key}"
        );
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "{key}: {:?}",
            t.elapsed()
        );
    }
    assert_eq!(nodes[1].locks.acquire_unless_held(&k("d/g")), Ok(()));
    nodes[1].locks.release(&k("d/g"));
    // The holder's own node is not refused.
    common::wait_until("node 1 takes d", || {
        nodes[0].locks.acquire_unless_held(&k("d")).is_ok()
    });
    shutdown(&nodes);
}

#[test]
fn waiter_gets_the_lock_when_released() {
    let nodes = cluster(3, "5s");
    nodes[0].locks.acquire(&k("f")).unwrap();
    let locks = nodes[2].locks.clone();
    let t = Instant::now();
    let waiter = std::thread::spawn(move || (locks.acquire(&k("f")), Instant::now()));
    std::thread::sleep(Duration::from_millis(500));
    let released = Instant::now();
    nodes[0].locks.release(&k("f"));
    let (res, got) = waiter.join().unwrap();
    assert_eq!(res, Ok(()));
    assert!(got >= released, "acquired while held");
    assert!(got - t < Duration::from_secs(3));
    shutdown(&nodes);
}

#[test]
fn renewal_keeps_the_lock_past_the_lease() {
    let nodes = cluster(3, "0s");
    nodes[0].locks.acquire(&k("f")).unwrap();
    std::thread::sleep(LEASE * 3);
    assert!(nodes[0].locks.is_valid(&k("f")));
    assert_eq!(
        nodes[1].locks.acquire(&k("f")),
        Err(LockError::Busy(Some(1)))
    );
    shutdown(&nodes);
}

#[test]
fn holder_cut_off_lapses_before_others_acquire() {
    let nodes = cluster(3, "5s");
    nodes[0].locks.acquire(&k("f")).unwrap();
    // Node 1 vanishes from the network without releasing.
    let cut = Instant::now();
    nodes[0].net.shutdown();
    // Writes there must stop: the lock lapses within one lease.
    let locks = nodes[0].locks.clone();
    let lapsed = std::thread::spawn(move || {
        while locks.is_valid(&k("f")) {
            std::thread::sleep(Duration::from_millis(5));
        }
        Instant::now()
    });
    let res = nodes[1].locks.acquire(&k("f"));
    let got = Instant::now();
    let lapsed = lapsed.join().unwrap();
    assert_eq!(res, Ok(()));
    assert!(lapsed < got, "old holder still valid when the lock moved");
    assert!(got - cut >= LEASE / 2, "acquired after {:?}", got - cut);
    assert!(got - cut < LEASE * 4, "acquired after {:?}", got - cut);
    // Without its peers node 1 cannot even try.
    assert_eq!(nodes[0].locks.acquire(&k("g")), Err(LockError::NoQuorum));
    shutdown(&nodes);
}

#[test]
fn quorum_loss_fails_fast_with_no_quorum() {
    let nodes = cluster(3, "5s");
    nodes[0].locks.acquire(&k("held")).unwrap();
    nodes[1].net.shutdown();
    // 2 of 3 is still a quorum.
    assert_eq!(nodes[0].locks.acquire(&k("f")), Ok(()));
    nodes[2].net.shutdown();
    assert!(nodes[0].net.wait_until(WAIT, |s| !s.have_quorum));
    let t = Instant::now();
    assert_eq!(nodes[0].locks.acquire(&k("g")), Err(LockError::NoQuorum));
    assert!(
        t.elapsed() < Duration::from_millis(100),
        "{:?}",
        t.elapsed()
    );
    // Held locks lapse.
    std::thread::sleep(LEASE);
    assert!(!nodes[0].locks.is_valid(&k("held")));
    assert_eq!(nodes[0].locks.ensure(&k("held")), Err(LockError::NoQuorum));
    shutdown(&nodes);
}

#[test]
fn concurrent_writers_take_turns() {
    for n in [3, 5] {
        let nodes = cluster(n, "10s");
        let inside = Arc::new(AtomicUsize::new(0));
        let wins: Vec<_> = (0..n).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let mut threads = Vec::new();
        for (i, node) in nodes.iter().enumerate() {
            let (locks, inside, wins) = (node.locks.clone(), inside.clone(), wins[i].clone());
            threads.push(std::thread::spawn(move || {
                for _ in 0..10 {
                    locks.acquire(&k("shared")).unwrap();
                    assert_eq!(inside.fetch_add(1, Ordering::SeqCst), 0, "two holders");
                    assert!(locks.is_valid(&k("shared")));
                    std::thread::sleep(Duration::from_millis(3));
                    inside.fetch_sub(1, Ordering::SeqCst);
                    locks.release(&k("shared"));
                    wins.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert!(wins.iter().all(|w| w.load(Ordering::SeqCst) == 10));
        shutdown(&nodes);
    }
}

/// Random keys (some nested), many threads per node, random hold times;
/// checks that no two nodes are ever inside conflicting critical sections.
#[test]
fn randomized_stress() {
    let nodes = cluster(5, "20s");
    let keys = ["a", "a/x", "a/y", "b", "b/z/w", "c"];
    let active: Arc<Mutex<Vec<(usize, Key)>>> = Arc::default();
    let done = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        for t in 0..3 {
            let (locks, active, done, stop) = (
                node.locks.clone(),
                active.clone(),
                done.clone(),
                stop.clone(),
            );
            threads.push(std::thread::spawn(move || {
                let mut seed = (i * 31 + t * 7 + 1) as u64;
                let mut next = move || {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed
                };
                for _ in 0..15 {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let key = k(keys[next() as usize % keys.len()]);
                    match locks.acquire(&key) {
                        Ok(()) => {}
                        Err(e) => panic!("node {} {key}: {e}", i + 1),
                    }
                    {
                        let mut a = active.lock().unwrap();
                        for (other, ok) in a.iter() {
                            assert!(
                                *other == i || !ok.conflicts(&key),
                                "node {} got {key} while node {} holds {ok}",
                                i + 1,
                                other + 1
                            );
                        }
                        a.push((i, key.clone()));
                    }
                    std::thread::sleep(Duration::from_millis(next() % 8));
                    assert!(locks.is_valid(&key));
                    {
                        let mut a = active.lock().unwrap();
                        let pos = a.iter().position(|e| e.0 == i && e.1 == key).unwrap();
                        a.remove(pos);
                    }
                    locks.release(&key);
                    done.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
    }
    for t in threads {
        if let Err(e) = t.join() {
            stop.store(true, Ordering::SeqCst);
            std::panic::resume_unwind(e);
        }
    }
    assert_eq!(done.load(Ordering::SeqCst), 5 * 3 * 15);
    shutdown(&nodes);
}

#[test]
fn grants_carry_versions_and_sessions_announce_theirs() {
    use syncfs::lock::FileCtx;
    use syncfs::net::wire::FileId;
    let nodes = cluster(3, "2s");
    let f = FileId([7; 16]);
    let ctx = |local| Some(FileCtx { file: f, local });
    // First session on node 1: nothing known, version 1 announced.
    let a = nodes[0]
        .locks
        .acquire_file(&k("f"), ctx(None), false)
        .unwrap();
    assert!(a.new_session);
    let v1 = a.version.unwrap();
    assert_eq!((v1.seq, v1.node), (1, 1));
    // A second reference continues the session.
    let again = nodes[0]
        .locks
        .acquire_file(&k("f"), ctx(None), false)
        .unwrap();
    assert!(!again.new_session);
    assert_eq!(again.version, Some(v1));
    nodes[0].locks.release(&k("f"));
    assert_eq!(nodes[0].locks.mark_written(&k("f")), Some((f, v1)));
    nodes[0].locks.release(&k("f"));

    // Node 2's grants report node 1's written version with node 1 as the
    // holder (at least one granting voter saw it: majorities intersect; a
    // voter that missed the confirm reports nothing); node 2 announces
    // version 2.
    let b = common::wait_for("node 2 acquires", || {
        nodes[1].locks.acquire_file(&k("f"), ctx(None), false).ok()
    });
    let latest: Vec<_> = b.known.iter().filter_map(|i| i.latest).collect();
    assert!(
        !latest.is_empty() && latest.iter().all(|v| *v == v1),
        "{:?}",
        b.known
    );
    assert!(b
        .known
        .iter()
        .filter(|i| i.latest.is_some())
        .all(|i| !i.pending && i.holders == [1]));
    let v2 = b.version.unwrap();
    assert_eq!((v2.seq, v2.node), (2, 2));
    // Node 2 writes nothing: its version is withdrawn on release.
    nodes[1].locks.release(&k("f"));
    // (Releases reach the voters asynchronously.)
    let c = common::wait_for("node 3 sees the withdrawal", || {
        let c = nodes[2]
            .locks
            .acquire_file(&k("f"), ctx(Some(v1)), false)
            .ok()?;
        let latest: Vec<_> = c.known.iter().filter_map(|i| i.latest).collect();
        if !latest.is_empty() && latest.iter().all(|v| *v == v1) {
            return Some(c);
        }
        nodes[2].locks.release(&k("f"));
        None
    });
    assert_eq!(c.version.unwrap().seq, 2);
    nodes[2].locks.release(&k("f"));
    shutdown(&nodes);
}
