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

/// Nodes 1..=n, connected, past their start-up quarantine.
fn cluster(n: usize, lock_wait: &str) -> Vec<Node> {
    let ports = common::listen_ports(n);
    let members: Vec<(NodeId, u16)> = (1..=n as NodeId).zip(ports.iter().copied()).collect();
    let nodes: Vec<Node> = members
        .iter()
        .map(|&(id, port)| {
            let cfg = config(id, &members, port, lock_wait);
            let net = Network::start(&cfg).unwrap();
            let locks = LockManager::start(Settings::from_config(&cfg), net.clone());
            Node { net, locks }
        })
        .collect();
    for node in &nodes {
        assert!(node
            .net
            .wait_until(WAIT, |s| s.reachable == n && s.have_quorum));
    }
    std::thread::sleep(LEASE + Duration::from_millis(50));
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

#[test]
fn voters_refuse_during_startup_quarantine() {
    let ports = common::listen_ports(3);
    let members: Vec<(NodeId, u16)> = (1..=3).zip(ports.iter().copied()).collect();
    let nodes: Vec<Node> = members
        .iter()
        .map(|&(id, port)| {
            let cfg = config(id, &members, port, "300ms");
            let net = Network::start(&cfg).unwrap();
            let locks = LockManager::start(Settings::from_config(&cfg), net.clone());
            Node { net, locks }
        })
        .collect();
    for node in &nodes {
        assert!(node.net.wait_until(WAIT, |s| s.reachable == 3));
    }
    // Still within one lease of starting: no votes, so no lock (EROFS).
    assert_eq!(nodes[0].locks.acquire(&k("f")), Err(LockError::NoQuorum));
    std::thread::sleep(LEASE);
    assert_eq!(nodes[0].locks.acquire(&k("f")), Ok(()));
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
