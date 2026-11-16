//! Multi-process clusters on localhost: nodes are killed, stopped and
//! restarted, and every survivor's view of peers and quorum must converge
//! within a bounded time.

mod common;

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

use common::{is_mounted, unmount, wait_until, BIN};

/// Heartbeat used by the test clusters; peers are down after 3 of these.
const HEARTBEAT: &str = "100ms";
/// Bound on every convergence: down detection (300 ms), the reconnect
/// backoff cap (1 s) and process start-up, with slack for slow CI.
const CONVERGE: Duration = Duration::from_secs(8);

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Mount,
    Daemon,
}

struct Node {
    id: u16,
    kind: Kind,
    dir: PathBuf,
    args: Vec<String>,
    child: Option<Child>,
}

impl Node {
    fn mnt(&self) -> PathBuf {
        self.dir.join("mnt")
    }

    fn status_file(&self) -> PathBuf {
        match self.kind {
            Kind::Mount => self.mnt().join(".syncfs/status.json"),
            Kind::Daemon => self.dir.join("state/status.json"),
        }
    }

    fn start(&mut self) {
        let mut cmd = Command::new(BIN);
        if self.kind == Kind::Mount {
            cmd.arg(self.dir.join("backing")).arg(self.mnt());
        } else {
            cmd.args(["--mode", "daemon"]);
        }
        let child = cmd
            .args(&self.args)
            .arg("--foreground")
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.child = Some(child);
        let f = self.status_file();
        wait_until(&format!("node {} start", self.id), || {
            if let Some(st) = self.child.as_mut().unwrap().try_wait().unwrap() {
                panic!("node {} exited: {st}", self.id);
            }
            (self.kind == Kind::Daemon || is_mounted(&self.mnt())) && f.exists()
        });
    }

    fn signal(&mut self, sig: i32) {
        let pid = self.child.as_ref().unwrap().id() as i32;
        unsafe { libc::kill(pid, sig) };
    }

    /// Stops the node: SIGTERM for mounts (clean unmount), SIGKILL for
    /// daemons (no goodbye; peers see the connection drop).
    fn stop(&mut self) {
        let sig = match self.kind {
            Kind::Mount => libc::SIGTERM,
            Kind::Daemon => libc::SIGKILL,
        };
        self.signal(sig);
        self.child.take().unwrap().wait().unwrap();
        if self.kind == Kind::Mount && is_mounted(&self.mnt()) {
            unmount(&self.mnt());
        }
    }

    fn peers(&self) -> Option<Value> {
        let raw = std::fs::read(self.status_file()).ok()?;
        let v: Value = serde_json::from_slice(&raw).ok()?;
        Some(v["peers"].clone())
    }

    fn running(&self) -> bool {
        self.child.is_some()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            // SIGTERM unmounts cleanly; a stopped (SIGSTOP) node needs SIGCONT.
            unsafe {
                libc::kill(c.id() as i32, libc::SIGCONT);
                libc::kill(c.id() as i32, libc::SIGTERM);
            }
            let _ = c.wait();
            if self.kind == Kind::Mount && is_mounted(&self.mnt()) {
                for (cmd, arg) in [("fusermount3", "-uz"), ("umount", "-l")] {
                    let _ = Command::new(cmd)
                        .arg(arg)
                        .arg(self.mnt())
                        .stderr(Stdio::null())
                        .status();
                }
            }
        }
    }
}

struct Cluster {
    _tmp: tempfile::TempDir,
    nodes: Vec<Node>,
}

impl Cluster {
    fn new(kinds: &[Kind]) -> Cluster {
        let tmp = tempfile::Builder::new()
            .prefix("syncfs-cluster-")
            .tempdir()
            .unwrap();
        let ls: Vec<_> = kinds
            .iter()
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<u16> = ls.iter().map(|l| l.local_addr().unwrap().port()).collect();
        drop(ls);
        let peers: Vec<String> = ports
            .iter()
            .enumerate()
            .flat_map(|(i, p)| ["--peer".to_string(), format!("{}@127.0.0.1:{p}", i + 1)])
            .collect();
        let nodes = kinds
            .iter()
            .enumerate()
            .map(|(i, &kind)| {
                let id = i as u16 + 1;
                let dir = tmp.path().join(format!("n{id}"));
                for d in ["backing", "mnt", "state"] {
                    std::fs::create_dir_all(dir.join(d)).unwrap();
                }
                let mut args: Vec<String> = [
                    "--config",
                    "/dev/null",
                    "--cluster-name",
                    "test",
                    "--heartbeat-interval",
                    HEARTBEAT,
                    "--node-id",
                ]
                .iter()
                .map(|s| s.to_string())
                .collect();
                args.push(id.to_string());
                args.push("--listen".into());
                args.push(format!("127.0.0.1:{}", ports[i]));
                args.push("--state-dir".into());
                args.push(dir.join("state").display().to_string());
                args.extend(peers.iter().cloned());
                Node {
                    id,
                    kind,
                    dir,
                    args,
                    child: None,
                }
            })
            .collect();
        let mut c = Cluster { _tmp: tmp, nodes };
        for n in &mut c.nodes {
            n.start();
        }
        c
    }

    fn node(&mut self, id: u16) -> &mut Node {
        &mut self.nodes[id as usize - 1]
    }

    /// Waits until every running node sees exactly the running nodes up,
    /// with the matching quorum verdict. Returns how long it took.
    fn converge(&self, what: &str) -> Duration {
        let t = Instant::now();
        let up: Vec<u16> = self
            .nodes
            .iter()
            .filter(|n| n.running())
            .map(|n| n.id)
            .collect();
        let size = self.nodes.len();
        let quorum = size / 2 + 1;
        let mut last = Vec::new();
        loop {
            last.clear();
            let ok = self.nodes.iter().filter(|n| n.running()).all(|n| {
                let Some(p) = n.peers() else { return false };
                let seen: Vec<u16> = p["up"]
                    .as_array()
                    .map(|a| a.iter().map(|v| v.as_u64().unwrap() as u16).collect())
                    .unwrap_or_default();
                let good = p["implemented"] == true
                    && seen == up
                    && p["reachable"] == up.len()
                    && p["quorum"] == quorum
                    && p["cluster_size"] == size
                    && p["have_quorum"] == (up.len() >= quorum);
                last.push(format!(
                    "node {}: up {seen:?} have_quorum {}",
                    n.id, p["have_quorum"]
                ));
                good
            });
            if ok {
                return t.elapsed();
            }
            assert!(
                t.elapsed() < CONVERGE,
                "{what}: no convergence on up={up:?} within {CONVERGE:?}: {last:#?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn peer_field(&self, observer: u16, peer: u16, field: &str) -> Value {
        let p = self.nodes[observer as usize - 1].peers().unwrap();
        p["peers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["id"] == peer)
            .unwrap()[field]
            .clone()
    }
}

fn report(name: &str, d: Duration) {
    eprintln!("converged after {name} in {} ms", d.as_millis());
}

#[test]
fn daemons_only_three_nodes() {
    let mut c = Cluster::new(&[Kind::Daemon, Kind::Daemon, Kind::Daemon]);
    report("start", c.converge("start"));
    let inc = c.peer_field(1, 2, "incarnation");
    c.node(2).stop();
    report("kill 2", c.converge("kill 2"));
    assert_eq!(c.peer_field(1, 2, "state"), "down");
    c.node(3).stop();
    report("kill 3 (quorum lost)", c.converge("kill 3"));
    c.node(2).start();
    c.node(3).start();
    report("restart 2 and 3", c.converge("restart"));
    assert_ne!(c.peer_field(1, 2, "incarnation"), inc);
    assert_eq!(c.peer_field(1, 2, "connects"), 2);
    // A clean exit removes the status file.
    let f = c.node(1).status_file();
    c.node(1).signal(libc::SIGTERM);
    let st = c.node(1).child.take().unwrap().wait().unwrap();
    assert!(st.success(), "{st}");
    assert!(!f.exists());
    report("node 1 leaves", c.converge("leave"));
    assert_eq!(c.peer_field(2, 1, "last_error"), "left: shutting down");
}

#[test]
fn mixed_three_nodes() {
    require_fuse!();
    let mut c = Cluster::new(&[Kind::Mount, Kind::Mount, Kind::Daemon]);
    report("start", c.converge("start"));
    c.node(3).stop(); // SIGKILL the voter
    report("kill voter 3", c.converge("kill 3"));
    c.node(2).stop(); // SIGTERM mount 2
    report("stop mount 2", c.converge("stop 2"));
    assert_eq!(c.node(1).peers().unwrap()["have_quorum"], false);
    c.node(3).start();
    report("voter 3 back", c.converge("restart 3"));
    assert_eq!(c.node(1).peers().unwrap()["have_quorum"], true);
    c.node(2).start();
    report("mount 2 back", c.converge("restart 2"));
}

#[test]
fn mixed_five_nodes() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = Cluster::new(&[Mount, Daemon, Daemon, Mount, Daemon]);
    report("start", c.converge("start"));
    // A hung node (SIGSTOP) is detected by heartbeat timeout.
    c.node(5).signal(libc::SIGSTOP);
    let t = Instant::now();
    loop {
        let states: Vec<Value> = [1, 2, 3, 4]
            .iter()
            .map(|&o| c.peer_field(o, 5, "state"))
            .collect();
        if states.iter().all(|s| s == "down") {
            break;
        }
        assert!(
            t.elapsed() < CONVERGE,
            "stopped node 5 not detected: {states:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    report("SIGSTOP 5", t.elapsed());
    assert_eq!(c.peer_field(1, 5, "last_error"), "heartbeat timeout");
    c.node(5).signal(libc::SIGCONT);
    report("SIGCONT 5", c.converge("cont 5"));
    // Lose two of five: 3 >= quorum 3.
    c.node(2).stop();
    c.node(4).stop();
    report("kill 2, stop 4", c.converge("lose two"));
    assert_eq!(c.node(1).peers().unwrap()["have_quorum"], true);
    // A third loss breaks quorum.
    c.node(3).stop();
    report("kill 3", c.converge("lose three"));
    assert_eq!(c.node(1).peers().unwrap()["have_quorum"], false);
    for id in [2, 3, 4] {
        c.node(id).start();
    }
    report("restart 2, 3, 4", c.converge("restart"));
}

/// The maximum cluster size: 99 voter processes (each with ~250 network
/// threads), full mesh. Heavy, so opt-in:
/// `cargo test --release --test cluster -- --ignored --nocapture`.
#[test]
#[ignore = "heavy: 99 processes"]
fn daemons_99_nodes() {
    let t = Instant::now();
    let mut c = Cluster::new(&[Kind::Daemon; 99]);
    eprintln!("99 nodes started in {:?}", t.elapsed());
    report("start (99)", c.converge("start"));
    for id in [7, 50, 99] {
        c.node(id).stop();
    }
    report("kill 3 of 99", c.converge("kill 3"));
    for id in [7, 50, 99] {
        c.node(id).start();
    }
    report("restart 3 of 99", c.converge("restart 3"));
}
