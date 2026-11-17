//! Multi-process clusters on localhost: nodes are killed, stopped and
//! restarted, and every survivor's view of peers and quorum must converge
//! within a bounded time.

mod common;

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

    /// The node's stderr, appended across restarts.
    fn log(&self) -> std::fs::File {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("log"))
            .unwrap()
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
            .stderr(self.log())
            .spawn()
            .unwrap();
        self.child = Some(child);
        let f = self.status_file();
        wait_until(&format!("node {} start", self.id), || {
            if let Some(st) = self.child.as_mut().unwrap().try_wait().unwrap() {
                let log = std::fs::read_to_string(self.dir.join("log")).unwrap_or_default();
                panic!("node {} exited: {st}\n{log}", self.id);
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
        self.status("peers")
    }

    fn status(&self, section: &str) -> Option<Value> {
        let raw = std::fs::read(self.status_file()).ok()?;
        let v: Value = serde_json::from_slice(&raw).ok()?;
        Some(v[section].clone())
    }

    /// SIGKILL, even for a mount (which then needs a lazy unmount).
    fn kill(&mut self) {
        self.signal(libc::SIGKILL);
        self.child.take().unwrap().wait().unwrap();
        if self.kind == Kind::Mount {
            for (cmd, arg) in [("fusermount3", "-uz"), ("umount", "-l")] {
                if !is_mounted(&self.mnt()) {
                    break;
                }
                let _ = Command::new(cmd)
                    .arg(arg)
                    .arg(self.mnt())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }

    fn backing(&self) -> PathBuf {
        self.dir.join("backing")
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
        let mut c = Cluster::build(kinds, &[]);
        c.start_all();
        c
    }

    /// Nodes with `extra` arguments each, not started yet.
    fn build(kinds: &[Kind], extra: &[&str]) -> Cluster {
        let tmp = tempfile::Builder::new()
            .prefix("syncfs-cluster-")
            .tempdir()
            .unwrap();
        let ports = common::listen_ports(kinds.len());
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
                args.extend(extra.iter().map(|s| s.to_string()));
                Node {
                    id,
                    kind,
                    dir,
                    args,
                    child: None,
                }
            })
            .collect();
        Cluster { _tmp: tmp, nodes }
    }

    fn start_all(&mut self) {
        for n in &mut self.nodes {
            n.start();
        }
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

/// Different peer lists (same cluster_name) are accepted; both sides warn
/// in `.syncfs/peers`, `status.json` and the daemon's status file.
#[test]
fn mismatched_peer_lists_warn() {
    require_fuse!();
    let mut c = Cluster::build(&[Kind::Mount, Kind::Daemon], &[]);
    // Node 2 also lists a node 3 that node 1 does not know.
    c.node(2)
        .args
        .extend(["--peer".into(), "3@127.0.0.1:1".into()]);
    c.start_all();
    let warned = |n: &Node| {
        n.peers()
            .and_then(|p| p["warnings"].as_array().map(|w| w.len()))
            == Some(1)
    };
    wait_until("both nodes warn", || {
        warned(&c.nodes[0]) && warned(&c.nodes[1])
    });
    let p = c.node(1).peers().unwrap();
    assert_eq!(p["peers"][1]["state"], "up");
    assert_eq!(
        p["peers"][1]["members_mismatch"],
        serde_json::json!([1, 2, 3])
    );
    let yaml = std::fs::read_to_string(c.node(1).mnt().join(".syncfs/peers")).unwrap();
    assert!(
        yaml.contains("warnings:") && yaml.contains("disagree"),
        "{yaml}"
    );
    let p2 = c.node(2).peers().unwrap();
    assert_eq!(
        p2["peers"][0]["members_mismatch"],
        serde_json::json!([1, 2])
    );
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

// ---------------------------------------------------------------------------
// Write locks across processes. Leases are short (1 s) so expiry and the
// start-up quarantine take about a second. Replication does not exist yet
// (step 7), so files that several nodes write are created in every backing
// directory before the cluster starts.

const LEASE: Duration = Duration::from_secs(1);

fn lock_cluster(kinds: &[Kind], lock_wait: &str, files: &[&str]) -> Cluster {
    let mut c = Cluster::build(
        kinds,
        &["--lock-lease-timeout", "1s", "--lock-wait", lock_wait],
    );
    for n in &c.nodes {
        for f in files {
            std::fs::write(n.backing().join(f), b"initial\n").unwrap();
        }
    }
    c.start_all();
    c.converge("start");
    // Voters grant nothing during their first lease.
    std::thread::sleep(LEASE + Duration::from_millis(100));
    c
}

fn open_rw(p: &std::path::Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new().read(true).write(true).open(p)
}

fn errno(r: std::io::Result<impl Sized>) -> Option<i32> {
    r.err().and_then(|e| e.raw_os_error())
}

/// Retries `f` until it succeeds (releases reach voters asynchronously).
fn eventually<T>(what: &str, mut f: impl FnMut() -> std::io::Result<T>) -> T {
    let t = Instant::now();
    loop {
        match f() {
            Ok(v) => return v,
            Err(e) => assert!(t.elapsed() < CONVERGE, "{what}: {e}"),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn locks_conflicting_open_is_busy_and_reads_stay_local() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "0s", &["f"]);
    let f1 = c.node(1).mnt().join("f");
    let f2 = c.node(2).mnt().join("f");
    let mut w = open_rw(&f1).unwrap();
    use std::io::Write;
    w.write_all(b"from node 1\n").unwrap();
    // Node 2 may read its own copy, but neither open it for writing nor
    // create, remove or rename over it.
    assert_eq!(std::fs::read(&f2).unwrap(), b"initial\n");
    assert_eq!(errno(open_rw(&f2)), Some(libc::EBUSY));
    assert_eq!(errno(std::fs::remove_file(&f2)), Some(libc::EBUSY));
    assert_eq!(
        errno(std::fs::rename(&f2, c.node(2).mnt().join("g"))),
        Some(libc::EBUSY)
    );
    std::fs::write(c.node(2).mnt().join("other"), b"ok").unwrap();
    // .syncfs/locks on the holder and the voter's view of it.
    let held = c.node(1).status("locks").unwrap();
    assert_eq!(held["held"], 1, "{held:#}");
    assert_eq!(held["locks"][0]["path"], "/f");
    assert_eq!(held["locks"][0]["valid"], true);
    // The voter writes status.json every 5 heartbeats.
    eventually("voter shows the lease", || {
        let v = c.node(3).status("locks").unwrap_or_default();
        let leased = v["votes"].as_array().is_some_and(|a| {
            a.iter()
                .any(|g| g["path"] == "/f" && g["holder"] == 1 && g["state"] == "leased")
        });
        leased
            .then_some(())
            .ok_or_else(|| std::io::Error::other(format!("{v:#}")))
    });
    // Closing releases.
    drop(w);
    let mut w2 = eventually("node 2 opens after release", || open_rw(&f2));
    w2.write_all(b"node 2\n").unwrap();
    assert_eq!(errno(open_rw(&f1)), Some(libc::EBUSY));
    drop(w2);
    c.converge("end");
}

/// Writers on every mount contend for one file; their critical sections
/// (open for writing .. close) never overlap.
fn contend(kinds: &[Kind]) {
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    let c = lock_cluster(kinds, "20s", &["shared"]);
    let epoch = Instant::now();
    let spans: Arc<Mutex<Vec<(u16, Duration, Duration)>>> = Arc::default();
    let threads: Vec<_> = c
        .nodes
        .iter()
        .filter(|n| n.kind == Kind::Mount)
        .map(|n| {
            let (id, path, spans) = (n.id, n.mnt().join("shared"), spans.clone());
            std::thread::spawn(move || {
                for round in 0..8 {
                    let mut f = std::fs::OpenOptions::new()
                        .append(true)
                        .open(&path)
                        .unwrap_or_else(|e| panic!("node {id} round {round}: {e}"));
                    let start = epoch.elapsed();
                    writeln!(f, "node {id} round {round}").unwrap();
                    std::thread::sleep(Duration::from_millis(15));
                    f.sync_all().unwrap();
                    let end = epoch.elapsed();
                    drop(f);
                    spans.lock().unwrap().push((id, start, end));
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let mut spans = spans.lock().unwrap().clone();
    spans.sort_by_key(|s| s.1);
    for w in spans.windows(2) {
        assert!(
            w[0].0 == w[1].0 || w[0].2 <= w[1].1,
            "overlapping writers: {:?} and {:?}",
            w[0],
            w[1]
        );
    }
    let writers = kinds.iter().filter(|k| **k == Kind::Mount).count();
    assert_eq!(spans.len(), writers * 8);
    eprintln!(
        "{} lock hand-overs in {:?}",
        spans.len(),
        spans.last().unwrap().2 - spans[0].1
    );
}

#[test]
fn locks_three_writers_take_turns() {
    require_fuse!();
    contend(&[Kind::Mount, Kind::Mount, Kind::Mount]);
}

#[test]
fn locks_five_nodes_with_voters_take_turns() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    contend(&[Mount, Daemon, Mount, Daemon, Mount]);
}

/// Renames involving a path another node has locked fail with EBUSY at
/// once (even with a long lock_wait); the holder's own renames move its
/// lock.
#[test]
fn locks_rename_of_locked_paths_is_busy() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let files = ["d/f", "d/g", "other", "free"];
    let mut c = Cluster::build(
        &[Mount, Mount, Daemon],
        &["--lock-lease-timeout", "1s", "--lock-wait", "10s"],
    );
    for n in &c.nodes {
        std::fs::create_dir_all(n.backing().join("d")).unwrap();
        for f in files {
            std::fs::write(n.backing().join(f), b"x").unwrap();
        }
    }
    c.start_all();
    c.converge("start");
    std::thread::sleep(LEASE + Duration::from_millis(100));
    let m1 = c.node(1).mnt();
    let m2 = c.node(2).mnt();
    let _w = open_rw(&m1.join("d/f")).unwrap();
    let busy = |from: &str, to: &str| {
        let t = Instant::now();
        let e = errno(std::fs::rename(m2.join(from), m2.join(to)));
        assert!(
            t.elapsed() < Duration::from_secs(2),
            "{from} -> {to} took {:?}",
            t.elapsed()
        );
        e
    };
    // The locked file, its directory, and onto the locked file.
    assert_eq!(busy("d/f", "f2"), Some(libc::EBUSY));
    assert_eq!(busy("d", "d2"), Some(libc::EBUSY));
    assert_eq!(busy("other", "d/f"), Some(libc::EBUSY));
    // Nothing moved on node 2.
    assert!(m2.join("d/f").exists() && m2.join("other").exists());
    // Unrelated renames work, also next to the locked file.
    std::fs::rename(m2.join("free"), m2.join("free2")).unwrap();
    std::fs::rename(m2.join("d/g"), m2.join("d/g2")).unwrap();
    // The holder renames its own open file: the lock moves with it.
    std::fs::rename(m1.join("d/f"), m1.join("d/moved")).unwrap();
    eventually("lock moved to /d/moved", || {
        let l = c.node(1).status("locks").unwrap();
        let paths: Vec<_> = l["locks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["path"].clone())
            .collect();
        (paths == [serde_json::json!("/d/moved")])
            .then_some(())
            .ok_or_else(|| std::io::Error::other(format!("{paths:?}")))
    });
    // Node 2 has no d/moved (no replication yet); renaming onto it is busy.
    assert_eq!(busy("other", "d/moved"), Some(libc::EBUSY));
    // The old path is free again.
    eventually("rename d/f on node 2", || {
        std::fs::rename(m2.join("d/f"), m2.join("d/f3"))
    });
}

#[test]
fn locks_killed_holder_expires() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "10s", &["f"]);
    let f2 = c.node(2).mnt().join("f");
    let _w = open_rw(&c.node(1).mnt().join("f")).unwrap();
    // Renewals keep it held well past one lease.
    std::thread::sleep(LEASE * 2);
    assert!(c.node(1).status("locks").unwrap()["locks"][0]["valid"] == true);
    c.node(1).kill();
    let t = Instant::now();
    // Blocks (lock_wait 10 s) until node 1's lease runs out at the voters.
    let w2 = open_rw(&f2).expect("lock not freed by lease expiry");
    let waited = t.elapsed();
    eprintln!("lock moved {waited:?} after the holder was killed");
    assert!(waited >= LEASE / 3, "{waited:?}: lease ignored?");
    assert!(waited < LEASE * 3, "{waited:?}");
    drop(w2);
}

#[test]
fn locks_quorum_loss_is_erofs_while_reads_work() {
    require_fuse!();
    use std::io::{Read, Seek, Write};
    use Kind::{Daemon, Mount};
    // One mount and two voter-only daemons: the daemons alone make the
    // quorum for the mount's writes.
    let mut c = lock_cluster(&[Mount, Daemon, Daemon], "2s", &["f"]);
    let mnt = c.node(1).mnt();
    let mut open = open_rw(&mnt.join("f")).unwrap();
    open.write_all(b"one\n").unwrap();
    std::fs::write(mnt.join("new"), b"x").unwrap();

    // Partition: both voters hang. Node 1 sees them down after 3
    // heartbeats; its lease lapses within one lease.
    c.node(2).signal(libc::SIGSTOP);
    c.node(3).signal(libc::SIGSTOP);
    eventually("quorum lost", || {
        let p = c.node(1).peers().unwrap();
        (p["have_quorum"] == false)
            .then_some(())
            .ok_or_else(|| std::io::Error::other("still quorum"))
    });
    let t = Instant::now();
    assert_eq!(
        errno(std::fs::write(mnt.join("g"), b"x")),
        Some(libc::EROFS)
    );
    assert_eq!(errno(open_rw(&mnt.join("new"))), Some(libc::EROFS));
    assert_eq!(
        errno(std::fs::remove_file(mnt.join("new"))),
        Some(libc::EROFS)
    );
    assert_eq!(errno(std::fs::create_dir(mnt.join("d"))), Some(libc::EROFS));
    assert!(
        t.elapsed() < Duration::from_millis(500),
        "EROFS took {:?}",
        t.elapsed()
    );
    std::thread::sleep(LEASE);
    // The already open file: its lease lapsed, re-acquiring fails.
    assert_eq!(errno(open.write_all(b"two\n")), Some(libc::EROFS));
    // Reads are local and keep working.
    assert_eq!(std::fs::read(mnt.join("new")).unwrap(), b"x");
    let mut s = String::new();
    open.rewind().unwrap();
    open.read_to_string(&mut s).unwrap();
    assert_eq!(s, "one\nial\n");
    assert!(std::fs::read_dir(&mnt).unwrap().count() >= 2);

    // Heal: writes work again, including on the open handle.
    c.node(2).signal(libc::SIGCONT);
    c.node(3).signal(libc::SIGCONT);
    c.converge("healed");
    eventually("write after heal", || open.write_all(b"three\n"));
    std::fs::write(mnt.join("g"), b"x").unwrap();
    // Restarted voters wait one lease before voting again.
    c.node(2).kill();
    c.node(3).kill();
    c.node(2).start();
    c.node(3).start();
    c.converge("voters restarted");
    let t = Instant::now();
    eventually("write after voter restart", || {
        std::fs::write(mnt.join("h"), b"x")
    });
    eprintln!("writable {:?} after the voters came back", t.elapsed());
}
