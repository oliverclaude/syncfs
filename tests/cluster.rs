//! Multi-process clusters on localhost: nodes are killed, stopped and
//! restarted, and every survivor's view of peers and quorum must converge
//! within a bounded time.

mod common;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

use common::{is_mounted, unmount, wait_for, wait_until, BIN};

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
    env: Vec<(String, String)>,
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
            .envs(self.env.iter().map(|(k, v)| (k, v)))
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
                    env: Vec::new(),
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
                    && p["have_quorum"] == (up.len() >= quorum)
                    // Mounts are done rebuilding (tests that watch a
                    // rebuild slow it down and wait themselves).
                    && (n.kind != Kind::Mount
                        || n.env.iter().any(|(k, _)| k == "SYNCFS_TEST_REBUILD_DELAY_MS")
                        || n.status("replication")
                            .is_some_and(|r| r["rebuild"]["active"] == false));
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
    lock_cluster_with(kinds, lock_wait, |backing| {
        for f in files {
            std::fs::write(backing.join(f), b"initial\n").unwrap();
        }
    })
}

/// A started lock cluster whose backing directories `setup` filled first.
fn lock_cluster_with(kinds: &[Kind], lock_wait: &str, setup: impl Fn(&std::path::Path)) -> Cluster {
    let mut c = Cluster::build(
        kinds,
        &[
            "--lock-lease-timeout",
            "1s",
            "--lock-wait",
            lock_wait,
            "--freshness-timeout",
            "2s",
        ],
    );
    for n in &c.nodes {
        if n.kind == Kind::Mount {
            setup(&n.backing());
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

/// A file replaced by a peer's rename while this node waited for its lock
/// is gone: the open gets `ESTALE` and the kernel looks the path up again,
/// so it opens the renamed file instead of writing to one nobody else will
/// see (found by the soak test).
#[test]
fn locks_open_of_a_file_replaced_while_waiting_opens_the_new_file() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f", "g"]);
    let (m1, m2) = (c.node(1).mnt(), c.node(2).mnt());
    let w = open_rw(&m1.join("f")).unwrap();
    let f2 = m2.join("f");
    let waiter = std::thread::spawn(move || {
        let mut f = open_rw(&f2)?;
        let mut s = String::new();
        std::io::Read::read_to_string(&mut f, &mut s)?;
        Ok::<_, std::io::Error>(s)
    });
    std::thread::sleep(Duration::from_millis(500));
    std::fs::write(m1.join("g"), b"renamed\n").unwrap();
    std::fs::rename(m1.join("g"), m1.join("f")).unwrap();
    drop(w);
    assert_eq!(waiter.join().unwrap().unwrap(), "renamed\n");
    c.converge("end");
}

/// A file a peer renamed away while this node waited for its lock: the
/// open gets `ESTALE`, and the kernel's second try (with `O_CREAT`)
/// creates the path anew instead of writing into the renamed file (found
/// by the soak test as a lost committed write).
#[test]
fn locks_open_of_a_file_renamed_away_while_waiting_does_not_follow_it() {
    require_fuse!();
    use std::io::Write;
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f"]);
    let (m1, m2) = (c.node(1).mnt(), c.node(2).mnt());
    let w = open_rw(&m1.join("f")).unwrap();
    let f2 = m2.join("f");
    let waiter = std::thread::spawn(move || {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&f2)?;
        f.write_all(b"node 2\n")
    });
    std::thread::sleep(Duration::from_millis(500));
    std::fs::rename(m1.join("f"), m1.join("h")).unwrap();
    drop(w);
    waiter.join().unwrap().unwrap();
    c.converge("end");
    let b1 = c.node(1).backing();
    eventually("node 1 has both files", || {
        let f = std::fs::read_to_string(b1.join("f"))?;
        let h = std::fs::read_to_string(b1.join("h"))?;
        (f == "node 2\n" && h == "initial\n")
            .then_some(())
            .ok_or_else(|| std::io::Error::other(format!("f {f:?}, h {h:?}")))
    });
}

/// An open through a name the kernel still has cached after a peer
/// renamed the file away gets `ESTALE`, and the kernel's second lookup
/// finds the name gone; the write does not land in the renamed file (found
/// by the soak test). Node 2's unlink of `x` waits for node 1's lock with
/// the directory locked, which holds back the kernel's invalidation of `f`.
#[test]
fn locks_open_through_a_name_a_peer_renamed_away_does_not_follow_it() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f", "x"]);
    let (m1, m2) = (c.node(1).mnt(), c.node(2).mnt());
    let x1 = open_rw(&m1.join("x")).unwrap();
    std::fs::metadata(m2.join("f")).unwrap();
    let x2 = m2.join("x");
    let remover = std::thread::spawn(move || std::fs::remove_file(&x2));
    std::thread::sleep(Duration::from_millis(100));
    std::fs::rename(m1.join("f"), m1.join("g")).unwrap();
    let b2 = c.node(2).backing();
    eventually("node 2 applies the rename", || {
        std::fs::metadata(b2.join("g")).map(|_| ())
    });
    // With the fix the kernel's second lookup waits for the directory, so
    // node 1 lets the unlink go on a little later.
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        drop(x1);
    });
    let opened = std::fs::OpenOptions::new().write(true).open(m2.join("f"));
    if let Ok(f) = &opened {
        use std::io::Write;
        (&*f).write_all(b"through the old name\n").unwrap();
    }
    releaser.join().unwrap();
    remover.join().unwrap().unwrap();
    c.converge("end");
    assert_eq!(std::fs::read_to_string(b2.join("g")).unwrap(), "initial\n");
    assert!(!b2.join("f").exists());
    assert_eq!(errno(opened.map(|_| ())), Some(libc::ENOENT));
}

/// Unlinking a file a peer just wrote, without opening it here first: the
/// tombstone descends from the peer's version (no conflict copy on the
/// writer), although this node never fetched that content (found by the
/// soak test).
#[test]
fn locks_unlink_after_a_peer_write_is_not_a_conflict() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f"]);
    let (m1, m2) = (c.node(1).mnt(), c.node(2).mnt());
    for round in 0..3 {
        let name = format!("f{round}");
        std::fs::write(m2.join(&name), b"from node 2\n").unwrap();
        c.converge(&format!("created {round}"));
        std::fs::write(m1.join(&name), b"from node 1\n").unwrap();
        std::fs::remove_file(m2.join(&name)).unwrap();
        let b1 = c.node(1).backing().join(&name);
        eventually("node 1 applies the remove", || {
            (!b1.exists())
                .then_some(())
                .ok_or_else(|| std::io::Error::other("still there"))
        });
    }
    c.converge("end");
    let leftovers: Vec<_> = std::fs::read_dir(c.node(1).backing())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains("syncfs-conflict"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    assert_eq!(c.node(1).status("stats").unwrap()["conflict_copies"], 0);
}

/// An unlink whose file the lock handoff replaced (a peer renamed another
/// file onto the path while this node waited) gets `ESTALE`; the kernel
/// looks the path up again and removes the file that is there now.
#[test]
fn locks_unlink_of_a_file_replaced_while_waiting_removes_the_new_file() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f", "g"]);
    let (m1, m2) = (c.node(1).mnt(), c.node(2).mnt());
    let w = open_rw(&m1.join("f")).unwrap();
    let f2 = m2.join("f");
    let waiter = std::thread::spawn(move || std::fs::remove_file(&f2));
    std::thread::sleep(Duration::from_millis(500));
    std::fs::write(m1.join("g"), b"renamed\n").unwrap();
    std::fs::rename(m1.join("g"), m1.join("f")).unwrap();
    drop(w);
    waiter.join().unwrap().unwrap();
    c.converge("end");
    let b1 = c.node(1).backing();
    eventually("node 1 applies the remove", || {
        (!b1.join("f").exists() && !b1.join("g").exists())
            .then_some(())
            .ok_or_else(|| std::io::Error::other("still there"))
    });
    assert_eq!(c.node(1).status("stats").unwrap()["conflict_copies"], 0);
    assert_eq!(c.node(2).status("stats").unwrap()["conflict_copies"], 0);
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
    // Renaming onto d/moved is busy whether or not node 2 has seen the
    // rename yet (the lock is on the path, not on a local file).
    assert_eq!(busy("other", "d/moved"), Some(libc::EBUSY));
    // Once the rename has replicated, the old path is free again.
    eventually("d/moved on node 2", || {
        std::fs::metadata(m2.join("d/moved"))
    });
    assert!(!m2.join("d/f").exists());
    eventually("create d/f on node 2", || {
        std::fs::write(m2.join("d/f"), b"new")
    });
}

/// With a 30 s lease, a whole-cluster cold start is writable within the
/// convergence bound (the grace is skipped); a voter restarted after locks
/// were taken keeps its full grace.
#[test]
fn locks_cold_start_skips_grace_restart_does_not() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = Cluster::build(&[Mount, Daemon, Daemon], &["--lock-lease-timeout", "30s"]);
    c.start_all();
    c.converge("start");
    let mnt = c.node(1).mnt();
    let t = Instant::now();
    eventually("first write", || std::fs::write(mnt.join("a"), b"x"));
    eprintln!("cold start: writable after {:?}", t.elapsed());
    // At least a quorum skipped (a node still probing after the first
    // lock was taken sees a non-fresh holder and keeps its grace). Daemon
    // status files are rewritten every 5 heartbeats.
    // Restart a daemon whose loss leaves a granting quorum: node 1 and the
    // other daemon must have skipped the grace.
    let victim = eventually("a quorum skipped the grace", || {
        let skipped =
            |id: u16| c.nodes[id as usize - 1].status("locks").unwrap()["grace_skipped"] == true;
        match (skipped(1), skipped(2), skipped(3)) {
            (true, true, _) => Ok(3),
            (true, false, true) => Ok(2),
            s => Err(std::io::Error::other(format!("skipped (1, 2, 3): {s:?}"))),
        }
    });
    c.node(victim).kill();
    c.node(victim).start();
    c.converge("voter back");
    let l = eventually("restarted voter status", || {
        c.node(victim)
            .status("locks")
            .filter(|l| l.is_object())
            .ok_or_else(|| std::io::Error::other("no status"))
    });
    assert_eq!(l["grace_skipped"], false, "{l:#}");
    assert!(l["grants_after_ms"].as_u64().unwrap() > 20_000, "{l:#}");
    // Nodes 1 and 2 still make a quorum.
    std::fs::write(mnt.join("b"), b"x").unwrap();
}

#[test]
fn locks_killed_holder_expires() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "10s", &["f"]);
    let f2 = c.node(2).mnt().join("f");
    // Node 1's session never wrote, but the voters cannot know: node 2
    // also waits freshness_timeout (2 s) for node 1's copy.
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
    assert!(waited < LEASE * 3 + Duration::from_secs(2), "{waited:?}");
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

/// Every entry of a backing tree (but the hidden `.syncfs`): type, mode,
/// size, content or target, and `user.*` xattrs.
fn tree(root: &std::path::Path) -> std::collections::BTreeMap<String, String> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let mut out = std::collections::BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            let rel = p.strip_prefix(root).unwrap().display().to_string();
            if rel == ".syncfs" {
                continue;
            }
            // Replication may be renaming or removing it right now; the
            // caller compares again.
            let Ok(md) = std::fs::symlink_metadata(&p) else {
                continue;
            };
            let mut xattrs = Vec::new();
            let mut buf = vec![0u8; 65536];
            if let Ok(n) = rustix::fs::llistxattr(&p, &mut buf[..]) {
                for name in buf[..n]
                    .split(|b| *b == 0)
                    .filter(|n| n.starts_with(b"user."))
                {
                    let name = <std::ffi::OsStr as std::os::unix::ffi::OsStrExt>::from_bytes(name);
                    let mut v = vec![0u8; 65536];
                    let len = rustix::fs::lgetxattr(&p, name, &mut v[..]).unwrap_or(0);
                    xattrs.push(format!("{name:?}={:?}", &v[..len]));
                }
            }
            let what = if md.is_dir() {
                stack.push(p.clone());
                "dir".to_string()
            } else if md.file_type().is_symlink() {
                format!("link->{:?}", std::fs::read_link(&p).ok())
            } else if md.is_file() {
                let data = std::fs::read(&p).unwrap_or_default();
                format!("file {} {:x}", data.len(), md5ish(&data))
            } else {
                format!("special {:o}", md.mode() & libc::S_IFMT)
            };
            out.insert(
                rel,
                format!(
                    "{what} mode {:o} nlink {} {}",
                    md.permissions().mode() & 0o7777,
                    if md.is_dir() { 0 } else { md.nlink() },
                    xattrs.join(",")
                ),
            );
        }
    }
    out
}

fn md5ish(data: &[u8]) -> u64 {
    // FNV-1a: enough to tell contents apart in a test.
    data.iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ *b as u64).wrapping_mul(0x100000001b3)
    })
}

/// Waits until the backing trees of mount nodes `a` and `b` match.
fn wait_same_tree(c: &Cluster, a: u16, b: u16) {
    let (ra, rb) = (
        c.nodes[a as usize - 1].backing(),
        c.nodes[b as usize - 1].backing(),
    );
    let t = Instant::now();
    loop {
        let (ta, tb) = (tree(&ra), tree(&rb));
        if ta == tb {
            return;
        }
        if t.elapsed() > Duration::from_secs(20) {
            let diff: Vec<_> = ta
                .iter()
                .filter(|(k, v)| tb.get(*k) != Some(v))
                .map(|(k, v)| format!("{k}: {v} vs {:?}", tb.get(k)))
                .chain(
                    tb.keys()
                        .filter(|k| !ta.contains_key(*k))
                        .map(|k| format!("{k}: only on {b}")),
                )
                .collect();
            panic!("nodes {a} and {b} differ:\n{}", diff.join("\n"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn in_sync(c: &Cluster, n: u16) -> Value {
    let node = &c.nodes[n as usize - 1];
    let t = Instant::now();
    loop {
        let r = node.status("replication").unwrap();
        if r["in_sync"] == true {
            return r;
        }
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "node {n} not in sync: {r:#}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn replicates_changes_between_mounts() {
    require_fuse!();
    use std::os::unix::fs::{FileExt, PermissionsExt};
    use Kind::{Daemon, Mount};
    const B: usize = 64 * 1024;
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f"]);
    let m1 = c.node(1).mnt();
    let m2 = c.node(2).mnt();
    // The tree starts equal (pre-existing files are adopted by path).
    wait_same_tree(&c, 1, 2);

    std::fs::create_dir(m1.join("d")).unwrap();
    let big: Vec<u8> = (0..3 * B + 100).map(|i| (i % 251) as u8).collect();
    std::fs::write(m1.join("d/big"), &big).unwrap();
    std::os::unix::fs::symlink("big", m1.join("d/sym")).unwrap();
    std::fs::hard_link(m1.join("d/big"), m1.join("hard")).unwrap();
    std::fs::set_permissions(m1.join("d/big"), std::fs::Permissions::from_mode(0o640)).unwrap();
    let xattrs = rustix::fs::setxattr(
        m1.join("d/big"),
        "user.k",
        b"v",
        rustix::fs::XattrFlags::empty(),
    )
    .is_ok();
    let f = open_rw(&m1.join("f")).unwrap();
    f.write_all_at(b"changed", 2).unwrap();
    drop(f);
    std::fs::write(m1.join("gone"), b"x").unwrap();
    std::fs::remove_file(m1.join("gone")).unwrap();
    std::fs::rename(m1.join("d/sym"), m1.join("sym2")).unwrap();
    wait_same_tree(&c, 1, 2);
    assert_eq!(std::fs::read(m2.join("d/big")).unwrap(), big);
    assert_eq!(std::fs::read(m2.join("f")).unwrap(), b"inchanged");
    if xattrs {
        let mut v = [0u8; 16];
        let n = rustix::fs::getxattr(m2.join("d/big"), "user.k", &mut v[..]).unwrap();
        assert_eq!(&v[..n], b"v");
    }
    let r = in_sync(&c, 1);
    assert!(
        r["counters"]["entries_made"].as_u64().unwrap() >= 8,
        "{r:#}"
    );
    let r2 = c.node(2).status("replication").unwrap();
    assert!(
        r2["counters"]["blocks_received"].as_u64().unwrap() >= 4,
        "{r2:#}"
    );

    // Only changed blocks travel: one byte in the middle of block 1.
    let before = r2["counters"]["blocks_received"].as_u64().unwrap();
    let f = open_rw(&m2.join("d/big")).unwrap();
    f.write_all_at(b"Z", B as u64 + 5).unwrap();
    f.set_len((2 * B) as u64).unwrap();
    drop(f);
    wait_same_tree(&c, 1, 2);
    let r1 = in_sync(&c, 1);
    assert_eq!(
        r1["counters"]["blocks_received"].as_u64().unwrap(),
        1,
        "{r1:#}"
    );
    let r2 = in_sync(&c, 2);
    assert_eq!(r2["counters"]["blocks_received"].as_u64().unwrap(), before);

    // A renamed file keeps its version: writing it again needs no
    // freshness wait.
    std::fs::write(m1.join("r1"), b"one").unwrap();
    std::fs::rename(m1.join("r1"), m1.join("r2")).unwrap();
    std::fs::write(m1.join("r2"), b"two").unwrap();
    wait_same_tree(&c, 1, 2);
    let r1 = in_sync(&c, 1);
    assert_eq!(r1["counters"]["fresh_waits"], 0, "{r1:#}");
    assert_eq!(std::fs::read(m2.join("r2")).unwrap(), b"two");

    // The daemon keeps the version table across a restart (its status
    // file is rewritten every 5 heartbeats).
    let files =
        |c: &mut Cluster| c.node(3).status("replication").unwrap()["versions"]["files"].clone();
    let want = c.node(1).status("replication").unwrap()["versions"]["files"].clone();
    assert!(want.as_u64().unwrap() >= 2, "{want}");
    wait_until("daemon has every version", || files(&mut c) == want);
    c.node(3).stop();
    c.node(3).start();
    assert_eq!(files(&mut c), want);
}

#[test]
fn pre_existing_hard_links_stay_one_file() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster_with(&[Mount, Mount, Daemon], "5s", |b| {
        std::fs::create_dir(b.join("d")).unwrap();
        std::fs::write(b.join("d/x"), b"initial\n").unwrap();
        std::fs::hard_link(b.join("d/x"), b.join("y")).unwrap();
    });
    wait_same_tree(&c, 1, 2);
    std::fs::write(c.node(1).mnt().join("y"), b"through one link\n").unwrap();
    wait_same_tree(&c, 1, 2);
    let b2 = c.node(2).backing();
    assert_eq!(
        std::fs::read(b2.join("d/x")).unwrap(),
        b"through one link\n"
    );
    use std::os::unix::fs::MetadataExt;
    assert_eq!(std::fs::metadata(b2.join("y")).unwrap().nlink(), 2);
    let r = in_sync(&c, 1);
    assert_eq!(r["counters"]["apply_errors"], 0, "{r:#}");
}

#[test]
fn stale_copy_is_brought_up_to_date_when_locked() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = Cluster::build(
        &[Mount, Mount, Daemon],
        &[
            "--lock-lease-timeout",
            "1s",
            "--lock-wait",
            "5s",
            "--freshness-timeout",
            "2s",
        ],
    );
    for n in &c.nodes {
        std::fs::write(n.backing().join("f"), b"initial\n").unwrap();
    }
    c.start_all();
    c.converge("start");
    std::thread::sleep(LEASE + Duration::from_millis(100));

    // Node 2 misses a write; when it locks the file again, it first
    // fetches the newest version (by journal or by the freshness check).
    c.node(2).stop();
    std::fs::write(c.node(1).mnt().join("f"), b"version two\n").unwrap();
    c.node(2).start();
    c.converge("node 2 back");
    let m2 = c.node(2).mnt();
    let f = eventually("lock on node 2", || open_rw(&m2.join("f")));
    assert_eq!(std::fs::read(m2.join("f")).unwrap(), b"version two\n");
    drop(f);

    // Now node 1 writes and goes away before node 2 can pull: node 2
    // waits freshness_timeout for it, then continues from its own copy
    // (the newest copy a majority has).
    c.node(2).signal(libc::SIGSTOP);
    std::fs::write(c.node(1).mnt().join("f"), b"version three\n").unwrap();
    c.node(1).kill();
    c.node(2).signal(libc::SIGCONT);
    let t = Instant::now();
    let f = eventually("lock on node 2 without node 1", || open_rw(&m2.join("f")));
    let waited = t.elapsed();
    drop(f);
    let r = c.node(2).status("replication").unwrap();
    let pulled = std::fs::read(m2.join("f")).unwrap();
    if pulled == b"version three\n" {
        // The journal entry got through before the kill.
        eprintln!("node 2 had the write already: {r:#}");
    } else {
        assert_eq!(pulled, b"version two\n");
        assert!(waited >= Duration::from_secs(2), "{waited:?}");
        assert!(
            r["counters"]["fresh_fallbacks"].as_u64().unwrap() >= 1,
            "{r:#}"
        );
    }
}

#[test]
fn crash_while_writing_resends_after_restart() {
    require_fuse!();
    use std::os::unix::fs::FileExt;
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f"]);
    let m1 = c.node(1).mnt();
    let f = open_rw(&m1.join("f")).unwrap();
    let data = vec![7u8; 200_000];
    f.write_all_at(&data, 0).unwrap();
    // Killed with the file open: its changes may not be journaled.
    c.node(1).kill();
    drop(f);
    c.node(1).start();
    c.converge("node 1 back");
    let r = c.node(1).status("replication").unwrap();
    assert_eq!(r["unclean_start"], true, "{r:#}");
    wait_same_tree(&c, 1, 2);
    assert_eq!(std::fs::read(c.node(2).backing().join("f")).unwrap(), data);
    let r = in_sync(&c, 1);
    assert_eq!(r["suspects"], 0, "{r:#}");
    // A clean stop after that is recorded as clean.
    c.node(1).stop();
    c.node(1).start();
    let r = c.node(1).status("replication").unwrap();
    assert_eq!(r["unclean_start"], false, "{r:#}");
}

/// A change reaches a node whose origin went away before it could send it:
/// a third mount forwards it, ordered and once.
#[test]
fn changes_are_forwarded_when_the_origin_is_down() {
    require_fuse!();
    let mut c = lock_cluster(&[Kind::Mount, Kind::Mount, Kind::Mount], "5s", &["f"]);
    c.node(3).signal(libc::SIGSTOP);
    let m1 = c.node(1).mnt();
    std::fs::write(m1.join("f"), b"one\n").unwrap();
    std::fs::write(m1.join("g"), b"two\n").unwrap();
    let b2 = c.node(2).backing();
    wait_for("node 2 has both changes", || {
        (std::fs::read(b2.join("f")).ok()? == b"one\n"
            && std::fs::read(b2.join("g")).ok()? == b"two\n")
            .then_some(())
    });
    c.node(1).kill();
    c.node(3).signal(libc::SIGCONT);
    let b3 = c.node(3).backing();
    let t = Instant::now();
    while std::fs::read(b3.join("f")).ok().as_deref() != Some(b"one\n")
        || std::fs::read(b3.join("g")).ok().as_deref() != Some(b"two\n")
    {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "node 3 lacks the changes: {:?} {:?}\n{:#}\n{:#}\n{}",
            std::fs::read(b3.join("f")).map(String::from_utf8),
            std::fs::read(b3.join("g")).map(String::from_utf8),
            c.node(3).status("replication").unwrap(),
            c.node(2).status("replication").unwrap(),
            std::fs::read_to_string(c.node(3).dir.join("log")).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let r = c.node(3).status("replication").unwrap();
    assert!(
        r["counters"]["entries_relayed"].as_u64().unwrap() >= 1,
        "{r:#}"
    );
    let r2 = c.node(2).status("replication").unwrap();
    assert!(
        r2["counters"]["entries_forwarded"].as_u64().unwrap() >= 1,
        "{r2:#}"
    );
    // Node 3 is writable without waiting for node 1.
    let t = Instant::now();
    std::fs::write(c.node(3).mnt().join("f"), b"three\n").unwrap();
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    // Node 1 comes back, takes the newer write, and has nothing left to
    // send.
    c.node(1).start();
    c.converge("node 1 back");
    wait_same_tree(&c, 1, 3);
    wait_same_tree(&c, 2, 3);
    let r = in_sync(&c, 1);
    assert_eq!(r["suspects"], 0, "{r:#}");
    in_sync(&c, 3);
}

/// A node's change that a peer wrote over without having it (after the
/// freshness fallback) is kept as a conflict copy when the node returns.
#[test]
fn diverged_change_is_kept_as_a_conflict_copy() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f"]);
    // Node 1 writes while node 2 is frozen, and dies before node 2 gets it.
    c.node(2).signal(libc::SIGSTOP);
    std::fs::write(c.node(1).mnt().join("f"), b"from node 1\n").unwrap();
    std::thread::sleep(Duration::from_millis(500));
    c.node(1).kill();
    c.node(2).signal(libc::SIGCONT);
    // Node 2 falls back to its own copy after freshness_timeout and writes.
    let m2 = c.node(2).mnt();
    eventually("write on node 2", || {
        std::fs::write(m2.join("f"), b"from node 2\n")
    });
    // Node 1 comes back: its change is kept aside, node 2's wins.
    c.node(1).start();
    c.converge("node 1 back");
    wait_same_tree(&c, 1, 2);
    let names: Vec<String> = std::fs::read_dir(c.node(1).backing())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("f.syncfs-conflict-1-"))
        .collect();
    assert_eq!(names.len(), 1, "{names:?}");
    let b1 = c.node(1).backing();
    assert_eq!(std::fs::read(b1.join(&names[0])).unwrap(), b"from node 1\n");
    assert_eq!(std::fs::read(b1.join("f")).unwrap(), b"from node 2\n");
    let b2 = c.node(2).backing();
    assert_eq!(std::fs::read(b2.join(&names[0])).unwrap(), b"from node 1\n");
    assert_eq!(c.node(1).status("stats").unwrap()["conflict_copies"], 1);
    assert_eq!(c.node(2).status("stats").unwrap()["conflict_copies"], 0);
    let r = in_sync(&c, 1);
    assert_eq!(r["counters"]["conflict_copies"], 1, "{r:#}");
    in_sync(&c, 2);
}

/// Removing a file leaves a versioned tombstone: a node with a stale copy
/// cannot write it back to life unless the remover is unreachable; a write
/// made after the fallback wins over the remove when the remover returns.
#[test]
fn removes_leave_tombstones() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f", "g"]);
    let (m1, m2) = (c.node(1).mnt(), c.node(2).mnt());
    // Node 2 has a stale copy when it opens f: ENOENT, never a write.
    c.node(2).signal(libc::SIGSTOP);
    std::fs::remove_file(m1.join("f")).unwrap();
    c.node(2).signal(libc::SIGCONT);
    let r = open_rw(&m2.join("f"));
    assert_eq!(errno(r), Some(libc::ENOENT));
    wait_same_tree(&c, 1, 2);
    assert!(!c.node(2).backing().join("f").exists());

    // Node 1 removes g and dies before node 2 hears of it; node 2 falls
    // back and writes g: when node 1 returns, the write wins.
    c.node(2).signal(libc::SIGSTOP);
    std::fs::remove_file(m1.join("g")).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    c.node(1).kill();
    c.node(2).signal(libc::SIGCONT);
    eventually("write on node 2", || {
        std::fs::write(m2.join("g"), b"written after\n")
    });
    c.node(1).start();
    c.converge("node 1 back");
    wait_same_tree(&c, 1, 2);
    assert_eq!(
        std::fs::read(c.node(1).backing().join("g")).ok(),
        Some(b"written after\n".to_vec()),
        "{}\n{}",
        std::fs::read_to_string(c.node(1).dir.join("log")).unwrap_or_default(),
        std::fs::read_to_string(c.node(2).dir.join("log")).unwrap_or_default()
    );
    in_sync(&c, 1);
    in_sync(&c, 2);
}

/// A node that restarts takes changes only once it has caught up with its
/// peers: its mkdir of a directory a peer made meanwhile fails with EEXIST
/// instead of racing the peer's entry.
#[test]
fn restarted_node_catches_up_before_changes() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &[]);
    c.node(2).stop();
    let m1 = c.node(1).mnt();
    for i in 0..50 {
        std::fs::write(m1.join(format!("f{i}")), format!("{i}\n")).unwrap();
    }
    std::fs::create_dir(m1.join("d")).unwrap();
    c.node(2).start();
    let r = std::fs::create_dir(c.node(2).mnt().join("d"));
    assert_eq!(errno(r), Some(libc::EEXIST));
    let r = c.node(2).status("replication").unwrap();
    assert_eq!(r["rejoining"], false, "{r:#}");
    wait_same_tree(&c, 1, 2);
}

/// Namespace changes whose journal entries were not flushed when the node
/// died are found by comparing trees on restart: paths only there are
/// sent, paths only on the peer are restored (a lost rename keeps both
/// names).
#[test]
fn crash_window_namespace_changes_are_reconciled() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster_with(&[Mount, Mount, Daemon], "5s", |b| {
        std::fs::write(b.join("f"), b"f\n").unwrap();
    });
    // Node 1 flushes its store rarely: its next changes die with it.
    c.node(1).stop();
    c.node(1)
        .env
        .push(("SYNCFS_TEST_FLUSH_INTERVAL_MS".into(), "60000".into()));
    c.node(1).start();
    c.converge("node 1 back");
    std::thread::sleep(LEASE + Duration::from_millis(100));
    let m1 = c.node(1).mnt();
    std::fs::create_dir(m1.join("d2")).unwrap();
    std::os::unix::fs::symlink("f", m1.join("s")).unwrap();
    std::fs::rename(m1.join("f"), m1.join("g")).unwrap();
    c.node(1).kill();
    c.node(1).env.clear();
    c.node(1).start();
    c.converge("node 1 restarted");
    wait_same_tree(&c, 1, 2);
    let m2 = c.node(2).mnt();
    for m in [&m1, &m2] {
        assert!(m.join("d2").is_dir());
        assert_eq!(
            std::fs::read_link(m.join("s")).unwrap(),
            std::path::Path::new("f")
        );
        assert_eq!(std::fs::read(m.join("f")).unwrap(), b"f\n");
        assert_eq!(std::fs::read(m.join("g")).unwrap(), b"f\n");
    }
    let r = c.node(1).status("replication").unwrap();
    assert_eq!(r["counters"]["reconcile_sent"], 3, "{r:#}");
    assert_eq!(r["counters"]["reconcile_restored"], 1, "{r:#}");
}

/// A new lock holder first applies the previous holder's journal up to
/// the head it released with. If those entries do not arrive within
/// `freshness_timeout` while their origin is connected, the operation
/// fails with EBUSY instead of acting on a namespace without them.
#[test]
fn handoff_timeout_refuses_with_ebusy() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &[]);
    // Node 1 flushes its store rarely, so its journal entries are not
    // sent (only durable entries are) while it stays connected.
    c.node(1).stop();
    c.node(1)
        .env
        .push(("SYNCFS_TEST_FLUSH_INTERVAL_MS".into(), "60000".into()));
    c.node(1).start();
    c.converge("node 1 back");
    std::thread::sleep(LEASE + Duration::from_millis(100));
    let (m1, m2) = (c.node(1).mnt(), c.node(2).mnt());
    std::fs::create_dir(m1.join("d")).unwrap();
    let t = Instant::now();
    let r = std::fs::create_dir(m2.join("d"));
    assert_eq!(errno(r), Some(libc::EBUSY));
    // After waiting freshness_timeout (2 s), not much longer.
    assert!(
        t.elapsed() >= Duration::from_millis(1900) && t.elapsed() < Duration::from_secs(5),
        "{:?}",
        t.elapsed()
    );
    assert!(!m2.join("d").exists());
    let l = c.node(2).status("locks").unwrap();
    assert_eq!(l["counters"]["handoff_refused"], 1, "{l:#}");
    // Once node 1's entries go out, the same operation sees them.
    c.node(1).stop();
    c.node(1).env.clear();
    c.node(1).start();
    c.converge("node 1 flushing again");
    wait_same_tree(&c, 1, 2);
    assert_eq!(errno(std::fs::create_dir(m2.join("d"))), Some(libc::EEXIST));
}

/// After a reconnect, local changes wait (briefly) until the node has
/// caught up with the peer: a node that was cut off gets EEXIST for a
/// directory made meanwhile instead of racing the peer's entry.
#[test]
fn reconnected_node_catches_up_before_changes() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &[]);
    let (m1, m2) = (c.node(1).mnt(), c.node(2).mnt());
    for round in 0..3 {
        c.node(2).signal(libc::SIGSTOP);
        eventually("node 2 seen down", || {
            (c.peer_field(1, 2, "state") == "down")
                .then_some(())
                .ok_or_else(|| std::io::Error::other("still up"))
        });
        let d = format!("d{round}");
        std::fs::create_dir(m1.join(&d)).unwrap();
        // Durable on node 1 (one store flush) before node 2 is back.
        std::thread::sleep(Duration::from_millis(300));
        c.node(2).signal(libc::SIGCONT);
        let t = Instant::now();
        // EROFS while the quorum comes back; never a second mkdir.
        let r = loop {
            match errno(std::fs::create_dir(m2.join(&d))) {
                Some(libc::EROFS) if t.elapsed() < Duration::from_secs(4) => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                r => break r,
            }
        };
        assert_eq!(r, Some(libc::EEXIST), "round {round}");
        assert!(t.elapsed() < Duration::from_secs(4), "{:?}", t.elapsed());
    }
    let r = c.node(2).status("replication").unwrap();
    assert_eq!(r["rejoining"], false, "{r:#}");
    wait_same_tree(&c, 1, 2);
}

/// The reconnect wait stays short: a peer leaving costs nothing, a peer
/// coming back delays the next change by at most a moment.
#[test]
fn reconnect_waits_are_short() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f"]);
    let m2 = c.node(2).mnt();
    let timed = |what: &str| {
        let t = Instant::now();
        std::fs::write(m2.join("f"), what).unwrap();
        t.elapsed()
    };
    timed("warm");
    let steady = timed("steady");
    assert!(steady < Duration::from_millis(500), "steady {steady:?}");
    c.node(1).stop();
    let left = timed("node 1 left");
    assert!(left < Duration::from_millis(500), "left {left:?}");
    c.node(1).start();
    c.converge("node 1 back");
    let back = timed("node 1 back");
    assert!(back < Duration::from_millis(2500), "back {back:?}");
    let again = timed("again");
    assert!(again < Duration::from_millis(500), "again {again:?}");
    wait_same_tree(&c, 1, 2);
}

/// A new member (new store, empty backing directory) rebuilds from a peer
/// instead of starting empty.
#[test]
fn new_member_rebuilds_from_peers() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &[]);
    let m1 = c.node(1).mnt();
    std::fs::create_dir(m1.join("d")).unwrap();
    std::fs::write(m1.join("d/x"), b"x\n").unwrap();
    c.node(2).stop();
    let dir = c.node(2).dir.clone();
    for sub in ["backing", "state"] {
        std::fs::remove_dir_all(dir.join(sub)).unwrap();
        std::fs::create_dir(dir.join(sub)).unwrap();
    }
    std::fs::write(m1.join("later"), b"later\n").unwrap();
    c.node(2).start();
    c.converge("new member");
    let r = c.node(2).status("replication").unwrap();
    assert_eq!(r["rebuild"]["cause"], "new_member", "{r:#}");
    let m2 = c.node(2).mnt();
    assert_eq!(std::fs::read(m2.join("d/x")).unwrap(), b"x\n");
    assert_eq!(std::fs::read(m2.join("later")).unwrap(), b"later\n");
    std::fs::write(m2.join("new"), b"n\n").unwrap();
    wait_same_tree(&c, 1, 2);
}

/// A node whose disk is lost (backing directory wiped, marker gone)
/// rebuilds from a peer; local leftovers move to `.syncfs/orphans`, and a
/// version only the lost disk had does not block writers.
#[test]
fn lost_disk_is_rebuilt_and_its_lost_writes_do_not_block() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "5s", &["f"]);
    let m1 = c.node(1).mnt();
    std::fs::create_dir(m1.join("d")).unwrap();
    std::fs::write(m1.join("d/x"), vec![7u8; 200_000]).unwrap();
    let f0 = std::fs::read(m1.join("f")).unwrap();
    eventually("replicated to node 2", || {
        std::fs::read(c.node(2).mnt().join("d/x")).and_then(|b| {
            (b.len() == 200_000)
                .then_some(())
                .ok_or(std::io::Error::other("short"))
        })
    });
    // Node 2 writes f while node 1 is down, then loses its disk.
    c.node(1).stop();
    std::fs::write(c.node(2).mnt().join("f"), b"only on the lost disk\n").unwrap();
    std::thread::sleep(Duration::from_millis(300));
    c.node(2).stop();
    let b2 = c.node(2).backing();
    std::fs::remove_dir_all(&b2).unwrap();
    std::fs::create_dir(&b2).unwrap();
    std::fs::write(b2.join("stray"), b"left over\n").unwrap();
    c.node(1).start();
    c.node(2).start();
    c.converge("both back");
    eventually("rebuild finishes", || {
        let r = c.node(2).status("replication").unwrap();
        (r["rebuild"]["active"] == false)
            .then_some(r)
            .ok_or_else(|| std::io::Error::other("still rebuilding"))
    });
    let r = c.node(2).status("replication").unwrap();
    assert_eq!(r["rebuild"]["cause"], "disk_lost", "{r:#}");
    assert_eq!(r["rebuild"]["orphans"], 1, "{r:#}");
    let m2 = c.node(2).mnt();
    assert_eq!(std::fs::read(m2.join("d/x")).unwrap(), vec![7u8; 200_000]);
    assert_eq!(std::fs::read(m2.join("f")).unwrap(), f0);
    assert!(!m2.join("stray").exists());
    let orphans: Vec<_> = std::fs::read_dir(b2.join(".syncfs/orphans"))
        .unwrap()
        .map(|e| e.unwrap().path().join("stray"))
        .collect();
    assert_eq!(orphans.len(), 1);
    assert_eq!(std::fs::read(&orphans[0]).unwrap(), b"left over\n");
    // The version only node 2's lost disk had does not make f read-only.
    std::fs::write(m1.join("f"), b"after the loss\n").unwrap();
    wait_same_tree(&c, 1, 2);
    assert_eq!(std::fs::read(b2.join("f")).unwrap(), b"after the loss\n");
}

/// A voter-only daemon restarted as a mount has an empty backing directory
/// with no marker: it rebuilds from its peers like a node that lost its
/// disk (tree at once, fetch on demand, fill in the background, `EROFS`
/// until done), with no lost writes to wait for.
#[test]
fn daemon_upgraded_to_mount_rebuilds_from_peers() {
    require_fuse!();
    use Kind::{Daemon, Mount};
    let mut c = lock_cluster(&[Mount, Mount, Daemon], "2s", &[]);
    let m1 = c.node(1).mnt();
    std::fs::create_dir(m1.join("d")).unwrap();
    std::fs::write(m1.join("d/f"), vec![5u8; 300_000]).unwrap();
    std::fs::write(m1.join("g"), b"small").unwrap();
    eventually("replicated to node 2", || {
        std::fs::read(c.node(2).mnt().join("g"))
    });

    // Upgrade node 3: same id and peers, now with a backing directory.
    c.node(3).stop();
    c.node(3).kind = Mount;
    // Slow the background copies down so the test sees the rebuild going.
    c.node(3)
        .env
        .push(("SYNCFS_TEST_REBUILD_DELAY_MS".into(), "1500".into()));
    c.node(3).start();
    c.converge("node 3 back as a mount");
    let m3 = c.node(3).mnt();
    let rep = c.node(3).status("replication").unwrap();
    assert_eq!(rep["rebuild"]["active"], true, "{rep:#}");
    // The tree is there at once; contents come on demand, checksummed.
    assert!(m3.join("d").is_dir());
    assert_eq!(std::fs::read(m3.join("g")).unwrap(), b"small");
    // No writes until caught up.
    assert_eq!(
        errno(std::fs::write(m3.join("new"), b"x")),
        Some(libc::EROFS)
    );
    eventually("rebuild finishes", || {
        let r = c.node(3).status("replication").unwrap();
        (r["rebuild"]["active"] == false)
            .then_some(r)
            .ok_or_else(|| std::io::Error::other("still rebuilding"))
    });
    let r = c.node(3).status("replication").unwrap();
    assert_eq!(r["rebuild"]["files_done"], r["rebuild"]["files_total"]);
    assert_eq!(
        std::fs::read(c.node(3).backing().join("d/f")).unwrap(),
        vec![5u8; 300_000]
    );
    // A daemon had no unreplicated writes: nothing to keep as a conflict.
    assert_eq!(c.node(1).status("stats").unwrap()["conflict_copies"], 0);
    // Caught up: a normal writer with a new disk id.
    eventually("write after rebuild", || {
        std::fs::write(m3.join("new"), b"x")
    });
}
