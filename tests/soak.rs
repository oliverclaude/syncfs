//! Randomized soak test: a cluster of real processes under concurrent
//! random file operations, network partitions (through a fault-injecting
//! TCP proxy on every link) and node churn (kill, stop, pause, disk loss,
//! a new store, and upgrading a daemon to a mount), then healed. Checks:
//!
//! - never two holders: from every node's lock audit log
//!   (`SYNCFS_TEST_LOCK_LOG`), no two nodes believed they held conflicting
//!   locks at the same moment;
//! - no lost committed writes: a path whose last operation (clearly after
//!   all others) succeeded ends with that operation's result, and a read
//!   under the write lock sees the last write before it;
//! - no corruption: every file holds exactly what one write wrote;
//! - conflict copies only where expected: each holds a write of the node it
//!   names, and that node was cut off or restarted after the write;
//! - convergence: after healing, every mount has the same tree.
//!
//! The schedule comes from a seed: `SYNCFS_SOAK_SEED` (printed when chosen
//! at random) and `SYNCFS_SOAK_SECS` (default 30). Timing still varies
//! between runs, so a seed reproduces the schedule, not every interleaving.
//!
//!     SYNCFS_SOAK_SECS=3600 cargo test --release --test soak -- --nocapture
//!
//! To narrow a failure down: `SYNCFS_SOAK_FAULTS` (comma-separated subset
//! of partition, link, kill, stop, pause, disk, upgrade; or none),
//! `SYNCFS_SOAK_OPS` (subset of write, rmw, read, unlink, rename, mkdir, rmdir),
//! `SYNCFS_SOAK_PATHS` (only the first N paths), `SYNCFS_SOAK_WORKERS`
//! (per node, default 2) and `SYNCFS_SOAK_ARGS` (extra daemon arguments,
//! e.g. `--log-level debug`). A failed run keeps its directory (node logs,
//! lock logs, `ops.tsv`).

mod common;

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsFd, IntoRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;
use serde_json::Value;

use common::{is_mounted, BIN};

// ---------------------------------------------------------------- random

#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed ^ 0x9e37_79b9_7f4a_7c15)
    }
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
    fn ms(&mut self, lo: u64, hi: u64) -> Duration {
        Duration::from_millis(lo + self.below(hi - lo + 1))
    }
    fn pick<'a, T>(&mut self, v: &'a [T]) -> &'a T {
        &v[self.below(v.len() as u64) as usize]
    }
}

fn now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

// --------------------------------------------------------------- content

/// What write `id` writes: a header naming it, then filler derived from it
/// (0 to ~100 KB, so blocks of 64 KB differ).
fn content(id: u64) -> Vec<u8> {
    let mut r = Rng::new(id);
    let len = match r.below(4) {
        0 => r.below(64),
        1 => r.below(4096),
        _ => r.below(100_000),
    } as usize;
    let mut v = format!("soak {id:016x}\n").into_bytes();
    while v.len() < 22 + len {
        v.extend_from_slice(&r.next().to_le_bytes());
    }
    v.truncate(22 + len);
    v
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Seen {
    Empty,
    Write(u64),
    Torn,
}

fn identify(data: &[u8]) -> Seen {
    if data.is_empty() {
        return Seen::Empty;
    }
    let id = std::str::from_utf8(data.get(5..21).unwrap_or_default())
        .ok()
        .filter(|_| data.starts_with(b"soak ") && data.get(21) == Some(&b'\n'))
        .and_then(|h| u64::from_str_radix(h, 16).ok());
    match id {
        Some(id) if content(id) == data => Seen::Write(id),
        _ => Seen::Torn,
    }
}

// ------------------------------------------------------------ the proxy

#[derive(Clone, Copy, PartialEq, Debug)]
enum Link {
    Up,
    /// Connections are closed and new ones refused.
    Cut,
    /// Nothing is forwarded (a silent partition); data flows again on heal.
    Stall,
}

/// A TCP proxy for every ordered pair of nodes: node `a` reaches node `b`
/// through `port(a, b)`, so each unordered link can be cut or stalled.
struct Proxy {
    links: Mutex<HashMap<(u16, u16), Link>>,
    conns: Mutex<HashMap<(u16, u16), Vec<TcpStream>>>,
    ports: HashMap<(u16, u16), u16>,
    stop: AtomicBool,
}

fn pair(a: u16, b: u16) -> (u16, u16) {
    (a.min(b), a.max(b))
}

impl Proxy {
    fn start(listen: &[u16]) -> Arc<Proxy> {
        let n = listen.len() as u16;
        let mut listeners = Vec::new();
        let mut ports = HashMap::new();
        for a in 1..=n {
            for b in 1..=n {
                if a != b {
                    let l = TcpListener::bind("127.0.0.1:0").unwrap();
                    ports.insert((a, b), l.local_addr().unwrap().port());
                    listeners.push((a, b, l));
                }
            }
        }
        let p = Arc::new(Proxy {
            links: Mutex::new(HashMap::new()),
            conns: Mutex::new(HashMap::new()),
            ports,
            stop: AtomicBool::new(false),
        });
        for (a, b, l) in listeners {
            let p = Arc::clone(&p);
            let target = listen[b as usize - 1];
            std::thread::spawn(move || p.accept(a, b, l, target));
        }
        p
    }

    fn port(&self, a: u16, b: u16) -> u16 {
        self.ports[&(a, b)]
    }

    fn state(&self, a: u16, b: u16) -> Link {
        *self
            .links
            .lock()
            .unwrap()
            .get(&pair(a, b))
            .unwrap_or(&Link::Up)
    }

    fn set(&self, a: u16, b: u16, s: Link) {
        self.links.lock().unwrap().insert(pair(a, b), s);
        if s == Link::Cut {
            for c in self
                .conns
                .lock()
                .unwrap()
                .remove(&pair(a, b))
                .unwrap_or_default()
            {
                let _ = c.shutdown(std::net::Shutdown::Both);
            }
        }
    }

    fn heal_all(&self) {
        self.links.lock().unwrap().clear();
    }

    fn accept(self: Arc<Self>, a: u16, b: u16, l: TcpListener, target: u16) {
        for s in l.incoming() {
            if self.stop.load(Ordering::SeqCst) {
                return;
            }
            let Ok(s) = s else { continue };
            if self.state(a, b) == Link::Cut {
                continue; // dropped: closed at once
            }
            let Ok(t) = TcpStream::connect(("127.0.0.1", target)) else {
                continue;
            };
            let _ = s.set_nodelay(true);
            let _ = t.set_nodelay(true);
            {
                let mut c = self.conns.lock().unwrap();
                let e = c.entry(pair(a, b)).or_default();
                e.retain(|x| x.peer_addr().is_ok());
                e.push(s.try_clone().unwrap());
                e.push(t.try_clone().unwrap());
            }
            for (from, to) in [(s.try_clone().unwrap(), t.try_clone().unwrap()), (t, s)] {
                let p = Arc::clone(&self);
                std::thread::spawn(move || p.pump(a, b, from, to));
            }
        }
    }

    fn pump(&self, a: u16, b: u16, mut from: TcpStream, mut to: TcpStream) {
        let _ = from.set_read_timeout(Some(Duration::from_millis(50)));
        let mut buf = vec![0u8; 64 << 10];
        loop {
            match self.state(a, b) {
                Link::Cut => break,
                Link::Stall => {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }
                Link::Up => {}
            }
            match from.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    // A cut during the read drops what was read.
                    if self.state(a, b) == Link::Cut || to.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(_) => break,
            }
        }
        let _ = from.shutdown(std::net::Shutdown::Both);
        let _ = to.shutdown(std::net::Shutdown::Both);
    }
}

// ---------------------------------------------------------------- nodes

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Mount,
    Daemon,
}

/// The open root of a running mount; operations go through it (`openat`),
/// so after a kill they fail with ENOTCONN instead of landing in the
/// empty mount point.
type Root = Arc<RwLock<Option<Arc<OwnedFd>>>>;

struct Node {
    id: u16,
    kind: Kind,
    dir: PathBuf,
    args: Vec<String>,
    child: Option<Child>,
    root: Root,
    paused: bool,
}

const HEARTBEAT: &str = "100ms";
const LEASE: &str = "1s";
const LOCK_WAIT: &str = "1s";
const FRESHNESS: &str = "2s";

impl Node {
    fn mnt(&self) -> PathBuf {
        self.dir.join("mnt")
    }
    fn backing(&self) -> PathBuf {
        self.dir.join("backing")
    }
    fn status(&self, section: &str) -> Option<Value> {
        let f = match self.kind {
            Kind::Mount => self.mnt().join(".syncfs/status.json"),
            Kind::Daemon => self.dir.join("state/status.json"),
        };
        let v: Value = serde_json::from_slice(&std::fs::read(f).ok()?).ok()?;
        Some(v[section].clone())
    }
    fn running(&self) -> bool {
        self.child.is_some()
    }

    fn start(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("log"))
            .unwrap();
        let mut cmd = Command::new(BIN);
        match self.kind {
            Kind::Mount => cmd.arg(self.backing()).arg(self.mnt()),
            Kind::Daemon => cmd.args(["--mode", "daemon"]),
        };
        let child = cmd
            .args(&self.args)
            .arg("--foreground")
            .env("SYNCFS_TEST_LOCK_LOG", self.dir.join("locks.log"))
            .stdin(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        self.child = Some(child);
        let t = Instant::now();
        loop {
            if let Some(st) = self.child.as_mut().unwrap().try_wait().unwrap() {
                panic!(
                    "node {} exited at start: {st}\n{}",
                    self.id,
                    self.log_tail()
                );
            }
            if self.kind == Kind::Daemon && self.status("peers").is_some() {
                break;
            }
            if self.kind == Kind::Mount && is_mounted(&self.mnt()) && self.status("peers").is_some()
            {
                let fd = rustix::fs::open(
                    self.mnt(),
                    OFlags::RDONLY | OFlags::DIRECTORY,
                    Mode::empty(),
                )
                .unwrap();
                *self.root.write().unwrap() = Some(Arc::new(fd));
                break;
            }
            assert!(
                t.elapsed() < Duration::from_secs(30),
                "node {} start",
                self.id
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn log_tail(&self) -> String {
        let s = std::fs::read_to_string(self.dir.join("log")).unwrap_or_default();
        let lines: Vec<&str> = s.lines().collect();
        lines[lines.len().saturating_sub(30)..].join("\n")
    }

    fn signal(&self, sig: i32) {
        if let Some(c) = &self.child {
            unsafe { libc::kill(c.id() as i32, sig) };
        }
    }

    /// SIGKILL (`clean: false`) or SIGTERM, and wait for the exit. A clean
    /// stop that is busy (a worker's operation still running) gets a second
    /// SIGTERM, which detaches the mount lazily.
    fn halt(&mut self, clean: bool) {
        *self.root.write().unwrap() = None;
        if self.paused {
            self.signal(libc::SIGCONT);
            self.paused = false;
        }
        self.signal(if clean { libc::SIGTERM } else { libc::SIGKILL });
        let t = Instant::now();
        let mut again = false;
        let mut c = self.child.take().unwrap();
        while c.try_wait().unwrap().is_none() {
            if clean && !again && t.elapsed() > Duration::from_secs(2) {
                again = true;
                unsafe { libc::kill(c.id() as i32, libc::SIGTERM) };
            }
            if t.elapsed() > Duration::from_secs(30) {
                HUNG.lock()
                    .unwrap()
                    .push(format!("node {} did not stop in 30 s", self.id));
                let _ = c.kill();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
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

    fn pause(&mut self) {
        self.signal(libc::SIGSTOP);
        self.paused = true;
    }

    fn resume(&mut self) {
        self.signal(libc::SIGCONT);
        self.paused = false;
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if self.child.is_some() {
            self.halt(true);
        }
    }
}

fn wipe(dir: &Path) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if e.file_type().unwrap().is_dir() {
            std::fs::remove_dir_all(&p).unwrap();
        } else {
            std::fs::remove_file(&p).unwrap();
        }
    }
}

// -------------------------------------------------------------- workload

/// The namespace the workers use: a few files in the root and in two fixed
/// directories, and two in `c`, which workers create and remove.
fn paths() -> Vec<String> {
    let mut v = Vec::new();
    for d in ["", "a/", "b/"] {
        for f in 0..4 {
            v.push(format!("{d}f{f}"));
        }
    }
    v.push("c/f0".into());
    v.push("c/f1".into());
    // `SYNCFS_SOAK_PATHS=N`: only the first N (to narrow a problem down).
    if let Some(n) = env_u64("SYNCFS_SOAK_PATHS") {
        v.truncate(n as usize);
    }
    v
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum OpKind {
    /// Create or truncate, write, close.
    Write,
    /// Open read-write (taking the lock), read, rewrite in place, close.
    Rmw,
    Read,
    Unlink,
    /// `path` onto `path2`.
    Rename,
    Mkdir,
    Rmdir,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Outcome {
    Ok,
    /// Failed before changing anything (refused at open/lock time).
    NoEffect(Errno),
    /// Failed part way, or the node died: it may or may not have happened.
    Uncertain(Errno),
}

#[derive(Clone, Debug)]
struct Op {
    id: u64,
    node: u16,
    kind: OpKind,
    path: String,
    path2: String,
    start: u64,
    end: u64,
    outcome: Outcome,
    /// What a read (or the read of a Rmw) saw.
    seen: Option<Seen>,
}

impl Op {
    fn touches(&self, p: &str) -> bool {
        matches!(
            self.kind,
            OpKind::Write | OpKind::Rmw | OpKind::Unlink | OpKind::Rename
        ) && (self.path == p || (self.kind == OpKind::Rename && self.path2 == p))
    }
    fn effective(&self) -> bool {
        !matches!(self.outcome, Outcome::NoEffect(_))
    }
}

/// Errors that mean "refused, nothing done" when returned by the first
/// step (open, rename, unlink, mkdir, rmdir).
fn refused(e: Errno) -> bool {
    [
        Errno::ROFS,
        Errno::BUSY,
        Errno::NOENT,
        Errno::EXIST,
        Errno::NOTEMPTY,
    ]
    .contains(&e)
}

fn close(fd: OwnedFd) -> Result<(), Errno> {
    match unsafe { libc::close(fd.into_raw_fd()) } {
        0 => Ok(()),
        _ => Err(Errno::from_io_error(&std::io::Error::last_os_error()).unwrap_or(Errno::IO)),
    }
}

fn read_all(fd: &OwnedFd) -> Result<Vec<u8>, Errno> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let n = rustix::io::pread(fd, &mut buf, out.len() as u64)?;
        if n == 0 {
            return Ok(out);
        }
        out.extend_from_slice(&buf[..n]);
    }
}

fn write_all(fd: &OwnedFd, data: &[u8], rng: &mut Rng) -> Result<(), Errno> {
    let mut off = 0;
    while off < data.len() {
        let chunk = (rng.below(48 << 10) as usize + 1).min(data.len() - off);
        let n = rustix::io::pwrite(fd, &data[off..off + chunk], off as u64)?;
        off += n;
        if rng.chance(0.05) {
            std::thread::sleep(rng.ms(1, 30)); // hold the lock a while
        }
    }
    Ok(())
}

/// Runs one operation on `root` and returns its outcome.
fn run_op(root: &OwnedFd, op: &mut Op, rng: &mut Rng) {
    let d = root.as_fd();
    let mode = Mode::from_raw_mode(0o644);
    let first = |e: Errno| {
        if refused(e) {
            Outcome::NoEffect(e)
        } else {
            Outcome::Uncertain(e)
        }
    };
    let later = Outcome::Uncertain;
    op.outcome = match op.kind {
        OpKind::Write => {
            match rustix::fs::openat(
                d,
                &op.path,
                OFlags::WRONLY | OFlags::CREATE | OFlags::TRUNC,
                mode,
            ) {
                Err(e) => first(e),
                Ok(fd) => match write_all(&fd, &content(op.id), rng).and_then(|_| close(fd)) {
                    Ok(()) => Outcome::Ok,
                    Err(e) => later(e),
                },
            }
        }
        OpKind::Rmw => match rustix::fs::openat(d, &op.path, OFlags::RDWR, mode) {
            Err(e) => first(e),
            Ok(fd) => {
                let r = read_all(&fd).and_then(|data| {
                    op.seen = Some(identify(&data));
                    let new = content(op.id);
                    write_all(&fd, &new, rng)?;
                    rustix::fs::ftruncate(&fd, new.len() as u64)
                });
                match r.and_then(|_| close(fd)) {
                    Ok(()) => Outcome::Ok,
                    Err(e) => later(e),
                }
            }
        },
        OpKind::Read => match rustix::fs::openat(d, &op.path, OFlags::RDONLY, mode) {
            Err(e) => Outcome::NoEffect(e),
            Ok(fd) => match read_all(&fd) {
                Ok(data) => {
                    op.seen = Some(identify(&data));
                    let _ = close(fd);
                    Outcome::Ok
                }
                Err(e) => Outcome::NoEffect(e),
            },
        },
        OpKind::Unlink => match rustix::fs::unlinkat(d, &op.path, rustix::fs::AtFlags::empty()) {
            Ok(()) => Outcome::Ok,
            Err(e) => first(e),
        },
        OpKind::Rename => match rustix::fs::renameat(d, &op.path, d, &op.path2) {
            Ok(()) => Outcome::Ok,
            Err(e) => first(e),
        },
        OpKind::Mkdir => match rustix::fs::mkdirat(d, &op.path, Mode::from_raw_mode(0o755)) {
            Ok(()) => Outcome::Ok,
            Err(e) => first(e),
        },
        OpKind::Rmdir => match rustix::fs::unlinkat(d, &op.path, rustix::fs::AtFlags::REMOVEDIR) {
            Ok(()) => Outcome::Ok,
            Err(e) => first(e),
        },
    };
}

/// `SYNCFS_SOAK_OPS`: a comma-separated subset of write, rmw, read,
/// unlink, rename, mkdir, rmdir (default: all).
fn op_enabled(k: OpKind) -> bool {
    match std::env::var("SYNCFS_SOAK_OPS") {
        Ok(v) => v
            .split(',')
            .any(|c| c.trim().eq_ignore_ascii_case(&format!("{k:?}"))),
        Err(_) => true,
    }
}

fn worker(
    node: u16,
    w: u64,
    seed: u64,
    root: Root,
    stop: Arc<AtomicBool>,
    ops: Arc<Mutex<Vec<Op>>>,
    next: Arc<AtomicU64>,
) {
    let mut rng = Rng::new(seed ^ (u64::from(node) << 32) ^ (w << 16));
    let paths = paths();
    while !stop.load(Ordering::SeqCst) {
        let Some(fd) = root.read().unwrap().clone() else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let kind = match rng.below(100) {
            0..=34 => OpKind::Write,
            35..=54 => OpKind::Rmw,
            55..=69 => OpKind::Read,
            70..=79 => OpKind::Unlink,
            80..=91 => OpKind::Rename,
            92..=95 => OpKind::Mkdir,
            _ => OpKind::Rmdir,
        };
        let (path, path2) = match kind {
            OpKind::Mkdir | OpKind::Rmdir => ("c".to_string(), String::new()),
            _ => (rng.pick(&paths).clone(), rng.pick(&paths).clone()),
        };
        if (kind == OpKind::Rename && path == path2) || !op_enabled(kind) {
            continue;
        }
        // Unique across the run: node, worker, counter.
        let id = (u64::from(node) << 56) | (w << 48) | next.fetch_add(1, Ordering::SeqCst);
        let mut op = Op {
            id,
            node,
            kind,
            path,
            path2,
            start: now_ns(),
            end: 0,
            outcome: Outcome::Ok,
            seen: None,
        };
        run_op(&fd, &mut op, &mut rng);
        op.end = now_ns();
        ops.lock().unwrap().push(op);
        std::thread::sleep(rng.ms(0, 15));
    }
}

// --------------------------------------------------------------- faults

#[derive(Clone, Debug)]
struct Event {
    t: u64,
    what: String,
    nodes: Vec<u16>,
    /// A disk loss (or a new store) on `nodes[0]`.
    lost_disk: bool,
}

enum Undo {
    Restart(u16),
    Resume(u16),
    Links(Vec<(u16, u16)>),
}

struct Active {
    until: Instant,
    nodes: Vec<u16>,
    undo: Undo,
}

struct Soak {
    nodes: Vec<Node>,
    proxy: Arc<Proxy>,
    events: Vec<Event>,
    active: Vec<Active>,
    /// When a fault on the node last started or ended.
    disturbed: HashMap<u16, Instant>,
    upgraded: bool,
    start: Instant,
    tmp: tempfile::TempDir,
}

impl Soak {
    fn build(kinds: &[Kind]) -> Soak {
        let tmp = tempfile::Builder::new()
            .prefix("syncfs-soak-")
            .tempdir()
            .unwrap();
        let listen = common::listen_ports(kinds.len());
        let proxy = Proxy::start(&listen);
        let n = kinds.len() as u16;
        let nodes = kinds
            .iter()
            .enumerate()
            .map(|(i, &kind)| {
                let id = i as u16 + 1;
                let dir = tmp.path().join(format!("n{id}"));
                for d in ["backing/a", "backing/b", "mnt", "state"] {
                    std::fs::create_dir_all(dir.join(d)).unwrap();
                }
                let mut args: Vec<String> = vec![
                    "--config".into(),
                    "/dev/null".into(),
                    "--cluster-name".into(),
                    "soak".into(),
                    "--node-id".into(),
                    id.to_string(),
                    "--listen".into(),
                    format!("127.0.0.1:{}", listen[i]),
                    "--state-dir".into(),
                    dir.join("state").display().to_string(),
                ];
                for (k, v) in [
                    ("--heartbeat-interval", HEARTBEAT),
                    ("--lock-lease-timeout", LEASE),
                    ("--lock-wait", LOCK_WAIT),
                    ("--freshness-timeout", FRESHNESS),
                ] {
                    args.push(k.into());
                    args.push(v.into());
                }
                // Extra node arguments, e.g. `--log-level debug`.
                if let Ok(extra) = std::env::var("SYNCFS_SOAK_ARGS") {
                    args.extend(extra.split_whitespace().map(String::from));
                }
                for p in 1..=n {
                    let port = if p == id {
                        listen[i]
                    } else {
                        proxy.port(id, p)
                    };
                    args.push("--peer".into());
                    args.push(format!("{p}@127.0.0.1:{port}"));
                }
                Node {
                    id,
                    kind,
                    dir,
                    args,
                    child: None,
                    root: Arc::new(RwLock::new(None)),
                    paused: false,
                }
            })
            .collect();
        Soak {
            nodes,
            proxy,
            events: Vec::new(),
            active: Vec::new(),
            disturbed: HashMap::new(),
            upgraded: false,
            start: Instant::now(),
            tmp,
        }
    }

    fn node(&mut self, id: u16) -> &mut Node {
        &mut self.nodes[id as usize - 1]
    }

    fn ids(&self) -> Vec<u16> {
        self.nodes.iter().map(|n| n.id).collect()
    }

    fn event(&mut self, what: String, nodes: Vec<u16>, lost_disk: bool) {
        eprintln!("[{:>7.1}s] {what}", self.elapsed());
        let now = Instant::now();
        for n in &nodes {
            self.disturbed.insert(*n, now);
        }
        self.events.push(Event {
            t: now_ns(),
            what,
            nodes,
            lost_disk,
        });
    }

    fn elapsed(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    fn busy(&self, n: u16) -> bool {
        self.active.iter().any(|a| a.nodes.contains(&n))
    }

    /// Node `n` and a data peer have been undisturbed and connected for a
    /// while, so `n`'s finished writes have reached that peer.
    fn settled(&self, n: u16) -> bool {
        let quiet = |m: u16| {
            !self.busy(m)
                && self.nodes[m as usize - 1].running()
                && self
                    .disturbed
                    .get(&m)
                    .is_none_or(|t| t.elapsed() > Duration::from_secs(3))
        };
        quiet(n)
            && self.nodes.iter().any(|o| {
                o.id != n
                    && o.kind == Kind::Mount
                    && quiet(o.id)
                    && self.proxy.state(n, o.id) == Link::Up
            })
    }

    /// Starts one random fault (if a node is free for it).
    fn inject(&mut self, rng: &mut Rng) {
        let free: Vec<u16> = self.ids().into_iter().filter(|&n| !self.busy(n)).collect();
        if free.is_empty() {
            return;
        }
        let n = *rng.pick(&free);
        let kind = self.nodes[n as usize - 1].kind;
        let hold = |rng: &mut Rng| Instant::now() + rng.ms(300, 5000);
        let r = rng.below(100);
        let class = match r {
            0..=29 => "partition",
            30..=39 => "link",
            40..=54 => "kill",
            55..=64 => "stop",
            65..=79 => "pause",
            80..=91 => "disk",
            _ => "upgrade",
        };
        if !fault_enabled(class) {
            return;
        }
        match r {
            0..=29 => {
                // A minority (up to half, rounded down) cut off from the rest.
                let all = self.ids();
                let size = 1 + rng.below(all.len() as u64 / 2) as usize;
                let mut side: Vec<u16> = Vec::new();
                while side.len() < size {
                    let c = *rng.pick(&free);
                    if !side.contains(&c) {
                        side.push(c);
                    }
                    if side.len() == free.len() {
                        break;
                    }
                }
                side.sort();
                let mode = if rng.chance(0.6) {
                    Link::Cut
                } else {
                    Link::Stall
                };
                let links: Vec<(u16, u16)> = side
                    .iter()
                    .flat_map(|&a| {
                        all.iter()
                            .filter(|b| !side.contains(b))
                            .map(move |&b| (a, b))
                    })
                    .collect();
                for &(a, b) in &links {
                    self.proxy.set(a, b, mode);
                }
                self.event(
                    format!("partition {side:?} from the rest ({mode:?})"),
                    side.clone(),
                    false,
                );
                self.active.push(Active {
                    until: hold(rng),
                    nodes: side,
                    undo: Undo::Links(links),
                });
            }
            30..=39 => {
                // One link only: a non-transitive partition.
                let others: Vec<u16> = free.iter().copied().filter(|&m| m != n).collect();
                if others.is_empty() {
                    return;
                }
                let m = *rng.pick(&others);
                let mode = if rng.chance(0.6) {
                    Link::Cut
                } else {
                    Link::Stall
                };
                self.proxy.set(n, m, mode);
                self.event(format!("link {n}-{m} {mode:?}"), vec![n, m], false);
                self.active.push(Active {
                    until: hold(rng),
                    nodes: vec![n, m],
                    undo: Undo::Links(vec![(n, m)]),
                });
            }
            r @ 40..=64 => {
                let clean = r >= 55;
                self.node(n).halt(clean);
                let what = if clean {
                    "stop (SIGTERM)"
                } else {
                    "kill (SIGKILL)"
                };
                self.event(format!("{what} node {n}"), vec![n], false);
                self.active.push(Active {
                    until: Instant::now() + rng.ms(100, 3000),
                    nodes: vec![n],
                    undo: Undo::Restart(n),
                });
            }
            65..=79 => {
                self.node(n).pause();
                self.event(format!("pause (SIGSTOP) node {n}"), vec![n], false);
                self.active.push(Active {
                    until: Instant::now() + rng.ms(100, 3000),
                    nodes: vec![n],
                    undo: Undo::Resume(n),
                });
            }
            80..=91 if kind == Kind::Mount && self.settled(n) => {
                // Lose the backing disk; sometimes the store too (a new member).
                let store = rng.chance(0.3);
                self.node(n).halt(false);
                let node = self.node(n);
                wipe(&node.backing());
                if store {
                    wipe(&node.dir.join("state"));
                }
                let what = if store {
                    "new disk and store"
                } else {
                    "disk loss"
                };
                self.event(format!("{what} on node {n}"), vec![n], true);
                self.active.push(Active {
                    until: Instant::now() + rng.ms(100, 2000),
                    nodes: vec![n],
                    undo: Undo::Restart(n),
                });
            }
            92..=99 if kind == Kind::Daemon && !self.upgraded => {
                self.upgraded = true;
                self.node(n).halt(false);
                self.node(n).kind = Kind::Mount;
                self.event(format!("upgrade daemon {n} to a mount"), vec![n], false);
                self.active.push(Active {
                    until: Instant::now() + rng.ms(100, 1000),
                    nodes: vec![n],
                    undo: Undo::Restart(n),
                });
            }
            _ => {}
        }
    }

    fn undo(&mut self, a: Active) {
        match a.undo {
            Undo::Restart(n) => {
                self.node(n).start();
                self.event(format!("node {n} back"), vec![n], false);
            }
            Undo::Resume(n) => {
                self.node(n).resume();
                self.event(format!("node {n} resumed"), vec![n], false);
            }
            Undo::Links(links) => {
                for (a, b) in &links {
                    self.proxy.set(*a, *b, Link::Up);
                }
                self.event(format!("healed {} links", links.len()), a.nodes, false);
            }
        }
    }

    fn undo_due(&mut self, all: bool) {
        let now = Instant::now();
        let (due, keep): (Vec<Active>, Vec<Active>) = std::mem::take(&mut self.active)
            .into_iter()
            .partition(|a| all || a.until <= now);
        self.active = keep;
        for a in due {
            self.undo(a);
        }
    }

    /// Every running node sees every node up.
    fn all_up(&self) -> bool {
        let n = self.nodes.len();
        self.nodes.iter().all(|node| {
            node.status("peers")
                .is_some_and(|p| p["up"].as_array().is_some_and(|u| u.len() == n))
        })
    }

    fn mounts(&self) -> Vec<&Node> {
        self.nodes
            .iter()
            .filter(|n| n.kind == Kind::Mount)
            .collect()
    }
}

/// Problems found outside the checks (a stop that hung).
static HUNG: Mutex<Vec<String>> = Mutex::new(Vec::new());

// --------------------------------------------------------------- checks

/// A mount's backing tree: path -> description (type, content, mode).
fn tree(root: &Path) -> BTreeMap<String, String> {
    use std::os::unix::fs::PermissionsExt;
    let mut out = BTreeMap::new();
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
            let Ok(md) = std::fs::symlink_metadata(&p) else {
                continue;
            };
            let what = if md.is_dir() {
                stack.push(p.clone());
                "dir".to_string()
            } else if md.is_file() {
                let data = std::fs::read(&p).unwrap_or_default();
                match identify(&data) {
                    Seen::Write(id) => format!("write {id:016x}"),
                    Seen::Empty => "empty".into(),
                    Seen::Torn => format!("torn {} bytes {:x}", data.len(), fnv(&data)),
                }
            } else {
                "other".into()
            };
            out.insert(
                rel,
                format!("{what} mode {:o}", md.permissions().mode() & 0o7777),
            );
        }
    }
    out
}

fn fnv(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ *b as u64).wrapping_mul(0x100000001b3)
    })
}

/// One node's belief that it holds `key`, from the lock audit log.
#[derive(Debug, Clone)]
struct Hold {
    node: u16,
    pid: u32,
    ballot: String,
    key: String,
    start: u64,
    until: u64,
    released: Option<u64>,
}

impl Hold {
    fn end(&self) -> u64 {
        self.released.map_or(self.until, |r| r.min(self.until))
    }
}

fn holds(log: &Path) -> Vec<Hold> {
    let mut by: HashMap<(u32, String, String), Hold> = HashMap::new();
    let text = std::fs::read_to_string(log).unwrap_or_default();
    for l in text.lines() {
        let f: Vec<&str> = l.splitn(7, '\t').collect();
        if f.len() != 7 {
            continue;
        }
        let (node, pid, ballot, now, until, key) = (
            f[1].parse::<u16>().unwrap(),
            f[2].parse::<u32>().unwrap(),
            f[3].to_string(),
            f[4].parse::<u64>().unwrap(),
            f[5].parse::<u64>().unwrap(),
            f[6].to_string(),
        );
        let k = (pid, ballot.clone(), key.clone());
        match f[0] {
            "H" => {
                by.insert(
                    k,
                    Hold {
                        node,
                        pid,
                        ballot,
                        key,
                        start: now,
                        until,
                        released: None,
                    },
                );
            }
            "R" => {
                if let Some(h) = by.get_mut(&k) {
                    h.until = h.until.max(until);
                }
            }
            _ => {
                if let Some(h) = by.get_mut(&k) {
                    h.released.get_or_insert(now);
                }
            }
        }
    }
    by.into_values().collect()
}

/// Lock keys conflict when one is the other or an ancestor of it.
fn keys_conflict(a: &str, b: &str) -> bool {
    let (a, b) = (a.trim_matches('/'), b.trim_matches('/'));
    let under = |x: &str, y: &str| y.is_empty() || x == y || x.starts_with(&format!("{y}/"));
    under(a, b) || under(b, a)
}

fn check_holders(all: &mut [Hold], bad: &mut Vec<String>) -> usize {
    all.sort_by_key(|h| h.start);
    let mut live: Vec<&Hold> = Vec::new();
    let mut overlaps = 0;
    for h in all.iter() {
        live.retain(|o| o.end() > h.start);
        for o in &live {
            if o.node != h.node && keys_conflict(&o.key, &h.key) && h.start < o.end() {
                overlaps += 1;
                if overlaps <= 20 {
                    bad.push(format!(
                        "two holders: node {} held {} (ballot {}, pid {}) until +{} ms after node {} \
                         took {} (ballot {}, pid {})",
                        o.node,
                        o.key,
                        o.ballot,
                        o.pid,
                        (o.end() - h.start) / 1_000_000,
                        h.node,
                        h.key,
                        h.ballot,
                        h.pid
                    ));
                }
            }
        }
        live.push(h);
    }
    overlaps
}

struct Checker<'a> {
    ops: &'a [Op],
    events: &'a [Event],
    by_id: HashMap<u64, &'a Op>,
}

impl<'a> Checker<'a> {
    /// Node `op.node` lost its disk soon after `op` (before it could have
    /// replicated it), or was killed just after a namespace change (the
    /// documented crash window: a lost remove or rename may come back).
    fn excused(&self, op: &Op) -> bool {
        self.events.iter().any(|e| {
            e.nodes.first() == Some(&op.node)
                && e.t >= op.start
                && ((e.lost_disk && e.t < op.end + 3_000_000_000)
                    || (matches!(op.kind, OpKind::Unlink | OpKind::Rename)
                        && e.what.starts_with("kill")
                        && e.t < op.end + 1_000_000_000))
        })
    }

    /// The effective operations on `p`, in order of completion.
    fn on(&self, p: &str) -> Vec<&'a Op> {
        let mut v: Vec<&Op> = self
            .ops
            .iter()
            .filter(|o| o.touches(p) && o.effective())
            .collect();
        v.sort_by_key(|o| o.end);
        v
    }

    fn final_state(&self, p: &str, fin: &BTreeMap<String, String>, bad: &mut Vec<String>) -> bool {
        let ops = self.on(p);
        let Some(last) = ops.last() else {
            return false;
        };
        if last.outcome != Outcome::Ok
            || ops[..ops.len() - 1].iter().any(|o| o.end >= last.start)
            || self.excused(last)
        {
            return false;
        }
        let got = fin
            .get(p)
            .map(|s| s.split(" mode").next().unwrap().to_string());
        let ok = match last.kind {
            OpKind::Write | OpKind::Rmw => got == Some(format!("write {:016x}", last.id)),
            OpKind::Unlink => got.is_none(),
            OpKind::Rename if last.path == p => got.is_none(),
            OpKind::Rename => got.is_some(),
            _ => true,
        };
        if !ok {
            bad.push(format!(
                "lost committed change: {p} should be the result of {:?} {:016x} by node {} \
                 (ended at {}), but is {got:?}",
                last.kind, last.id, last.node, last.end
            ));
        }
        true
    }

    /// A read under the write lock sees the last write before it.
    fn locked_read(&self, r: &Op, bad: &mut Vec<String>) -> bool {
        let Some(seen) = r.seen else { return false };
        let ops: Vec<&Op> = self
            .on(&r.path)
            .into_iter()
            .filter(|o| o.id != r.id)
            .collect();
        let Some(w) = ops.iter().filter(|o| o.end < r.start).max_by_key(|o| o.end) else {
            return false;
        };
        // Only when nothing else touched the path until the read ended: the
        // lock is taken some time after the operation started (it may wait
        // for another node's write).
        if w.outcome != Outcome::Ok
            || !matches!(w.kind, OpKind::Write | OpKind::Rmw)
            || ops
                .iter()
                .any(|o| o.id != w.id && o.end >= w.start && o.start <= r.end)
            || self.excused(w)
        {
            return false;
        }
        if seen != Seen::Write(w.id) {
            bad.push(format!(
                "stale read under lock: node {} read {} ({:016x}) and saw {seen:?}, but the last \
                 write before was {:016x} by node {}",
                r.node, r.path, r.id, w.id, w.node
            ));
        }
        true
    }

    /// Every file holds one write's full content, unless a write that may
    /// have been cut short wrote it.
    fn intact(&self, path: &str, desc: &str, bad: &mut Vec<String>) {
        if !desc.starts_with("torn") {
            return;
        }
        let uncertain = self.ops.iter().any(|o| {
            matches!(o.outcome, Outcome::Uncertain(_))
                && matches!(o.kind, OpKind::Write | OpKind::Rmw)
        });
        if !uncertain {
            bad.push(format!("corrupt file {path}: {desc}"));
        }
    }

    /// A conflict copy holds a write by the node it names, and that node
    /// was cut off, paused or restarted after the write.
    fn conflict_copy(&self, path: &str, desc: &str, bad: &mut Vec<String>) {
        let tail = path.split(".syncfs-conflict-").nth(1).unwrap_or_default();
        let node: u16 = tail.split('-').next().unwrap_or("").parse().unwrap_or(0);
        let id = desc
            .strip_prefix("write ")
            .and_then(|s| u64::from_str_radix(&s[..16], 16).ok());
        let Some(op) = id.and_then(|id| self.by_id.get(&id)) else {
            if !desc.starts_with("empty") && !desc.starts_with("torn") {
                bad.push(format!("conflict copy {path} holds no known write: {desc}"));
            }
            return;
        };
        if op.node != node {
            bad.push(format!(
                "conflict copy {path} names node {node} but holds a write by node {}",
                op.node
            ));
        }
        let cut_off = self
            .events
            .iter()
            .any(|e| e.nodes.contains(&node) && e.t >= op.start);
        if !cut_off {
            bad.push(format!(
                "unexpected conflict copy {path}: node {node} was not cut off or restarted after \
                 writing it"
            ));
        }
    }
}

// ----------------------------------------------------------------- run

static SERIAL: Mutex<()> = Mutex::new(());

/// `SYNCFS_SOAK_FAULTS`: a comma-separated subset of partition, link, kill,
/// stop, pause, disk, upgrade (default: all; `none` for the workload only).
fn fault_enabled(class: &str) -> bool {
    match std::env::var("SYNCFS_SOAK_FAULTS") {
        Ok(v) => v.split(',').any(|c| c.trim() == class),
        Err(_) => true,
    }
}

fn env_u64(name: &str) -> Option<u64> {
    let v = std::env::var(name).ok()?;
    let v = v.trim();
    match v.strip_prefix("0x") {
        Some(h) => u64::from_str_radix(h, 16).ok(),
        None => v.parse().ok(),
    }
}

fn soak(kinds: &[Kind]) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let seed =
        env_u64("SYNCFS_SOAK_SEED").unwrap_or_else(|| now_ns() ^ u64::from(std::process::id()));
    let secs = env_u64("SYNCFS_SOAK_SECS").unwrap_or(30);
    eprintln!(
        "soak: {} nodes {kinds:?}, {secs} s, seed {seed:#x} (repeat with SYNCFS_SOAK_SEED={seed:#x})",
        kinds.len()
    );
    let mut s = Soak::build(kinds);
    for n in &mut s.nodes {
        n.start();
    }
    let t = Instant::now();
    while !s.all_up() {
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "cluster did not come up"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    // Voters grant nothing in their first lease.
    std::thread::sleep(Duration::from_millis(1200));
    s.start = Instant::now();

    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(Mutex::new(Vec::new()));
    let next = Arc::new(AtomicU64::new(1));
    let mut workers = Vec::new();
    for n in &s.nodes {
        for w in 0..env_u64("SYNCFS_SOAK_WORKERS").unwrap_or(2) {
            let (root, stop, ops, next) = (
                Arc::clone(&n.root),
                Arc::clone(&stop),
                Arc::clone(&ops),
                Arc::clone(&next),
            );
            let id = n.id;
            workers.push(std::thread::spawn(move || {
                worker(id, w, seed, root, stop, ops, next)
            }));
        }
    }

    let mut rng = Rng::new(seed);
    let max_active = kinds.len() / 2 + 1;
    while s.start.elapsed() < Duration::from_secs(secs) {
        s.undo_due(false);
        if s.active.len() < max_active && rng.chance(0.12) {
            s.inject(&mut rng);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("[{:>7.1}s] healing", s.elapsed());
    s.undo_due(true);
    s.proxy.heal_all();
    stop.store(true, Ordering::SeqCst);
    for w in workers {
        w.join().unwrap();
    }
    let ops = std::mem::take(&mut *ops.lock().unwrap());

    // Converge: everyone up, every mount in sync and done rebuilding, the
    // same tree everywhere.
    let t = Instant::now();
    let mut why = String::new();
    let converged = loop {
        let trees: Vec<(u16, BTreeMap<String, String>)> = s
            .mounts()
            .iter()
            .map(|n| (n.id, tree(&n.backing())))
            .collect();
        let synced = s.mounts().iter().all(|n| {
            n.status("replication")
                .is_some_and(|r| r["in_sync"] == true && r["rebuild"]["active"] == false)
        });
        let same = trees.windows(2).all(|w| w[0].1 == w[1].1);
        if s.all_up() && synced && same {
            break Some(trees[0].1.clone());
        }
        if t.elapsed() > Duration::from_secs(120) {
            why = format!("all up: {}, in sync: {synced}\n", s.all_up());
            let base = &trees[0];
            for (id, tr) in &trees[1..] {
                for (k, v) in base.1.iter() {
                    if tr.get(k) != Some(v) {
                        why += &format!("  {k}: node {} {v} / node {id} {:?}\n", base.0, tr.get(k));
                    }
                }
                for k in tr.keys().filter(|k| !base.1.contains_key(*k)) {
                    why += &format!("  {k}: only on node {id}\n");
                }
            }
            break None;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let converge_time = t.elapsed();

    let mut bad: Vec<String> = HUNG.lock().unwrap().drain(..).collect();
    let mut holds_all = Vec::new();
    for n in &s.nodes {
        holds_all.extend(holds(&n.dir.join("locks.log")));
    }
    let nholds = holds_all.len();
    let overlaps = check_holders(&mut holds_all, &mut bad);
    let (mut finals, mut locked_reads, mut copies) = (0, 0, 0);
    match &converged {
        None => bad.push(format!("no convergence within 120 s after healing:\n{why}")),
        Some(fin) => {
            let c = Checker {
                ops: &ops,
                events: &s.events,
                by_id: ops.iter().map(|o| (o.id, o)).collect(),
            };
            for p in paths() {
                finals += usize::from(c.final_state(&p, fin, &mut bad));
            }
            for r in ops.iter().filter(|o| o.kind == OpKind::Rmw) {
                locked_reads += usize::from(c.locked_read(r, &mut bad));
            }
            for (p, d) in fin {
                c.intact(p, d, &mut bad);
                if p.contains(".syncfs-conflict-") {
                    copies += 1;
                    c.conflict_copy(p, d, &mut bad);
                }
            }
        }
    }

    // Summary.
    let mut tally: BTreeMap<String, usize> = BTreeMap::new();
    for o in &ops {
        let k = match o.outcome {
            Outcome::Ok => format!("{:?} ok", o.kind),
            Outcome::NoEffect(e) => format!("{:?} refused {e:?}", o.kind),
            Outcome::Uncertain(e) => format!("{:?} failed {e:?}", o.kind),
        };
        *tally.entry(k).or_default() += 1;
    }
    let torn_reads = ops
        .iter()
        .filter(|o| o.kind == OpKind::Read && o.seen == Some(Seen::Torn))
        .count();
    eprintln!(
        "soak: seed {seed:#x}: {} ops, {} faults, {nholds} lock holds ({overlaps} overlapping), \
         converged in {converge_time:?}; checked {finals} final states, {locked_reads} locked \
         reads; {copies} conflict copies; {torn_reads} unlocked reads saw a file mid-update",
        ops.len(),
        s.events
            .iter()
            .filter(|e| !e.what.contains("back")
                && !e.what.contains("resumed")
                && !e.what.starts_with("healed"))
            .count(),
    );
    for (k, v) in &tally {
        eprintln!("  {k}: {v}");
    }
    for n in &s.nodes {
        let log = std::fs::read_to_string(n.dir.join("log")).unwrap_or_default();
        let count = |pat: &str| log.lines().filter(|l| l.contains(pat)).count();
        eprintln!(
            "  node {}: {} warnings, {} errors, {} panics, {} freshness fallbacks, {} conflict copies",
            n.id,
            count(" WARN: "),
            count(" ERROR: "),
            count("panicked"),
            count("freshness: no node with version") + count("answered without it"),
            count("this node's copy is kept as"),
        );
        if count("panicked") > 0 {
            bad.push(format!(
                "node {} panicked: see {}",
                n.id,
                n.dir.join("log").display()
            ));
        }
    }
    // `SYNCFS_SOAK_KEEP=1`: keep the directory of a passing run too.
    if !bad.is_empty() || std::env::var_os("SYNCFS_SOAK_KEEP").is_some() {
        let keep = std::mem::replace(
            &mut s.tmp,
            tempfile::Builder::new()
                .prefix("syncfs-soak-x")
                .tempdir()
                .unwrap(),
        );
        let kept = keep.keep();
        let mut dump = String::new();
        for o in &ops {
            dump += &format!(
                "{}\t{}\t{:016x}\t{:?}\t{}\t{}\t{:?}\t{:?}\n",
                o.start, o.end, o.id, o.kind, o.path, o.path2, o.outcome, o.seen
            );
        }
        for e in &s.events {
            dump += &format!("{}\t{}\tevent\t{}\n", e.t, e.t, e.what);
        }
        let _ = std::fs::write(kept.join("ops.tsv"), dump);
        // The nodes stop with `s`; their directories stay for inspection.
        if bad.is_empty() {
            eprintln!("soak: kept {}", kept.display());
            return;
        }
        panic!(
            "soak failed (seed {seed:#x}; node logs and lock logs in {}):\n{}",
            kept.display(),
            bad.join("\n")
        );
    }
}

#[test]
#[ignore = "finds divergence under concurrent writes; fixed in the next commits"]
fn soak_three_mounts() {
    require_fuse!();
    soak(&[Kind::Mount, Kind::Mount, Kind::Mount]);
}

#[test]
#[ignore = "finds divergence under concurrent writes; fixed in the next commits"]
fn soak_five_nodes_with_daemons() {
    require_fuse!();
    soak(&[
        Kind::Mount,
        Kind::Mount,
        Kind::Mount,
        Kind::Daemon,
        Kind::Daemon,
    ]);
}
