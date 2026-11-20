//! Shared helpers for tests that mount a real syncfs.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const BIN: &str = env!("CARGO_BIN_EXE_syncfs");

/// Returns why FUSE mounts cannot be tested here, if they cannot.
pub fn fuse_unavailable() -> Option<String> {
    if std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")
        .is_err()
    {
        return Some("/dev/fuse is not accessible".into());
    }
    let is_root = unsafe { libc::geteuid() } == 0;
    if !is_root && which("fusermount3").is_none() {
        return Some("fusermount3 is not installed (apt install fuse3)".into());
    }
    None
}

pub fn which(cmd: &str) -> Option<PathBuf> {
    std::env::var_os("PATH")?
        .to_str()?
        .split(':')
        .map(|d| Path::new(d).join(cmd))
        .find(|p| p.is_file())
}

/// Skips the calling test (by returning) when FUSE is unavailable.
#[macro_export]
macro_rules! require_fuse {
    () => {
        if let Some(why) = common::fuse_unavailable() {
            eprintln!("SKIPPED: {why}");
            return;
        }
    };
}

pub fn is_mounted(mountpoint: &Path) -> bool {
    let want = mountpoint.to_string_lossy().into_owned();
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap_or_default()
        .lines()
        .any(|l| l.split(' ').nth(4) == Some(want.as_str()))
}

/// Default bound for [`wait_until`]. Mount and daemon-start waits use
/// [`MOUNT_WAIT`] instead: under load a FUSE mount can take longer than
/// this (issue #12).
pub const DEFAULT_WAIT: Duration = Duration::from_secs(10);

/// Bound for waiting until a test mount or daemon is up.
pub const MOUNT_WAIT: Duration = Duration::from_secs(30);

pub fn wait_until(what: &str, cond: impl FnMut() -> bool) {
    wait_until_for(what, DEFAULT_WAIT, cond)
}

pub fn wait_until_for(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let timeout = wait_floor(timeout);
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// If `SYNCFS_TEST_WAIT` is set to a number of seconds, use at least that
/// long, so a loaded machine can stretch waits without a rebuild.
pub fn wait_floor(timeout: Duration) -> Duration {
    wait_floor_from(timeout, std::env::var("SYNCFS_TEST_WAIT").ok().as_deref())
}

pub fn wait_floor_from(timeout: Duration, env: Option<&str>) -> Duration {
    match env.and_then(|s| s.parse::<u64>().ok()) {
        Some(secs) => timeout.max(Duration::from_secs(secs)),
        None => timeout,
    }
}

/// Unmounts, retrying briefly while the kernel still reports the mount
/// busy (closing a file finishes asynchronously).
pub fn unmount(mountpoint: &Path) {
    let is_root = unsafe { libc::geteuid() } == 0;
    let start = Instant::now();
    loop {
        let out = if is_root {
            Command::new("umount").arg(mountpoint).output()
        } else {
            Command::new("fusermount3")
                .arg("-u")
                .arg(mountpoint)
                .output()
        }
        .unwrap();
        if out.status.success() {
            return;
        }
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains("busy") && start.elapsed() < Duration::from_secs(5),
            "unmount {}: {err}",
            mountpoint.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A syncfs mounted in the foreground in a temporary directory.
pub struct Mounted {
    pub tmp: tempfile::TempDir,
    pub backing: PathBuf,
    pub mnt: PathBuf,
    pub child: Option<Child>,
}

impl Mounted {
    pub fn new(extra: &[&str]) -> Self {
        Self::with_backing(|_| {}, extra)
    }

    /// Like [`new`](Self::new), running `prepare` on the backing
    /// directory before mounting.
    pub fn with_backing(prepare: impl FnOnce(&Path), extra: &[&str]) -> Self {
        let tmp = tempfile::Builder::new()
            .prefix("syncfs-test-")
            .tempdir()
            .unwrap();
        let backing = tmp.path().join("backing");
        let mnt = tmp.path().join("mnt");
        std::fs::create_dir(&backing).unwrap();
        std::fs::create_dir(&mnt).unwrap();
        prepare(&backing);
        let child = Command::new(BIN)
            .arg(&backing)
            .arg(&mnt)
            .arg("--foreground")
            .arg("--state-dir")
            .arg(tmp.path().join("state"))
            .arg("--config")
            .arg("/dev/null")
            .args(extra)
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let mut m = Mounted {
            tmp,
            backing,
            mnt,
            child: Some(child),
        };
        let mnt = m.mnt.clone();
        wait_until_for("mount", MOUNT_WAIT, || {
            if let Some(status) = m.child.as_mut().unwrap().try_wait().unwrap() {
                panic!("syncfs exited early: {status}");
            }
            is_mounted(&mnt)
        });
        m
    }

    pub fn m(&self, rel: &str) -> PathBuf {
        self.mnt.join(rel)
    }

    pub fn b(&self, rel: &str) -> PathBuf {
        self.backing.join(rel)
    }

    /// Unmounts and checks that the daemon exits cleanly.
    pub fn finish(mut self) {
        unmount(&self.mnt);
        let status = self.child.take().unwrap().wait().unwrap();
        assert!(status.success(), "syncfs exit status {status}");
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if is_mounted(&self.mnt) {
                let _ = Command::new("fusermount3")
                    .arg("-u")
                    .arg(&self.mnt)
                    .status();
                let _ = Command::new("umount").arg("-l").arg(&self.mnt).status();
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// `n` free TCP ports on 127.0.0.1 for node listeners, chosen below the
/// kernel's ephemeral range so that outgoing connections (of this or a
/// concurrent test) cannot take them before or between node restarts.
pub fn listen_ports(n: usize) -> Vec<u16> {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let low = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse::<u16>().ok())
        .unwrap_or(32768);
    let (base, span) = (10_000u16, low.saturating_sub(10_000).max(1000));
    // Spread concurrent test binaries apart by pid.
    let _ = NEXT.compare_exchange(
        0,
        (std::process::id() % u32::from(span)) as u16 + 1,
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    let mut out = Vec::new();
    while out.len() < n {
        let off = NEXT.fetch_add(1, Ordering::SeqCst) % span;
        let port = base + off;
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            out.push(port);
        }
    }
    out
}

/// Retries `f` until it returns `Some`, for up to 10 s.
#[allow(dead_code)]
pub fn wait_for<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
