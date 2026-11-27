//! The mount watchdog (issue #3): a small child process that aborts the
//! FUSE connection when the mount daemon dies without closing it.
//!
//! A SIGKILLed daemon can stay alive as a zombie leader with one thread in
//! uninterruptible sleep: the kernel's `fuse_reverse_inval_entry` waits for
//! a directory lock that a pending request holds, and that request waits
//! for the dead daemon's answer. The thread keeps `/dev/fuse` open, so the
//! kernel never aborts the connection and the mount hangs. The watchdog
//! holds no FUSE descriptor and makes no FUSE calls; when the daemon's
//! leader is a zombie it writes to `/sys/fs/fuse/connections/N/abort`, so
//! the pending requests fail and the mount answers `ENOTCONN`.

use std::ffi::CString;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;

/// How often the watchdog looks at the daemon.
const POLL_MS: libc::c_int = 200;

/// The running watchdog. Dropping it stops the watchdog: do that once the
/// connection has ended, before its number can name another mount.
pub struct Watchdog {
    pid: libc::pid_t,
    stop: Option<OwnedFd>,
}

impl Watchdog {
    /// Starts a watchdog for the FUSE mount at `mountpoint`, which this
    /// process serves. None (after a warning) if the connection cannot be
    /// found or aborted, or the fork fails.
    pub fn start(mountpoint: &Path) -> Option<Watchdog> {
        let warn = |why: String| {
            log::warn!(
                "no watchdog for {}: {why}; if this daemon is killed, the mount may hang",
                mountpoint.display()
            );
        };
        let info = match std::fs::read_to_string("/proc/self/mountinfo") {
            Ok(i) => i,
            Err(e) => {
                warn(format!("reading /proc/self/mountinfo: {e}"));
                return None;
            }
        };
        let Some(conn) = connection_id(&info, mountpoint) else {
            warn("its FUSE connection is not in /proc/self/mountinfo".into());
            return None;
        };
        let abort = format!("/sys/fs/fuse/connections/{conn}/abort");
        let abort_c = CString::new(abort.clone()).expect("no NUL");
        // SAFETY: access(2) on a valid C string.
        if unsafe { libc::access(abort_c.as_ptr(), libc::W_OK) } != 0 {
            warn(format!(
                "cannot write {abort}: {}",
                std::io::Error::last_os_error()
            ));
            return None;
        }
        // SAFETY: getpid has no preconditions.
        let daemon = unsafe { libc::getpid() };
        let stat = CString::new(format!("/proc/{daemon}/stat")).expect("no NUL");
        let (rx, tx) = match rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC) {
            Ok(p) => p,
            Err(e) => {
                warn(format!("pipe: {e}"));
                return None;
            }
        };
        // SAFETY: the child runs only async-signal-safe system calls on
        // data prepared above, then _exits; see `watch`.
        match unsafe { libc::fork() } {
            -1 => {
                warn(format!("fork: {}", std::io::Error::last_os_error()));
                None
            }
            0 => watch(daemon, rx.as_raw_fd(), &stat, &abort_c),
            pid => {
                drop(rx);
                log::debug!("watchdog {pid} aborts {abort} if this daemon dies");
                Some(Watchdog {
                    pid,
                    stop: Some(tx),
                })
            }
        }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        // The closed pipe tells it to exit; reap it.
        drop(self.stop.take());
        let mut status = 0;
        // SAFETY: waits for our own child.
        unsafe { libc::waitpid(self.pid, &mut status, 0) };
    }
}

/// The watchdog child. Forked from a multithreaded process, so it only
/// makes system calls (no allocation, locks or logging) until `_exit`.
fn watch(daemon: libc::pid_t, stop: libc::c_int, stat: &CString, abort: &CString) -> ! {
    // SAFETY: plain system calls on valid arguments.
    unsafe {
        // Hold nothing of the daemon's: above all not /dev/fuse, which
        // would keep the connection alive like the stuck thread does.
        close_all_but(stop);
        for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP, libc::SIGPIPE] {
            libc::signal(sig, libc::SIG_IGN);
        }
        libc::prctl(
            libc::PR_SET_NAME,
            c"syncfs-watchdog".as_ptr() as libc::c_ulong,
            0,
            0,
            0,
        );
        libc::chdir(c"/".as_ptr());
        loop {
            let mut pfd = libc::pollfd {
                fd: stop,
                events: libc::POLLIN,
                revents: 0,
            };
            let n = libc::poll(&mut pfd, 1, POLL_MS);
            // The daemon closed the pipe (it is done with the connection,
            // or it exited and the kernel released it).
            if n > 0 || libc::getppid() != daemon {
                libc::_exit(0);
            }
            match daemon_state(stat) {
                // Gone: its descriptors are closed.
                None => libc::_exit(0),
                // Its leader is dead. A stuck thread may keep the process
                // and /dev/fuse alive; abort the connection.
                Some(b'Z' | b'X') => {
                    let fd = libc::open(abort.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
                    if fd >= 0 {
                        libc::write(fd, b"1".as_ptr().cast(), 1);
                        libc::close(fd);
                    }
                    libc::_exit(0);
                }
                Some(_) => {}
            }
        }
    }
}

/// Closes every descriptor except `keep` (and does not allocate).
unsafe fn close_all_but(keep: libc::c_int) {
    let keep = keep as libc::c_uint;
    let range =
        |lo: libc::c_uint, hi: libc::c_uint| libc::syscall(libc::SYS_close_range, lo, hi, 0) == 0;
    let low = keep == 0 || range(0, keep - 1);
    let high = range(keep + 1, libc::c_uint::MAX);
    if low && high {
        return;
    }
    // close_range(2) is Linux 5.9+.
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let max = if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
        lim.rlim_cur.min(1 << 20) as libc::c_uint
    } else {
        1024
    };
    for fd in 0..max {
        if fd != keep {
            libc::close(fd as libc::c_int);
        }
    }
}

/// The state letter in /proc/<daemon>/stat, None if it cannot be read
/// (the process is gone). Does not allocate.
unsafe fn daemon_state(stat: &CString) -> Option<u8> {
    let fd = libc::open(stat.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC);
    if fd < 0 {
        return None;
    }
    let mut buf = [0u8; 512];
    let n = libc::read(fd, buf.as_mut_ptr().cast(), buf.len());
    libc::close(fd);
    if n <= 0 {
        return None;
    }
    let buf = &buf[..n as usize];
    // "pid (comm) S ...": comm may hold spaces and parentheses.
    let close = buf.iter().rposition(|&b| b == b')')?;
    buf.get(close + 2).copied()
}

/// The FUSE connection number (the minor of the device in `info`, a
/// mountinfo) of the topmost FUSE mount at `mountpoint`.
pub fn connection_id(info: &str, mountpoint: &Path) -> Option<u32> {
    let want = escape(&mountpoint.to_string_lossy());
    info.lines().rev().find_map(|l| {
        let f: Vec<&str> = l.split(' ').collect();
        if f.get(4) != Some(&want.as_str()) {
            return None;
        }
        let sep = f.iter().position(|&x| x == "-")?;
        let fstype = f.get(sep + 1)?;
        if *fstype != "fuse" && !fstype.starts_with("fuse.") {
            return None;
        }
        f.get(2)?.split_once(':')?.1.parse().ok()
    })
}

/// A path as mountinfo writes it: space, tab, newline and backslash in
/// octal.
fn escape(p: &str) -> String {
    let mut s = String::with_capacity(p.len());
    for c in p.chars() {
        match c {
            ' ' => s.push_str("\\040"),
            '\t' => s.push_str("\\011"),
            '\n' => s.push_str("\\012"),
            '\\' => s.push_str("\\134"),
            c => s.push(c),
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const INFO: &str = "\
22 1 0:21 / /proc rw,nosuid - proc proc rw
40 22 0:42 / /tmp/a\\040b rw,nosuid,nodev - fuse.syncfs /dev/fuse rw,user_id=1000
41 22 0:43 / /tmp/x rw - fuse syncfs rw
42 41 0:44 / /tmp/x rw - fuse.syncfs syncfs rw
43 22 0:45 / /tmp/t rw - tmpfs tmpfs rw
";

    #[test]
    fn connection_id_reads_the_topmost_fuse_mount() {
        assert_eq!(connection_id(INFO, Path::new("/tmp/a b")), Some(42));
        assert_eq!(connection_id(INFO, Path::new("/tmp/x")), Some(44));
        assert_eq!(connection_id(INFO, Path::new("/tmp/t")), None);
        assert_eq!(connection_id(INFO, Path::new("/tmp/none")), None);
    }
}
