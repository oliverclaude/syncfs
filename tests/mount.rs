//! Mounts syncfs as the current user and exercises filesystem operations.
//! Skipped (with a message) when /dev/fuse or fusermount3 is unavailable.

mod common;

use std::ffi::CString;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::Command;

use common::{is_mounted, unmount, wait_until_for, Mounted, BIN, MOUNT_WAIT};

fn cpath(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).unwrap()
}

#[test]
fn files_read_write_and_mirror_backing() {
    require_fuse!();
    let m = Mounted::new(&[]);
    fs::write(m.m("a.txt"), b"hello").unwrap();
    assert_eq!(fs::read(m.b("a.txt")).unwrap(), b"hello");
    assert_eq!(fs::read(m.m("a.txt")).unwrap(), b"hello");

    // Append and overwrite in place.
    let mut f = fs::OpenOptions::new()
        .append(true)
        .open(m.m("a.txt"))
        .unwrap();
    f.write_all(b" world").unwrap();
    drop(f);
    let mut f = fs::OpenOptions::new()
        .write(true)
        .open(m.m("a.txt"))
        .unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    f.write_all(b"J").unwrap();
    f.sync_all().unwrap();
    drop(f);
    assert_eq!(fs::read(m.b("a.txt")).unwrap(), b"Jello world");

    // A large file with a non-repeating pattern, read back in odd chunks.
    let data: Vec<u8> = (0..5_000_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    fs::write(m.m("big"), &data).unwrap();
    assert_eq!(fs::read(m.b("big")).unwrap(), data);
    let mut f = fs::File::open(m.m("big")).unwrap();
    f.seek(SeekFrom::Start(1_234_567)).unwrap();
    let mut chunk = vec![0; 777_777];
    f.read_exact(&mut chunk).unwrap();
    assert_eq!(chunk, data[1_234_567..1_234_567 + 777_777]);
    drop(f);

    // O_TRUNC and O_EXCL.
    fs::write(m.m("a.txt"), b"x").unwrap();
    assert_eq!(fs::read(m.b("a.txt")).unwrap(), b"x");
    let err = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(m.m("a.txt"))
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

    // Changes made directly in the backing directory show up.
    fs::write(m.b("outside"), b"from backing").unwrap();
    assert_eq!(fs::read(m.m("outside")).unwrap(), b"from backing");
    fs::write(m.b("outside"), b"changed").unwrap();
    assert_eq!(fs::read(m.m("outside")).unwrap(), b"changed");
    m.finish();
}

#[test]
fn directories_and_readdir() {
    require_fuse!();
    let m = Mounted::new(&[]);
    fs::create_dir(m.m("d")).unwrap();
    fs::create_dir_all(m.m("d/e/f")).unwrap();
    assert!(m.b("d/e/f").is_dir());
    for i in 0..1500 {
        fs::write(m.m(&format!("d/file-{i:04}-with-a-longish-name")), b"").unwrap();
    }
    let mut names: Vec<String> = fs::read_dir(m.m("d"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    let mut want: Vec<String> = fs::read_dir(m.b("d"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    want.sort();
    assert_eq!(names.len(), 1501);
    assert_eq!(names, want);

    // readdir inode numbers agree with stat.
    for e in fs::read_dir(m.m("d/e")).unwrap() {
        let e = e.unwrap();
        use std::os::unix::fs::DirEntryExt;
        assert_eq!(e.ino(), fs::symlink_metadata(e.path()).unwrap().ino());
    }

    assert_eq!(
        fs::remove_dir(m.m("d/e")).unwrap_err().raw_os_error(),
        Some(libc::ENOTEMPTY)
    );
    fs::remove_dir(m.m("d/e/f")).unwrap();
    fs::remove_dir(m.m("d/e")).unwrap();
    assert!(!m.b("d/e").exists());
    fs::remove_dir_all(m.m("d")).unwrap();
    assert!(!m.b("d").exists());
    m.finish();
}

#[test]
fn rename_link_symlink_unlink() {
    require_fuse!();
    let m = Mounted::new(&[]);
    fs::write(m.m("a"), b"A").unwrap();
    fs::write(m.m("b"), b"B").unwrap();
    let ino_a = fs::metadata(m.m("a")).unwrap().ino();

    // Rename keeps the inode; rename over an existing file replaces it.
    fs::rename(m.m("a"), m.m("c")).unwrap();
    assert_eq!(fs::metadata(m.m("c")).unwrap().ino(), ino_a);
    fs::rename(m.m("c"), m.m("b")).unwrap();
    assert_eq!(fs::read(m.b("b")).unwrap(), b"A");
    assert!(!m.b("c").exists());

    // renameat2 flags.
    fs::write(m.m("x"), b"X").unwrap();
    let r = |a: &str, b: &str, flags: u32| unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            cpath(&m.m(a)).as_ptr(),
            libc::AT_FDCWD,
            cpath(&m.m(b)).as_ptr(),
            flags,
        )
    };
    assert_eq!(r("x", "b", libc::RENAME_NOREPLACE), -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EEXIST)
    );
    assert_eq!(r("x", "b", libc::RENAME_EXCHANGE), 0);
    assert_eq!(fs::read(m.b("b")).unwrap(), b"X");
    assert_eq!(fs::read(m.b("x")).unwrap(), b"A");

    // Directory rename with contents.
    fs::create_dir_all(m.m("dir/sub")).unwrap();
    fs::write(m.m("dir/sub/f"), b"deep").unwrap();
    fs::rename(m.m("dir"), m.m("dir2")).unwrap();
    assert_eq!(fs::read(m.m("dir2/sub/f")).unwrap(), b"deep");
    assert_eq!(fs::read(m.b("dir2/sub/f")).unwrap(), b"deep");

    // Hard links share an inode and the link count.
    fs::hard_link(m.m("b"), m.m("b2")).unwrap();
    let (s1, s2) = (
        fs::metadata(m.m("b")).unwrap(),
        fs::metadata(m.m("b2")).unwrap(),
    );
    assert_eq!(s1.ino(), s2.ino());
    assert_eq!(s1.nlink(), 2);
    assert_eq!(
        fs::metadata(m.b("b")).unwrap().ino(),
        fs::metadata(m.b("b2")).unwrap().ino()
    );

    // Symlinks.
    std::os::unix::fs::symlink("dir2/sub/f", m.m("ln")).unwrap();
    assert_eq!(fs::read_link(m.m("ln")).unwrap(), Path::new("dir2/sub/f"));
    assert_eq!(fs::read_link(m.b("ln")).unwrap(), Path::new("dir2/sub/f"));
    assert_eq!(fs::read(m.m("ln")).unwrap(), b"deep");
    assert!(fs::symlink_metadata(m.m("ln"))
        .unwrap()
        .file_type()
        .is_symlink());
    std::os::unix::fs::symlink("nowhere", m.m("dangling")).unwrap();
    assert!(fs::metadata(m.m("dangling")).is_err());

    // An unlinked file stays readable while open.
    let mut f = fs::File::open(m.m("b2")).unwrap();
    fs::remove_file(m.m("b2")).unwrap();
    fs::remove_file(m.m("b")).unwrap();
    assert!(!m.b("b").exists());
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    assert_eq!(s, "X");
    drop(f);

    // Special files.
    assert_eq!(
        unsafe { libc::mkfifo(cpath(&m.m("fifo")).as_ptr(), 0o640) },
        0
    );
    assert!(fs::symlink_metadata(m.b("fifo")).unwrap().mode() & libc::S_IFMT == libc::S_IFIFO);
    m.finish();
}

#[test]
fn attributes_xattrs_statfs() {
    require_fuse!();
    let m = Mounted::new(&[]);
    fs::write(m.m("f"), b"0123456789").unwrap();

    fs::set_permissions(m.m("f"), fs::Permissions::from_mode(0o640)).unwrap();
    assert_eq!(fs::metadata(m.b("f")).unwrap().mode() & 0o7777, 0o640);
    assert_eq!(fs::metadata(m.m("f")).unwrap().mode() & 0o7777, 0o640);

    // Truncate shrink and grow.
    fs::OpenOptions::new()
        .write(true)
        .open(m.m("f"))
        .unwrap()
        .set_len(4)
        .unwrap();
    assert_eq!(fs::read(m.b("f")).unwrap(), b"0123");
    let rc = unsafe { libc::truncate(cpath(&m.m("f")).as_ptr(), 8) };
    assert_eq!(rc, 0);
    assert_eq!(fs::read(m.m("f")).unwrap(), b"0123\0\0\0\0");

    // Timestamps.
    let t = std::time::UNIX_EPOCH + std::time::Duration::new(1_000_000_000, 123_456_789);
    fs::File::options()
        .write(true)
        .open(m.m("f"))
        .unwrap()
        .set_modified(t)
        .unwrap();
    assert_eq!(fs::metadata(m.b("f")).unwrap().modified().unwrap(), t);
    assert_eq!(fs::metadata(m.m("f")).unwrap().modified().unwrap(), t);

    // chown to ourselves works; to someone else is refused for non-root.
    let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    std::os::unix::fs::chown(m.m("f"), Some(uid), Some(gid)).unwrap();
    if uid != 0 {
        let e = std::os::unix::fs::chown(m.m("f"), Some(0), None).unwrap_err();
        assert_eq!(e.raw_os_error(), Some(libc::EPERM));
    }

    // Permission checks are done by the kernel (default_permissions).
    if uid != 0 {
        fs::set_permissions(m.m("f"), fs::Permissions::from_mode(0o000)).unwrap();
        assert_eq!(
            fs::read(m.m("f")).unwrap_err().raw_os_error(),
            Some(libc::EACCES)
        );
        fs::set_permissions(m.m("f"), fs::Permissions::from_mode(0o644)).unwrap();
    }

    // Extended attributes.
    let p = cpath(&m.m("f"));
    let name = c"user.syncfs.test";
    let rc = unsafe { libc::setxattr(p.as_ptr(), name.as_ptr(), b"v1".as_ptr().cast(), 2, 0) };
    if rc == 0 {
        let mut buf = [0u8; 16];
        let n = unsafe { libc::getxattr(p.as_ptr(), name.as_ptr(), buf.as_mut_ptr().cast(), 16) };
        assert_eq!(&buf[..n as usize], b"v1");
        let n = unsafe { libc::getxattr(p.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
        assert_eq!(n, 2);
        let n = unsafe { libc::getxattr(p.as_ptr(), name.as_ptr(), buf.as_mut_ptr().cast(), 1) };
        assert_eq!(
            (n, std::io::Error::last_os_error().raw_os_error()),
            (-1, Some(libc::ERANGE))
        );
        let mut list = [0u8; 256];
        let n = unsafe { libc::listxattr(p.as_ptr(), list.as_mut_ptr().cast(), 256) };
        assert!(list[..n as usize]
            .split(|&b| b == 0)
            .any(|n| n == b"user.syncfs.test"));
        let rc = unsafe {
            libc::setxattr(
                p.as_ptr(),
                name.as_ptr(),
                b"x".as_ptr().cast(),
                1,
                libc::XATTR_CREATE,
            )
        };
        assert_eq!(
            (rc, std::io::Error::last_os_error().raw_os_error()),
            (-1, Some(libc::EEXIST))
        );
        assert_eq!(unsafe { libc::removexattr(p.as_ptr(), name.as_ptr()) }, 0);
        let n = unsafe { libc::getxattr(p.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
        assert_eq!(
            (n, std::io::Error::last_os_error().raw_os_error()),
            (-1, Some(libc::ENODATA))
        );
    } else {
        eprintln!("backing filesystem has no user xattrs; skipping xattr checks");
    }

    // statfs reports the backing filesystem.
    let mut a: libc::statvfs = unsafe { std::mem::zeroed() };
    let mut b: libc::statvfs = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::statvfs(cpath(&m.mnt).as_ptr(), &mut a) }, 0);
    assert_eq!(
        unsafe { libc::statvfs(cpath(&m.backing).as_ptr(), &mut b) },
        0
    );
    assert_eq!((a.f_blocks, a.f_bsize), (b.f_blocks, b.f_bsize));

    // fallocate and SEEK_HOLE/SEEK_DATA.
    let f = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(m.m("f"))
        .unwrap();
    use std::os::fd::AsRawFd;
    assert_eq!(unsafe { libc::fallocate(f.as_raw_fd(), 0, 0, 1 << 20) }, 0);
    assert_eq!(fs::metadata(m.b("f")).unwrap().len(), 1 << 20);
    let hole = unsafe { libc::lseek(f.as_raw_fd(), 0, libc::SEEK_HOLE) };
    assert!(hole >= 0);
    drop(f);
    m.finish();
}

#[test]
fn mount_options_visible_in_mount_table() {
    require_fuse!();
    let m = Mounted::new(&["-o", "noexec,noatime"]);
    let line = fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .find(|l| l.split(' ').nth(4) == Some(m.mnt.to_str().unwrap()))
        .unwrap()
        .to_string();
    assert!(line.contains("fuse.syncfs"), "{line}");
    assert!(line.contains(m.backing.to_str().unwrap()), "{line}");
    assert!(line.contains("noexec"), "{line}");
    assert!(line.contains("noatime"), "{line}");
    assert!(line.contains("default_permissions"), "{line}");
    m.finish();
}

#[test]
fn allow_other_mount() {
    require_fuse!();
    let conf = fs::read_to_string("/etc/fuse.conf").unwrap_or_default();
    let is_root = unsafe { libc::geteuid() } == 0;
    if !is_root && !conf.lines().any(|l| l.trim() == "user_allow_other") {
        eprintln!("SKIPPED: user_allow_other not set in /etc/fuse.conf");
        return;
    }
    let m = Mounted::new(&["--allow-other"]);
    let info = fs::read_to_string("/proc/self/mountinfo").unwrap();
    let line = info
        .lines()
        .find(|l| l.split(' ').nth(4) == Some(m.mnt.to_str().unwrap()))
        .unwrap();
    assert!(line.contains("allow_other"), "{line}");
    m.finish();
}

#[test]
fn wait_until_for_times_out_at_the_given_bound() {
    let start = std::time::Instant::now();
    let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_until_for("never", std::time::Duration::from_millis(80), || false);
    }));
    assert!(err.is_err());
    let elapsed = start.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_millis(80),
        "{elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(400),
        "{elapsed:?}"
    );
}

#[test]
fn wait_until_for_accepts_a_condition_that_becomes_true_before_the_bound() {
    let start = std::time::Instant::now();
    wait_until_for("soon", std::time::Duration::from_millis(400), || {
        start.elapsed() >= std::time::Duration::from_millis(80)
    });
}

#[test]
fn test_wait_env_raises_the_timeout_floor() {
    use std::time::Duration;
    assert_eq!(
        common::wait_floor_from(Duration::from_secs(10), None),
        Duration::from_secs(10)
    );
    assert_eq!(
        common::wait_floor_from(Duration::from_secs(10), Some("not-a-number")),
        Duration::from_secs(10)
    );
    assert_eq!(
        common::wait_floor_from(Duration::from_secs(30), Some("10")),
        Duration::from_secs(30)
    );
    assert_eq!(
        common::wait_floor_from(Duration::from_secs(10), Some("30")),
        Duration::from_secs(30)
    );
}

#[test]
fn daemonizes_by_default_and_debug_stays_in_foreground() {
    require_fuse!();
    let tmp = tempfile::tempdir().unwrap();
    let (b, mnt) = (tmp.path().join("b"), tmp.path().join("m"));
    fs::create_dir(&b).unwrap();
    fs::create_dir(&mnt).unwrap();
    let state = tmp.path().join("state");

    // Default: returns once mounted, daemon keeps serving.
    let out = Command::new(BIN)
        .args([&b, &mnt])
        .args(["--config", "/dev/null", "--state-dir"])
        .arg(&state)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(is_mounted(&mnt));
    fs::write(mnt.join("f"), b"daemon").unwrap();
    assert_eq!(fs::read(b.join("f")).unwrap(), b"daemon");
    let daemon_pid: u32 = fs::read_to_string(state.join("syncfs.pid"))
        .unwrap()
        .trim()
        .parse()
        .expect("daemon pid file");
    unmount(&mnt);
    wait_until_for("unmount", MOUNT_WAIT, || !is_mounted(&mnt));
    // After an external unmount the daemon still drops its fuser Session,
    // which calls fusermount on the path. Remounting the same mountpoint
    // before that process exits can unmount the new filesystem (#22).
    // store.redb is never opened for a standalone mount, so a lock on it
    // is not a valid wait.
    wait_until_for("daemon exit", MOUNT_WAIT, || !process_exists(daemon_pid));

    // A failing mount reports the error and a non-zero status.
    let out = Command::new(BIN)
        .arg(&b)
        .arg(tmp.path().join("missing"))
        .args(["--config", "/dev/null"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("mountpoint"));

    // -o debug: stays attached and logs FUSE requests.
    let mut child = Command::new(BIN)
        .args([&b, &mnt])
        .args(["--config", "/dev/null", "-o", "debug", "--state-dir"])
        .arg(&state)
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // Drain stderr while it runs: debug output can fill the pipe and block
    // the mount.
    let mut stderr = child.stderr.take().unwrap();
    let drain = std::thread::spawn(move || {
        let mut s = String::new();
        std::io::Read::read_to_string(&mut stderr, &mut s).unwrap();
        s
    });
    let drain = wait_for_debug_mount(&mut child, drain, &mnt);
    let _ = fs::read_dir(&mnt).unwrap().count();
    assert!(
        child.try_wait().unwrap().is_none(),
        "debug mode must not daemonize"
    );
    unmount(&mnt);
    assert!(child.wait().unwrap().success());
    let err = drain.join().unwrap();
    assert!(err.contains("DEBUG"), "{err}");
}

#[test]
fn sigterm_unmounts_cleanly() {
    require_fuse!();
    let mut m = Mounted::new(&[]);
    let pid = m.child.as_ref().unwrap().id() as i32;
    unsafe { libc::kill(pid, libc::SIGTERM) };
    let status = m.child.take().unwrap().wait().unwrap();
    assert!(status.success(), "{status}");
    assert!(!is_mounted(&m.mnt));
}

/// A busy mount survives the first SIGTERM (as root; a user mount is
/// detached lazily at once by fusermount3); a second one detaches it, and
/// the daemon exits once the last open file is closed.
#[test]
fn sigterm_on_a_busy_mount_detaches_on_the_second_signal() {
    require_fuse!();
    let mut m = Mounted::new(&[]);
    std::fs::write(m.m("f"), b"x").unwrap();
    let held = std::fs::File::open(m.m("f")).unwrap();
    let pid = m.child.as_ref().unwrap().id() as i32;
    unsafe { libc::kill(pid, libc::SIGTERM) };
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert!(m.child.as_mut().unwrap().try_wait().unwrap().is_none());
    unsafe { libc::kill(pid, libc::SIGTERM) };
    common::wait_until("detached", || !is_mounted(&m.mnt));
    // The open file still works until it is closed.
    let mut s = String::new();
    std::io::Read::read_to_string(&mut &held, &mut s).unwrap();
    assert_eq!(s, "x");
    assert!(m.child.as_mut().unwrap().try_wait().unwrap().is_none());
    drop(held);
    let status = m.child.take().unwrap().wait().unwrap();
    assert!(status.success(), "{status}");
}

#[test]
fn mount_helper_invocation() {
    require_fuse!();
    let tmp = tempfile::tempdir().unwrap();
    let (b, mnt) = (tmp.path().join("b"), tmp.path().join("m"));
    fs::create_dir(&b).unwrap();
    fs::create_dir(&mnt).unwrap();
    let helper = tmp.path().join("mount.syncfs");
    std::os::unix::fs::symlink(BIN, &helper).unwrap();
    let opts = format!(
        "rw,nosuid,nodev,_netdev,nofail,x-systemd.automount,node_id=1,config=/dev/null,state_dir={}",
        tmp.path().join("state").display()
    );

    // -f (fake) validates without mounting.
    let out = Command::new(&helper)
        .arg("-f")
        .args([&b, &mnt])
        .args(["-o", &opts])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!is_mounted(&mnt));

    // Unknown options fail, unless sloppy (-s).
    let out = Command::new(&helper)
        .args([&b, &mnt])
        .args(["-o", "bogus"])
        .output()
        .unwrap();
    assert!(!out.status.success());

    let out = Command::new(&helper)
        .arg("-s")
        .args([&b, &mnt])
        .args(["-o", &format!("{opts},bogus")])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(is_mounted(&mnt));
    fs::write(mnt.join("via-helper"), b"ok").unwrap();
    assert_eq!(fs::read(b.join("via-helper")).unwrap(), b"ok");
    unmount(&mnt);
}

/// As root with allow_other: other users can use the mount, new files belong
/// to whoever created them, and the kernel enforces permissions.
#[test]
fn multi_user_as_root() {
    require_fuse!();
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("SKIPPED: needs root (run the test binary with sudo)");
        return;
    }
    use std::os::unix::process::CommandExt;
    let m = Mounted::new(&["--allow-other"]);
    fs::set_permissions(&m.tmp, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(&m.mnt, fs::Permissions::from_mode(0o1777)).unwrap();
    let nobody = 65534;
    let as_nobody = |script: &str| {
        Command::new("sh")
            .args(["-c", script])
            .uid(nobody)
            .gid(nobody)
            .output()
            .unwrap()
    };
    let mnt = m.mnt.display().to_string();
    let out = as_nobody(&format!(
        "echo hi > {mnt}/n && mkdir {mnt}/nd && ln -s n {mnt}/nl"
    ));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    for name in ["n", "nd", "nl"] {
        let md = fs::symlink_metadata(m.b(name)).unwrap();
        assert_eq!((md.uid(), md.gid()), (nobody, nobody), "{name}");
    }
    // Root's private file is not readable by nobody; sticky dir protects it.
    fs::write(m.m("secret"), b"s").unwrap();
    fs::set_permissions(m.m("secret"), fs::Permissions::from_mode(0o600)).unwrap();
    assert!(!as_nobody(&format!("cat {mnt}/secret")).status.success());
    assert!(!as_nobody(&format!("rm -f {mnt}/secret")).status.success());
    assert!(m.b("secret").exists());
    // chown by root is passed through.
    std::os::unix::fs::chown(m.m("secret"), Some(nobody), None).unwrap();
    assert_eq!(fs::metadata(m.b("secret")).unwrap().uid(), nobody);
    m.finish();
}

fn process_exists(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

/// Waits until `mnt` is mounted. On timeout or if the child exits first,
/// joins the stderr drain (killing the child if it is still running) and
/// panics with that output so the failure says why.
fn wait_for_debug_mount(
    child: &mut std::process::Child,
    drain: std::thread::JoinHandle<String>,
    mnt: &Path,
) -> std::thread::JoinHandle<String> {
    let timeout = common::wait_floor(MOUNT_WAIT);
    let start = std::time::Instant::now();
    loop {
        if is_mounted(mnt) {
            return drain;
        }
        let status = child.try_wait().unwrap();
        if status.is_some() || start.elapsed() >= timeout {
            if status.is_none() {
                let _ = child.kill();
                let _ = child.wait();
            }
            let err = drain.join().unwrap();
            match status {
                Some(status) => {
                    panic!("debug mount exited before the mount appeared ({status}): {err}")
                }
                None => panic!("timed out waiting for debug mount: {err}"),
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}
