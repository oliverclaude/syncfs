//! The hidden `.syncfs` status directory. Needs FUSE; skips otherwise.

mod common;

use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use common::{wait_until, Mounted};
use serde_json::Value;

const FILES: &[&str] = &[
    "status.json",
    "node",
    "config",
    "stats",
    "peers",
    "locks",
    "replication",
];

fn is_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

fn status(m: &Mounted) -> Value {
    let raw = fs::read(m.m(".syncfs/status.json")).unwrap();
    serde_json::from_slice(&raw).unwrap()
}

fn stat_u64(v: &Value, key: &str) -> u64 {
    v["stats"][key]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.{key}"))
}

fn op(v: &Value, name: &str, field: &str) -> u64 {
    v["stats"]["ops"][name][field].as_u64().unwrap()
}

/// The error of a refused change: `EROFS` from syncfs, or `EACCES` from
/// the kernel's permission check for non-root callers.
fn assert_refused(what: &str, r: std::io::Result<impl std::fmt::Debug>) {
    let e = r.expect_err(what);
    let code = e.raw_os_error().unwrap();
    if is_root() {
        assert_eq!(code, libc::EROFS, "{what}: {e}");
    } else {
        assert!(code == libc::EROFS || code == libc::EACCES, "{what}: {e}");
    }
}

#[test]
fn hidden_from_listing_but_reachable() {
    require_fuse!();
    let m = Mounted::new(&[]);
    fs::write(m.m("a"), b"x").unwrap();
    let names: Vec<_> = fs::read_dir(&m.mnt)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names, ["a"]);
    let md = fs::metadata(m.m(".syncfs")).unwrap();
    assert!(md.is_dir());
    assert_eq!(md.permissions().mode() & 0o7777, 0o555);
    let mut listed: Vec<_> = fs::read_dir(m.m(".syncfs"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    listed.sort();
    let mut want: Vec<_> = FILES.iter().map(|s| s.to_string()).collect();
    want.sort();
    assert_eq!(listed, want);
    // Nothing was stored in the backing directory.
    assert!(!m.b(".syncfs").exists());
    // `ls -a` of the root still hides it.
    let out = Command::new("ls").arg("-a").arg(&m.mnt).output().unwrap();
    assert_eq!(String::from_utf8(out.stdout).unwrap(), ".\n..\na\n");
    m.finish();
}

#[test]
fn files_are_readable() {
    require_fuse!();
    let m = Mounted::new(&["--node-id", "7"]);
    for f in FILES {
        let p = m.m(&format!(".syncfs/{f}"));
        let md = fs::metadata(&p).unwrap();
        assert!(md.is_file(), "{f}");
        assert_eq!(md.permissions().mode() & 0o7777, 0o444, "{f}");
        let data = fs::read(&p).unwrap();
        assert!(!data.is_empty(), "{f}");
        // `cat` works despite the content being generated on read.
        let out = Command::new("cat").arg(&p).output().unwrap();
        assert!(out.status.success(), "{f}");
        assert!(!out.stdout.is_empty(), "{f}");
        if *f != "status.json" {
            let v: serde_yaml_ng::Value = serde_yaml_ng::from_slice(&data).unwrap();
            assert!(v.is_mapping(), "{f}");
        }
    }
    let v = status(&m);
    assert_eq!(v["format_version"], 1);
    for f in &FILES[1..] {
        assert!(v[f].is_object(), "status.json lacks {f}");
    }
    assert_eq!(v["node"]["node_id"], 7);
    assert_eq!(v["node"]["mode"], "mount");
    assert_eq!(v["node"]["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(v["node"]["shadowed_backing_entry"], false);
    assert!(v["node"]["mount_options"]
        .as_array()
        .unwrap()
        .contains(&"default_permissions".into()));
    assert_eq!(v["config"]["node_id"], 7);
    assert_eq!(v["peers"]["quorum"], 1);
    assert_eq!(v["locks"]["held"], 0);
    assert_eq!(v["replication"]["dirty_files"], 0);
    let node = fs::read_to_string(m.m(".syncfs/node")).unwrap();
    assert!(node.contains("node_id: 7\n"), "{node}");
    m.finish();
}

#[test]
fn rejects_changes() {
    require_fuse!();
    let m = Mounted::new(&[]);
    fs::write(m.m("f"), b"x").unwrap();
    let d = m.m(".syncfs");
    let node = m.m(".syncfs/node");
    assert_refused("write", fs::OpenOptions::new().write(true).open(&node));
    assert_refused(
        "truncate",
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&node),
    );
    assert_refused("create", fs::File::create(d.join("new")));
    assert_refused("mkdir in", fs::create_dir(d.join("sub")));
    assert_refused("unlink", fs::remove_file(&node));
    assert_refused("rename out", fs::rename(&node, m.m("node")));
    assert_refused("rename in", fs::rename(m.m("f"), d.join("f")));
    assert_refused("link in", fs::hard_link(m.m("f"), d.join("f")));
    assert_refused(
        "chmod",
        fs::set_permissions(&node, fs::Permissions::from_mode(0o666)),
    );
    // Changes to the directory itself go through the (writable) root.
    let e = fs::remove_dir(&d).unwrap_err();
    assert_eq!(e.raw_os_error(), Some(libc::EROFS), "rmdir: {e}");
    let e = fs::rename(&d, m.m("moved")).unwrap_err();
    assert_eq!(e.raw_os_error(), Some(libc::EROFS), "rename: {e}");
    let e = fs::create_dir(&d).unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists);
    assert_refused(
        "chmod dir",
        fs::set_permissions(&d, fs::Permissions::from_mode(0o777)),
    );
    let out = Command::new("setfattr")
        .args(["-n", "user.x", "-v", "1"])
        .arg(&node)
        .output();
    if let Ok(out) = out {
        assert!(!out.status.success());
    }
    // Nothing leaked into the backing directory.
    assert!(!m.b(".syncfs").exists());
    assert!(!m.b("node").exists() && !m.b("moved").exists());
    assert_eq!(fs::read(m.m("f")).unwrap(), b"x");
    m.finish();
}

#[test]
fn counters_follow_operations() {
    require_fuse!();
    let m = Mounted::new(&[]);
    let before = status(&m);
    let payload = vec![7u8; 10_000];
    {
        let mut f = fs::File::create(m.m("file")).unwrap();
        f.write_all(&payload).unwrap();
        f.write_all(&payload).unwrap();
        f.sync_all().unwrap();
    }
    assert_eq!(fs::read(m.m("file")).unwrap().len(), 20_000);
    fs::create_dir(m.m("dir")).unwrap();
    fs::remove_dir(m.m("dir")).unwrap();
    assert!(fs::metadata(m.m("missing")).is_err());
    fs::remove_file(m.m("file")).unwrap();
    let after = status(&m);

    let delta = |key: &str| stat_u64(&after, key) - stat_u64(&before, key);
    let op_delta = |name: &str, field: &str| op(&after, name, field) - op(&before, name, field);
    assert_eq!(delta("bytes_written"), 20_000);
    assert!(delta("bytes_read") >= 20_000, "{}", delta("bytes_read"));
    assert_eq!(op_delta("create", "calls"), 1);
    assert!(op_delta("write", "calls") >= 1);
    assert!(op_delta("read", "calls") >= 1);
    assert!(op_delta("fsync", "calls") >= 1);
    assert_eq!(op_delta("mkdir", "calls"), 1);
    assert_eq!(op_delta("rmdir", "calls"), 1);
    assert_eq!(op_delta("unlink", "calls"), 1);
    assert!(op_delta("lookup", "errors") >= 1, "ENOENT lookup counted");
    assert_eq!(op_delta("write", "errors"), 0);
    assert!(delta("errors_total") >= 1);
    assert!(delta("requests_total") >= 7);
    assert!(delta("status_requests") >= 1);
    // The kernel sends release after close returns, asynchronously.
    wait_until("open_files back to 0", || {
        status(&m)["stats"]["open_files"] == 0
    });
    // Reading .syncfs is not counted as backing I/O.
    let a = status(&m);
    let b = status(&m);
    assert_eq!(stat_u64(&a, "bytes_read"), stat_u64(&b, "bytes_read"));
    assert_eq!(op(&a, "read", "calls"), op(&b, "read", "calls"));
    assert!(stat_u64(&b, "status_requests") > stat_u64(&a, "status_requests"));
    m.finish();
}

#[test]
fn shadows_real_backing_entry() {
    require_fuse!();
    let m = Mounted::with_backing(
        |b| {
            fs::create_dir(b.join(".syncfs")).unwrap();
            fs::write(b.join(".syncfs/mine"), b"user data").unwrap();
        },
        &[],
    );
    assert!(!m.m(".syncfs/mine").exists());
    assert!(m.m(".syncfs/node").exists());
    assert_eq!(status(&m)["node"]["shadowed_backing_entry"], true);
    let names: Vec<_> = fs::read_dir(&m.mnt).unwrap().collect();
    assert!(names.is_empty());
    // rmdir of the name must not reach the real directory.
    assert!(fs::remove_dir(m.m(".syncfs")).is_err());
    assert_eq!(fs::read(m.b(".syncfs/mine")).unwrap(), b"user data");
    m.finish();
}

#[test]
fn allow_other_reports_and_serves_other_users() {
    require_fuse!();
    let m = Mounted::new(&["--allow-other"]);
    let v = status(&m);
    assert_eq!(v["node"]["allow_other"], true);
    assert!(v["node"]["mount_options"]
        .as_array()
        .unwrap()
        .contains(&"allow_other".into()));
    if !is_root() {
        eprintln!("SKIPPED: reading as another user needs root");
        return m.finish();
    }
    use std::os::unix::process::CommandExt;
    fs::set_permissions(&m.tmp, fs::Permissions::from_mode(0o755)).unwrap();
    let as_nobody = |script: String| {
        Command::new("sh")
            .args(["-c", &script])
            .uid(65534)
            .gid(65534)
            .output()
            .unwrap()
    };
    let d = m.m(".syncfs").display().to_string();
    let out = as_nobody(format!("cat {d}/status.json"));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["format_version"], 1);
    let out = as_nobody(format!("ls {d}"));
    assert!(out.status.success());
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).lines().count(),
        FILES.len()
    );
    // Other users are refused by permissions before reaching syncfs.
    let out = as_nobody(format!("echo x > {d}/node"));
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("Permission denied"));
    m.finish();
}
