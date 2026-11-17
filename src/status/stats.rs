//! Operation counters. Every counter is a relaxed atomic, so counting costs
//! one uncontended atomic add per request.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use serde_json::{json, Map, Value};

macro_rules! ops {
    ($($v:ident => $n:literal),* $(,)?) => {
        /// A FUSE operation, for counting.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(usize)]
        pub enum Op { $($v),* }

        impl Op {
            pub const ALL: &'static [Op] = &[$(Op::$v),*];
            pub fn name(self) -> &'static str {
                match self { $(Op::$v => $n),* }
            }
        }
    };
}

ops! {
    Lookup => "lookup", Forget => "forget", Getattr => "getattr", Setattr => "setattr",
    Readlink => "readlink", Mknod => "mknod", Mkdir => "mkdir", Unlink => "unlink",
    Rmdir => "rmdir", Symlink => "symlink", Rename => "rename", Link => "link",
    Open => "open", Read => "read", Write => "write", Flush => "flush",
    Release => "release", Fsync => "fsync", Opendir => "opendir", Readdir => "readdir",
    Releasedir => "releasedir", Fsyncdir => "fsyncdir", Statfs => "statfs",
    Setxattr => "setxattr", Getxattr => "getxattr", Listxattr => "listxattr",
    Removexattr => "removexattr", Create => "create", Fallocate => "fallocate",
    Lseek => "lseek",
}

const N: usize = Op::ALL.len();

/// Counters for one mount.
#[derive(Debug)]
pub struct Stats {
    started: Instant,
    calls: [AtomicU64; N],
    errors: [AtomicU64; N],
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    open_files: AtomicI64,
    open_dirs: AtomicI64,
    /// Requests served from the virtual `.syncfs` directory (not counted
    /// in the per-operation counters).
    status_requests: AtomicU64,
    /// Conflict copies replication made of this node's diverged changes.
    conflict_copies: AtomicU64,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            calls: std::array::from_fn(|_| AtomicU64::new(0)),
            errors: std::array::from_fn(|_| AtomicU64::new(0)),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            open_files: AtomicI64::new(0),
            open_dirs: AtomicI64::new(0),
            status_requests: AtomicU64::new(0),
            conflict_copies: AtomicU64::new(0),
        }
    }
}

impl Stats {
    pub fn call(&self, op: Op) {
        self.calls[op as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn error(&self, op: Op) {
        self.errors[op as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_bytes(&self, n: u64) {
        self.bytes_read.fetch_add(n, Ordering::Relaxed);
    }

    pub fn wrote_bytes(&self, n: u64) {
        self.bytes_written.fetch_add(n, Ordering::Relaxed);
    }

    pub fn file_opened(&self, delta: i64) {
        self.open_files.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn dir_opened(&self, delta: i64) {
        self.open_dirs.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn status_request(&self) {
        self.status_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn conflict_copy(&self) {
        self.conflict_copies.fetch_add(1, Ordering::Relaxed);
    }

    pub fn calls(&self, op: Op) -> u64 {
        self.calls[op as usize].load(Ordering::Relaxed)
    }

    pub fn errors(&self, op: Op) -> u64 {
        self.errors[op as usize].load(Ordering::Relaxed)
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// Snapshot as JSON. `extra` is merged in (gauges owned by others).
    pub fn render(&self, extra: Map<String, Value>) -> Value {
        let mut ops = Map::new();
        let (mut total, mut total_err) = (0, 0);
        for &op in Op::ALL {
            let (c, e) = (self.calls(op), self.errors(op));
            total += c;
            total_err += e;
            ops.insert(op.name().into(), json!({"calls": c, "errors": e}));
        }
        let mut v = json!({
            "uptime_secs": self.uptime_secs(),
            "requests_total": total,
            "errors_total": total_err,
            "bytes_read": self.bytes_read.load(Ordering::Relaxed),
            "bytes_written": self.bytes_written.load(Ordering::Relaxed),
            "open_files": self.open_files.load(Ordering::Relaxed),
            "open_dirs": self.open_dirs.load(Ordering::Relaxed),
            "status_requests": self.status_requests.load(Ordering::Relaxed),
            "conflict_copies": self.conflict_copies.load(Ordering::Relaxed),
        });
        let m = v.as_object_mut().expect("object");
        m.extend(extra);
        m.insert("ops".into(), Value::Object(ops));
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counting_and_render() {
        let s = Stats::default();
        s.call(Op::Read);
        s.call(Op::Read);
        s.error(Op::Read);
        s.read_bytes(10);
        s.file_opened(1);
        let v = s.render(Map::from_iter([("live_inodes".to_string(), json!(3))]));
        assert_eq!(v["ops"]["read"], json!({"calls": 2, "errors": 1}));
        assert_eq!(v["requests_total"], 2);
        assert_eq!(v["errors_total"], 1);
        assert_eq!(v["bytes_read"], 10);
        assert_eq!(v["open_files"], 1);
        assert_eq!(v["live_inodes"], 3);
        assert_eq!(v["ops"].as_object().unwrap().len(), Op::ALL.len());
    }

    #[test]
    fn op_names_unique() {
        let mut names: Vec<_> = Op::ALL.iter().map(|o| o.name()).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), Op::ALL.len());
    }
}
