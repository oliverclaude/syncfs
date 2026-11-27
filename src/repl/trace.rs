//! Debug trace of write sessions and dirty marks (#27): every time one is
//! created or cleared, with the file, version, path and cause, under the
//! `syncfs::trace` target at debug level. `t` is CLOCK_MONOTONIC in
//! nanoseconds, the clock the soak's ops.tsv uses.

use crate::config::NodeId;
use crate::net::wire::{FileId, Version};

/// CLOCK_MONOTONIC in nanoseconds.
pub fn now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime on a valid timespec.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn ver(v: Option<Version>) -> String {
    v.map_or("-".into(), |v| format!("{}.{}", v.seq, v.node))
}

fn path(p: Option<&[u8]>) -> String {
    p.map_or("<none>".into(), |p| String::from_utf8_lossy(p).into_owned())
}

/// A write session on `id` was opened or closed.
pub fn session(what: &str, id: FileId, version: Option<Version>, at: Option<&[u8]>, cause: &str) {
    log::debug!(
        target: "syncfs::trace",
        "t={} session {what} file={id} version={} path={} cause={cause}",
        now_ns(),
        ver(version),
        path(at)
    );
}

/// Dirty marks of `id` for `peer` (all peers if None) were created or
/// cleared.
pub fn marks(what: &str, id: FileId, peer: Option<NodeId>, version: Option<Version>, cause: &str) {
    if !log::log_enabled!(target: "syncfs::trace", log::Level::Debug) {
        return;
    }
    log::debug!(
        target: "syncfs::trace",
        "t={} marks {what} file={id} peer={} version={} cause={cause}",
        now_ns(),
        peer.map_or("*".into(), |p| p.to_string()),
        ver(version)
    );
}

/// The persisted dirty row of `id` changed (one line per flush).
pub fn row(id: FileId, at: Option<&[u8]>, remaining: bool) {
    log::debug!(
        target: "syncfs::trace",
        "t={} marks row file={id} path={} {}",
        now_ns(),
        path(at),
        if remaining { "kept" } else { "empty" }
    );
}
