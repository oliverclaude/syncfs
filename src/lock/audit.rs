//! Test-only lock audit log. With `SYNCFS_TEST_LOCK_LOG=PATH`, every change
//! to what this node believes it holds is appended to PATH as one line, with
//! CLOCK_MONOTONIC times in nanoseconds (comparable between processes on one
//! machine). The soak test (`tests/soak.rs`) checks from these logs that no
//! two nodes ever believed they held conflicting locks at the same time.
//!
//! Lines (tab-separated): `H|R|X  node  pid  ballot  now_ns  until_ns  key`.
//! `H` a hold starts (valid until `until_ns`), `R` a renewal moved the end,
//! `X` it ended (released or dropped; `until_ns` is 0).

use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use super::Key;
use crate::config::NodeId;
use crate::net::wire::Ballot;

struct Log {
    file: Mutex<std::fs::File>,
    base: Instant,
    base_ns: u64,
}

fn log() -> Option<&'static Log> {
    static LOG: OnceLock<Option<Log>> = OnceLock::new();
    LOG.get_or_init(|| {
        let path = std::env::var_os("SYNCFS_TEST_LOCK_LOG")?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()?;
        Some(Log {
            file: Mutex::new(file),
            base: Instant::now(),
            base_ns: monotonic_ns(),
        })
    })
    .as_ref()
}

fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is a valid timespec to write into.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[derive(Clone, Copy)]
pub enum Event {
    Hold,
    Renew,
    End,
}

/// Records `event` for `key` held with `ballot`, valid until `until`.
pub fn record(event: Event, me: NodeId, key: &Key, ballot: Ballot, until: Option<Instant>) {
    let Some(l) = log() else { return };
    let ns = |t: Instant| {
        let d = t.saturating_duration_since(l.base).as_nanos() as u64;
        let back = l.base.saturating_duration_since(t).as_nanos() as u64;
        (l.base_ns + d).saturating_sub(back)
    };
    let tag = match event {
        Event::Hold => 'H',
        Event::Renew => 'R',
        Event::End => 'X',
    };
    let line = format!(
        "{tag}\t{me}\t{}\t{}.{}\t{}\t{}\t{key}\n",
        std::process::id(),
        ballot.counter,
        ballot.node,
        ns(Instant::now()),
        until.map_or(0, ns),
    );
    // One write per line: O_APPEND keeps lines whole.
    let _ = l
        .file
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .write_all(line.as_bytes());
}
