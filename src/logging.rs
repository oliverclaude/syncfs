//! Minimal logger: stderr in the foreground, syslog once daemonized.

use std::ffi::CString;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

use log::{Level, LevelFilter, Log, Metadata, Record};

use crate::config::LogLevel;

static TO_SYSLOG: AtomicBool = AtomicBool::new(false);

struct Logger;

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let msg = format!("{}: {}", record.target(), record.args());
        if TO_SYSLOG.load(Ordering::Relaxed) {
            let prio = match record.level() {
                Level::Error => libc::LOG_ERR,
                Level::Warn => libc::LOG_WARNING,
                Level::Info => libc::LOG_INFO,
                Level::Debug | Level::Trace => libc::LOG_DEBUG,
            };
            if let Ok(c) = CString::new(msg.replace('\0', " ")) {
                // SAFETY: "%s" with a valid NUL-terminated string.
                unsafe { libc::syslog(prio, c"%s".as_ptr(), c.as_ptr()) };
            }
        } else {
            let _ = writeln!(
                std::io::stderr(),
                "syncfs[{}] {}: {msg}",
                std::process::id(),
                record.level()
            );
        }
    }

    fn flush(&self) {}
}

pub fn level_filter(level: LogLevel) -> LevelFilter {
    match level {
        LogLevel::Error => LevelFilter::Error,
        LogLevel::Warn => LevelFilter::Warn,
        LogLevel::Info => LevelFilter::Info,
        LogLevel::Debug => LevelFilter::Debug,
        LogLevel::Trace => LevelFilter::Trace,
    }
}

/// Installs the logger. Safe to call more than once; later calls only
/// change the level.
pub fn init(level: LogLevel) {
    let _ = log::set_logger(&Logger);
    log::set_max_level(level_filter(level));
}

/// Sends all further log output to syslog (facility daemon).
pub fn switch_to_syslog() {
    // SAFETY: the ident is a static C string, as openlog requires.
    unsafe { libc::openlog(c"syncfs".as_ptr(), libc::LOG_PID, libc::LOG_DAEMON) };
    TO_SYSLOG.store(true, Ordering::Relaxed);
}
