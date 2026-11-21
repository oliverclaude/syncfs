//! Process plumbing shared by mount and daemon mode: signals and
//! daemonizing.

use std::io::{Read, Write};

use anyhow::bail;
use signal_hook::iterator::Signals;

use crate::logging;

/// Catches SIGINT and SIGTERM (stop) and SIGHUP (reload the configuration,
/// see [`crate::reload`]). Install before the service becomes visible
/// (mounted, listening) so an early signal is not fatal: it stays pending
/// until the service handles it.
pub fn stop_signals() -> std::io::Result<Signals> {
    Signals::new([
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGHUP,
    ])
}

/// Runs `start` then `serve`, in the foreground or as a daemon.
///
/// As a daemon it forks: the parent waits until the child reports whether
/// `start` succeeded and exits with that result (as mount(8) expects from
/// a helper); the child detaches from the terminal, logs to syslog and
/// serves.
pub fn run<T>(
    foreground: bool,
    start: impl FnOnce() -> anyhow::Result<T>,
    serve: impl FnOnce(T, Signals) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    if foreground {
        let signals = stop_signals()?;
        let started = start()?;
        return serve(started, signals);
    }
    let (mut rx, mut tx) = {
        let (r, w) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)?;
        (std::fs::File::from(r), std::fs::File::from(w))
    };
    // SAFETY: single-threaded at this point (no sessions or threads yet).
    match unsafe { libc::fork() } {
        -1 => bail!("fork: {}", std::io::Error::last_os_error()),
        0 => {}
        _child => {
            drop(tx);
            let mut msg = Vec::new();
            rx.read_to_end(&mut msg)?;
            return match msg.split_first() {
                Some((0, _)) => Ok(()),
                Some((_, err)) => bail!("{}", String::from_utf8_lossy(err)),
                None => bail!("syncfs daemon exited during startup"),
            };
        }
    }
    drop(rx);
    let _ = rustix::process::setsid();
    let fail = |mut tx: std::fs::File, e: anyhow::Error| -> ! {
        let mut msg = vec![1u8];
        msg.extend_from_slice(format!("{e:#}").as_bytes());
        let _ = tx.write_all(&msg);
        std::process::exit(1);
    };
    let signals = match stop_signals() {
        Ok(s) => s,
        Err(e) => fail(
            tx,
            anyhow::Error::from(e).context("installing signal handlers"),
        ),
    };
    match start() {
        Ok(started) => {
            detach_stdio();
            logging::switch_to_syslog();
            let _ = tx.write_all(&[0]);
            drop(tx);
            if let Err(e) = serve(started, signals) {
                log::error!("{e:#}");
                std::process::exit(1);
            }
            std::process::exit(0);
        }
        Err(e) => fail(tx, e),
    }
}

fn detach_stdio() {
    use std::os::fd::AsRawFd;
    let _ = std::env::set_current_dir("/");
    let Ok(null) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")
    else {
        return;
    };
    for fd in 0..=2 {
        // SAFETY: repoints the standard descriptors at /dev/null.
        unsafe { libc::dup2(null.as_raw_fd(), fd) };
    }
}
