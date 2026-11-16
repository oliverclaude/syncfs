//! Mounting: turns a [`Config`] into a running FUSE session, in the
//! foreground or as a daemon.

use std::io::{Read, Write};
use std::path::Path;

use anyhow::{bail, Context};
use fuser::{MountOption, Session, SessionACL};

use crate::config::Config;
use crate::fs::{LocalOnly, SyncFs};
use crate::logging;

/// FUSE session settings for `cfg`. `kernel_flags` are the generic mount
/// flags (`nosuid`, `noexec`, ...) from `-o`.
pub fn fuse_config(cfg: &Config, kernel_flags: &[String]) -> fuser::Config {
    let backing = cfg
        .backing_dir
        .as_deref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "syncfs".into());
    let mut opts = vec![
        MountOption::FSName(backing),
        MountOption::Subtype("syncfs".into()),
        // The kernel checks permissions against the attributes we report,
        // like a local filesystem. Required for correct multi-user access
        // when the daemon runs as root.
        MountOption::DefaultPermissions,
    ];
    for f in kernel_flags {
        opts.push(match f.as_str() {
            "exec" => MountOption::Exec,
            "noexec" => MountOption::NoExec,
            "suid" => MountOption::Suid,
            "nosuid" => MountOption::NoSuid,
            "dev" => MountOption::Dev,
            "nodev" => MountOption::NoDev,
            "atime" => MountOption::Atime,
            "noatime" => MountOption::NoAtime,
            "dirsync" => MountOption::DirSync,
            "sync" => MountOption::Sync,
            "async" => MountOption::Async,
            other => MountOption::CUSTOM(other.to_string()),
        });
    }
    // Later flags win, as with mount(8): drop earlier conflicting ones.
    let mut seen = Vec::new();
    for o in opts.into_iter().rev() {
        let conflict = |a: &MountOption| -> Option<MountOption> {
            Some(match a {
                MountOption::Exec => MountOption::NoExec,
                MountOption::NoExec => MountOption::Exec,
                MountOption::Suid => MountOption::NoSuid,
                MountOption::NoSuid => MountOption::Suid,
                MountOption::Dev => MountOption::NoDev,
                MountOption::NoDev => MountOption::Dev,
                MountOption::Atime => MountOption::NoAtime,
                MountOption::NoAtime => MountOption::Atime,
                MountOption::Sync => MountOption::Async,
                MountOption::Async => MountOption::Sync,
                _ => return None,
            })
        };
        let clash = conflict(&o).is_some_and(|c| seen.contains(&c));
        if !clash && !seen.contains(&o) {
            seen.push(o);
        }
    }
    seen.reverse();

    let mut fc = fuser::Config::default();
    fc.mount_options = seen;
    fc.acl = if cfg.allow_other {
        SessionACL::All
    } else if cfg.allow_root {
        SessionACL::RootAndOwner
    } else {
        SessionACL::Owner
    };
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    fc.n_threads = Some(threads.clamp(2, 16));
    fc
}

fn check_dir(what: &str, p: &Path) -> anyhow::Result<()> {
    let md = std::fs::metadata(p).with_context(|| format!("{what} {}", p.display()))?;
    if !md.is_dir() {
        bail!("{what} {} is not a directory", p.display());
    }
    Ok(())
}

/// Raises the open-file limit to the hard limit: every inode the kernel
/// holds keeps a descriptor open.
fn raise_nofile_limit() {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: plain getrlimit/setrlimit on a valid struct.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 && lim.rlim_cur < lim.rlim_max {
            lim.rlim_cur = lim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
    }
}

/// Mounts and serves the filesystem until it is unmounted. Unless
/// `cfg.foreground`, returns in the calling process as soon as the mount
/// is up (or failed) and keeps serving from a detached child, as mount(8)
/// expects from a helper.
pub fn run(cfg: &Config, kernel_flags: &[String]) -> anyhow::Result<()> {
    let (Some(backing), Some(mountpoint)) = (&cfg.backing_dir, &cfg.mountpoint) else {
        bail!("mount mode needs backing_dir and mountpoint");
    };
    check_dir("backing_dir", backing)?;
    check_dir("mountpoint", mountpoint)?;
    std::fs::create_dir_all(&cfg.state_dir)
        .with_context(|| format!("creating state_dir {}", cfg.state_dir.display()))?;

    // Modes from the kernel are already final; don't mask them again.
    rustix::process::umask(rustix::fs::Mode::empty());
    raise_nofile_limit();

    let fuse_cfg = fuse_config(cfg, kernel_flags);
    let fs = SyncFs::new(backing, LocalOnly)
        .with_context(|| format!("opening backing_dir {}", backing.display()))?;
    log::debug!("fuse options: {:?}", fuse_cfg);

    if cfg.foreground {
        let session = Session::new(fs, mountpoint, &fuse_cfg)
            .with_context(|| format!("mounting {}", mountpoint.display()))?;
        log::info!("serving {} at {}", backing.display(), mountpoint.display());
        serve(session)
    } else {
        daemonize(|| {
            Session::new(fs, mountpoint, &fuse_cfg)
                .with_context(|| format!("mounting {}", mountpoint.display()))
        })
    }
}

/// Runs the session until unmounted; SIGINT/SIGTERM/SIGHUP unmount cleanly.
fn serve(mut session: Session<SyncFs<LocalOnly>>) -> anyhow::Result<()> {
    let mut unmounter = session.unmount_callable();
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGHUP,
    ])?;
    std::thread::spawn(move || {
        if let Some(sig) = signals.forever().next() {
            log::info!("signal {sig}: unmounting");
            if let Err(e) = unmounter.unmount() {
                log::error!("unmount failed: {e}");
            }
        }
    });
    session.run().context("serving filesystem")?;
    log::info!("unmounted");
    Ok(())
}

/// Forks. The parent waits until the child reports the mount result over a
/// pipe and exits with it; the child detaches and serves.
fn daemonize(
    mount: impl FnOnce() -> anyhow::Result<Session<SyncFs<LocalOnly>>>,
) -> anyhow::Result<()> {
    let (mut rx, mut tx) = {
        let (r, w) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)?;
        (std::fs::File::from(r), std::fs::File::from(w))
    };
    // SAFETY: single-threaded at this point (no session or threads yet).
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
                None => bail!("filesystem daemon exited during startup"),
            };
        }
    }
    drop(rx);
    let _ = rustix::process::setsid();
    match mount() {
        Ok(session) => {
            detach_stdio();
            logging::switch_to_syslog();
            let _ = tx.write_all(&[0]);
            drop(tx);
            if let Err(e) = serve(session) {
                log::error!("{e:#}");
                std::process::exit(1);
            }
            std::process::exit(0);
        }
        Err(e) => {
            let mut msg = vec![1u8];
            msg.extend_from_slice(format!("{e:#}").as_bytes());
            let _ = tx.write_all(&msg);
            std::process::exit(1);
        }
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
