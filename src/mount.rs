//! Mounting: turns a [`Config`] into a running FUSE session, in the
//! foreground or as a daemon.

use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use fuser::{MountOption, Session, SessionACL};
use signal_hook::iterator::Signals;

use crate::config::Config;
use crate::fs::{LocalOnly, SyncFs};
use crate::logging;
use crate::status::{providers, Registry};

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

/// `MountOption` as written in `-o`.
fn option_name(o: &MountOption) -> String {
    match o {
        MountOption::FSName(n) => format!("fsname={n}"),
        MountOption::Subtype(n) => format!("subtype={n}"),
        MountOption::CUSTOM(c) => c.clone(),
        MountOption::DefaultPermissions => "default_permissions".into(),
        MountOption::Exec => "exec".into(),
        MountOption::NoExec => "noexec".into(),
        MountOption::Suid => "suid".into(),
        MountOption::NoSuid => "nosuid".into(),
        MountOption::Dev => "dev".into(),
        MountOption::NoDev => "nodev".into(),
        MountOption::Atime => "atime".into(),
        MountOption::NoAtime => "noatime".into(),
        MountOption::DirSync => "dirsync".into(),
        MountOption::Sync => "sync".into(),
        MountOption::Async => "async".into(),
        MountOption::RO => "ro".into(),
        MountOption::RW => "rw".into(),
        other => format!("{other:?}").to_lowercase(),
    }
}

/// Registers the `.syncfs` providers, in file order.
fn register_status(status: &Registry, fs: &SyncFs<LocalOnly>, cfg: &Config, fc: &fuser::Config) {
    let mut mount_options: Vec<String> = fc.mount_options.iter().map(option_name).collect();
    match fc.acl {
        SessionACL::All => mount_options.push("allow_other".into()),
        SessionACL::RootAndOwner => mount_options.push("allow_root".into()),
        _ => {}
    }
    let info = providers::NodeInfo {
        config: cfg.clone(),
        mount_options,
        shadowed_backing_entry: fs.shadows_backing_entry(),
    };
    providers::register_builtin(status, info, fs.stats_provider());
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
    let status = Registry::new();
    let fs = SyncFs::new(backing, LocalOnly, Arc::clone(&status))
        .with_context(|| format!("opening backing_dir {}", backing.display()))?;
    register_status(&status, &fs, cfg, &fuse_cfg);
    log::debug!("fuse options: {:?}", fuse_cfg);

    let mount = || -> anyhow::Result<Served> {
        if rustix::process::geteuid().is_root() {
            mount_direct(fs, backing, mountpoint, &fuse_cfg, kernel_flags)
        } else {
            let session = Session::new(fs, mountpoint, &fuse_cfg)?;
            Ok(Served {
                session,
                direct: None,
            })
        }
        .with_context(|| format!("mounting {}", mountpoint.display()))
    };
    if cfg.foreground {
        let signals = stop_signals()?;
        let served = mount()?;
        log::info!("serving {} at {}", backing.display(), mountpoint.display());
        serve(served, signals)
    } else {
        daemonize(mount)
    }
}

/// A mounted session. `direct` is set when we called mount(2) ourselves
/// and must unmount ourselves too.
struct Served {
    session: Session<SyncFs<LocalOnly>>,
    direct: Option<PathBuf>,
}

/// Mounts with mount(2) as root, with filesystem type `fuse.syncfs` so the
/// mount table, `findmnt -t fuse.syncfs` and `mount -a` see the real type
/// (fuser's own root path always uses plain `fuse`).
fn mount_direct(
    fs: SyncFs<LocalOnly>,
    backing: &Path,
    mountpoint: &Path,
    fuse_cfg: &fuser::Config,
    kernel_flags: &[String],
) -> anyhow::Result<Served> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;

    let dev = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")
        .context("opening /dev/fuse (is the fuse module loaded?)")?;
    let root_mode = std::fs::metadata(mountpoint)?.mode() & libc::S_IFMT;
    let mut data = format!(
        "fd={},rootmode={:o},user_id=0,group_id=0,default_permissions",
        dev.as_raw_fd(),
        root_mode
    );
    if fuse_cfg.acl != SessionACL::Owner {
        data.push_str(",allow_other");
    }
    let has = |f: &str| {
        kernel_flags.iter().rev().find(|k| {
            let base = k.strip_prefix("no").unwrap_or(k);
            base == f
        })
    };
    let mut flags = libc::MS_NOSUID | libc::MS_NODEV;
    if has("suid").is_some_and(|k| k == "suid") {
        flags &= !libc::MS_NOSUID;
    }
    if has("dev").is_some_and(|k| k == "dev") {
        flags &= !libc::MS_NODEV;
    }
    for (name, bit) in [
        ("noexec", libc::MS_NOEXEC),
        ("noatime", libc::MS_NOATIME),
        ("sync", libc::MS_SYNCHRONOUS),
        ("dirsync", libc::MS_DIRSYNC),
    ] {
        if has(name.trim_start_matches("no")).is_some_and(|k| k == name) {
            flags |= bit;
        }
    }
    let c = |s: &str| std::ffi::CString::new(s).map_err(anyhow::Error::from);
    let source = c(&backing.display().to_string())?;
    let target = c(&mountpoint.display().to_string())?;
    let fstype = c("fuse.syncfs")?;
    let data = c(&data)?;
    // SAFETY: all pointers are valid NUL-terminated strings.
    let rc = unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            fstype.as_ptr(),
            flags,
            data.as_ptr().cast(),
        )
    };
    if rc != 0 {
        bail!("mount(2): {}", std::io::Error::last_os_error());
    }
    match Session::from_fd(fs, OwnedFd::from(dev), fuse_cfg.acl, fuse_cfg.clone()) {
        Ok(session) => Ok(Served {
            session,
            direct: Some(mountpoint.to_path_buf()),
        }),
        Err(e) => {
            // SAFETY: valid path string.
            unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) };
            Err(e.into())
        }
    }
}

/// Catches SIGINT, SIGTERM and SIGHUP. Installed before mounting so that a
/// signal arriving as soon as the mount is visible is not fatal: it stays
/// pending until [`serve`] handles it.
fn stop_signals() -> std::io::Result<Signals> {
    Signals::new([
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGHUP,
    ])
}

/// Runs the session until unmounted; `signals` unmount cleanly.
fn serve(served: Served, mut signals: Signals) -> anyhow::Result<()> {
    let Served {
        mut session,
        direct,
    } = served;
    let mut unmounter = session.unmount_callable();
    std::thread::spawn(move || {
        if let Some(sig) = signals.forever().next() {
            log::info!("signal {sig}: unmounting");
            let res = match &direct {
                Some(p) => std::ffi::CString::new(p.as_os_str().as_encoded_bytes())
                    .map_err(std::io::Error::other)
                    .and_then(|c| {
                        // SAFETY: valid path string.
                        if unsafe { libc::umount2(c.as_ptr(), 0) } == 0 {
                            Ok(())
                        } else {
                            Err(std::io::Error::last_os_error())
                        }
                    }),
                None => unmounter.unmount(),
            };
            if let Err(e) = res {
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
fn daemonize(mount: impl FnOnce() -> anyhow::Result<Served>) -> anyhow::Result<()> {
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
    let signals = match stop_signals() {
        Ok(s) => s,
        Err(e) => {
            let mut msg = vec![1u8];
            msg.extend_from_slice(format!("installing signal handlers: {e}").as_bytes());
            let _ = tx.write_all(&msg);
            std::process::exit(1);
        }
    };
    match mount() {
        Ok(session) => {
            detach_stdio();
            logging::switch_to_syslog();
            let _ = tx.write_all(&[0]);
            drop(tx);
            if let Err(e) = serve(session, signals) {
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
