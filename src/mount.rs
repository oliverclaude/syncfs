//! Mounting: turns a [`Config`] into a running FUSE session, in the
//! foreground or as a daemon.

use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use fuser::{MountOption, Session, SessionACL};
use signal_hook::iterator::Signals;

use crate::config::Config;
use crate::dirty::DirtyTracker;
use crate::fs::{LocalOnly, SyncFs, WriteHooks};
use crate::lock::{ClusterHooks, LockManager, Settings};
use crate::net::Network;
use crate::process;
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
fn register_status(
    status: &Registry,
    fs: &SyncFs<Hooks>,
    net: &Network,
    locks: Option<&LockManager>,
    dirty: Option<&Arc<DirtyTracker>>,
    cfg: &Config,
    fc: &fuser::Config,
) {
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
    providers::register_builtin(
        status,
        info,
        Some(fs.stats_provider()),
        net.provider(),
        locks.map(LockManager::provider),
        dirty.map(DirtyTracker::provider),
    );
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
    log::debug!("fuse options: {:?}", fuse_cfg);

    // Runs after daemonizing: threads do not survive fork.
    let start = || -> anyhow::Result<Served> {
        let status = Registry::new();
        let net = Network::start(cfg).with_context(|| format!("listening on {}", cfg.listen))?;
        // A standalone node has nobody to agree with: no locks.
        let locks = (!cfg.peers.is_empty())
            .then(|| LockManager::start(Settings::from_config(cfg), net.clone()));
        let dirty = locks
            .as_ref()
            .map(|_| Arc::new(DirtyTracker::new(cfg.block_size.0)));
        let hooks: Hooks = match (&locks, &dirty) {
            (Some(l), Some(d)) => Arc::new(ClusterHooks::new(l.clone(), Arc::clone(d))),
            _ => Arc::new(LocalOnly),
        };
        let stop = |net: &Network, locks: &Option<LockManager>| {
            if let Some(l) = locks {
                l.shutdown();
            }
            net.shutdown();
        };
        let fs = match SyncFs::new(backing, hooks, Arc::clone(&status)) {
            Ok(fs) => fs,
            Err(e) => {
                stop(&net, &locks);
                return Err(anyhow::Error::from(e))
                    .with_context(|| format!("opening backing_dir {}", backing.display()));
            }
        };
        register_status(
            &status,
            &fs,
            &net,
            locks.as_ref(),
            dirty.as_ref(),
            cfg,
            &fuse_cfg,
        );
        let mounted = if rustix::process::geteuid().is_root() {
            mount_direct(fs, backing, mountpoint, &fuse_cfg, kernel_flags)
        } else {
            Session::new(fs, mountpoint, &fuse_cfg)
                .map(|session| Served {
                    session,
                    direct: None,
                    net: None,
                    locks: None,
                })
                .map_err(Into::into)
        };
        let mut served = match mounted.with_context(|| format!("mounting {}", mountpoint.display()))
        {
            Ok(s) => s,
            Err(e) => {
                stop(&net, &locks);
                return Err(e);
            }
        };
        served.net = Some(net);
        served.locks = locks;
        log::info!("serving {} at {}", backing.display(), mountpoint.display());
        Ok(served)
    };
    process::run(cfg.foreground, start, serve)
}

/// A mounted session. `direct` is set when we called mount(2) ourselves
/// and must unmount ourselves too.
struct Served {
    session: Session<SyncFs<Hooks>>,
    direct: Option<PathBuf>,
    net: Option<Network>,
    locks: Option<LockManager>,
}

type Hooks = Arc<dyn WriteHooks>;

/// Mounts with mount(2) as root, with filesystem type `fuse.syncfs` so the
/// mount table, `findmnt -t fuse.syncfs` and `mount -a` see the real type
/// (fuser's own root path always uses plain `fuse`).
fn mount_direct(
    fs: SyncFs<Hooks>,
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
            net: None,
            locks: None,
        }),
        Err(e) => {
            // SAFETY: valid path string.
            unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) };
            Err(e.into())
        }
    }
}

/// Runs the session until unmounted; `signals` unmount cleanly.
fn serve(served: Served, mut signals: Signals) -> anyhow::Result<()> {
    let Served {
        mut session,
        direct,
        net,
        locks,
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
    let res = session.run().context("serving filesystem");
    if let Some(locks) = locks {
        locks.shutdown();
    }
    if let Some(net) = net {
        net.shutdown();
    }
    res?;
    log::info!("unmounted");
    Ok(())
}
