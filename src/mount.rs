//! Mounting: turns a [`Config`] into a running FUSE session, in the
//! foreground or as a daemon.

use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use fuser::{MountOption, Session, SessionACL};
use signal_hook::iterator::Signals;

use crate::config::Config;
use crate::fs::{LocalOnly, SyncFs, WriteHooks};
use crate::lock::{ClusterHooks, LockManager, Settings};
use crate::net::Network;
use crate::process;
use crate::reload::{self, Registration, Reloader};
use crate::repl::core::Repl;
use crate::repl::store::{Flusher, Store};
use crate::repl::{marker, rebuild, version};
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
    repl: Option<&Arc<Repl>>,
    cfg: &Config,
    fc: &fuser::Config,
) -> crate::config::SharedConfig {
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
        repl.map(Repl::provider),
    )
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
///
/// SIGHUP reloads the configuration (see [`crate::reload`]); SIGINT and
/// SIGTERM unmount.
pub fn run(cfg: &Config, kernel_flags: &[String], base: reload::Base) -> anyhow::Result<()> {
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
        // A standalone node has nobody to agree with: no locks and no
        // replication.
        let cluster = if cfg.peers.is_empty() {
            None
        } else {
            Some(start_cluster(cfg, backing)?)
        };
        let net = match &cluster {
            Some(c) => c.net.clone(),
            None => Network::start(cfg).with_context(|| format!("listening on {}", cfg.listen))?,
        };
        let locks = cluster.as_ref().map(|c| c.locks.clone());
        let repl = cluster.as_ref().map(|c| Arc::clone(&c.repl));
        let hooks: Hooks = match &cluster {
            Some(c) => Arc::new(ClusterHooks::new(c.locks.clone(), Arc::clone(&c.repl))),
            None => Arc::new(LocalOnly),
        };
        let stop = |net: &Network, cluster: Option<Cluster>| match cluster {
            Some(c) => c.stop(),
            None => net.shutdown(),
        };
        let fs = match SyncFs::new(backing, hooks, Arc::clone(&status)) {
            Ok(fs) => fs,
            Err(e) => {
                stop(&net, cluster);
                return Err(anyhow::Error::from(e))
                    .with_context(|| format!("opening backing_dir {}", backing.display()));
            }
        };
        let inodes = fs.inode_table();
        if let Some(r) = &repl {
            r.set_stats(Arc::clone(fs.stats()));
        }
        let shown = register_status(
            &status,
            &fs,
            &net,
            locks.as_ref(),
            repl.as_ref(),
            cfg,
            &fuse_cfg,
        );
        let reloader = Reloader::new(
            base,
            cfg.clone(),
            reload::Targets {
                net: Some(net.clone()),
                locks: locks.clone(),
                repl: repl.clone(),
                shown: Some(shown),
            },
        );
        let mounted = if rustix::process::geteuid().is_root() {
            mount_direct(fs, backing, mountpoint, &fuse_cfg, kernel_flags)
        } else {
            Session::new(fs, mountpoint, &fuse_cfg)
                .map(|session| Served {
                    session,
                    mountpoint: mountpoint.to_path_buf(),
                    direct: None,
                    net: None,
                    cluster: None,
                    reloader: None,
                    registration: None,
                })
                .map_err(Into::into)
        };
        let mounted = mounted.inspect(|s| {
            if let Some(repl) = &repl {
                // The kernel takes the directory's or file's lock to
                // invalidate, so the calls go through threads of their
                // own: an operation holding that lock may be waiting for
                // replication to apply the entry that invalidates (e.g. a
                // rename waiting for the lock handoff). Entries and inodes
                // have a thread each, so that a new lock session can wait
                // for the data invalidations (see `Repl::set_inval_barrier`)
                // without waiting behind a directory one.
                let (entries_tx, entries_rx) =
                    std::sync::mpsc::channel::<(u64, std::ffi::OsString)>();
                let (inodes_tx, inodes_rx) = std::sync::mpsc::channel::<u64>();
                // (queued, done) inode invalidations.
                let progress = Arc::new((
                    std::sync::Mutex::new((0u64, 0u64)),
                    std::sync::Condvar::new(),
                ));
                let notifier = s.session.notifier();
                let spawned = std::thread::Builder::new()
                    .name("syncfs-inval-e".into())
                    .spawn(move || {
                        for (ino, name) in entries_rx {
                            if let Err(e) = notifier.inval_entry(fuser::INodeNo(ino), &name) {
                                log::debug!("invalidating entry {name:?} of inode {ino}: {e}");
                            }
                        }
                    });
                let notifier = s.session.notifier();
                let done = Arc::clone(&progress);
                let spawned = spawned.and_then(|_| {
                    std::thread::Builder::new()
                        .name("syncfs-inval-i".into())
                        .spawn(move || {
                            for ino in inodes_rx {
                                if let Err(e) = notifier.inval_inode(fuser::INodeNo(ino), 0, 0) {
                                    log::debug!("invalidating inode {ino}: {e}");
                                }
                                let (m, cv) = &*done;
                                crate::repl::core::lock(m).1 += 1;
                                cv.notify_all();
                            }
                        })
                });
                if let Err(e) = spawned {
                    log::error!("cannot start the cache invalidation threads: {e}");
                    return;
                }
                let dirs = Arc::clone(&inodes);
                let moved = Arc::clone(&inodes);
                repl.set_entry_invalidator(Box::new(move |st, name| {
                    if let Some(ino) = dirs.live_number(st) {
                        let _ = entries_tx.send((ino, name.to_os_string()));
                    }
                }));
                let queued = Arc::clone(&progress);
                let inodes_tx = std::sync::Mutex::new(inodes_tx);
                repl.set_invalidator(Box::new(move |st| {
                    if let Some(ino) = inodes.live_number(st) {
                        // Counted under the lock the sender sits behind, so
                        // a barrier never misses one in flight.
                        let tx = crate::repl::core::lock(&inodes_tx);
                        crate::repl::core::lock(&queued.0).0 += 1;
                        let _ = tx.send(ino);
                    }
                }));
                repl.set_move_notifier(Box::new(move |file, dir, name| {
                    moved.mark_moved(file, dir, name);
                }));
                repl.set_inval_barrier(Box::new(move |timeout| {
                    let (m, cv) = &*progress;
                    let mut g = crate::repl::core::lock(m);
                    let want = g.0;
                    let deadline = std::time::Instant::now() + timeout;
                    while g.1 < want {
                        let now = std::time::Instant::now();
                        if now >= deadline {
                            return false;
                        }
                        g = cv
                            .wait_timeout(g, deadline - now)
                            .unwrap_or_else(|e| e.into_inner())
                            .0;
                    }
                    true
                }));
            }
        });
        let mut served = match mounted.with_context(|| format!("mounting {}", mountpoint.display()))
        {
            Ok(s) => s,
            Err(e) => {
                stop(&net, cluster);
                return Err(e);
            }
        };
        served.net = Some(net);
        served.cluster = cluster;
        served.reloader = Some(reloader);
        // Before mount(8) returns, so that a remount right after finds it.
        served.registration = Some(Registration::new(cfg));
        log::info!("serving {} at {}", backing.display(), mountpoint.display());
        Ok(served)
    };
    process::run(cfg.foreground, start, serve)
}

/// A mounted session. `direct` is set when we called mount(2) ourselves
/// and must unmount ourselves too.
struct Served {
    session: Session<SyncFs<Hooks>>,
    mountpoint: PathBuf,
    direct: Option<PathBuf>,
    net: Option<Network>,
    cluster: Option<Cluster>,
    reloader: Option<Reloader>,
    registration: Option<Registration>,
}

/// The cluster services of a mount node.
struct Cluster {
    net: Network,
    locks: LockManager,
    repl: Arc<Repl>,
    flusher: Arc<Flusher>,
    flush_thread: std::thread::JoinHandle<()>,
}

impl Cluster {
    /// Stops replication (recording a clean stop), locks, the network and
    /// the flusher, in that order.
    fn stop(self) {
        self.repl.shutdown();
        self.locks.shutdown();
        self.net.shutdown();
        self.flusher.stop();
        let _ = self.flush_thread.join();
        if let Err(e) = self.flusher.flush_now() {
            log::error!("final flush of the replication store failed: {e}");
        }
    }
}

fn start_cluster(cfg: &Config, backing: &Path) -> anyhow::Result<Cluster> {
    let found = marker::load(cfg, backing)?;
    let existed = cfg.state_dir.join("store.redb").exists();
    let store = Store::open(&cfg.state_dir)
        .with_context(|| format!("opening the store in {}", cfg.state_dir.display()))?;
    let cause = rebuild::cause(
        found.as_ref(),
        existed,
        &store,
        backing,
        cfg.peers.iter().any(|p| p.id != cfg.node_id),
    )?;
    let disk = match (&found, cause) {
        (Some(m), None) => m.disk,
        (Some(m), Some(rebuild::Cause::Resume)) => m.disk,
        (_, cause) => {
            let m = marker::fresh(cfg, cause.is_some())?;
            marker::store(backing, &m)
                .with_context(|| format!("writing {}", marker::path(backing).display()))?;
            log::info!(
                "new disk id {:016x} in {}",
                m.disk,
                marker::path(backing).display()
            );
            m.disk
        }
    };
    if let Some(c) = cause {
        log::warn!("{c}: rebuilding from peers; writes fail (EROFS) until that is done");
    }
    let flusher = Flusher::new(Arc::new(store));
    let mut settings = Settings::from_config(cfg);
    settings.disk = disk;
    let versions = version::persisted(settings.quorum(), &flusher)?;
    let net = Network::start_with_disk(cfg, disk)
        .with_context(|| format!("listening on {}", cfg.listen))?;
    let locks = LockManager::start_with_versions(settings, net.clone(), versions);
    let repl = match Repl::start(
        cfg,
        backing,
        disk,
        Arc::clone(&flusher),
        net.clone(),
        locks.clone(),
        cause,
    ) {
        Ok(r) => r,
        Err(e) => {
            locks.shutdown();
            net.shutdown();
            return Err(anyhow::Error::from(e).context("starting replication"));
        }
    };
    let flush_thread = flusher.start();
    if cause.is_some_and(|c| c != rebuild::Cause::NewMember) {
        // Mount with the tree complete if the peers answer in time (a new
        // member mounts at once: its peers may not be up yet).
        repl.wait_for_tree(cfg.freshness_timeout.get());
    }
    Ok(Cluster {
        net,
        locks,
        repl,
        flusher,
        flush_thread,
    })
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
            mountpoint: mountpoint.to_path_buf(),
            direct: Some(mountpoint.to_path_buf()),
            net: None,
            cluster: None,
            reloader: None,
            registration: None,
        }),
        Err(e) => {
            // SAFETY: valid path string.
            unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) };
            Err(e.into())
        }
    }
}

fn umount(p: &Path, flags: libc::c_int) -> std::io::Result<()> {
    let c =
        std::ffi::CString::new(p.as_os_str().as_encoded_bytes()).map_err(std::io::Error::other)?;
    // SAFETY: valid path string.
    if unsafe { libc::umount2(c.as_ptr(), flags) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// A lazy unmount for a non-root mount, through fusermount3.
fn fusermount_lazy(p: &Path) -> std::io::Result<()> {
    // fuser's unmount may have detached it already.
    let mounted = std::fs::read_to_string("/proc/self/mountinfo")?
        .lines()
        .any(|l| l.split(' ').nth(4).map(Path::new) == Some(p));
    if !mounted {
        return Ok(());
    }
    let st = std::process::Command::new("fusermount3")
        .args(["-u", "-z"])
        .arg(p)
        .status()?;
    if st.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!("fusermount3 -u -z: {st}")))
    }
}

/// Runs the session until unmounted; SIGINT and SIGTERM unmount cleanly,
/// SIGHUP reloads the configuration.
fn serve(served: Served, mut signals: Signals) -> anyhow::Result<()> {
    let Served {
        mut session,
        mountpoint,
        direct,
        net,
        cluster,
        reloader,
        registration,
    } = served;
    let mut reloader = reloader.expect("set once mounted");
    let mut unmounter = session.unmount_callable();
    let unmounting = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = std::sync::Arc::clone(&unmounting);
    std::thread::spawn(move || {
        // The first signal unmounts; if the mount is busy, it keeps serving
        // and a later signal detaches it lazily (it then goes away when the
        // last open file is closed, like `umount -l`).
        let stops = signals.forever().filter(|&sig| {
            if sig == signal_hook::consts::SIGHUP {
                reloader.reload();
                return false;
            }
            true
        });
        for (i, sig) in stops.enumerate() {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            let res = if i == 0 {
                log::info!("signal {sig}: unmounting");
                match &direct {
                    Some(p) => umount(p, 0),
                    None => unmounter.unmount(),
                }
            } else {
                log::warn!("signal {sig} again: detaching the mount lazily");
                match &direct {
                    Some(p) => umount(p, libc::MNT_DETACH),
                    None => fusermount_lazy(&mountpoint),
                }
            };
            match res {
                Ok(()) => {}
                Err(e) if i == 0 => log::warn!(
                    "unmount failed: {e}; send the signal again to detach the mount lazily"
                ),
                Err(e) => log::error!("lazy unmount failed: {e}"),
            }
        }
    });
    let res = match session.run() {
        // After a lazy detach some kernels abort the connection when the
        // last file is closed instead of ending it: that is the unmount.
        Err(e)
            if e.raw_os_error() == Some(libc::ECONNABORTED)
                && unmounting.load(std::sync::atomic::Ordering::SeqCst) =>
        {
            Ok(())
        }
        r => r.context("serving filesystem"),
    };
    match cluster {
        Some(c) => c.stop(),
        None => {
            if let Some(net) = net {
                net.shutdown();
            }
        }
    }
    drop(registration);
    res?;
    log::info!("unmounted");
    Ok(())
}
