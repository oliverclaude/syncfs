//! Reloading the configuration in place: `mount -o remount` and SIGHUP
//! (`systemctl reload`).
//!
//! A signal cannot carry options, so `mount -o remount` hands its options
//! over in a file: the helper ([`request`]) finds the daemon serving the
//! mountpoint through its registration (see [`Registration`]), writes the
//! options to `<state_dir>/remount.json` (atomically, by rename) and sends
//! SIGHUP. The daemon ([`Reloader::reload`]) takes the file if there is
//! one, rebuilds the configuration with the usual precedence (defaults <
//! YAML < `-o` options < its original command-line flags), and either
//! applies every change or none: one refused setting (see
//! [`crate::config::classify`]) leaves the running configuration as it
//! was. The outcome is only logged. Open files, held locks and cluster
//! connections are never touched.
//!
//! The `-o` options of a remount are added after the ones in effect, so
//! options given at mount time stay unless the remount overrides them:
//! without an fstab entry mount(8) passes only the kernel's view of the
//! mount, which lacks every syncfs option.

use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::config::{self, Class, Config, Diff, Environment, SharedConfig, Sources};
use crate::lock::LockManager;
use crate::logging;
use crate::net::Network;
use crate::repl::core::Repl;

/// Options handed over by `mount -o remount`, in `<state_dir>/remount.json`.
pub const PENDING_FILE: &str = "remount.json";
/// The daemon's process id, in `<state_dir>`.
pub const PID_FILE: &str = "syncfs.pid";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    /// `-o` groups, in order.
    pub options: Vec<String>,
    /// mount's `-s`: tolerate unknown options.
    pub sloppy: bool,
}

/// What the process was started with, to rebuild the configuration from.
#[derive(Clone, Debug)]
pub struct Base {
    pub sources: Sources,
    pub env: Environment,
    /// Generic kernel mount flags from `-o` at start.
    pub kernel_flags: Vec<String>,
}

/// The running services a reload changes. Absent ones are skipped (a
/// standalone mount has no locks or replication; a voter no replication).
#[derive(Clone, Default)]
pub struct Targets {
    pub net: Option<Network>,
    pub locks: Option<LockManager>,
    pub repl: Option<Arc<Repl>>,
    /// Shown in `.syncfs/config` (or a voter's status.json).
    pub shown: Option<SharedConfig>,
}

/// The outcome of one reload, as logged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Unchanged,
    Applied(Diff),
    Refused(Diff),
    Failed(String),
}

pub struct Reloader {
    base: Base,
    /// The `-o` options in effect.
    options: Vec<String>,
    current: Config,
    targets: Targets,
}

impl Reloader {
    pub fn new(base: Base, current: Config, targets: Targets) -> Reloader {
        Reloader {
            options: base.sources.mount_options.clone(),
            base,
            current,
            targets,
        }
    }

    /// The configuration in effect.
    pub fn current(&self) -> &Config {
        &self.current
    }

    /// Reloads (on SIGHUP), taking the options of a pending remount if
    /// there are any, and logs what happened.
    pub fn reload(&mut self) -> Outcome {
        let out = self.reload_inner();
        match &out {
            Outcome::Unchanged => log::info!("reload: configuration unchanged"),
            Outcome::Applied(_) => log::info!("reload: done"),
            Outcome::Refused(d) => {
                let names: Vec<String> = d.refused().map(|c| c.to_string()).collect();
                log::error!(
                    "reload refused, nothing changed: these settings cannot change while \
                     syncfs runs (unmount and mount again to change them): {}",
                    names.join("; ")
                );
            }
            Outcome::Failed(e) => log::error!("reload failed, nothing changed: {e}"),
        }
        out
    }

    fn reload_inner(&mut self) -> Outcome {
        let (options, sloppy) = match take_pending(&self.current.state_dir) {
            Ok(Some(req)) => {
                log::info!(
                    "reload: mount -o remount with options {:?}",
                    req.options.join(",")
                );
                self.warn_overridden(&req);
                let mut o = self.options.clone();
                o.extend(req.options);
                (o, req.sloppy || self.base.sources.sloppy)
            }
            Ok(None) => {
                log::info!("reload: re-reading the configuration (SIGHUP)");
                (self.options.clone(), self.base.sources.sloppy)
            }
            Err(e) => return Outcome::Failed(format!("{e:#}")),
        };
        let sources = Sources {
            mount_options: options.clone(),
            sloppy,
            ..self.base.sources.clone()
        };
        let loaded = match config::load(&sources, &self.base.env, |p| std::fs::read_to_string(p)) {
            Ok(l) => l,
            Err(e) => return Outcome::Failed(e.to_string()),
        };
        for w in &loaded.resolved.warnings {
            log::warn!("reload: {w}");
        }
        let new = loaded.resolved.config;
        let diff = Diff::between(&self.current, &new);
        if !diff.is_allowed() {
            return Outcome::Refused(diff);
        }
        if loaded.kernel_flags != self.base.kernel_flags {
            log::warn!(
                "reload: kernel mount flags {:?} are not changed by a reload (in effect: {:?})",
                loaded.kernel_flags,
                self.base.kernel_flags
            );
        }
        self.options = options;
        if diff.is_empty() {
            return Outcome::Unchanged;
        }
        self.apply(&new);
        for c in &diff.changes {
            log::info!("reload: {c}");
            if c.class == Class::ClusterWide {
                log::warn!(
                    "reload: {} is cluster-wide: change it to {} on every node of the \
                     cluster. Until all nodes agree, a node with a longer lock_lease_timeout \
                     than a voter has its lock requests and renewals refused by that voter, \
                     and nodes whose heartbeat_interval differs may call each other down",
                    c.setting,
                    c.new
                );
            }
        }
        self.current = new;
        Outcome::Applied(diff)
    }

    /// Settings a remount sets that a command-line flag overrides (flags
    /// win over `-o`), so that the remount cannot change them.
    fn warn_overridden(&self, req: &Request) {
        let Ok(o) = config::parse_mount_options(
            req.options.iter().map(String::as_str),
            &self.base.env.cwd,
            true,
        ) else {
            return;
        };
        let flags = self.base.sources.cli.set_fields();
        let shadowed: Vec<&str> = o
            .layer
            .set_fields()
            .into_iter()
            .filter(|f| flags.contains(f))
            .collect();
        if !shadowed.is_empty() {
            log::warn!(
                "reload: {} from mount -o cannot take effect: syncfs was started with \
                 command-line flags for them, and those win over -o",
                shadowed.join(", ")
            );
        }
    }

    fn apply(&self, new: &Config) {
        logging::init(new.log_level);
        let t = &self.targets;
        if let Some(l) = &t.locks {
            l.set_timings(
                new.lock_lease_timeout.get(),
                new.lock_wait.get(),
                new.heartbeat_interval.get(),
            );
        }
        if let Some(n) = &t.net {
            n.set_heartbeat(new.heartbeat_interval.get());
        }
        if let Some(r) = &t.repl {
            r.set_timings(new.freshness_timeout.get(), new.heartbeat_interval.get());
        }
        if let Some(s) = &t.shown {
            *s.write().unwrap_or_else(|e| e.into_inner()) = new.clone();
        }
    }
}

/// Takes the pending remount request out of `state_dir`, if any. The file
/// is renamed before it is read, so a request written meanwhile waits for
/// the next signal instead of being lost.
pub fn take_pending(state_dir: &Path) -> anyhow::Result<Option<Request>> {
    let path = state_dir.join(PENDING_FILE);
    let taken = state_dir.join(format!("{PENDING_FILE}.taken"));
    match std::fs::rename(&path, &taken) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("taking {}", path.display())),
    }
    let raw = std::fs::read(&taken);
    let _ = std::fs::remove_file(&taken);
    let raw = raw.with_context(|| format!("reading {}", taken.display()))?;
    let req = serde_json::from_slice(&raw)
        .with_context(|| format!("invalid remount request in {}", path.display()))?;
    Ok(Some(req))
}

/// Writes `data` to `path` atomically: a temporary file in the same
/// directory, then rename.
pub fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
    let res = (|| {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if res.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    res
}

/// Where mount daemons register by mountpoint, so that `mount -o remount`
/// (which knows only the mountpoint) can find them: `/run/syncfs` for
/// root; otherwise `$XDG_RUNTIME_DIR/syncfs`, else `/run/user/UID/syncfs`
/// if that exists, else `syncfs/run` in the user's state directory.
pub fn run_dir() -> Option<PathBuf> {
    if rustix::process::geteuid().is_root() {
        return Some(PathBuf::from("/run/syncfs"));
    }
    if let Some(x) = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
    {
        return Some(x.join("syncfs"));
    }
    let user = PathBuf::from(format!("/run/user/{}", rustix::process::geteuid().as_raw()));
    if user.is_dir() {
        return Some(user.join("syncfs"));
    }
    let env = Environment {
        xdg_state_home: None,
        ..Environment::current().ok()?
    };
    env.default_state_dir().map(|d| d.join("run"))
}

/// The registration file of the mount at `mountpoint` in `dir`.
fn entry_path(dir: &Path, mountpoint: &Path) -> PathBuf {
    let hash = blake3::hash(mountpoint.as_os_str().as_encoded_bytes());
    dir.join(format!("mount-{}.json", &hash.to_hex()[..16]))
}

/// A daemon's registration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub pid: u32,
    pub mountpoint: Option<PathBuf>,
    pub state_dir: PathBuf,
}

/// Written when the daemon starts serving and removed when it stops:
/// `<state_dir>/syncfs.pid` always, and for a mount the entry in
/// [`run_dir`] keyed by its mountpoint.
pub struct Registration {
    files: Vec<PathBuf>,
}

impl Registration {
    pub fn new(cfg: &Config) -> Registration {
        let pid = std::process::id();
        let mut files = Vec::new();
        let pidfile = cfg.state_dir.join(PID_FILE);
        match write_atomic(&pidfile, format!("{pid}\n").as_bytes()) {
            Ok(()) => files.push(pidfile),
            Err(e) => log::warn!("writing {}: {e}", pidfile.display()),
        }
        if let Some(mp) = &cfg.mountpoint {
            let res = (|| -> anyhow::Result<PathBuf> {
                let dir = run_dir().context("no runtime directory")?;
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&dir)
                    .with_context(|| format!("creating {}", dir.display()))?;
                let entry = Entry {
                    pid,
                    mountpoint: Some(mp.clone()),
                    state_dir: cfg.state_dir.clone(),
                };
                let path = entry_path(&dir, mp);
                write_atomic(&path, &serde_json::to_vec(&entry)?)
                    .with_context(|| format!("writing {}", path.display()))?;
                Ok(path)
            })();
            match res {
                Ok(p) => files.push(p),
                Err(e) => log::warn!("mount -o remount will not find this mount: {e:#}"),
            }
        }
        Registration { files }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let me = std::process::id();
        for f in &self.files {
            // Only our own: a newer daemon may have taken the name over.
            let ours = std::fs::read_to_string(f).is_ok_and(|s| {
                s.trim() == me.to_string()
                    || serde_json::from_str::<Entry>(&s).is_ok_and(|e| e.pid == me)
            });
            if ours {
                let _ = std::fs::remove_file(f);
            }
        }
    }
}

/// Whether `pid` is a running syncfs process.
fn is_syncfs(pid: u32) -> bool {
    let Ok(cmdline) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
        return false;
    };
    let argv0 = cmdline.split(|&b| b == 0).next().unwrap_or_default();
    let name = Path::new(std::ffi::OsStr::from_bytes(argv0))
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    matches!(
        name.as_str(),
        "syncfs" | "mount.syncfs" | "mount.fuse.syncfs"
    )
}

/// Finds the daemon serving `mountpoint` (absolute).
pub fn find(mountpoint: &Path) -> anyhow::Result<Entry> {
    let dir = run_dir().context("no runtime directory to look up mounts in")?;
    let mut candidates = vec![mountpoint.to_path_buf()];
    // The daemon registered the path it was given; mount(8) passes the
    // canonical one. Resolve the parent only: the mountpoint itself is
    // served by the daemon in question.
    if let (Some(parent), Some(name)) = (mountpoint.parent(), mountpoint.file_name()) {
        if let Ok(p) = std::fs::canonicalize(parent) {
            candidates.push(p.join(name));
        }
    }
    for mp in &candidates {
        let path = entry_path(&dir, mp);
        let Ok(raw) = std::fs::read(&path) else {
            continue;
        };
        let e: Entry = serde_json::from_slice(&raw)
            .with_context(|| format!("invalid registration {}", path.display()))?;
        if e.mountpoint.as_deref() != Some(mp.as_path()) {
            continue;
        }
        if !is_syncfs(e.pid) {
            bail!(
                "the syncfs daemon for {} (pid {}) is not running",
                mountpoint.display(),
                e.pid
            );
        }
        return Ok(e);
    }
    bail!(
        "no running syncfs daemon is registered for {} (in {})",
        mountpoint.display(),
        dir.display()
    )
}

/// `mount -o remount`: hands `req` to the daemon serving `mountpoint` and
/// signals it. Returns its pid. The outcome is in the daemon's log.
pub fn request(mountpoint: &Path, req: &Request) -> anyhow::Result<u32> {
    let e = find(mountpoint)?;
    let path = e.state_dir.join(PENDING_FILE);
    write_atomic(&path, &serde_json::to_vec(req)?)
        .with_context(|| format!("writing {}", path.display()))?;
    let pid = rustix::process::Pid::from_raw(e.pid as i32).context("invalid pid")?;
    rustix::process::kill_process(pid, rustix::process::Signal::HUP)
        .with_context(|| format!("signalling the syncfs daemon (pid {})", e.pid))?;
    Ok(e.pid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_request_round_trip() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(take_pending(d.path()).unwrap(), None);
        let req = Request {
            options: vec!["lock_wait=5s".into(), "log_level=debug".into()],
            sloppy: true,
        };
        write_atomic(
            &d.path().join(PENDING_FILE),
            &serde_json::to_vec(&req).unwrap(),
        )
        .unwrap();
        assert_eq!(take_pending(d.path()).unwrap(), Some(req));
        // Taken once; nothing left behind.
        assert_eq!(take_pending(d.path()).unwrap(), None);
        assert_eq!(std::fs::read_dir(d.path()).unwrap().count(), 0);
    }

    #[test]
    fn broken_request_is_an_error_and_removed() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join(PENDING_FILE), b"{").unwrap();
        assert!(take_pending(d.path()).is_err());
        assert_eq!(take_pending(d.path()).unwrap(), None);
    }

    #[test]
    fn entries_are_keyed_by_mountpoint() {
        let d = Path::new("/run/syncfs");
        assert_ne!(
            entry_path(d, Path::new("/mnt/a")),
            entry_path(d, Path::new("/mnt/b"))
        );
        assert_eq!(
            entry_path(d, Path::new("/mnt/a")),
            entry_path(d, Path::new("/mnt/a"))
        );
    }
}
