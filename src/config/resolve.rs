//! Turns a stack of [`PartialConfig`] layers into a complete, validated
//! [`Config`], filling built-in defaults for anything left unset.

use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Serialize;
use thiserror::Error;

use super::partial::PartialConfig;
use super::types::{check_host_port, LogLevel, Mode, NodeId, Peer};
use super::units::{ByteSize, HumanDuration};

/// Config file read when none is given with `--config` / `-o config=`.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/syncfs/syncfs.yaml";
/// Default TCP port for node-to-node traffic.
pub const DEFAULT_PORT: u16 = 7420;
/// Largest supported cluster, counting every node (voters included).
pub const MAX_NODES: usize = 99;
pub const DEFAULT_NODE_ID: NodeId = 1;
pub const DEFAULT_CLUSTER_NAME: &str = "syncfs";
/// Longest cluster name.
pub const MAX_CLUSTER_NAME_LEN: usize = 64;
pub const DEFAULT_LOCK_LEASE_TIMEOUT: HumanDuration = HumanDuration::from_secs(30);
pub const DEFAULT_LOCK_WAIT: HumanDuration = HumanDuration::from_secs(10);
pub const DEFAULT_HEARTBEAT_INTERVAL: HumanDuration = HumanDuration::from_secs(1);
pub const DEFAULT_BLOCK_SIZE: ByteSize = ByteSize(64 * ByteSize::KIB);
pub const MIN_BLOCK_SIZE: ByteSize = ByteSize(4 * ByteSize::KIB);
pub const MAX_BLOCK_SIZE: ByteSize = ByteSize(16 * ByteSize::MIB);
/// A lock lease must outlive at least this many heartbeats, so a single
/// late heartbeat cannot expire a live holder's lock.
pub const MIN_LEASE_HEARTBEATS: u32 = 3;
/// State directory used when running as root.
pub const ROOT_STATE_DIR: &str = "/var/lib/syncfs";

/// The process environment that defaults depend on. Injected so tests do
/// not depend on who runs them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Environment {
    pub is_root: bool,
    pub home: Option<PathBuf>,
    pub xdg_state_home: Option<PathBuf>,
    pub cwd: PathBuf,
}

impl Environment {
    pub fn current() -> std::io::Result<Self> {
        let abs = |v: std::ffi::OsString| Some(PathBuf::from(v)).filter(|p| p.is_absolute());
        Ok(Self {
            // SAFETY: geteuid has no preconditions and cannot fail.
            is_root: unsafe { libc::geteuid() } == 0,
            home: std::env::var_os("HOME").and_then(abs),
            xdg_state_home: std::env::var_os("XDG_STATE_HOME").and_then(abs),
            cwd: std::env::current_dir()?,
        })
    }

    /// `/var/lib/syncfs` for root, otherwise `$XDG_STATE_HOME/syncfs` or
    /// `$HOME/.local/state/syncfs`.
    pub fn default_state_dir(&self) -> Option<PathBuf> {
        if self.is_root {
            return Some(PathBuf::from(ROOT_STATE_DIR));
        }
        self.xdg_state_home
            .clone()
            .or_else(|| self.home.as_ref().map(|h| h.join(".local/state")))
            .map(|d| d.join("syncfs"))
    }
}

/// The complete effective configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Config {
    /// Identifies the cluster; nodes only talk to nodes with the same name
    /// and membership.
    pub cluster_name: String,
    pub node_id: NodeId,
    pub mode: Mode,
    pub listen: SocketAddr,
    /// Full cluster membership including this node. Empty means a
    /// standalone single-node cluster.
    pub peers: Vec<Peer>,
    /// Always `Some` in mount mode, always `None` in daemon mode.
    pub backing_dir: Option<PathBuf>,
    /// Always `Some` in mount mode, always `None` in daemon mode.
    pub mountpoint: Option<PathBuf>,
    pub state_dir: PathBuf,
    pub lock_lease_timeout: HumanDuration,
    /// How long acquiring a lock held by another node may wait.
    pub lock_wait: HumanDuration,
    pub heartbeat_interval: HumanDuration,
    pub block_size: ByteSize,
    pub debug: bool,
    pub allow_other: bool,
    pub allow_root: bool,
    pub log_level: LogLevel,
    /// Stay in the foreground instead of daemonizing. Implied by `debug`.
    pub foreground: bool,
}

impl Config {
    /// Number of voting nodes in the cluster, this node included.
    pub fn cluster_size(&self) -> usize {
        self.peers.len().max(1)
    }

    /// Votes needed for quorum: a strict majority of all configured nodes.
    pub fn quorum(&self) -> usize {
        self.cluster_size() / 2 + 1
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("node_id must be between 1 and 65535, got 0")]
    NodeIdZero,
    #[error(
        "cluster_name {0:?} must be 1 to {MAX_CLUSTER_NAME_LEN} characters of A-Z a-z 0-9 . _ -"
    )]
    BadClusterName(String),
    #[error("peer ids must be between 1 and 65535, got 0")]
    PeerIdZero,
    #[error("cluster has {0} nodes; the maximum is {MAX_NODES}")]
    TooManyNodes(usize),
    #[error("peer id {0} appears more than once in peers")]
    DuplicatePeerId(NodeId),
    #[error("peer address {0:?} appears more than once in peers")]
    DuplicatePeerAddress(String),
    #[error("peer {id} has invalid address {address:?}: {reason}")]
    BadPeerAddress {
        id: NodeId,
        address: String,
        reason: String,
    },
    #[error("node_id {0} is not in peers; the peer list must include this node")]
    SelfNotInPeers(NodeId),
    #[error("listen port must not be 0")]
    ListenPortZero,
    #[error("mount mode needs backing_dir (first argument, --backing-dir or -o backing_dir=)")]
    MissingBackingDir,
    #[error("mount mode needs mountpoint (second argument, --mountpoint or -o mountpoint=)")]
    MissingMountpoint,
    #[error("{field} must be an absolute path, got {path:?}")]
    RelativePath { field: &'static str, path: PathBuf },
    #[error("{a} {pa:?} and {b} {pb:?} must not be the same or nested inside each other")]
    OverlappingPaths {
        a: &'static str,
        pa: PathBuf,
        b: &'static str,
        pb: PathBuf,
    },
    #[error("no state_dir given and neither HOME nor XDG_STATE_HOME is set")]
    NoStateDir,
    #[error("heartbeat_interval must be greater than 0")]
    HeartbeatZero,
    #[error(
        "lock_lease_timeout ({lease}) must be at least {MIN_LEASE_HEARTBEATS} x heartbeat_interval ({heartbeat})"
    )]
    LeaseTooShort {
        lease: HumanDuration,
        heartbeat: HumanDuration,
    },
    #[error("block_size {0} must be a power of two between {MIN_BLOCK_SIZE} and {MAX_BLOCK_SIZE}")]
    BadBlockSize(ByteSize),
    #[error("allow_other and allow_root cannot both be set")]
    AllowOtherAndRoot,
}

/// All problems found in a configuration, reported together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationErrors(pub Vec<ValidationError>);

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid configuration:")?;
        for e in &self.0 {
            write!(f, "\n  - {e}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {}

/// A validated configuration plus non-fatal warnings worth showing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub config: Config,
    pub warnings: Vec<String>,
}

/// Fills defaults into `p` and validates the result.
pub fn resolve(p: PartialConfig, env: &Environment) -> Result<Resolved, ValidationErrors> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let node_id = p.node_id.unwrap_or(DEFAULT_NODE_ID);
    let cluster_name = p
        .cluster_name
        .unwrap_or_else(|| DEFAULT_CLUSTER_NAME.to_string());
    let mode = p.mode.unwrap_or_default();
    let listen = p
        .listen
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], DEFAULT_PORT)));
    let peers = p.peers.unwrap_or_default();
    let debug = p.debug.unwrap_or(false);
    let log_level = p.log_level.unwrap_or(if debug {
        LogLevel::Debug
    } else {
        LogLevel::Info
    });
    let lock_lease_timeout = p.lock_lease_timeout.unwrap_or(DEFAULT_LOCK_LEASE_TIMEOUT);
    let lock_wait = p.lock_wait.unwrap_or(DEFAULT_LOCK_WAIT);
    let heartbeat_interval = p.heartbeat_interval.unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);
    let block_size = p.block_size.unwrap_or(DEFAULT_BLOCK_SIZE);
    let allow_other = p.allow_other.unwrap_or(false);
    let allow_root = p.allow_root.unwrap_or(false);
    let foreground = p.foreground.unwrap_or(false) || debug;

    if node_id == 0 {
        errors.push(ValidationError::NodeIdZero);
    }
    let name_ok = (1..=MAX_CLUSTER_NAME_LEN).contains(&cluster_name.len())
        && cluster_name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
    if !name_ok {
        errors.push(ValidationError::BadClusterName(cluster_name.clone()));
    }
    if listen.port() == 0 {
        errors.push(ValidationError::ListenPortZero);
    }
    check_peers(node_id, listen, &peers, &mut errors, &mut warnings);

    let (backing_dir, mountpoint) = match mode {
        Mode::Mount => {
            if p.backing_dir.is_none() {
                errors.push(ValidationError::MissingBackingDir);
            }
            if p.mountpoint.is_none() {
                errors.push(ValidationError::MissingMountpoint);
            }
            (p.backing_dir, p.mountpoint)
        }
        Mode::Daemon => {
            for (field, v) in [
                ("backing_dir", &p.backing_dir),
                ("mountpoint", &p.mountpoint),
            ] {
                if v.is_some() {
                    warnings.push(format!("daemon mode stores no data: ignoring {field}"));
                }
            }
            (None, None)
        }
    };

    let state_dir = p.state_dir.or_else(|| env.default_state_dir());
    if state_dir.is_none() {
        errors.push(ValidationError::NoStateDir);
    }

    check_paths(
        &[
            ("backing_dir", backing_dir.as_deref()),
            ("mountpoint", mountpoint.as_deref()),
            ("state_dir", state_dir.as_deref()),
        ],
        &mut errors,
    );

    if heartbeat_interval.get().is_zero() {
        errors.push(ValidationError::HeartbeatZero);
    } else if lock_lease_timeout.get() < heartbeat_interval.get() * MIN_LEASE_HEARTBEATS {
        errors.push(ValidationError::LeaseTooShort {
            lease: lock_lease_timeout,
            heartbeat: heartbeat_interval,
        });
    }

    let b = block_size.bytes();
    if !b.is_power_of_two() || block_size < MIN_BLOCK_SIZE || block_size > MAX_BLOCK_SIZE {
        errors.push(ValidationError::BadBlockSize(block_size));
    }

    if allow_other && allow_root {
        errors.push(ValidationError::AllowOtherAndRoot);
    }

    if !errors.is_empty() {
        return Err(ValidationErrors(errors));
    }
    Ok(Resolved {
        config: Config {
            cluster_name,
            node_id,
            mode,
            listen,
            peers,
            backing_dir,
            mountpoint,
            state_dir: state_dir.expect("checked above"),
            lock_lease_timeout,
            lock_wait,
            heartbeat_interval,
            block_size,
            debug,
            allow_other,
            allow_root,
            log_level,
            foreground,
        },
        warnings,
    })
}

fn check_peers(
    node_id: NodeId,
    listen: SocketAddr,
    peers: &[Peer],
    errors: &mut Vec<ValidationError>,
    warnings: &mut Vec<String>,
) {
    if peers.is_empty() {
        return;
    }
    if peers.len() > MAX_NODES {
        errors.push(ValidationError::TooManyNodes(peers.len()));
    }
    let mut ids = HashSet::new();
    let mut addrs = HashSet::new();
    let mut zero_reported = false;
    for peer in peers {
        if peer.id == 0 && !zero_reported {
            errors.push(ValidationError::PeerIdZero);
            zero_reported = true;
        }
        if !ids.insert(peer.id) {
            errors.push(ValidationError::DuplicatePeerId(peer.id));
        }
        if let Err(reason) = check_host_port(&peer.address) {
            errors.push(ValidationError::BadPeerAddress {
                id: peer.id,
                address: peer.address.clone(),
                reason,
            });
        } else if !addrs.insert(peer.address.to_ascii_lowercase()) {
            errors.push(ValidationError::DuplicatePeerAddress(peer.address.clone()));
        }
    }
    match peers.iter().find(|p| p.id == node_id) {
        None => errors.push(ValidationError::SelfNotInPeers(node_id)),
        Some(me) => {
            let port = me
                .address
                .rsplit_once(':')
                .and_then(|(_, p)| p.parse::<u16>().ok());
            if port.is_some_and(|p| p != listen.port()) {
                warnings.push(format!(
                    "this node's peer address {} uses a different port than listen {listen}",
                    me.address
                ));
            }
        }
    }
    if peers.len() > 1 && peers.len().is_multiple_of(2) {
        warnings.push(format!(
            "{} nodes: an even cluster tolerates no more failures than {} nodes; \
             consider adding a voter-only daemon",
            peers.len(),
            peers.len() - 1
        ));
    }
}

fn check_paths(paths: &[(&'static str, Option<&Path>)], errors: &mut Vec<ValidationError>) {
    let present: Vec<(&'static str, &Path)> = paths
        .iter()
        .filter_map(|(f, p)| p.map(|p| (*f, p)))
        .collect();
    let mut all_absolute = true;
    for (field, path) in &present {
        if !path.is_absolute() {
            all_absolute = false;
            errors.push(ValidationError::RelativePath {
                field,
                path: path.to_path_buf(),
            });
        }
    }
    if !all_absolute {
        return;
    }
    for (i, (a, pa)) in present.iter().enumerate() {
        for (b, pb) in &present[i + 1..] {
            if pa.starts_with(pb) || pb.starts_with(pa) {
                errors.push(ValidationError::OverlappingPaths {
                    a,
                    pa: pa.to_path_buf(),
                    b,
                    pb: pb.to_path_buf(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn env() -> Environment {
        Environment {
            is_root: false,
            home: Some("/home/u".into()),
            xdg_state_home: None,
            cwd: "/".into(),
        }
    }

    fn mount_layer() -> PartialConfig {
        PartialConfig {
            backing_dir: Some("/srv/data".into()),
            mountpoint: Some("/mnt/sync".into()),
            ..Default::default()
        }
    }

    fn peers(n: usize) -> Vec<Peer> {
        (1..=n)
            .map(|i| Peer {
                id: i as NodeId,
                address: format!("10.0.{}.{}:7420", i / 256, i % 256),
            })
            .collect()
    }

    fn errs(p: PartialConfig) -> Vec<ValidationError> {
        resolve(p, &env()).unwrap_err().0
    }

    #[test]
    fn cluster_name_rules() {
        assert_eq!(
            resolve(mount_layer(), &env()).unwrap().config.cluster_name,
            "syncfs"
        );
        for bad in ["", "has space", "slash/", &"x".repeat(65)] {
            let mut p = mount_layer();
            p.cluster_name = Some(bad.to_string());
            assert_eq!(errs(p), [ValidationError::BadClusterName(bad.to_string())]);
        }
        let mut p = mount_layer();
        p.cluster_name = Some("prod-eu_1.a".into());
        assert_eq!(
            resolve(p, &env()).unwrap().config.cluster_name,
            "prod-eu_1.a"
        );
    }

    #[test]
    fn defaults() {
        let c = resolve(mount_layer(), &env()).unwrap();
        assert!(c.warnings.is_empty());
        let c = c.config;
        assert_eq!(c.node_id, 1);
        assert_eq!(c.mode, Mode::Mount);
        assert_eq!(c.listen, "0.0.0.0:7420".parse().unwrap());
        assert!(c.peers.is_empty());
        assert_eq!(c.cluster_size(), 1);
        assert_eq!(c.quorum(), 1);
        assert_eq!(c.state_dir, PathBuf::from("/home/u/.local/state/syncfs"));
        assert_eq!(c.lock_lease_timeout, HumanDuration::from_secs(30));
        assert_eq!(c.lock_wait, HumanDuration::from_secs(10));
        assert_eq!(c.heartbeat_interval, HumanDuration::from_secs(1));
        assert_eq!(c.block_size, ByteSize(65536));
        assert!(!c.debug && !c.allow_other && !c.allow_root);
        assert_eq!(c.log_level, LogLevel::Info);
    }

    #[test]
    fn default_state_dir_depends_on_user() {
        let mut e = env();
        e.xdg_state_home = Some("/xdg".into());
        assert_eq!(e.default_state_dir(), Some("/xdg/syncfs".into()));
        e.is_root = true;
        assert_eq!(e.default_state_dir(), Some(ROOT_STATE_DIR.into()));
        let e = Environment {
            is_root: false,
            home: None,
            xdg_state_home: None,
            cwd: "/".into(),
        };
        assert_eq!(
            resolve(mount_layer(), &e).unwrap_err().0,
            vec![ValidationError::NoStateDir]
        );
    }

    #[test]
    fn debug_implies_debug_log_level_unless_set() {
        let mut p = mount_layer();
        p.debug = Some(true);
        assert_eq!(
            resolve(p.clone(), &env()).unwrap().config.log_level,
            LogLevel::Debug
        );
        p.log_level = Some(LogLevel::Trace);
        assert_eq!(
            resolve(p, &env()).unwrap().config.log_level,
            LogLevel::Trace
        );
    }

    #[test]
    fn quorum_is_strict_majority() {
        for (n, q) in [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (98, 50), (99, 50)] {
            let mut p = mount_layer();
            p.peers = Some(peers(n));
            let c = resolve(p, &env()).unwrap().config;
            assert_eq!((c.cluster_size(), c.quorum()), (n, q), "n={n}");
        }
    }

    #[test]
    fn node_limit_is_99() {
        let mut p = mount_layer();
        p.peers = Some(peers(99));
        assert!(resolve(p.clone(), &env()).is_ok());
        p.peers = Some(peers(100));
        assert_eq!(errs(p), vec![ValidationError::TooManyNodes(100)]);
    }

    #[test]
    fn duplicates_rejected() {
        let mut p = mount_layer();
        let mut list = peers(3);
        list.push(Peer {
            id: 2,
            address: "10.9.9.9:7420".into(),
        });
        list.push(Peer {
            id: 4,
            address: "10.0.0.1:7420".into(),
        });
        list.push(Peer {
            id: 5,
            address: "10.0.0.3:7420".to_uppercase(),
        });
        p.peers = Some(list);
        assert_eq!(
            errs(p),
            vec![
                ValidationError::DuplicatePeerId(2),
                ValidationError::DuplicatePeerAddress("10.0.0.1:7420".into()),
                ValidationError::DuplicatePeerAddress("10.0.0.3:7420".into()),
            ]
        );
    }

    #[test]
    fn self_must_be_in_peers() {
        let mut p = mount_layer();
        p.peers = Some(peers(3));
        p.node_id = Some(7);
        assert_eq!(errs(p), vec![ValidationError::SelfNotInPeers(7)]);
    }

    #[test]
    fn zero_ids_and_bad_addresses() {
        let mut p = mount_layer();
        p.node_id = Some(0);
        p.peers = Some(vec![
            Peer {
                id: 0,
                address: "a:1".into(),
            },
            Peer {
                id: 1,
                address: "nope".into(),
            },
        ]);
        let e = errs(p);
        assert!(e.contains(&ValidationError::NodeIdZero));
        assert!(e.contains(&ValidationError::PeerIdZero));
        assert!(e
            .iter()
            .any(|e| matches!(e, ValidationError::BadPeerAddress { id: 1, .. })));
    }

    #[test]
    fn mount_mode_requires_paths() {
        assert_eq!(
            errs(PartialConfig::default()),
            vec![
                ValidationError::MissingBackingDir,
                ValidationError::MissingMountpoint
            ]
        );
    }

    #[test]
    fn daemon_mode_ignores_paths() {
        let mut p = mount_layer();
        p.mode = Some(Mode::Daemon);
        let r = resolve(p, &env()).unwrap();
        assert_eq!(r.config.backing_dir, None);
        assert_eq!(r.config.mountpoint, None);
        assert_eq!(r.warnings.len(), 2);
        let p = PartialConfig {
            mode: Some(Mode::Daemon),
            ..Default::default()
        };
        let r = resolve(p, &env()).unwrap();
        assert!(r.warnings.is_empty());
    }

    #[test]
    fn paths_must_be_absolute_and_disjoint() {
        let mut p = mount_layer();
        p.backing_dir = Some("data".into());
        assert!(matches!(
            errs(p)[..],
            [ValidationError::RelativePath {
                field: "backing_dir",
                ..
            }]
        ));

        let mut p = mount_layer();
        p.mountpoint = Some("/srv/data".into());
        assert!(matches!(
            errs(p)[..],
            [ValidationError::OverlappingPaths { .. }]
        ));

        let mut p = mount_layer();
        p.mountpoint = Some("/srv/data/mnt".into());
        assert!(matches!(
            errs(p)[..],
            [ValidationError::OverlappingPaths { .. }]
        ));

        let mut p = mount_layer();
        p.state_dir = Some("/srv/data/.state".into());
        assert!(matches!(
            errs(p)[..],
            [ValidationError::OverlappingPaths {
                a: "backing_dir",
                b: "state_dir",
                ..
            }]
        ));

        // Sibling with a shared name prefix is fine.
        let mut p = mount_layer();
        p.mountpoint = Some("/srv/data2".into());
        assert!(resolve(p, &env()).is_ok());
    }

    #[test]
    fn timing_rules() {
        let mut p = mount_layer();
        p.heartbeat_interval = Some(HumanDuration(Duration::ZERO));
        assert_eq!(errs(p), vec![ValidationError::HeartbeatZero]);

        let mut p = mount_layer();
        p.heartbeat_interval = Some(HumanDuration::from_secs(2));
        p.lock_lease_timeout = Some(HumanDuration::from_secs(5));
        assert!(matches!(
            errs(p)[..],
            [ValidationError::LeaseTooShort { .. }]
        ));

        let mut p = mount_layer();
        p.heartbeat_interval = Some(HumanDuration::from_secs(2));
        p.lock_lease_timeout = Some(HumanDuration::from_secs(6));
        assert!(resolve(p, &env()).is_ok());
    }

    #[test]
    fn block_size_rules() {
        for (bytes, ok) in [
            (2048, false),
            (4096, true),
            (65536, true),
            (65537, false),
            (100_000, false),
            (16 << 20, true),
            (32 << 20, false),
        ] {
            let mut p = mount_layer();
            p.block_size = Some(ByteSize(bytes));
            assert_eq!(resolve(p, &env()).is_ok(), ok, "{bytes}");
        }
    }

    #[test]
    fn allow_other_and_root_exclusive() {
        let mut p = mount_layer();
        p.allow_other = Some(true);
        p.allow_root = Some(true);
        assert_eq!(errs(p), vec![ValidationError::AllowOtherAndRoot]);
    }

    #[test]
    fn warnings() {
        let mut p = mount_layer();
        p.peers = Some(peers(4));
        p.listen = Some("0.0.0.0:7000".parse().unwrap());
        let w = resolve(p, &env()).unwrap().warnings;
        assert_eq!(w.len(), 2, "{w:?}");
    }

    #[test]
    fn all_errors_reported_together() {
        let p = PartialConfig {
            node_id: Some(0),
            block_size: Some(ByteSize(3)),
            allow_other: Some(true),
            allow_root: Some(true),
            ..Default::default()
        };
        let e = resolve(p, &env()).unwrap_err();
        assert_eq!(e.0.len(), 5);
        assert!(e.to_string().starts_with("invalid configuration:\n  - "));
    }
}
