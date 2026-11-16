//! Parser for mount-style options: `-o key=value,flag,noflag,...`.
//!
//! This is what `mount(8)` and `/etc/fstab` hand to a FUSE helper, so every
//! YAML setting has an `-o` form. Keys may use `_` or `-`
//! (`node_id` = `node-id`).

use std::path::{Path, PathBuf};

use thiserror::Error;

use super::absolutize;
use super::partial::PartialConfig;
use super::types::Peer;

/// Generic mount(8) / fstab options that mean nothing to syncfs itself and
/// are accepted silently.
const IGNORED: &[&str] = &[
    "defaults",
    "rw",
    "auto",
    "noauto",
    "nofail",
    "_netdev",
    "user",
    "nouser",
    "users",
    "owner",
    "group",
    "relatime",
    "norelatime",
    "strictatime",
    "nostrictatime",
    "lazytime",
    "nolazytime",
    "diratime",
    "nodiratime",
    "loud",
    "silent",
];

/// Generic mount flags that are handed on to the kernel FUSE mount.
pub const KERNEL_FLAGS: &[&str] = &[
    "exec", "noexec", "suid", "nosuid", "dev", "nodev", "atime", "noatime", "dirsync", "sync",
    "async",
];

/// Prefixes of options that belong to other tools (systemd, userspace
/// mount helpers) and are ignored.
/// Prefixes of options that belong to other tools (systemd, mount(8),
/// userspace mount helpers) and are ignored. mount(8) adds `user=NAME` for
/// fstab `user` mounts.
const IGNORED_PREFIXES: &[&str] = &["x-", "comment=", "user=", "uhelper=", "helper="];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OptionError {
    #[error("unknown mount option {0:?}")]
    Unknown(String),
    #[error("mount option {0:?} needs a value ({0}=...)")]
    MissingValue(String),
    #[error("invalid value {value:?} for mount option {key:?}: {reason}")]
    Invalid {
        key: String,
        value: String,
        reason: String,
    },
    #[error("mount option {0:?} is not supported: {1}")]
    Unsupported(String, &'static str),
}

/// The result of parsing all `-o` groups.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MountOptions {
    /// Settings for the `-o` layer of the configuration.
    pub layer: PartialConfig,
    /// `config=PATH`, if given.
    pub config_file: Option<PathBuf>,
    /// Generic flags for the kernel mount (`nosuid`, `noexec`, ...), in order.
    pub kernel_flags: Vec<String>,
    /// Unknown options skipped because `sloppy` was set (mount -s).
    pub ignored_unknown: Vec<String>,
}

/// Parses one or more `-o` arguments (each a comma-separated list). Later
/// occurrences of a key win, except `peer=`, which accumulates; any `peer=`
/// replaces the YAML peer list as a whole. Relative paths are resolved
/// against `cwd`. With `sloppy` (mount's `-s`), unknown options are
/// collected in [`MountOptions::ignored_unknown`] instead of failing.
pub fn parse_mount_options<'a>(
    groups: impl IntoIterator<Item = &'a str>,
    cwd: &Path,
    sloppy: bool,
) -> Result<MountOptions, OptionError> {
    let mut out = MountOptions::default();
    let mut peers: Vec<Peer> = Vec::new();

    for item in groups.into_iter().flat_map(|g| g.split(',')) {
        let item = item.trim();
        if item.is_empty()
            || IGNORED.contains(&item)
            || IGNORED_PREFIXES.iter().any(|p| item.starts_with(p))
        {
            continue;
        }
        if KERNEL_FLAGS.contains(&item) {
            out.kernel_flags.push(item.to_string());
            continue;
        }
        let (raw_key, value) = match item.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (item, None),
        };
        let key = raw_key.replace('-', "_");
        let l = &mut out.layer;

        let need = || value.ok_or_else(|| OptionError::MissingValue(raw_key.to_string()));
        let invalid = |reason: String| OptionError::Invalid {
            key: raw_key.to_string(),
            value: value.unwrap_or_default().to_string(),
            reason,
        };
        let path = |v: &str| -> Result<PathBuf, OptionError> {
            if v.is_empty() {
                return Err(invalid("empty path".into()));
            }
            Ok(absolutize(Path::new(v), cwd))
        };

        match key.as_str() {
            "config" => out.config_file = Some(path(need()?)?),
            "node_id" => {
                l.node_id = Some(
                    need()?
                        .parse()
                        .map_err(|_| invalid("expected 1..=65535".into()))?,
                )
            }
            "mode" => l.mode = Some(need()?.parse().map_err(invalid)?),
            "listen" => {
                l.listen = Some(
                    need()?
                        .parse()
                        .map_err(|_| invalid("expected IP:PORT".into()))?,
                )
            }
            "peer" => peers.push(need()?.parse().map_err(invalid)?),
            "backing_dir" => l.backing_dir = Some(path(need()?)?),
            "mountpoint" => l.mountpoint = Some(path(need()?)?),
            "state_dir" => l.state_dir = Some(path(need()?)?),
            "cluster_name" => l.cluster_name = Some(need()?.to_string()),
            "lock_lease_timeout" => l.lock_lease_timeout = Some(need()?.parse().map_err(invalid)?),
            "heartbeat_interval" => l.heartbeat_interval = Some(need()?.parse().map_err(invalid)?),
            "block_size" => l.block_size = Some(need()?.parse().map_err(invalid)?),
            "log_level" => l.log_level = Some(need()?.parse().map_err(invalid)?),
            "debug" => l.debug = Some(parse_bool(value).map_err(invalid)?),
            "allow_other" => l.allow_other = Some(parse_bool(value).map_err(invalid)?),
            "allow_root" => l.allow_root = Some(parse_bool(value).map_err(invalid)?),
            "foreground" => l.foreground = Some(parse_bool(value).map_err(invalid)?),
            "nodebug" | "noallow_other" | "noallow_root" if value.is_none() => match &key[2..] {
                "debug" => l.debug = Some(false),
                "allow_other" => l.allow_other = Some(false),
                _ => l.allow_root = Some(false),
            },
            "ro" => {
                return Err(OptionError::Unsupported(
                    raw_key.to_string(),
                    "read-only mounts are not implemented yet",
                ))
            }
            _ if sloppy => out.ignored_unknown.push(item.to_string()),
            _ => return Err(OptionError::Unknown(item.to_string())),
        }
    }

    if !peers.is_empty() {
        out.layer.peers = Some(peers);
    }
    Ok(out)
}

fn parse_bool(value: Option<&str>) -> Result<bool, String> {
    match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        None | Some("true" | "yes" | "on" | "1") => Ok(true),
        Some("false" | "no" | "off" | "0") => Ok(false),
        Some(_) => Err("expected true/false, yes/no, on/off or 1/0".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ByteSize, HumanDuration, LogLevel, Mode};

    fn parse(s: &str) -> Result<MountOptions, OptionError> {
        parse_mount_options([s], Path::new("/work"), false)
    }

    #[test]
    fn all_keys() {
        let o = parse(
            "config=/etc/x.yaml,node_id=3,mode=daemon,listen=127.0.0.1:9000,\
             peer=1@a:1,peer=2@b:2,peer=3@c:3,backing_dir=/srv/d,mountpoint=/mnt/m,\
             state_dir=/var/s,lock_lease_timeout=1m,heartbeat_interval=250ms,\
             block_size=1M,log_level=warn,debug,allow_other=yes,allow_root=off,\
             cluster_name=prod",
        )
        .unwrap();
        assert_eq!(o.config_file, Some("/etc/x.yaml".into()));
        let l = o.layer;
        assert_eq!(l.node_id, Some(3));
        assert_eq!(l.mode, Some(Mode::Daemon));
        assert_eq!(l.listen, Some("127.0.0.1:9000".parse().unwrap()));
        assert_eq!(l.peers.as_ref().map(Vec::len), Some(3));
        assert_eq!(l.backing_dir, Some("/srv/d".into()));
        assert_eq!(l.mountpoint, Some("/mnt/m".into()));
        assert_eq!(l.state_dir, Some("/var/s".into()));
        assert_eq!(l.lock_lease_timeout, Some(HumanDuration::from_secs(60)));
        assert_eq!(
            l.heartbeat_interval,
            Some(HumanDuration(std::time::Duration::from_millis(250)))
        );
        assert_eq!(l.block_size, Some(ByteSize(1 << 20)));
        assert_eq!(l.log_level, Some(LogLevel::Warn));
        assert_eq!(l.cluster_name.as_deref(), Some("prod"));
        assert_eq!(l.debug, Some(true));
        assert_eq!(l.allow_other, Some(true));
        assert_eq!(l.allow_root, Some(false));
    }

    #[test]
    fn dashes_and_multiple_groups_and_last_wins() {
        let o = parse_mount_options(
            ["node-id=1,debug", "node_id=2,nodebug,block-size=4K"],
            Path::new("/"),
            false,
        )
        .unwrap();
        assert_eq!(o.layer.node_id, Some(2));
        assert_eq!(o.layer.debug, Some(false));
        assert_eq!(o.layer.block_size, Some(ByteSize(4096)));
    }

    #[test]
    fn generic_mount_options_ignored() {
        let o = parse("defaults,rw,noauto,nofail,_netdev,user,users,user=henk,x-systemd.automount,x-systemd.requires=network-online.target,comment=hi,,").unwrap();
        assert_eq!(o, MountOptions::default());
    }

    #[test]
    fn kernel_flags_passed_on() {
        let o = parse("nosuid,nodev,noexec,noatime,foreground").unwrap();
        assert_eq!(o.kernel_flags, vec!["nosuid", "nodev", "noexec", "noatime"]);
        assert_eq!(o.layer.foreground, Some(true));
    }

    #[test]
    fn sloppy_collects_unknown() {
        let o = parse_mount_options(["node_id=2,bogus,what=1"], Path::new("/"), true).unwrap();
        assert_eq!(o.layer.node_id, Some(2));
        assert_eq!(o.ignored_unknown, vec!["bogus", "what=1"]);
        // Sloppy does not hide invalid values of known options.
        assert!(parse_mount_options(["node_id=x"], Path::new("/"), true).is_err());
    }

    #[test]
    fn relative_paths_resolved_against_cwd() {
        let o = parse("backing_dir=data,mountpoint=./mnt,config=../c.yaml").unwrap();
        assert_eq!(o.layer.backing_dir, Some("/work/data".into()));
        assert_eq!(o.layer.mountpoint, Some("/work/mnt".into()));
        assert_eq!(o.config_file, Some("/c.yaml".into()));
    }

    #[test]
    fn errors() {
        assert_eq!(parse("bogus"), Err(OptionError::Unknown("bogus".into())));
        assert_eq!(
            parse("bogus=1"),
            Err(OptionError::Unknown("bogus=1".into()))
        );
        assert_eq!(
            parse("node_id"),
            Err(OptionError::MissingValue("node_id".into()))
        );
        assert!(matches!(
            parse("node_id=0x"),
            Err(OptionError::Invalid { .. })
        ));
        assert!(matches!(
            parse("debug=maybe"),
            Err(OptionError::Invalid { .. })
        ));
        assert!(matches!(parse("nodebug=1"), Err(OptionError::Unknown(_))));
        assert!(matches!(
            parse("peer=10.0.0.1:7420"),
            Err(OptionError::Invalid { .. })
        ));
        assert!(matches!(
            parse("listen=host:1"),
            Err(OptionError::Invalid { .. })
        ));
        assert!(matches!(
            parse("backing_dir="),
            Err(OptionError::Invalid { .. })
        ));
        assert!(matches!(parse("ro"), Err(OptionError::Unsupported(..))));
    }
}
