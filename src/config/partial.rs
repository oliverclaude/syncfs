//! A configuration layer: every field optional. The YAML file, the `-o`
//! mount options and the CLI each produce one of these, and they are stacked
//! with [`PartialConfig::overlay`].

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

use super::types::{LogLevel, Mode, NodeId, Peer};
use super::units::{ByteSize, HumanDuration};

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartialConfig {
    pub node_id: Option<NodeId>,
    pub mode: Option<Mode>,
    pub listen: Option<SocketAddr>,
    pub peers: Option<Vec<Peer>>,
    pub backing_dir: Option<PathBuf>,
    pub mountpoint: Option<PathBuf>,
    pub state_dir: Option<PathBuf>,
    pub lock_lease_timeout: Option<HumanDuration>,
    pub heartbeat_interval: Option<HumanDuration>,
    pub block_size: Option<ByteSize>,
    pub debug: Option<bool>,
    pub allow_other: Option<bool>,
    pub allow_root: Option<bool>,
    pub log_level: Option<LogLevel>,
}

impl PartialConfig {
    /// Returns `self` with every field that `upper` sets replaced by
    /// `upper`'s value. Lists (`peers`) are replaced as a whole, not merged.
    #[must_use]
    pub fn overlay(self, upper: PartialConfig) -> PartialConfig {
        PartialConfig {
            node_id: upper.node_id.or(self.node_id),
            mode: upper.mode.or(self.mode),
            listen: upper.listen.or(self.listen),
            peers: upper.peers.or(self.peers),
            backing_dir: upper.backing_dir.or(self.backing_dir),
            mountpoint: upper.mountpoint.or(self.mountpoint),
            state_dir: upper.state_dir.or(self.state_dir),
            lock_lease_timeout: upper.lock_lease_timeout.or(self.lock_lease_timeout),
            heartbeat_interval: upper.heartbeat_interval.or(self.heartbeat_interval),
            block_size: upper.block_size.or(self.block_size),
            debug: upper.debug.or(self.debug),
            allow_other: upper.allow_other.or(self.allow_other),
            allow_root: upper.allow_root.or(self.allow_root),
            log_level: upper.log_level.or(self.log_level),
        }
    }

    /// Parses a YAML config document. An empty document (or one that is
    /// only comments) is an empty layer.
    pub fn from_yaml(text: &str) -> Result<PartialConfig, serde_yaml_ng::Error> {
        let parsed: Option<PartialConfig> = serde_yaml_ng::from_str(text)?;
        Ok(parsed.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_yaml_is_empty_layer() {
        assert_eq!(
            PartialConfig::from_yaml("").unwrap(),
            PartialConfig::default()
        );
        assert_eq!(
            PartialConfig::from_yaml("# only a comment\n").unwrap(),
            PartialConfig::default()
        );
    }

    #[test]
    fn full_yaml_parses() {
        let y = r#"
node_id: 2
mode: mount
listen: "0.0.0.0:7421"
peers:
  - { id: 1, address: "10.0.0.1:7420" }
  - id: 2
    address: "10.0.0.2:7421"
  - id: 3
    address: "voter.lan:7420"
backing_dir: /srv/syncfs
mountpoint: /mnt/syncfs
state_dir: /var/lib/syncfs
lock_lease_timeout: 45s
heartbeat_interval: 500ms
block_size: 128K
debug: true
allow_other: true
allow_root: false
log_level: trace
"#;
        let p = PartialConfig::from_yaml(y).unwrap();
        assert_eq!(p.node_id, Some(2));
        assert_eq!(p.mode, Some(Mode::Mount));
        assert_eq!(p.listen, Some("0.0.0.0:7421".parse().unwrap()));
        let peers = p.peers.unwrap();
        assert_eq!(peers.len(), 3);
        assert_eq!(peers[2].address, "voter.lan:7420");
        assert_eq!(p.backing_dir, Some("/srv/syncfs".into()));
        assert_eq!(p.lock_lease_timeout, Some(HumanDuration::from_secs(45)));
        assert_eq!(
            p.heartbeat_interval,
            Some(HumanDuration(std::time::Duration::from_millis(500)))
        );
        assert_eq!(p.block_size, Some(ByteSize(128 * 1024)));
        assert_eq!(p.debug, Some(true));
        assert_eq!(p.allow_root, Some(false));
        assert_eq!(p.log_level, Some(LogLevel::Trace));
    }

    #[test]
    fn unknown_keys_rejected() {
        let err = PartialConfig::from_yaml("node_id: 1\nnodeid: 2\n").unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
        let err = PartialConfig::from_yaml("peers:\n  - {id: 1, address: \"a:1\", port: 3}\n")
            .unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    #[test]
    fn bad_values_rejected() {
        assert!(PartialConfig::from_yaml("mode: voter").is_err());
        assert!(PartialConfig::from_yaml("node_id: -1").is_err());
        assert!(PartialConfig::from_yaml("node_id: 70000").is_err());
        assert!(PartialConfig::from_yaml("listen: nowhere").is_err());
        assert!(PartialConfig::from_yaml("lock_lease_timeout: soon").is_err());
        assert!(PartialConfig::from_yaml("block_size: big").is_err());
        assert!(PartialConfig::from_yaml("- just\n- a list\n").is_err());
    }

    #[test]
    fn overlay_upper_wins_and_lists_replace() {
        let lower = PartialConfig {
            node_id: Some(1),
            debug: Some(true),
            peers: Some(vec!["1@a:1".parse().unwrap(), "2@b:1".parse().unwrap()]),
            ..Default::default()
        };
        let upper = PartialConfig {
            node_id: Some(5),
            peers: Some(vec!["5@c:1".parse().unwrap()]),
            ..Default::default()
        };
        let merged = lower.overlay(upper);
        assert_eq!(merged.node_id, Some(5));
        assert_eq!(merged.debug, Some(true));
        assert_eq!(merged.peers.unwrap().len(), 1);
    }
}
