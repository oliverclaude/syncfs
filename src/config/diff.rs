//! What a configuration reload may change: the difference between the
//! running configuration and a newly loaded one, setting by setting.

use std::fmt;

use super::Config;

/// How a reload treats a change to one setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Applied on this node at once.
    Live,
    /// Applied at once, but every node of the cluster relies on it: it
    /// should be changed to the same value on all of them.
    ClusterWide,
    /// Fixed for the life of the process (the node's identity, or set when
    /// the kernel mount was made): a reload that changes it is refused.
    Refused,
}

/// Classifies a setting by its name in [`Config`]. Settings not listed
/// here are refused, so that a new setting is never changed live by
/// accident.
pub fn classify(setting: &str) -> Class {
    match setting {
        "log_level" | "lock_wait" | "freshness_timeout" => Class::Live,
        "lock_lease_timeout" | "heartbeat_interval" => Class::ClusterWide,
        // cluster_name, node_id, mode, listen, peers, backing_dir,
        // mountpoint, state_dir, block_size, allow_other, allow_root,
        // debug (FUSE debug output and foreground), foreground (whether
        // the process daemonized): fixed.
        _ => Class::Refused,
    }
}

/// One setting whose value differs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub setting: String,
    pub old: String,
    pub new: String,
    pub class: Class,
}

impl fmt::Display for Change {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} -> {}", self.setting, self.old, self.new)
    }
}

/// The result of comparing the running configuration with a new one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Diff {
    pub changes: Vec<Change>,
}

impl Diff {
    /// Every setting whose value differs, in [`Config`] field order.
    pub fn between(old: &Config, new: &Config) -> Diff {
        let (Ok(serde_json::Value::Object(a)), Ok(serde_json::Value::Object(b))) =
            (serde_json::to_value(old), serde_json::to_value(new))
        else {
            unreachable!("Config serializes to a JSON object");
        };
        let changes = a
            .iter()
            .filter_map(|(k, va)| {
                let vb = b.get(k).unwrap_or(&serde_json::Value::Null);
                (va != vb).then(|| Change {
                    setting: k.clone(),
                    old: show(va),
                    new: show(vb),
                    class: classify(k),
                })
            })
            .collect();
        Diff { changes }
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Changes that make the reload be refused as a whole.
    pub fn refused(&self) -> impl Iterator<Item = &Change> {
        self.changes.iter().filter(|c| c.class == Class::Refused)
    }

    pub fn is_allowed(&self) -> bool {
        self.refused().next().is_none()
    }
}

fn show(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "(none)".into(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{resolve, Environment, LogLevel, PartialConfig, Peer};

    fn base() -> Config {
        let p = PartialConfig {
            node_id: Some(1),
            peers: Some(vec![
                Peer {
                    id: 1,
                    address: "10.0.0.1:7420".into(),
                },
                Peer {
                    id: 2,
                    address: "10.0.0.2:7420".into(),
                },
                Peer {
                    id: 3,
                    address: "10.0.0.3:7420".into(),
                },
            ]),
            backing_dir: Some("/srv/b".into()),
            mountpoint: Some("/mnt/m".into()),
            ..Default::default()
        };
        let env = Environment {
            is_root: true,
            home: None,
            xdg_state_home: None,
            cwd: "/".into(),
        };
        resolve(p, &env).unwrap().config
    }

    #[test]
    fn same_config_is_no_change() {
        let d = Diff::between(&base(), &base());
        assert!(d.is_empty() && d.is_allowed());
    }

    #[test]
    fn live_and_cluster_wide_changes_are_allowed() {
        let old = base();
        let mut new = old.clone();
        new.log_level = LogLevel::Debug;
        new.lock_wait = "20s".parse().unwrap();
        new.freshness_timeout = "1m".parse().unwrap();
        new.lock_lease_timeout = "45s".parse().unwrap();
        new.heartbeat_interval = "2s".parse().unwrap();
        let d = Diff::between(&old, &new);
        assert!(d.is_allowed());
        let got: Vec<(String, Class)> =
            d.changes.iter().map(|c| (c.to_string(), c.class)).collect();
        assert_eq!(
            got,
            [
                (
                    "lock_lease_timeout: 30s -> 45s".to_string(),
                    Class::ClusterWide
                ),
                ("lock_wait: 10s -> 20s".to_string(), Class::Live),
                ("freshness_timeout: 30s -> 1m".to_string(), Class::Live),
                (
                    "heartbeat_interval: 1s -> 2s".to_string(),
                    Class::ClusterWide
                ),
                ("log_level: info -> debug".to_string(), Class::Live),
            ]
        );
    }

    #[test]
    fn any_refused_change_refuses_the_reload() {
        let old = base();
        let mut new = old.clone();
        new.lock_wait = "20s".parse().unwrap();
        new.cluster_name = "other".into();
        new.peers.pop();
        let d = Diff::between(&old, &new);
        assert!(!d.is_allowed());
        let refused: Vec<&str> = d.refused().map(|c| c.setting.as_str()).collect();
        assert_eq!(refused, ["cluster_name", "peers"]);
        assert_eq!(d.changes.len(), 3);
    }

    /// Every setting has a deliberate class; this list must change with
    /// [`Config`].
    #[test]
    fn every_setting_is_classified() {
        let serde_json::Value::Object(fields) = serde_json::to_value(base()).unwrap() else {
            panic!()
        };
        let names: Vec<&str> = fields.keys().map(String::as_str).collect();
        let refused = [
            "cluster_name",
            "node_id",
            "mode",
            "listen",
            "peers",
            "backing_dir",
            "mountpoint",
            "state_dir",
            "block_size",
            "debug",
            "allow_other",
            "allow_root",
            "foreground",
        ];
        let live = ["lock_wait", "freshness_timeout", "log_level"];
        let cluster = ["lock_lease_timeout", "heartbeat_interval"];
        let mut all: Vec<&str> = refused
            .iter()
            .chain(&live)
            .chain(&cluster)
            .copied()
            .collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        all.sort_unstable();
        assert_eq!(sorted, all, "Config fields changed: classify the new ones");
        for n in refused {
            assert_eq!(classify(n), Class::Refused, "{n}");
        }
        for n in live {
            assert_eq!(classify(n), Class::Live, "{n}");
        }
        for n in cluster {
            assert_eq!(classify(n), Class::ClusterWide, "{n}");
        }
    }
}
