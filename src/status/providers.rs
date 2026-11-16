//! The built-in status providers for `.syncfs`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use super::{FnProvider, Provider, Registry};
use crate::config::Config;

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn hostname() -> String {
    rustix::system::uname()
        .nodename()
        .to_string_lossy()
        .into_owned()
}

/// Static facts about this node and mount, captured at mount time.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub config: Config,
    /// Kernel/FUSE mount options in effect (e.g. `default_permissions`).
    pub mount_options: Vec<String>,
    /// The backing directory contains a real `.syncfs`, hidden by ours.
    pub shadowed_backing_entry: bool,
}

/// `node`: identity, version, uptime and mount details.
pub fn node(info: NodeInfo) -> Arc<dyn Provider> {
    let started = unix_now();
    let host = hostname();
    let pid = std::process::id();
    Arc::new(FnProvider::new("node", move || {
        let c = &info.config;
        json!({
            "node_id": c.node_id,
            "mode": c.mode,
            "version": env!("CARGO_PKG_VERSION"),
            "hostname": host,
            "pid": pid,
            "started_unix": started,
            "uptime_secs": unix_now().saturating_sub(started),
            "backing_dir": c.backing_dir,
            "mountpoint": c.mountpoint,
            "state_dir": c.state_dir,
            "listen": c.listen.to_string(),
            "debug": c.debug,
            "allow_other": c.allow_other,
            "allow_root": c.allow_root,
            "mount_options": info.mount_options,
            "shadowed_backing_entry": info.shadowed_backing_entry,
        })
    }))
}

/// `config`: the effective configuration.
pub fn config(config: Config) -> Arc<dyn Provider> {
    let v = serde_json::to_value(&config).unwrap_or(Value::Null);
    Arc::new(FnProvider::new("config", move || v.clone()))
}

/// `peers`: cluster membership. Connection state arrives in step 4; until
/// then every remote peer is `unknown`.
pub fn peers(config: Config) -> Arc<dyn Provider> {
    Arc::new(FnProvider::new("peers", move || {
        let peers: Vec<Value> = config
            .peers
            .iter()
            .map(|p| {
                let me = p.id == config.node_id;
                json!({
                    "id": p.id,
                    "address": p.address,
                    "self": me,
                    "state": if me { "up" } else { "unknown" },
                    "last_seen_secs_ago": Value::Null,
                })
            })
            .collect();
        json!({
            "implemented": false,
            "cluster_size": config.cluster_size(),
            "quorum": config.quorum(),
            "reachable": 1,
            "have_quorum": config.quorum() <= 1,
            "peers": peers,
        })
    }))
}

/// `locks`: the whole-file lock table (step 5).
pub fn locks(config: Config) -> Arc<dyn Provider> {
    Arc::new(FnProvider::new("locks", move || {
        json!({
            "implemented": false,
            "lease_timeout": config.lock_lease_timeout.to_string(),
            "held": 0,
            "locks": [],
        })
    }))
}

/// `replication`: dirty tracking and the journal queue (step 7).
pub fn replication(config: Config) -> Arc<dyn Provider> {
    Arc::new(FnProvider::new("replication", move || {
        json!({
            "implemented": false,
            "block_size": config.block_size.to_string(),
            "dirty_files": 0,
            "dirty_blocks": 0,
            "journal_queue": 0,
            "in_sync": true,
        })
    }))
}

/// Registers every built-in provider except `stats` (which the filesystem
/// registers itself, as it owns the gauges).
pub fn register_builtin(registry: &Registry, info: NodeInfo) {
    let cfg = info.config.clone();
    registry.register(node(info));
    registry.register(config(cfg.clone()));
    registry.register(peers(cfg.clone()));
    registry.register(locks(cfg.clone()));
    registry.register(replication(cfg));
}
