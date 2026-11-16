//! Daemon mode: a voter-only node with no backing directory and no mount.
//! It takes part in membership (and, from step 5, in quorum and lock
//! votes).
//!
//! Having no `.syncfs` directory, it writes the same status as JSON to
//! `<state_dir>/status.json`, replaced atomically on every membership change
//! and at least every 5 heartbeat intervals, and removed on exit.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Context;
use signal_hook::iterator::Signals;

use crate::config::Config;
use crate::net::Network;
use crate::process;
use crate::status::{providers, Registry};

/// Where daemon mode publishes its status.
pub fn status_path(cfg: &Config) -> PathBuf {
    cfg.state_dir.join("status.json")
}

pub fn run(cfg: &Config) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.state_dir)
        .with_context(|| format!("creating state_dir {}", cfg.state_dir.display()))?;
    if cfg.peers.is_empty() {
        log::warn!("daemon mode without peers: a standalone voter has nothing to vote on");
    }
    let start = || -> anyhow::Result<(Network, Arc<Registry>)> {
        let net = Network::start(cfg).with_context(|| format!("listening on {}", cfg.listen))?;
        let status = Registry::new();
        let info = providers::NodeInfo {
            config: cfg.clone(),
            mount_options: Vec::new(),
            shadowed_backing_entry: false,
        };
        providers::register_builtin(&status, info, None, net.provider());
        log::info!("voter node {} running", cfg.node_id);
        Ok((net, status))
    };
    let path = status_path(cfg);
    let every = cfg.heartbeat_interval.get() * 5;
    process::run(cfg.foreground, start, move |(net, status), signals| {
        serve(&net, &status, &path, every, signals)
    })
}

fn write_status(status: &Registry, path: &Path) {
    let tmp = path.with_extension("json.tmp");
    let res = std::fs::write(&tmp, status.render_json()).and_then(|()| std::fs::rename(&tmp, path));
    if let Err(e) = res {
        log::warn!("writing {}: {e}", path.display());
    }
}

fn serve(
    net: &Network,
    status: &Arc<Registry>,
    path: &Path,
    every: std::time::Duration,
    mut signals: Signals,
) -> anyhow::Result<()> {
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (net, status, path, stop) = (
            net.clone(),
            Arc::clone(status),
            path.to_path_buf(),
            Arc::clone(&stop),
        );
        std::thread::Builder::new()
            .name("syncfs-status".into())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    write_status(&status, &path);
                    net.wait_change(every);
                }
            })?
    };
    if let Some(sig) = signals.forever().next() {
        log::info!("signal {sig}: shutting down");
    }
    stop.store(true, Ordering::SeqCst);
    net.shutdown();
    let _ = writer.join();
    let _ = std::fs::remove_file(path);
    log::info!("stopped");
    Ok(())
}
