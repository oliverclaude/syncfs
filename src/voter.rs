//! Daemon mode: a voter-only node with no backing directory and no mount.
//! It takes part in membership and in lock votes (it counts towards every
//! quorum), but never takes a lock itself.
//!
//! It keeps the voter table of file versions in `<state_dir>/store.redb`.
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
use crate::lock::{LockManager, Settings};
use crate::net::Network;
use crate::process;
use crate::reload::{self, Registration, Reloader};
use crate::repl::store::{Flusher, Store};
use crate::repl::version;
use crate::status::{providers, FnProvider, Registry};

/// Where daemon mode publishes its status.
pub fn status_path(cfg: &Config) -> PathBuf {
    cfg.state_dir.join("status.json")
}

/// Runs the voter until SIGINT or SIGTERM; SIGHUP reloads the
/// configuration (see [`crate::reload`]).
pub fn run(cfg: &Config, base: reload::Base) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.state_dir)
        .with_context(|| format!("creating state_dir {}", cfg.state_dir.display()))?;
    if cfg.peers.is_empty() {
        log::warn!("daemon mode without peers: a standalone voter has nothing to vote on");
    }
    type Running = (
        Network,
        Arc<Registry>,
        LockManager,
        Arc<Flusher>,
        std::thread::JoinHandle<()>,
        Reloader,
    );
    let start = || -> anyhow::Result<Running> {
        let store = Store::open(&cfg.state_dir)
            .with_context(|| format!("opening the store in {}", cfg.state_dir.display()))?;
        let flusher = Flusher::new(Arc::new(store));
        let settings = Settings::from_config(cfg);
        let versions = version::persisted(settings.quorum(), &flusher)?;
        let flush_thread = flusher.start();
        let net = Network::start(cfg).with_context(|| format!("listening on {}", cfg.listen))?;
        let locks = LockManager::start_with_versions(settings, net.clone(), Arc::clone(&versions));
        let status = Registry::new();
        let info = providers::NodeInfo {
            config: cfg.clone(),
            mount_options: Vec::new(),
            shadowed_backing_entry: false,
        };
        let shown = providers::register_builtin(
            &status,
            info,
            None,
            net.provider(),
            Some(locks.provider()),
            Some(Arc::new(FnProvider::new("replication", move || {
                serde_json::json!({
                    "implemented": true,
                    "role": "voter-only (stores no data)",
                    "versions": versions.lock().unwrap_or_else(|e| e.into_inner()).summary(),
                })
            }))),
        );
        log::info!("voter node {} running", cfg.node_id);
        let reloader = Reloader::new(
            base,
            cfg.clone(),
            reload::Targets {
                net: Some(net.clone()),
                locks: Some(locks.clone()),
                repl: None,
                shown: Some(shown),
            },
        );
        Ok((net, status, locks, flusher, flush_thread, reloader))
    };
    let path = status_path(cfg);
    process::run(
        cfg.foreground,
        start,
        move |(net, status, locks, flusher, flush_thread, reloader), signals| {
            let registration = Registration::new(cfg);
            let res = serve(&net, &status, &path, reloader, signals);
            drop(registration);
            locks.shutdown();
            flusher.stop();
            let _ = flush_thread.join();
            let _ = flusher.flush_now();
            res
        },
    )
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
    mut reloader: Reloader,
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
                    // Every 5 heartbeats, as they are now (a reload may
                    // change them).
                    net.wait_change(net.heartbeat() * 5);
                }
            })?
    };
    for sig in signals.forever() {
        if sig == signal_hook::consts::SIGHUP {
            reloader.reload();
            continue;
        }
        log::info!("signal {sig}: shutting down");
        break;
    }
    stop.store(true, Ordering::SeqCst);
    net.shutdown();
    let _ = writer.join();
    let _ = std::fs::remove_file(path);
    log::info!("stopped");
    Ok(())
}
