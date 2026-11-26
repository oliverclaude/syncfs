//! The replication service of a mount node: local changes become dirty
//! marks and journal entries; peers' journals are applied here.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::config::{Config, LiveDuration, NodeId};
use crate::fs::{AttrChange, Mutation};
use crate::lock::LockManager;
use crate::net::wire::{Entry, FileId, Message, Version};
use crate::net::Network;

use super::backing::{attrs_of, Backing};
use super::dirty::Dirty;
use super::files::Index;
use super::journal::Journal;
use super::relay::{Inbox, RelayLog};
use super::store::{decode, encode, Batch, Flusher, Op, Table};

/// How often an open session's changes go out while it is still writing.
pub const INTERIM_EVERY: Duration = Duration::from_secs(1);

pub(crate) struct Session {
    pub version: Version,
    pub data: bool,
    pub meta: bool,
    pub last_sent: Instant,
    /// A remove session's newest known version of the file, which this
    /// node may not have fetched: the tombstone's lineage lists it.
    pub superseded: Option<Version>,
}

#[derive(Default, Serialize)]
pub(crate) struct Counters {
    pub entries_made: u64,
    pub entries_applied: u64,
    pub entries_skipped: u64,
    pub apply_errors: u64,
    pub blocks_sent: u64,
    pub blocks_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub fresh_checks: u64,
    pub fresh_pulls: u64,
    pub fresh_waits: u64,
    pub fresh_fallbacks: u64,
    pub suspects_resolved: u64,
    /// Entries applied that came through another node than their origin.
    pub entries_relayed: u64,
    /// Entries of other origins this node forwarded.
    pub entries_forwarded: u64,
    /// This node's diverged changes kept as conflict copies.
    pub conflict_copies: u64,
    /// After an unclean stop: unrecorded local paths sent to peers, and
    /// paths restored from a peer (see `reconcile`).
    pub reconcile_sent: u64,
    pub reconcile_restored: u64,
    /// Lock handoff: acquisitions that waited for a previous holder's
    /// entries, and those that went ahead without them because nobody
    /// connected had them (a timeout refuses instead: the lock counter
    /// `handoff_refused`).
    pub handoff_waits: u64,
    pub handoff_gaps: u64,
    /// Local changes not replicated because their file had been deleted
    /// meanwhile (written through a handle still open after an unlink or
    /// a rename over it): nobody else can see the file.
    pub deleted_changes: u64,
    /// Local changes that could not be recorded for any other reason (a
    /// file that still has links but no path, say): a bug, since peers
    /// never get the change. Logged as a warning.
    pub unrecorded_changes: u64,
}

pub(crate) struct State {
    pub index: Index,
    pub dirty: Dirty,
    pub journal: Journal,
    /// Per origin: last applied journal entry.
    pub applied: BTreeMap<NodeId, u64>,
    /// The same, as far as it is durable (what peers are told).
    pub applied_durable: BTreeMap<NodeId, u64>,
    pub applied_changed: bool,
    /// Other origins' entries kept for nodes that cannot reach them.
    pub relay: RelayLog,
    pub sessions: HashMap<FileId, Session>,
    /// Files whose changes may not be recorded durably yet.
    pub window: BTreeSet<FileId>,
    pub window_add: BTreeSet<FileId>,
    pub window_drop: BTreeSet<FileId>,
    /// After an unclean stop: files that were in the window.
    pub suspects: BTreeSet<FileId>,
    /// Session-final entries whose version peers confirm by acknowledging.
    pub awaiting: BTreeMap<u64, (FileId, Version)>,
    pub holders: HashMap<(FileId, Version), BTreeSet<NodeId>>,
    pub counters: Counters,
    /// When expired tombstones were last dropped.
    pub tombstones_expired: Option<Instant>,
    /// Files this node holds the write lock of in a session (counted per
    /// session). A peer's entry for one of them is older than the session
    /// and waits until it ends.
    pub locked_here: HashMap<FileId, usize>,
    /// A namespace change journaled and not yet confirmed (or compensated).
    pub inflight: Option<super::prepare::Inflight>,
    pub inflight_changed: bool,
}

const CLEAN_KEY: &[u8] = b"clean";
pub(crate) const LINKS_SCANNED_KEY: &[u8] = b"links_scanned";

fn applied_key(origin: NodeId) -> Vec<u8> {
    format!("applied.{origin}").into_bytes()
}

/// `peer` acknowledged this node's journal up to `seq`: trims the journal,
/// drops the marks of what it pulled, and, for every session-final entry
/// up to `seq` (in `awaiting`), the marks of that version and older it did
/// not pull (it has the final state of the version, from here or from a
/// relay). Returns those entries.
pub(crate) fn on_journal_ack(
    journal: &mut Journal,
    dirty: &mut Dirty,
    awaiting: &BTreeMap<u64, (FileId, Version)>,
    peer: NodeId,
    seq: u64,
) -> Vec<(u64, FileId, Version)> {
    journal.ack(peer, seq);
    dirty.acked(peer);
    let done: Vec<(u64, FileId, Version)> = awaiting
        .range(..=seq)
        .map(|(s, (id, v))| (*s, *id, *v))
        .collect();
    for (_, id, v) in &done {
        dirty.discard(peer, *id, *v);
    }
    done
}

/// The file and version a `Data`, `Attr` or tombstone `Remove` entry
/// carries.
pub(crate) fn versioned(e: &Entry) -> Option<(FileId, Version)> {
    match e {
        Entry::Data { id, version, .. } | Entry::Attr { id, version, .. } if version.seq > 0 => {
            Some((*id, *version))
        }
        Entry::Remove {
            id: Some(id),
            version: Some(version),
            ..
        } if version.seq > 0 => Some((*id, *version)),
        _ => None,
    }
}

pub fn path_bytes(p: &Path) -> Vec<u8> {
    p.as_os_str().as_bytes().to_vec()
}

pub(crate) fn join(parent: &[u8], name: &std::ffi::OsStr) -> Vec<u8> {
    let mut p = parent.to_vec();
    if !p.is_empty() {
        p.push(b'/');
    }
    p.extend_from_slice(name.as_bytes());
    p
}

pub struct Repl {
    pub(crate) me: NodeId,
    pub(crate) disk: u64,
    pub(crate) block_size: u64,
    pub(crate) peers: Vec<NodeId>,
    /// A reload can change it ([`Repl::set_timings`]).
    pub(crate) freshness_timeout: LiveDuration,
    pub(crate) backing: Backing,
    pub(crate) st: Mutex<State>,
    pub(crate) cv: Condvar,
    pub(crate) flusher: Arc<Flusher>,
    pub(crate) net: Network,
    pub(crate) locks: LockManager,
    /// Files being written by replication (catch-up or applying a peer's
    /// data); others wait.
    pub(crate) busy: Mutex<HashSet<FileId>>,
    pub(crate) busy_cv: Condvar,
    pub(crate) rpc: Mutex<HashMap<u64, std::sync::mpsc::Sender<Message>>>,
    pub(crate) next_req: AtomicU64,
    pub(crate) stop: AtomicBool,
    pub(crate) threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
    pub(crate) inbox: Mutex<HashMap<NodeId, Inbox>>,
    pub(crate) unclean_start: bool,
    pub(crate) started: Instant,
    /// Caught up after the start (or gave up waiting): see `rejoin`.
    pub(crate) rejoined: AtomicBool,
    /// Rebuilding from peers (see `rebuild`): no writes.
    pub(crate) rebuilding: AtomicBool,
    /// The namespace was compared with a peer after an unclean stop (or
    /// needs no comparison): see `reconcile`.
    pub(crate) reconciled: AtomicBool,
    /// Held while comparing; the rejoin gate takes it before opening.
    pub(crate) reconcile_lock: Mutex<()>,
    /// The connections the rejoin gate last passed with (see `rejoin`).
    pub(crate) gate: Mutex<super::rejoin::Gate>,
    /// A pause of this process longer than this (the peers' down timeout)
    /// makes changes wait (see `rejoin`).
    pub(crate) pause_limit: LiveDuration,
    pub(crate) rebuild: Mutex<super::rebuild::Rebuild>,
    this: std::sync::OnceLock<Weak<Repl>>,
    /// Drops the kernel's cached attributes and data of a backing file.
    invalidator: std::sync::OnceLock<Invalidator>,
    entry_invalidator: std::sync::OnceLock<EntryInvalidator>,
    inval_barrier: std::sync::OnceLock<InvalBarrier>,
    move_notifier: std::sync::OnceLock<MoveNotifier>,
    /// The mount's counters (`.syncfs/stats`).
    pub(crate) stats: std::sync::OnceLock<Arc<crate::status::Stats>>,
}

pub type Invalidator = Box<dyn Fn(&rustix::fs::Stat) + Send + Sync>;
/// Drops the kernel's cached directory entry `name` of the directory with
/// that stat.
pub type EntryInvalidator = Box<dyn Fn(&rustix::fs::Stat, &std::ffi::OsStr) + Send + Sync>;
/// Waits (at most the given time) until the inode invalidations queued so
/// far reached the kernel; false if they did not in time.
pub type InvalBarrier = Box<dyn Fn(std::time::Duration) -> bool + Send + Sync>;
/// Replication renamed the file with that stat (if known) away from `name`
/// in the directory with that stat.
pub type MoveNotifier =
    Box<dyn Fn(Option<&rustix::fs::Stat>, &rustix::fs::Stat, &std::ffi::OsStr) + Send + Sync>;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Holds a file against concurrent replication writes until dropped.
pub(crate) struct BusyGuard<'a> {
    repl: &'a Repl,
    id: FileId,
}

impl Drop for BusyGuard<'_> {
    fn drop(&mut self) {
        lock(&self.repl.busy).remove(&self.id);
        self.repl.busy_cv.notify_all();
    }
}

impl Repl {
    /// Loads the replication state from `flusher`'s store and starts the
    /// service.
    pub fn start(
        cfg: &Config,
        backing_dir: &Path,
        disk: u64,
        flusher: Arc<Flusher>,
        net: Network,
        locks: LockManager,
        rebuild: Option<super::rebuild::Cause>,
    ) -> std::io::Result<Arc<Repl>> {
        let store = Arc::clone(flusher.store());
        let peers: Vec<NodeId> = cfg
            .peers
            .iter()
            .map(|p| p.id)
            .filter(|&id| id != cfg.node_id)
            .collect();
        let mut index = Index::load(store.load(Table::Files)?)?;
        if store.get(Table::Meta, LINKS_SCANNED_KEY)?.is_none() {
            // Once per store: files that were hard-linked before syncfs saw
            // them become one file with one id (see `scan_hard_links`).
            let links = super::files::scan_hard_links(backing_dir)?;
            let n = links.iter().filter(|l| index.adopt_links(l)).count();
            if n > 0 {
                log::info!(
                    "replication: {n} pre-existing hard-linked file(s) adopted as one file each"
                );
            }
            let mut ops = index.take_changed();
            ops.push((Table::Meta, LINKS_SCANNED_KEY.to_vec(), Some(encode(&true))));
            store.commit(&ops)?;
        }
        let mut dirty = Dirty::new(peers.iter().copied());
        dirty.load(store.load(Table::Dirty)?)?;
        let positions = store.load(Table::Positions)?;
        let journal = Journal::load(
            peers.iter().copied(),
            store.load(Table::Journal)?,
            store.get(Table::Meta, b"journal.next")?,
            &positions,
        )?;
        let mut applied = BTreeMap::new();
        for &p in &peers {
            if let Some((_, v)) = positions.iter().find(|(k, _)| *k == applied_key(p)) {
                applied.insert(p, decode::<u64>(v)?);
            }
        }
        let relay = RelayLog::load(peers.iter().copied(), store.load(Table::Relay)?)?;
        // Session-final entries not acknowledged by everyone yet: every
        // session ended with the last process, so each version's last
        // entry is final.
        let mut awaiting = BTreeMap::new();
        let mut last_of: HashMap<(FileId, Version), u64> = HashMap::new();
        for (seq, rec) in journal.entries() {
            if let Some(k) = versioned(&rec.entry) {
                last_of.insert(k, seq);
            }
        }
        for ((id, v), seq) in last_of {
            awaiting.insert(seq, (id, v));
        }
        let window: BTreeSet<FileId> = store
            .load(Table::Window)?
            .into_iter()
            .filter_map(|(k, _)| <[u8; 16]>::try_from(k.as_slice()).ok().map(FileId))
            .collect();
        let clean = store
            .get(Table::Meta, CLEAN_KEY)?
            .map(|v| decode::<bool>(&v))
            .transpose()?
            // A new store has nothing to recover.
            .unwrap_or(true);
        if rebuild.is_some() {
            // The data the marks point at is gone; every file is compared.
            dirty.clear();
            let mut ops = dirty.take_changed();
            ops.extend(index.take_changed());
            store.commit(&ops)?;
        }
        let suspects = if clean || rebuild.is_some() {
            BTreeSet::new()
        } else {
            window.clone()
        };
        if !suspects.is_empty() {
            log::warn!(
                "replication: unclean stop; {} file(s) may have unrecorded changes and are \
                 compared with peers before they take writes",
                suspects.len()
            );
        }
        store.commit(&[(Table::Meta, CLEAN_KEY.to_vec(), Some(encode(&false)))])?;
        let repl = Arc::new(Repl {
            me: cfg.node_id,
            disk,
            block_size: cfg.block_size.0,
            peers,
            freshness_timeout: LiveDuration::new(cfg.freshness_timeout.get()),
            backing: Backing::open(backing_dir)?,
            st: Mutex::new(State {
                index,
                dirty,
                journal,
                applied_durable: applied.clone(),
                applied,
                applied_changed: false,
                relay,
                sessions: HashMap::new(),
                window,
                window_add: BTreeSet::new(),
                window_drop: BTreeSet::new(),
                suspects,
                awaiting,
                holders: HashMap::new(),
                counters: Counters::default(),
                tombstones_expired: None,
                locked_here: HashMap::new(),
                inflight: Repl::load_inflight(store.as_ref())?,
                inflight_changed: false,
            }),
            cv: Condvar::new(),
            flusher: Arc::clone(&flusher),
            net: net.clone(),
            locks,
            busy: Mutex::new(HashSet::new()),
            busy_cv: Condvar::new(),
            rpc: Mutex::new(HashMap::new()),
            next_req: AtomicU64::new(1),
            stop: AtomicBool::new(false),
            threads: Mutex::new(Vec::new()),
            inbox: Mutex::new(HashMap::new()),
            unclean_start: !clean,
            started: Instant::now(),
            rejoined: AtomicBool::new(cfg.peers.iter().all(|p| p.id == cfg.node_id)),
            rebuilding: AtomicBool::new(rebuild.is_some()),
            reconciled: AtomicBool::new(
                clean || rebuild.is_some() || cfg.peers.iter().all(|p| p.id == cfg.node_id),
            ),
            reconcile_lock: Mutex::new(()),
            gate: Mutex::new(Default::default()),
            pause_limit: LiveDuration::new(pause_limit(cfg.heartbeat_interval.get())),
            rebuild: Mutex::new(super::rebuild::Rebuild {
                cause: rebuild,
                marker: rebuild.map(|_| super::marker::Marker {
                    cluster: cfg.cluster_name.clone(),
                    node: u32::from(cfg.node_id),
                    disk,
                    rebuilding: true,
                }),
                ..Default::default()
            }),
            this: std::sync::OnceLock::new(),
            invalidator: std::sync::OnceLock::new(),
            stats: std::sync::OnceLock::new(),
            entry_invalidator: std::sync::OnceLock::new(),
            inval_barrier: std::sync::OnceLock::new(),
            move_notifier: std::sync::OnceLock::new(),
        });
        let _ = repl.this.set(Arc::downgrade(&repl));
        let weak = Arc::downgrade(&repl);
        flusher.add_source(Box::new(move || match weak.upgrade() {
            Some(r) => r.batch(),
            None => Batch::default(),
        }));
        super::net::start(&repl);
        {
            let weak = Arc::downgrade(&repl);
            let h = std::thread::Builder::new()
                .name("syncfs-ticker".into())
                .spawn(move || super::rejoin::ticker(weak))?;
            lock(&repl.threads).push(h);
        }
        if !repl.reconciled.load(Ordering::Relaxed) {
            let weak = Arc::downgrade(&repl);
            let h = std::thread::Builder::new()
                .name("syncfs-reconcile".into())
                .spawn(move || super::reconcile::reconcile_loop(weak))?;
            lock(&repl.threads).push(h);
        }
        if rebuild.is_some() {
            let weak = Arc::downgrade(&repl);
            let h = std::thread::Builder::new()
                .name("syncfs-rebuild".into())
                .spawn(move || super::rebuild::rebuild_loop(weak))?;
            lock(&repl.threads).push(h);
        }
        let (head, wait) = (Arc::downgrade(&repl), Arc::downgrade(&repl));
        repl.locks.set_chain(crate::lock::manager::Chain {
            head: Box::new(move || head.upgrade().map_or(0, |r| r.journal_head())),
            wait: Box::new(move |after| {
                wait.upgrade()
                    .map_or(crate::lock::manager::Applied::All, |r| {
                        r.wait_applied(after)
                    })
            }),
        });
        if repl.state().inflight.is_some() {
            repl.recover_inflight()?;
        }
        Ok(repl)
    }

    /// Sets how to wait for queued inode invalidations (once mounted).
    pub fn set_inval_barrier(&self, f: InvalBarrier) {
        let _ = self.inval_barrier.set(f);
    }

    /// A new lock session reads through the kernel's caches: they must not
    /// hold a copy older than what replication applied before the session
    /// started (the invalidations go through a thread of their own).
    pub fn wait_invalidated(&self) {
        if let Some(f) = self.inval_barrier.get() {
            if !f(self.freshness_timeout.get()) {
                log::warn!(
                    "replication: kernel cache invalidations still pending after {}",
                    humantime::format_duration(self.freshness_timeout.get())
                );
            }
        }
    }

    /// Sets how to drop the kernel's caches of a file (once mounted).
    pub fn set_invalidator(&self, f: Invalidator) {
        let _ = self.invalidator.set(f);
    }

    /// Sets how to drop the kernel's cached directory entries.
    pub fn set_entry_invalidator(&self, f: EntryInvalidator) {
        let _ = self.entry_invalidator.set(f);
    }

    /// Replication removed or renamed `path`: the kernel must look it up
    /// again.
    pub(crate) fn invalidate_entry(&self, path: &[u8]) {
        let Some(f) = self.entry_invalidator.get() else {
            return;
        };
        let (parent, name) = match path.iter().rposition(|&b| b == b'/') {
            Some(i) => (&path[..i], &path[i + 1..]),
            None => (&b""[..], path),
        };
        if let Ok(st) = self.backing.stat(parent) {
            f(&st, std::ffi::OsStr::from_bytes(name));
        }
    }

    /// Sets how to tell the mount which files replication renamed.
    pub fn set_move_notifier(&self, f: MoveNotifier) {
        let _ = self.move_notifier.set(f);
    }

    /// Replication renamed `file` (its stat from before) away from `path`.
    pub(crate) fn note_moved(&self, path: &[u8], file: Option<&rustix::fs::Stat>) {
        let Some(f) = self.move_notifier.get() else {
            return;
        };
        let (parent, name) = match path.iter().rposition(|&b| b == b'/') {
            Some(i) => (&path[..i], &path[i + 1..]),
            None => (&b""[..], path),
        };
        if let Ok(st) = self.backing.stat(parent) {
            f(file, &st, std::ffi::OsStr::from_bytes(name));
        }
    }

    /// Sets the mount's counters, where conflict copies are counted too.
    pub fn set_stats(&self, stats: Arc<crate::status::Stats>) {
        let _ = self.stats.set(stats);
    }

    /// Replication changed the file at `path`: the kernel must not serve
    /// its old size or data from cache.
    pub(crate) fn invalidate(&self, path: &[u8]) {
        if let (Some(f), Ok(st)) = (self.invalidator.get(), self.backing.stat(path)) {
            f(&st);
        }
    }

    pub(crate) fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.st)
    }

    pub(crate) fn stopping(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    pub(crate) fn hold(&self, id: FileId) -> BusyGuard<'_> {
        let mut b = lock(&self.busy);
        while b.contains(&id) {
            b = self.busy_cv.wait(b).unwrap_or_else(|e| e.into_inner());
        }
        b.insert(id);
        BusyGuard { repl: self, id }
    }

    /// The id of the file at `path` (adopting it if new to the index).
    pub fn id_for(&self, path: &[u8]) -> FileId {
        self.state().index.id_at(path)
    }

    pub fn local_version(&self, id: &FileId) -> Option<Version> {
        self.state().index.version(id)
    }

    /// Blocks covering bytes `[offset, offset + len)`.
    fn blocks(&self, offset: u64, len: u64) -> (u64, u64) {
        let end = offset.saturating_add(len);
        (offset / self.block_size, end.div_ceil(self.block_size))
    }

    /// A lock session on `id` starts here (before its freshness check).
    pub fn session_opened(&self, id: FileId) {
        *self.state().locked_here.entry(id).or_default() += 1;
    }

    /// The lock session on `id` ended.
    pub fn session_closed(&self, id: FileId) {
        let mut st = self.state();
        if let Some(n) = st.locked_here.get_mut(&id) {
            *n -= 1;
            if *n == 0 {
                st.locked_here.remove(&id);
            }
        }
        drop(st);
        self.cv.notify_all();
    }

    /// A session with `version` is about to change `id`. The first time,
    /// the file enters the durable window before anything is written.
    pub fn begin_change(&self, id: FileId, version: Version) -> std::io::Result<()> {
        let mut st = self.state();
        if let Some(s) = st.sessions.get(&id) {
            if s.version == version {
                return Ok(());
            }
        }
        st.sessions.insert(
            id,
            Session {
                version,
                data: false,
                // Whatever the change turns out to be, the version goes
                // out (peers' later versions must descend from it).
                meta: true,
                last_sent: Instant::now(),
                superseded: None,
            },
        );
        st.index.set_version(id, version);
        st.window_drop.remove(&id);
        let new = st.window.insert(id);
        if new {
            st.window_add.insert(id);
        }
        drop(st);
        if new {
            self.flusher.flush_now()?;
        }
        Ok(())
    }

    /// A session with `version` is about to remove `id`'s last link.
    /// Unlike [`begin_change`](Self::begin_change) the local copy does not
    /// take the version yet (only the tombstone does, once the remove is
    /// recorded), and the session sends nothing if the remove fails.
    /// `superseded`: the newest version the lock knows of, which the
    /// tombstone descends from without this node fetching it.
    pub fn begin_remove(
        &self,
        id: FileId,
        version: Version,
        superseded: Option<Version>,
    ) -> std::io::Result<()> {
        let mut st = self.state();
        if st.sessions.contains_key(&id) {
            return Ok(());
        }
        st.sessions.insert(
            id,
            Session {
                version,
                data: false,
                meta: false,
                last_sent: Instant::now(),
                superseded,
            },
        );
        st.window_drop.remove(&id);
        let new = st.window.insert(id);
        if new {
            st.window_add.insert(id);
        }
        drop(st);
        if new {
            self.flusher.flush_now()?;
        }
        Ok(())
    }

    /// Whether a session on `id` is still open (a recorded remove ends it).
    pub fn in_session(&self, id: &FileId) -> bool {
        self.state().sessions.contains_key(id)
    }

    fn deps(&self, st: &State) -> Vec<(NodeId, u64)> {
        st.applied
            .iter()
            .filter(|(_, s)| **s > 0)
            .map(|(o, s)| (*o, *s))
            .collect()
    }

    pub(crate) fn append(&self, st: &mut State, entry: Entry) -> u64 {
        let deps = self.deps(st);
        st.counters.entries_made += 1;
        let key = versioned(&entry);
        let seq = st.journal.append(deps, entry);
        if let Some(k) = key {
            // A later entry of an ended session's version (a suspect
            // resent after a restart): it is the final one now.
            let earlier: Vec<u64> = st
                .awaiting
                .iter()
                .filter(|(_, x)| **x == k)
                .map(|(s, _)| *s)
                .collect();
            if !earlier.is_empty() {
                for s in earlier {
                    st.awaiting.remove(&s);
                }
                st.awaiting.insert(seq, k);
            }
        }
        seq
    }

    /// Records a change that was applied locally.
    pub fn record(&self, op: &Mutation<'_>) {
        let Err(e) = self.try_record(op) else {
            return;
        };
        // A file deleted while still open has no path, and nothing to
        // replicate: no other node can reach it any more.
        if e.kind() == std::io::ErrorKind::NotFound && op.nodes().iter().any(|n| n.deleted()) {
            log::debug!("replication: not recording {op:?}: the file was deleted");
            self.state().counters.deleted_changes += 1;
            return;
        }
        log::warn!("replication: cannot record {op:?}: {e}");
        self.state().counters.unrecorded_changes += 1;
    }

    /// If `path` is the last link of a live file, bury it and append a
    /// Remove/tombstone so a peer applying an earlier Data for that
    /// FileId can skip instead of stalling when the bytes are gone
    /// (issue #2). Other links stay; `insert` / `rename` drop only this
    /// path. No-op when the path is free or is one of several links.
    pub(crate) fn displace_last_link(&self, st: &mut State, path: &[u8]) {
        let Some(id) = st.index.get(path) else {
            return;
        };
        let Some(rec) = st.index.rec(&id) else {
            return;
        };
        if rec.paths.len() != 1 {
            return;
        }
        let version = st.sessions.get(&id).map(|s| s.version).or(rec.version);
        let superseded = st.sessions.get(&id).and_then(|s| s.superseded);
        if let Some(version) = version {
            st.index.bury(id, version);
            if let Some(v) = superseded.filter(|v| *v < version) {
                st.index.note_lineage(id, v);
            }
            st.dirty.forget(id);
            st.sessions.remove(&id);
            st.window_drop.insert(id);
            let lineage = st.index.lineage(&id);
            let seq = self.append(
                st,
                Entry::Remove {
                    id: Some(id),
                    path: path.to_vec(),
                    dir: false,
                    version: Some(version),
                    lineage,
                },
            );
            st.awaiting.insert(seq, (id, version));
            st.holders.entry((id, version)).or_default().insert(self.me);
        } else {
            let id = st.index.remove_path(path).unwrap_or(id);
            st.dirty.forget(id);
            st.sessions.remove(&id);
            self.append(
                st,
                Entry::Remove {
                    id: Some(id),
                    path: path.to_vec(),
                    dir: false,
                    version: None,
                    lineage: Vec::new(),
                },
            );
        }
    }

    fn try_record(&self, op: &Mutation<'_>) -> std::io::Result<()> {
        match *op {
            Mutation::Create {
                parent,
                name,
                kind,
                mode,
                uid,
                gid,
                rdev,
                target,
            } => {
                // Namespace creates go through `prepare` (journal first).
                // Kept for any leftover record() caller.
                let path = join(&path_bytes(&parent.path()?), name);
                self.prepare_create(
                    path,
                    super::prepare::node_kind(kind),
                    super::prepare::intended_attrs(mode, uid, gid),
                    rdev,
                    target,
                )?;
            }
            Mutation::Link {
                node,
                new_parent,
                new_name,
            } => {
                let path = join(&path_bytes(&new_parent.path()?), new_name);
                let mut s = self.state();
                // The node's other path: any indexed one.
                let existing = path_bytes(&node.path()?);
                let existing = if existing == path {
                    // readlink may name the new link already.
                    let id = s.index.get(&path);
                    match id.and_then(|i| {
                        s.index
                            .rec(&i)
                            .and_then(|r| r.paths.iter().find(|p| **p != path).cloned())
                    }) {
                        Some(p) => p,
                        None => existing,
                    }
                } else {
                    existing
                };
                let id = s.index.id_at(&existing);
                s.index.insert(&path, id);
                self.append(&mut s, Entry::Link { id, existing, path });
            }
            Mutation::Remove {
                parent,
                name,
                is_dir,
            } => {
                let path = join(&path_bytes(&parent.path()?), name);
                let mut s = self.state();
                // The last link of a regular file, removed in a session:
                // a tombstone at the session's version.
                let buried = s.index.get(&path).and_then(|id| {
                    let last = s.index.rec(&id).is_some_and(|r| r.paths.len() == 1);
                    let x = s.sessions.get(&id)?;
                    (last && !is_dir).then_some((id, x.version, x.superseded))
                });
                if let Some((id, version, superseded)) = buried {
                    s.index.bury(id, version);
                    if let Some(v) = superseded.filter(|v| *v < version) {
                        s.index.note_lineage(id, v);
                    }
                    s.dirty.forget(id);
                    s.sessions.remove(&id);
                    s.window_drop.insert(id);
                    let lineage = s.index.lineage(&id);
                    let seq = self.append(
                        &mut s,
                        Entry::Remove {
                            id: Some(id),
                            path,
                            dir: false,
                            version: Some(version),
                            lineage,
                        },
                    );
                    s.awaiting.insert(seq, (id, version));
                    s.holders.entry((id, version)).or_default().insert(self.me);
                    drop(s);
                    self.cv.notify_all();
                    return Ok(());
                }
                let id = s.index.remove_path(&path);
                if let Some(id) = id {
                    if s.index.rec(&id).is_none() {
                        s.dirty.forget(id);
                        s.sessions.remove(&id);
                    }
                }
                self.append(
                    &mut s,
                    Entry::Remove {
                        id,
                        path,
                        dir: is_dir,
                        version: None,
                        lineage: Vec::new(),
                    },
                );
            }
            Mutation::Rename {
                parent,
                name,
                new_parent,
                new_name,
                flags,
            } => {
                let from = join(&path_bytes(&parent.path()?), name);
                let to = join(&path_bytes(&new_parent.path()?), new_name);
                let mut s = self.state();
                let id = s.index.get(&from);
                let replaced = s.index.get(&to);
                if flags & libc::RENAME_EXCHANGE != 0 {
                    s.index.exchange(&from, &to);
                } else {
                    // POSIX unlinks the destination first. A last link
                    // needs a tombstone in the journal before the Rename
                    // so a peer can skip earlier Data for that FileId.
                    // RENAME_EXCHANGE swaps two paths; nothing is dropped.
                    if replaced.filter(|r| Some(*r) != id).is_some() {
                        self.displace_last_link(&mut s, &to);
                    }
                    s.index.rename(&from, &to);
                    if let Some(r) = replaced.filter(|r| Some(*r) != id) {
                        if s.index.rec(&r).is_none() {
                            s.dirty.forget(r);
                            s.sessions.remove(&r);
                        }
                    }
                }
                self.append(
                    &mut s,
                    Entry::Rename {
                        id,
                        from,
                        to,
                        flags,
                    },
                );
                drop(s);
                self.cv.notify_all();
            }
            Mutation::SetAttr { node, change } => {
                let path = path_bytes(&node.path()?);
                let mut s = self.state();
                let id = s.index.id_at(&path);
                let AttrChange {
                    mode,
                    uid,
                    gid,
                    size,
                    times,
                } = change;
                if let Some(size) = size {
                    let blocks = size.div_ceil(self.block_size);
                    s.dirty.truncate(id, blocks);
                    if let Some(sess) = s.sessions.get_mut(&id) {
                        sess.data = true;
                    }
                }
                if mode.is_some() || uid.is_some() || gid.is_some() || times {
                    if let Some(sess) = s.sessions.get_mut(&id) {
                        sess.meta = true;
                    } else {
                        // Not a versioned session (a directory, symlink or
                        // special file): the attributes go out now.
                        drop(s);
                        let stat = node.stat()?;
                        let mut s = self.state();
                        let version = s.index.version(&id).unwrap_or(Version {
                            seq: 0,
                            node: self.me,
                            disk: self.disk,
                        });
                        let lineage = s.index.lineage(&id);
                        self.append(
                            &mut s,
                            Entry::Attr {
                                id,
                                path,
                                version,
                                attrs: attrs_of(&stat),
                                lineage,
                            },
                        );
                    }
                }
            }
            Mutation::Write { node, offset, len }
            | Mutation::Allocate {
                node, offset, len, ..
            } => {
                let path = path_bytes(&node.path()?);
                let (a, b) = self.blocks(offset, len.max(1));
                let mut s = self.state();
                let id = s.index.id_at(&path);
                let Some(version) = s.sessions.get(&id).map(|x| x.version) else {
                    log::debug!("replication: write to {id} outside a session (unlinked?)");
                    return Ok(());
                };
                s.dirty.mark(id, version, a, b);
                if let Some(sess) = s.sessions.get_mut(&id) {
                    sess.data = true;
                }
            }
            Mutation::Xattr { node, name, .. } => {
                let path = path_bytes(&node.path()?);
                let value = node.xattr(name)?;
                let mut s = self.state();
                let id = s.index.id_at(&path);
                if let Some(sess) = s.sessions.get_mut(&id) {
                    // The session's version goes out (with an `Attr`
                    // entry), so peers' later versions descend from it.
                    sess.meta = true;
                }
                self.append(
                    &mut s,
                    Entry::Xattr {
                        id,
                        path,
                        name: name.as_bytes().to_vec(),
                        value,
                    },
                );
            }
        }
        self.cv.notify_all();
        Ok(())
    }

    /// The journal entry describing `id`'s session changes so far.
    fn session_entry(&self, st: &State, id: FileId) -> Option<Entry> {
        let sess = st.sessions.get(&id)?;
        if !sess.data && !sess.meta {
            return None;
        }
        let path = st.index.path_of(&id)?.to_vec();
        let stat = self.backing.stat(&path).ok()?;
        let attrs = attrs_of(&stat);
        let lineage = st.index.lineage(&id);
        Some(if sess.data {
            Entry::Data {
                id,
                path,
                version: sess.version,
                size: stat.st_size as u64,
                attrs,
                lineage,
            }
        } else {
            Entry::Attr {
                id,
                path,
                version: sess.version,
                attrs,
                lineage,
            }
        })
    }

    /// The lock session on `id` ends: its changes go into the journal.
    pub fn end_session(&self, id: FileId) {
        let mut st = self.state();
        if let Some(entry) = self.session_entry(&st, id) {
            let version = st.sessions[&id].version;
            let seq = self.append(&mut st, entry);
            st.awaiting.insert(seq, (id, version));
            st.holders.entry((id, version)).or_default().insert(self.me);
        }
        if st.sessions.remove(&id).is_some() {
            st.window_drop.insert(id);
        }
        drop(st);
        self.cv.notify_all();
    }

    /// Changes for the next flush.
    fn batch(&self) -> Batch {
        let mut st = self.state();
        // Long sessions: send what they changed so far now and then.
        let now = Instant::now();
        let due: Vec<FileId> = st
            .sessions
            .iter()
            .filter(|(_, s)| s.data && now.duration_since(s.last_sent) >= INTERIM_EVERY)
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            if let Some(e) = self.session_entry(&st, id) {
                self.append(&mut st, e);
            }
            if let Some(s) = st.sessions.get_mut(&id) {
                s.last_sent = now;
                s.data = false;
                s.meta = false;
            }
        }
        if st
            .tombstones_expired
            .is_none_or(|t| now.duration_since(t) >= Duration::from_secs(3600))
        {
            st.tombstones_expired = Some(now);
            let n = st.index.expire_tombstones();
            if n > 0 {
                log::debug!("replication: dropped {n} expired tombstone(s)");
            }
        }
        let mut ops: Vec<Op> = st.index.take_changed();
        ops.extend(st.dirty.take_changed());
        let (jops, upto) = st.journal.take_changed();
        ops.extend(jops);
        ops.extend(st.relay.take_changed());
        if std::mem::take(&mut st.applied_changed) {
            for (o, s) in &st.applied {
                ops.push((Table::Positions, applied_key(*o), Some(encode(s))));
            }
        }
        for id in std::mem::take(&mut st.window_add) {
            ops.push((Table::Window, id.0.to_vec(), Some(Vec::new())));
        }
        let leaving: Vec<FileId> = std::mem::take(&mut st.window_drop)
            .into_iter()
            .filter(|id| !st.sessions.contains_key(id) && !st.suspects.contains(id))
            .collect();
        for id in leaving {
            st.window.remove(&id);
            ops.push((Table::Window, id.0.to_vec(), None));
        }
        if std::mem::take(&mut st.inflight_changed) {
            ops.push((
                Table::Meta,
                super::prepare::INFLIGHT_KEY.to_vec(),
                st.inflight.as_ref().map(encode),
            ));
        }
        drop(st);
        let weak: Weak<Repl> = self.this.get().cloned().unwrap_or_default();
        Batch {
            ops,
            after: vec![Box::new(move || {
                if let Some(r) = weak.upgrade() {
                    r.state().journal.committed(upto);
                    r.cv.notify_all();
                }
            })],
        }
    }

    /// Applies new timings from a configuration reload: waits that start
    /// from now on use the new `freshness_timeout`, and a gap in the
    /// replication tick counts as a pause after the new number of
    /// heartbeats. Waits already running keep their deadline.
    pub fn set_timings(&self, freshness_timeout: Duration, heartbeat: Duration) {
        self.freshness_timeout.set(freshness_timeout);
        self.pause_limit.set(pause_limit(heartbeat));
    }

    /// Stops the service after a final flush, marking a clean stop.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.cv.notify_all();
        let threads = std::mem::take(&mut *lock(&self.threads));
        for t in threads {
            let _ = t.join();
        }
        let ids: Vec<FileId> = self.state().sessions.keys().copied().collect();
        for id in ids {
            self.end_session(id);
        }
        let _ = self.flusher.flush_now();
        let st = self.state();
        // Suspects not compared yet stay suspect (unclean, window kept).
        let mut ops: Vec<Op> = Vec::new();
        if st.suspects.is_empty() {
            ops.extend(
                st.window
                    .iter()
                    .map(|id| (Table::Window, id.0.to_vec(), None)),
            );
            ops.push((Table::Meta, CLEAN_KEY.to_vec(), Some(encode(&true))));
        }
        drop(st);
        if let Err(e) = self.flusher.store().commit(&ops) {
            log::error!("replication: final commit failed: {e}");
        }
    }

    /// The `replication` status provider.
    pub fn provider(self: &Arc<Self>) -> Arc<dyn crate::status::Provider> {
        let weak = Arc::downgrade(self);
        Arc::new(crate::status::FnProvider::new(
            "replication",
            move || match weak.upgrade() {
                Some(r) => r.render(),
                None => serde_json::json!({"implemented": true, "running": false}),
            },
        ))
    }

    fn render(&self) -> serde_json::Value {
        let rebuild = self.rebuild_status();
        let reconnect_wait = lock(&self.gate).waiting();
        let snap = self.net.snapshot();
        let st = self.state();
        let blocks = st.dirty.blocks_per_peer();
        let lag = st.journal.lag();
        let peers: Vec<_> = self
            .peers
            .iter()
            .map(|p| {
                let role = snap.peers.iter().find(|x| x.id == *p).and_then(|x| x.role);
                serde_json::json!({
                    "id": p,
                    "stores_data": role.map(|r| r.data),
                    "disk": role.map(|r| format!("{:016x}", r.disk)),
                    "journal_unacked": lag.get(p).copied().unwrap_or(0),
                    "dirty_blocks": blocks.get(p).copied().unwrap_or(0),
                    "applied_from": st.applied.get(p).copied().unwrap_or(0),
                })
            })
            .collect();
        let dirty_blocks: u64 = blocks.values().copied().max().unwrap_or(0);
        let lagging = lag.values().any(|l| *l > 0);
        let versions = self.locks.versions();
        let versions = lock(&versions).summary();
        serde_json::json!({
            "implemented": true,
            "disk": format!("{:016x}", self.disk),
            "block_size": self.block_size,
            "files_indexed": st.index.len(),
            "dirty_files": st.dirty.files(),
            "dirty_blocks": dirty_blocks,
            "dirty_bytes_max": dirty_blocks.saturating_mul(self.block_size),
            "blocks_in_flight": st.dirty.inflight_blocks(),
            "journal_last": st.journal.last(),
            "journal_durable": st.journal.durable(),
            "journal_queue": st.journal.queued(),
            "open_sessions": st.sessions.len(),
            "sessions": st
                .sessions
                .iter()
                .take(20)
                .map(|(id, s)| serde_json::json!({
                    "file": id.to_string(),
                    "path": st.index.path_of(id).map(|p| String::from_utf8_lossy(p).into_owned()),
                    "version": s.version.seq,
                    "since_sent_ms": s.last_sent.elapsed().as_millis() as u64,
                }))
                .collect::<Vec<_>>(),
            "relay_entries": st.relay.len(),
            "window": st.window.len(),
            "suspects": st.suspects.len(),
            "unclean_start": self.unclean_start,
            "rejoining": !self.rejoined.load(Ordering::Relaxed) || reconnect_wait,
            "rebuild": rebuild,
            "in_sync": st.dirty.files() == 0 && !lagging && st.sessions.is_empty(),
            "freshness_timeout": humantime::format_duration(self.freshness_timeout.get()).to_string(),
            "store_commits": self.flusher.store().commits(),
            "store_failures": self.flusher.failures(),
            "versions": versions,
            "peers": peers,
            "counters": serde_json::to_value(&st.counters).unwrap_or_default(),
        })
    }

    /// Nodes known to hold `version` of `id` completely changed: tell the
    /// voters.
    pub(crate) fn announce_holders(&self, id: FileId, version: Version, holders: Vec<NodeId>) {
        self.locks.broadcast_versions(Message::VersionHolders {
            file: id,
            version,
            holders,
        });
    }
}

/// A gap in the replication tick longer than this means the process was
/// paused long enough for its peers to call it down.
fn pause_limit(heartbeat: Duration) -> Duration {
    heartbeat * crate::net::membership::DOWN_AFTER_HEARTBEATS
}
