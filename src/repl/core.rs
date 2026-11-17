//! The replication service of a mount node: local changes become dirty
//! marks and journal entries; peers' journals are applied here.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::config::{Config, NodeId};
use crate::fs::{AttrChange, Mutation};
use crate::lock::LockManager;
use crate::net::wire::{Entry, FileId, Message, Version};
use crate::net::Network;

use super::backing::{attrs_of, kind_of, Backing};
use super::dirty::Dirty;
use super::files::Index;
use super::journal::Journal;
use super::store::{decode, encode, Batch, Flusher, Op, Table};

/// How often an open session's changes go out while it is still writing.
pub const INTERIM_EVERY: Duration = Duration::from_secs(1);

pub(crate) struct Session {
    pub version: Version,
    pub data: bool,
    pub meta: bool,
    pub last_sent: Instant,
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
}

pub(crate) struct State {
    pub index: Index,
    pub dirty: Dirty,
    pub journal: Journal,
    /// Per origin: last applied journal entry.
    pub applied: BTreeMap<NodeId, u64>,
    pub applied_changed: bool,
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
}

const CLEAN_KEY: &[u8] = b"clean";

fn applied_key(origin: NodeId) -> Vec<u8> {
    format!("applied.{origin}").into_bytes()
}

pub fn path_bytes(p: &Path) -> Vec<u8> {
    p.as_os_str().as_bytes().to_vec()
}

fn join(parent: &[u8], name: &std::ffi::OsStr) -> Vec<u8> {
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
    pub(crate) freshness_timeout: Duration,
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
    pub(crate) inbox: Mutex<HashMap<NodeId, super::net::Inbox>>,
    pub(crate) unclean_start: bool,
    this: std::sync::OnceLock<Weak<Repl>>,
    /// Drops the kernel's cached attributes and data of a backing file.
    invalidator: std::sync::OnceLock<Invalidator>,
}

pub type Invalidator = Box<dyn Fn(&rustix::fs::Stat) + Send + Sync>;

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
    ) -> std::io::Result<Arc<Repl>> {
        let store = Arc::clone(flusher.store());
        let peers: Vec<NodeId> = cfg
            .peers
            .iter()
            .map(|p| p.id)
            .filter(|&id| id != cfg.node_id)
            .collect();
        let index = Index::load(store.load(Table::Files)?)?;
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
        let suspects = if clean {
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
            freshness_timeout: cfg.freshness_timeout.get(),
            backing: Backing::open(backing_dir)?,
            st: Mutex::new(State {
                index,
                dirty,
                journal,
                applied,
                applied_changed: false,
                sessions: HashMap::new(),
                window,
                window_add: BTreeSet::new(),
                window_drop: BTreeSet::new(),
                suspects,
                awaiting: BTreeMap::new(),
                holders: HashMap::new(),
                counters: Counters::default(),
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
            this: std::sync::OnceLock::new(),
            invalidator: std::sync::OnceLock::new(),
        });
        let _ = repl.this.set(Arc::downgrade(&repl));
        let weak = Arc::downgrade(&repl);
        flusher.add_source(Box::new(move || match weak.upgrade() {
            Some(r) => r.batch(),
            None => Batch::default(),
        }));
        super::net::start(&repl);
        Ok(repl)
    }

    /// Sets how to drop the kernel's caches of a file (once mounted).
    pub fn set_invalidator(&self, f: Invalidator) {
        let _ = self.invalidator.set(f);
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
                meta: false,
                last_sent: Instant::now(),
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
        st.journal.append(deps, entry)
    }

    /// Records a change that was applied locally.
    pub fn record(&self, op: &Mutation<'_>) {
        if let Err(e) = self.try_record(op) {
            log::warn!("replication: cannot record {op:?}: {e}");
        }
    }

    fn try_record(&self, op: &Mutation<'_>) -> std::io::Result<()> {
        match *op {
            Mutation::Create { parent, name, .. } => {
                let path = join(&path_bytes(&parent.path()?), name);
                let st = parent.stat_child(name)?;
                let kind = kind_of(&st);
                let target = if kind == crate::net::wire::NodeKind::Symlink {
                    parent.readlink_child(name)?
                } else {
                    Vec::new()
                };
                let id = FileId::random();
                let mut s = self.state();
                s.index.insert(&path, id);
                self.append(
                    &mut s,
                    Entry::Create {
                        id,
                        path,
                        kind,
                        attrs: attrs_of(&st),
                        rdev: st.st_rdev,
                        target,
                    },
                );
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
                        self.append(
                            &mut s,
                            Entry::Attr {
                                id,
                                path,
                                version,
                                attrs: attrs_of(&stat),
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
        Some(if sess.data {
            Entry::Data {
                id,
                path,
                version: sess.version,
                size: stat.st_size as u64,
                attrs,
            }
        } else {
            Entry::Attr {
                id,
                path,
                version: sess.version,
                attrs,
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
        let mut ops: Vec<Op> = st.index.take_changed();
        ops.extend(st.dirty.take_changed());
        let (jops, upto) = st.journal.take_changed();
        ops.extend(jops);
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
            "window": st.window.len(),
            "suspects": st.suspects.len(),
            "unclean_start": self.unclean_start,
            "in_sync": st.dirty.files() == 0 && !lagging && st.sessions.is_empty(),
            "freshness_timeout": humantime::format_duration(self.freshness_timeout).to_string(),
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
