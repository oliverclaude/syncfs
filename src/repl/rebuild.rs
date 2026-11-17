//! Rebuilding a lost disk from peers (option C).
//!
//! A node whose disk marker is gone although its store ran as a mount
//! (the disk was lost or replaced), whose backing directory was emptied,
//! or that ran as a voter-only daemon before, never treats its local data
//! as the truth. It takes a new disk id (so its old versions are not its
//! own any more) and rebuilds:
//!
//! 1. It lists the directory tree of a data peer that is not rebuilding
//!    itself (`TreeRequest`), with the positions that peer had applied.
//! 2. It makes the tree locally: directories, symlinks and special files
//!    as they are; regular files as sparse files of the right size, marked
//!    missing. Local files the source does not have move to
//!    `<backing>/.syncfs/orphans/<time>/`. Entries from peers wait until
//!    then; it then counts the source's positions as applied (the tree
//!    has their effect) and applies what follows.
//! 3. Missing files are copied in the background, from several peers at
//!    once, checked block by block (BLAKE3); a file opened before that is
//!    copied first. A missing file answers `Missing` to hash requests and
//!    has nothing to serve.
//!
//! Writes fail with `EROFS` until every missing file is copied. If every
//! peer is connected and every data peer is rebuilding too, nobody has
//! more: the rebuild ends with what is there.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Weak;
use std::time::{Duration, Instant};

use crate::config::NodeId;
use crate::net::wire::{FileId, FileMeta, Message, NodeKind, TreeNode};

use super::backing::{attrs_of, kind_of};
use super::core::{lock, Repl};
use super::fresh::Copy;
use super::marker::{self, Marker};
use super::store::{Store, Table};

/// Nodes per `TreeReply`.
const TREE_PAGE: usize = 1024;
/// Background copies at once.
const WORKERS: usize = 4;

/// Why a node rebuilds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    /// A rebuild was interrupted.
    Resume,
    /// The disk marker is gone but the store ran as a mount.
    DiskLost,
    /// The backing directory is empty but the store knows files.
    Emptied,
    /// The store ran as a voter-only daemon.
    Upgrade,
}

impl std::fmt::Display for Cause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Cause::Resume => "an earlier rebuild did not finish",
            Cause::DiskLost => "the disk marker is missing (disk lost or replaced)",
            Cause::Emptied => "the backing directory is empty",
            Cause::Upgrade => "this node ran as a voter-only daemon",
        })
    }
}

impl Cause {
    fn name(self) -> &'static str {
        match self {
            Cause::Resume => "resume",
            Cause::DiskLost => "disk_lost",
            Cause::Emptied => "emptied",
            Cause::Upgrade => "daemon_upgrade",
        }
    }
}

/// Whether (and why) a mount must rebuild: see the module docs. `existed`:
/// the store was there before this start. `others`: the cluster has other
/// nodes.
pub fn cause(
    found: Option<&Marker>,
    existed: bool,
    store: &Store,
    backing: &std::path::Path,
    others: bool,
) -> io::Result<Option<Cause>> {
    if !others {
        return Ok(None);
    }
    if found.is_some_and(|m| m.rebuilding) {
        return Ok(Some(Cause::Resume));
    }
    let ran_as_mount = store
        .get(Table::Meta, super::core::LINKS_SCANNED_KEY)?
        .is_some();
    Ok(match found {
        None if ran_as_mount => Some(Cause::DiskLost),
        None if existed => Some(Cause::Upgrade),
        Some(_)
            if ran_as_mount
                && marker::backing_empty(backing)
                && !store.load(Table::Files)?.is_empty() =>
        {
            Some(Cause::Emptied)
        }
        _ => None,
    })
}

/// Rebuild progress.
#[derive(Debug, Default)]
pub(crate) struct Rebuild {
    pub cause: Option<Cause>,
    /// The tree is made: entries from peers may apply.
    pub tree_done: bool,
    /// Files still to copy, with their size.
    pub missing: HashMap<FileId, u64>,
    pub files_total: u64,
    pub files_done: u64,
    pub bytes_total: u64,
    pub bytes_done: u64,
    pub orphans: u64,
    pub source: Option<NodeId>,
    pub started: Option<Instant>,
    /// Identifies the marker to rewrite when done.
    pub marker: Option<Marker>,
}

/// A tree listing: the source's positions and its nodes.
type Tree = (Vec<(NodeId, u64)>, Vec<TreeNode>);

impl Repl {
    pub fn is_rebuilding(&self) -> bool {
        self.rebuilding.load(Ordering::Acquire)
    }

    /// Whether the local copy of `id` is not there yet (rebuilding).
    pub(crate) fn rebuild_missing(&self, id: &FileId) -> bool {
        if !self.is_rebuilding() {
            return false;
        }
        let rb = lock(&self.rebuild);
        !rb.tree_done || rb.missing.contains_key(id)
    }

    /// Whether entries from peers must wait (the tree is being made).
    pub(crate) fn rebuild_holds_entries(&self) -> bool {
        self.is_rebuilding() && !lock(&self.rebuild).tree_done
    }

    /// The local copy of `id` is complete now (or `id` is gone).
    pub(crate) fn rebuild_file_done(&self, id: &FileId) {
        if !self.is_rebuilding() {
            return;
        }
        let mut rb = lock(&self.rebuild);
        if let Some(size) = rb.missing.remove(id) {
            rb.files_done += 1;
            rb.bytes_done += size;
        }
        drop(rb);
        self.cv.notify_all();
    }

    /// Waits up to `timeout` for the tree to be made (the mount shows it
    /// complete from the start then).
    pub fn wait_for_tree(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while self.rebuild_holds_entries() && Instant::now() < deadline && !self.stopping() {
            let st = self.state();
            let _ = self.cv.wait_timeout(st, Duration::from_millis(50));
        }
    }

    pub(crate) fn rebuild_status(&self) -> serde_json::Value {
        let active = self.is_rebuilding();
        let rb = lock(&self.rebuild);
        let eta = rb.started.and_then(|t| {
            let done = rb.bytes_done as f64;
            let left = rb.bytes_total.saturating_sub(rb.bytes_done) as f64;
            (active && done > 0.0).then(|| (t.elapsed().as_secs_f64() * left / done).round() as u64)
        });
        serde_json::json!({
            "active": active,
            "cause": rb.cause.map(Cause::name),
            "source": rb.source,
            "files_total": rb.files_total,
            "files_done": rb.files_done,
            "bytes_total": rb.bytes_total,
            "bytes_done": rb.bytes_done,
            "eta_secs": eta,
            "orphans": rb.orphans,
        })
    }

    /// Answers a `TreeRequest`.
    pub(crate) fn serve_tree(&self, peer: NodeId, req: u64, after: Vec<u8>) {
        let rebuilding = self.is_rebuilding();
        // The asker stores data: replicate to it from before the listing,
        // so no change after it is missed.
        self.add_data_peer(peer);
        let positions = self.positions();
        let (nodes, more) = if rebuilding {
            (Vec::new(), false)
        } else {
            match self.list_tree(&after) {
                Ok(x) => x,
                Err(e) => {
                    log::warn!("replication: listing the tree for node {peer}: {e}");
                    (Vec::new(), false)
                }
            }
        };
        self.net.send(
            peer,
            Message::TreeReply {
                req,
                rebuilding,
                positions,
                nodes,
                more,
            },
        );
    }

    fn list_tree(&self, after: &[u8]) -> io::Result<(Vec<TreeNode>, bool)> {
        let all = self.backing.list()?;
        let start = all.partition_point(|p| p.as_slice() <= after);
        let mut nodes = Vec::new();
        for path in all[start..].iter().take(TREE_PAGE) {
            let Ok(stat) = self.backing.stat(path) else {
                continue; // Gone meanwhile.
            };
            let kind = kind_of(&stat);
            let target = if kind == NodeKind::Symlink {
                self.backing.readlink(path).unwrap_or_default()
            } else {
                Vec::new()
            };
            let mut st = self.state();
            // Bind untouched pre-existing files, so hash requests find them.
            let id = st.index.id_at(path);
            nodes.push(TreeNode {
                path: path.clone(),
                id,
                kind,
                meta: FileMeta {
                    size: stat.st_size as u64,
                    attrs: attrs_of(&stat),
                },
                rdev: stat.st_rdev,
                target,
                version: st.index.version(&id),
                lineage: st.index.lineage(&id),
            });
        }
        let more = all.len() > start + nodes.len();
        Ok((nodes, more))
    }

    /// `source`'s whole tree, or `None` if it is rebuilding itself.
    fn fetch_tree(&self, source: NodeId) -> io::Result<Option<Tree>> {
        let mut nodes: Vec<TreeNode> = Vec::new();
        let mut first = None;
        loop {
            let req = self.req();
            let after = nodes.last().map(|n| n.path.clone()).unwrap_or_default();
            let rpc = self.call(source, req, Message::TreeRequest { req, after })?;
            match rpc.recv()? {
                Message::TreeReply {
                    rebuilding,
                    positions,
                    nodes: page,
                    more,
                    ..
                } => {
                    if rebuilding {
                        return Ok(None);
                    }
                    first.get_or_insert(positions);
                    nodes.extend(page);
                    if !more {
                        return Ok(first.map(|p| (p, nodes)));
                    }
                }
                other => return Err(io::Error::other(format!("unexpected {}", other.kind()))),
            }
        }
    }

    /// Makes `tree` locally (see the module docs, step 2).
    fn make_tree(&self, source: NodeId, tree: Tree) -> io::Result<()> {
        let (positions, nodes) = tree;
        let stamp = chrono_stamp();
        let mut orphan_dir: Option<Vec<u8>> = None;
        let mut orphans = 0u64;
        let mut orphan = |r: &Repl, path: &[u8]| -> io::Result<()> {
            let dir = orphan_dir.get_or_insert_with(|| {
                format!("{}/{}/{stamp}", crate::fs::virt::NAME, marker::ORPHANS).into_bytes()
            });
            let root = r.backing.path();
            let from = root.join(std::ffi::OsStr::from_bytes(path));
            let mut to_rel = dir.clone();
            to_rel.push(b'/');
            to_rel.extend_from_slice(path);
            let to = root.join(std::ffi::OsStr::from_bytes(&to_rel));
            if let Some(parent) = to.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::rename(&from, &to)?;
            log::warn!(
                "rebuild: {} is not on node {source}; moved to {}",
                String::from_utf8_lossy(path),
                to.display()
            );
            orphans += 1;
            Ok(())
        };
        use std::os::unix::ffi::OsStrExt;
        let wanted: BTreeSet<&[u8]> = nodes.iter().map(|n| n.path.as_slice()).collect();
        // Local paths the source does not have (outermost only).
        let mut gone: Vec<Vec<u8>> = Vec::new();
        for p in self.backing.list()? {
            if wanted.contains(p.as_slice())
                || gone.last().is_some_and(|g| super::files::under(&p, g))
            {
                continue;
            }
            gone.push(p);
        }
        for p in &gone {
            orphan(self, p)?;
        }
        let mut first_path: HashMap<FileId, Vec<u8>> = HashMap::new();
        let mut missing = HashMap::new();
        let mut dirs = Vec::new();
        for n in &nodes {
            let local = self.backing.stat(&n.path).ok();
            let same_kind = local.as_ref().is_some_and(|s| kind_of(s) == n.kind);
            if let Some(first) = first_path.get(&n.id) {
                // Another link of a file made already.
                let linked = match (&local, self.backing.stat(first).ok()) {
                    (Some(a), Some(b)) => a.st_ino == b.st_ino,
                    _ => false,
                };
                if !linked {
                    if local.is_some() {
                        orphan(self, &n.path)?;
                    }
                    self.backing.link(first, &n.path)?;
                }
                self.state().index.insert(&n.path, n.id);
                continue;
            }
            let keep = match n.kind {
                NodeKind::Dir | NodeKind::File => same_kind,
                NodeKind::Symlink => {
                    same_kind
                        && self.backing.readlink(&n.path).ok().as_deref() == Some(&n.target[..])
                }
                _ => same_kind && local.as_ref().is_some_and(|s| s.st_rdev == n.rdev),
            };
            if !keep {
                if local.is_some() {
                    orphan(self, &n.path)?;
                }
                self.backing
                    .create(&n.path, n.kind, &n.meta.attrs, n.rdev, &n.target)?;
            }
            if n.kind == NodeKind::File {
                // Sparse until copied; sizes show right meanwhile.
                self.backing.open_write(&n.path)?.set_len(n.meta.size)?;
                missing.insert(n.id, n.meta.size);
            }
            if n.kind == NodeKind::Dir {
                dirs.push(n);
            } else if n.kind != NodeKind::File {
                self.backing.set_attrs(&n.path, &n.meta.attrs)?;
            }
            let mut st = self.state();
            st.index.insert(&n.path, n.id);
            // The local copy is not to be trusted: compared in full.
            st.index.forget_version(n.id);
            drop(st);
            first_path.insert(n.id, n.path.clone());
        }
        // Directory times last (making entries changes them), innermost first.
        for n in dirs.iter().rev() {
            self.backing.set_attrs(&n.path, &n.meta.attrs)?;
        }
        let mut st = self.state();
        let stale: Vec<Vec<u8>> = st
            .index
            .paths()
            .filter(|p| !wanted.contains(p))
            .map(<[u8]>::to_vec)
            .collect();
        for p in stale {
            st.index.remove_path(&p);
        }
        for (o, s) in positions {
            if o == self.me || !self.peers.contains(&o) {
                continue;
            }
            let a = st.applied.entry(o).or_default();
            if *a < s {
                *a = s;
                st.applied_changed = true;
            }
        }
        let applied = st.applied.clone();
        drop(st);
        self.flusher.flush_now()?;
        {
            let mut st = self.state();
            for (o, s) in applied {
                let d = st.applied_durable.entry(o).or_default();
                *d = (*d).max(s);
            }
        }
        let mut rb = lock(&self.rebuild);
        rb.files_total = missing.len() as u64;
        rb.bytes_total = missing.values().sum();
        rb.files_done = 0;
        rb.bytes_done = 0;
        rb.missing = missing;
        rb.orphans += orphans;
        rb.source = Some(source);
        rb.started = Some(Instant::now());
        rb.tree_done = true;
        drop(rb);
        log::info!(
            "rebuild: tree of node {source} made ({} node(s), {} file(s) to copy, {orphans} \
             orphan(s))",
            nodes.len(),
            lock(&self.rebuild).files_total
        );
        self.cv.notify_all();
        Ok(())
    }

    /// Copies `id` from a peer unless that is done. Ok(true): done.
    pub(crate) fn fetch_missing(&self, id: FileId, first: Option<NodeId>) -> bool {
        let _g = self.hold(id);
        if !self.rebuild_missing(&id) {
            return true;
        }
        let mut sources: Vec<NodeId> = first.into_iter().collect();
        for p in self.data_peers_up() {
            if !sources.contains(&p) {
                sources.push(p);
            }
        }
        let mut absent = 0;
        for &s in &sources {
            match self.copy_from(id, s, Copy::Rebuild) {
                Ok(_) => {
                    self.rebuild_file_done(&id);
                    return true;
                }
                Err(e) if super::fresh::is_removed(&e) => {
                    self.rebuild_file_done(&id);
                    return true;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    absent += 1;
                    log::debug!("rebuild: {id} from node {s}: {e}");
                }
                Err(e) => log::debug!("rebuild: {id} from node {s}: {e}"),
            }
        }
        if absent > 0 && absent == sources.len() {
            // Gone everywhere since the listing; a later entry says what
            // happened.
            self.rebuild_file_done(&id);
            return true;
        }
        false
    }

    fn finish_rebuild(&self) -> io::Result<()> {
        let m = lock(&self.rebuild).marker.take();
        if let Some(mut m) = m {
            m.rebuilding = false;
            marker::store(self.backing.path(), &m)?;
        }
        self.rebuilding.store(false, Ordering::Release);
        let rb = lock(&self.rebuild);
        log::info!(
            "rebuild: done ({} file(s), {} byte(s)); taking writes",
            rb.files_done,
            rb.bytes_done
        );
        drop(rb);
        self.cv.notify_all();
        Ok(())
    }
}

/// UTC time as `YYYYMMDDTHHMMSSZ`.
fn chrono_stamp() -> String {
    humantime::format_rfc3339_seconds(std::time::SystemTime::now())
        .to_string()
        .chars()
        .filter(|c| *c != '-' && *c != ':')
        .collect()
}

/// Runs a rebuild (see the module docs).
pub(crate) fn rebuild_loop(weak: Weak<Repl>) {
    // 1. A source.
    loop {
        let Some(r) = weak.upgrade() else { return };
        if r.stopping() {
            return;
        }
        let up = r.data_peers_up();
        let mut rebuilding = 0;
        let mut made = false;
        for &p in &up {
            match r.fetch_tree(p) {
                Ok(Some(tree)) => match r.make_tree(p, tree) {
                    Ok(()) => {
                        made = true;
                        break;
                    }
                    Err(e) => log::warn!("rebuild: making the tree of node {p}: {e}"),
                },
                Ok(None) => rebuilding += 1,
                Err(e) => log::debug!("rebuild: listing the tree of node {p}: {e}"),
            }
        }
        if made {
            break;
        }
        let all_up = r.peers.iter().all(|p| r.net.session_of(*p) != 0);
        if all_up && rebuilding == up.len() {
            log::warn!(
                "rebuild: every data peer is rebuilding too; nobody has more than this node"
            );
            {
                let mut rb = lock(&r.rebuild);
                rb.tree_done = true;
            }
            let _ = r.finish_rebuild();
            return;
        }
        drop(r);
        std::thread::sleep(Duration::from_millis(500));
    }
    // 2. Copy the missing files.
    let queue: std::sync::Arc<std::sync::Mutex<VecDeque<FileId>>> = {
        let Some(r) = weak.upgrade() else { return };
        let rb = lock(&r.rebuild);
        let mut ids: Vec<FileId> = rb.missing.keys().copied().collect();
        ids.sort();
        std::sync::Arc::new(std::sync::Mutex::new(ids.into()))
    };
    let mut workers = Vec::new();
    for w in 0..WORKERS {
        let weak = weak.clone();
        let queue = std::sync::Arc::clone(&queue);
        let spawned = std::thread::Builder::new()
            .name(format!("syncfs-rebuild-{w}"))
            .spawn(move || fill(weak, queue, w));
        match spawned {
            Ok(h) => workers.push(h),
            Err(e) => log::error!("rebuild: cannot start a copy worker: {e}"),
        }
    }
    for h in workers {
        let _ = h.join();
    }
    let Some(r) = weak.upgrade() else { return };
    if r.stopping() {
        return;
    }
    if let Err(e) = r.finish_rebuild() {
        log::error!("rebuild: recording the end of the rebuild: {e}");
    }
}

/// One copy worker: takes files off `queue` until none is missing. Worker
/// `w` starts with the `w`-th connected data peer, spreading the load.
fn fill(weak: Weak<Repl>, queue: std::sync::Arc<std::sync::Mutex<VecDeque<FileId>>>, w: usize) {
    let mut failed: HashSet<FileId> = HashSet::new();
    // Tests slow the background copies down to watch a rebuild.
    let delay = std::env::var("SYNCFS_TEST_REBUILD_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map(Duration::from_millis);
    loop {
        if let Some(d) = delay {
            std::thread::sleep(d);
        }
        let Some(r) = weak.upgrade() else { return };
        if r.stopping() {
            return;
        }
        let next = lock(&queue).pop_front();
        let Some(id) = next else {
            if lock(&r.rebuild).missing.is_empty() {
                return;
            }
            // Others are still copying (or retrying): wait for them.
            let st = r.state();
            let _ = r.cv.wait_timeout(st, Duration::from_millis(200));
            continue;
        };
        if !r.rebuild_missing(&id) {
            continue;
        }
        let up = r.data_peers_up();
        let first = (!up.is_empty()).then(|| up[w % up.len()]);
        if !r.fetch_missing(id, first) {
            lock(&queue).push_back(id);
            if !failed.insert(id) {
                failed.clear();
                drop(r);
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}
