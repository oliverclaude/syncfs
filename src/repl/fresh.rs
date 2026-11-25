//! Freshness: before a new lock session writes a file, the local copy is
//! brought up to the newest version the voters know of, by comparing block
//! hashes with a node that has it and pulling the blocks that differ.
//!
//! The same comparison resolves suspects: files that were changing when
//! the node stopped uncleanly, whose changes may not all be recorded in the
//! dirty bitmap. Their differing blocks are marked dirty again.

use std::io;
use std::sync::Weak;
use std::time::{Duration, Instant};

use crate::config::NodeId;
use crate::net::wire::{Entry, FileId, NodeKind, PullMode, Version, VersionInfo};

use super::backing::{hash_block, Backing};
use super::core::Repl;
use super::net::RemoteFile;
use super::ranges::Ranges;
use super::version;

const POLL: Duration = Duration::from_millis(100);

/// Why a copy is made (see [`Repl::copy_from`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Copy {
    /// Freshness: the source holds a whole version.
    Fresh,
    /// Applying a forwarded entry.
    Relayed,
    /// Rebuilding: the local copy is not to be trusted.
    Rebuild,
}

/// How a session's local copy was made fresh.
#[derive(Debug, PartialEq, Eq)]
pub enum Fresh {
    /// It has the newest version the grants report.
    Current,
    /// The fallback: nobody reachable had the newest version, which `origin`
    /// announced; the session goes on from an older one.
    Fallback { origin: NodeId },
}

/// Why a session must not write.
#[derive(Debug, PartialEq, Eq)]
pub enum NotFresh {
    /// The local copy is older than a committed version that no reachable
    /// node has.
    Stale,
    /// The file was removed at a newer version (the removal is applied
    /// here now).
    Removed,
}

/// The payload of the `NotFound` error a copy returns for a file the
/// source removed at a newer version (after applying the removal).
#[derive(Debug)]
pub struct Removed;

impl std::fmt::Display for Removed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("removed at a newer version")
    }
}

impl std::error::Error for Removed {}

/// The payload of the error for a source that is rebuilding and does not
/// have the file yet.
#[derive(Debug)]
pub struct Missing;

impl std::fmt::Display for Missing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the node is rebuilding and does not have it yet")
    }
}

impl std::error::Error for Missing {}

/// Whether `e` says the source is rebuilding (see [`Missing`]).
pub fn is_missing(e: &io::Error) -> bool {
    e.get_ref().is_some_and(|x| x.is::<Missing>())
}

/// Whether `e` says the file was removed (see [`Removed`]).
pub fn is_removed(e: &io::Error) -> bool {
    e.get_ref().is_some_and(|x| x.is::<Removed>())
}

impl Repl {
    /// Connected peers that store data.
    pub(crate) fn data_peers_up(&self) -> Vec<NodeId> {
        let snap = self.net.snapshot();
        snap.peers
            .iter()
            .filter(|p| p.role.is_some_and(|r| r.data) && self.net.session_of(p.id) != 0)
            .map(|p| p.id)
            .collect()
    }

    /// Blocks where the local file at `path` differs from `remote` (and
    /// local blocks past its end).
    fn differing(&self, path: &[u8], remote: &RemoteFile) -> io::Result<(Ranges, u64)> {
        let f = self.backing.open_read(path)?;
        let local_size = f.metadata()?.len();
        let mut diff = Ranges::default();
        for (n, h) in remote.hashes.iter().enumerate() {
            let n = n as u64;
            if hash_block(&Backing::read_block(&f, n, self.block_size)?) != *h {
                diff.insert(n, n + 1);
            }
        }
        let local_blocks = local_size.div_ceil(self.block_size);
        let remote_blocks = remote.hashes.len() as u64;
        if local_blocks > remote_blocks {
            diff.insert(remote_blocks, local_blocks);
        }
        Ok((diff, local_blocks))
    }

    /// Brings `id` to the newest version the grants in `known` report
    /// (see the module docs). Waits up to `freshness_timeout` for a node
    /// that has it, then falls back to the newest committed version a
    /// reachable node has. Without that either (the local copy is older
    /// than a committed version and no holder of it answers), the session
    /// must not write: `Err(Stale)`. A node that removed the file at a
    /// newer version: the removal is applied here, `Err(Removed)`.
    pub fn ensure_fresh(&self, id: FileId, known: &[VersionInfo]) -> Result<Fresh, NotFresh> {
        self.resolve_suspect(id, true);
        {
            let st = self.state();
            if st.index.tombstone(&id).is_some() || st.index.path_of(&id).is_none() {
                // Removed (or replaced by a rename onto its path) by a
                // peer's entry since the file was looked up, e.g. while
                // the lock was waited for.
                return Err(NotFresh::Removed);
            }
        }
        let t = version::target(known);
        if t.satisfied_by(self.local_version(&id)) {
            return Ok(Fresh::Current);
        }
        let newest = t.newest.expect("unsatisfied targets have a version");
        if newest.node == self.me && newest.disk == self.disk && t.sources == [self.me] {
            // This node announced it on this disk but never recorded a
            // change (the version is recorded durably before the first
            // write): e.g. it stopped between the lock and the first write.
            // Nobody else can have it.
            log::debug!(
                "freshness: version {} of {id} was announced here but never written",
                newest.seq
            );
            return Ok(Fresh::Current);
        }
        self.state().counters.fresh_checks += 1;
        let deadline = Instant::now() + self.freshness_timeout.get();
        let mut waited = false;
        loop {
            for &s in t.sources.iter().filter(|&&s| s != self.me) {
                if self.net.session_of(s) == 0 {
                    continue;
                }
                match self.catch_up(id, s) {
                    Ok(v) if v.is_some_and(|v| v >= newest) => return Ok(Fresh::Current),
                    Ok(v) => log::debug!("freshness: node {s} has {v:?} of {id}, not {newest:?}"),
                    Err(e) if is_removed(&e) => return Err(NotFresh::Removed),
                    Err(e) => log::debug!("freshness: {id} from node {s}: {e}"),
                }
            }
            if Instant::now() >= deadline || self.stopping() {
                break;
            }
            if !waited {
                waited = true;
                self.state().counters.fresh_waits += 1;
                log::info!(
                    "freshness: waiting up to {} for a node with version {} of {id}",
                    humantime::format_duration(self.freshness_timeout.get()),
                    newest.seq
                );
            }
            std::thread::sleep(POLL);
        }
        self.state().counters.fresh_fallbacks += 1;
        let fallback = Fresh::Fallback {
            origin: newest.node,
        };
        // The newest copy any reachable node has (holders of the committed
        // version first: the voters may not know every holder).
        let mut order: Vec<NodeId> = t.committed.iter().flat_map(|(_, h)| h.clone()).collect();
        order.extend(self.data_peers_up());
        let mut tried = std::collections::BTreeSet::new();
        // Nodes that answered without the version (they lost it, e.g. with
        // their disk).
        let mut without = std::collections::BTreeSet::from([self.me]);
        for p in order {
            if p == self.me || !tried.insert(p) || self.net.session_of(p) == 0 {
                continue;
            }
            match self.catch_up(id, p) {
                Err(e) if is_removed(&e) => return Err(NotFresh::Removed),
                Err(e) if is_missing(&e) || e.kind() == io::ErrorKind::NotFound => {
                    without.insert(p);
                    log::debug!("freshness: {id} from node {p}: {e}");
                }
                Err(e) => log::debug!("freshness: {id} from node {p}: {e}"),
                Ok(_) => {
                    without.insert(p);
                }
            }
        }
        let local = self.local_version(&id);
        if let Some((cv, _)) = &t.committed {
            if local.is_some_and(|l| l >= *cv) {
                log::warn!(
                    "freshness: no node with version {} of {id} answered; continuing from version \
                     {:?} (committed: {})",
                    newest.seq,
                    local.map(|l| l.seq),
                    cv.seq
                );
                return Ok(fallback);
            }
            if t.committed
                .as_ref()
                .is_some_and(|(_, holders)| holders.iter().all(|h| without.contains(h)))
            {
                log::warn!(
                    "freshness: every node that held committed version {} of {id} answered \
                     without it (lost with a disk); continuing from version {:?}",
                    cv.seq,
                    local.map(|l| l.seq)
                );
                return Ok(fallback);
            }
            log::warn!(
                "freshness: the local copy of {id} is older than committed version {} and no \
                 node holding it is reachable; refusing to write",
                cv.seq
            );
            return Err(NotFresh::Stale);
        }
        log::warn!(
            "freshness: no node with version {} of {id} is reachable and none is committed; \
             continuing from version {:?}",
            newest.seq,
            local.map(|l| l.seq)
        );
        Ok(fallback)
    }

    /// Whether the local file at `path` has different block hashes from
    /// `source`'s copy of `id`.
    pub(crate) fn content_differs(
        &self,
        id: FileId,
        path: &[u8],
        source: NodeId,
    ) -> io::Result<bool> {
        let remote = self.remote_hashes(source, id)?;
        if remote.removed {
            return Ok(false);
        }
        let (diff, _) = self.differing(path, &remote)?;
        Ok(!diff.is_empty())
    }

    /// Makes the local copy of `id` equal to `source`'s. Returns the
    /// version it then has.
    pub(crate) fn catch_up(&self, id: FileId, source: NodeId) -> io::Result<Option<Version>> {
        let _g = self.hold(id);
        self.copy_from(id, source, Copy::Fresh)
    }

    /// [`catch_up`](Self::catch_up) for a caller that holds `id`.
    pub(crate) fn copy_from(
        &self,
        id: FileId,
        source: NodeId,
        how: Copy,
    ) -> io::Result<Option<Version>> {
        let path = self
            .state()
            .index
            .path_of(&id)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        let remote = self.remote_hashes(source, id)?;
        let local = self.local_version(&id);
        if remote.removed {
            if let Some(v) = remote.version.filter(|v| local.is_none_or(|l| *v > l)) {
                self.remove_local(id, v, &remote.lineage)?;
                return Err(io::Error::new(io::ErrorKind::NotFound, Removed));
            }
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("node {source} removed {id} at an older version"),
            ));
        }
        let older = match how {
            Copy::Fresh => remote.version <= local,
            // A relay's copy of the same version may have more of the
            // session's writes.
            Copy::Relayed => remote.version < local,
            Copy::Rebuild => local.is_some() && remote.version <= local,
        };
        if older {
            // Nothing newer there (never go back to an older copy).
            return Ok(remote.version);
        }
        if let Some(v) = remote.version {
            self.keep_if_diverged(id, v, &remote.lineage)?;
        }
        if !self.backing.exists(&path) {
            // Removed here (a tombstone) and written again elsewhere.
            self.backing
                .create(&path, NodeKind::File, &remote.attrs, 0, &[])?;
        }
        let (mut diff, _) = self.differing(&path, &remote)?;
        // Blocks past the remote end go with the truncation.
        diff.remove(remote.hashes.len() as u64, u64::MAX);
        let out = self.backing.open_write(&path)?;
        let max = (4 << 20) / self.block_size.max(1);
        while !diff.is_empty() {
            let chunk = diff.take_front(max.max(1));
            let pulled = self.pull(source, id, PullMode::Ranges(chunk.iter().collect()), &out)?;
            if !pulled.found {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
        }
        out.set_len(remote.size)?;
        out.sync_all()?;
        drop(out);
        self.backing.set_attrs(&path, &remote.attrs)?;
        if let Err(e) = self.backing.sync_xattrs(&path, &remote.xattrs) {
            log::warn!("replication: copying the xattrs of {id} from node {source}: {e}");
        }
        self.invalidate(&path);
        let mut st = self.state();
        if let Some(v) = remote.version {
            st.index.take_version(id, v, &remote.lineage);
        }
        if how == Copy::Fresh {
            st.counters.fresh_pulls += 1;
        }
        drop(st);
        self.flusher.flush_now()?;
        self.rebuild_file_done(&id);
        if let (Copy::Fresh | Copy::Rebuild, Some(v)) = (how, remote.version) {
            // This node holds that version now (durably). (A relay's copy
            // may be part of a session still writing: not announced.)
            self.announce_holders(id, v, vec![self.me, source]);
        }
        Ok(remote.version)
    }

    /// `id` was removed at `version` (by a peer): removes the local copy
    /// and keeps a tombstone. The caller holds `id`.
    pub(crate) fn remove_local(
        &self,
        id: FileId,
        version: Version,
        lineage: &[Version],
    ) -> io::Result<()> {
        self.keep_if_diverged(id, version, lineage)?;
        let paths = self
            .state()
            .index
            .rec(&id)
            .map(|r| r.paths.clone())
            .unwrap_or_default();
        for p in &paths {
            match self.backing.remove(p, false) {
                Ok(()) => self.invalidate_entry(p),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        let mut st = self.state();
        st.index.bury(id, version);
        st.dirty.forget(id);
        drop(st);
        self.flusher.flush_now()?;
        self.rebuild_file_done(&id);
        log::debug!("replication: {id} removed at version {}", version.seq);
        Ok(())
    }

    /// If `id` is a suspect, compares it with every connected data peer
    /// and marks the blocks that differ dirty, then sends a data entry.
    /// `wait`: wait up to `freshness_timeout` for a peer (otherwise give up
    /// for now if none is connected). Without a peer every block is marked.
    pub(crate) fn resolve_suspect(&self, id: FileId, wait: bool) {
        if !self.state().suspects.contains(&id) {
            return;
        }
        let _g = self.hold(id);
        let deadline = Instant::now() + self.freshness_timeout.get();
        let peers = loop {
            if !self.state().suspects.contains(&id) {
                return;
            }
            let up = self.data_peers_up();
            if !up.is_empty() || self.peers.is_empty() {
                break Some(up);
            }
            if !wait {
                return;
            }
            if Instant::now() >= deadline || self.stopping() {
                break None;
            }
            std::thread::sleep(POLL);
        };
        let path = self.state().index.path_of(&id).map(<[u8]>::to_vec);
        let regular = path
            .as_deref()
            .and_then(|p| self.backing.stat(p).ok())
            .is_some_and(|st| super::backing::kind_of(&st) == crate::net::wire::NodeKind::File);
        let mut marks = Ranges::default();
        let mut local_blocks = 0;
        if let (true, Some(path)) = (regular, &path) {
            local_blocks = self
                .backing
                .stat(path)
                .map(|s| (s.st_size as u64).div_ceil(self.block_size))
                .unwrap_or(0);
            match peers {
                Some(peers) if !peers.is_empty() => {
                    for p in peers {
                        match self.remote_hashes(p, id) {
                            Ok(remote) => match self.differing(path, &remote) {
                                Ok((d, _)) => marks.extend(&d),
                                Err(e) => {
                                    log::warn!("replication: comparing suspect {id}: {e}");
                                    marks.insert(0, local_blocks);
                                }
                            },
                            Err(_) => marks.insert(0, local_blocks),
                        }
                    }
                }
                _ => marks.insert(0, local_blocks),
            }
        }
        let mut st = self.state();
        if regular {
            let version = st.index.version(&id).unwrap_or(Version {
                seq: 0,
                node: self.me,
                disk: self.disk,
            });
            for (a, b) in marks.iter() {
                st.dirty.mark(id, version, a, b);
            }
            if let (Some(path), Ok(stat)) = (
                path.clone(),
                self.backing.stat(path.as_deref().unwrap_or_default()),
            ) {
                let entry = Entry::Data {
                    id,
                    path,
                    version,
                    size: stat.st_size as u64,
                    attrs: super::backing::attrs_of(&stat),
                    lineage: st.index.lineage(&id),
                };
                self.append(&mut st, entry);
            }
            log::info!(
                "replication: suspect {id} checked: {} of {local_blocks} block(s) to resend",
                marks.len()
            );
        }
        st.suspects.remove(&id);
        st.window_drop.insert(id);
        st.counters.suspects_resolved += 1;
        drop(st);
        let _ = self.flusher.flush_now();
        self.cv.notify_all();
    }
}

/// Resolves suspects in the background once peers are connected.
pub(crate) fn suspect_loop(weak: Weak<Repl>) {
    loop {
        let Some(r) = weak.upgrade() else { return };
        if r.stopping() {
            return;
        }
        let next = r.state().suspects.iter().next().copied();
        let Some(id) = next else { return };
        r.resolve_suspect(id, false);
        if r.state().suspects.contains(&id) {
            drop(r);
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}
