//! Wiring locks into the write path.
//!
//! * Opening a file for writing takes the write lock on its path and keeps
//!   it until the handle is released.
//! * Data and attribute changes on a file opened for writing check that its
//!   lock is still valid (re-acquiring it if the lease lapsed); otherwise
//!   they take the lock for just that operation.
//! * Namespace changes (create, link, unlink, rmdir, rename) lock the
//!   affected paths for the duration of the operation. Because a key covers
//!   everything below it, renaming or removing a directory waits for every
//!   lock under it.
//! * After a local rename, write locks on paths that moved are re-taken
//!   under the new path.
//!
//! * Locks on regular files take part in versioning: a new lock session
//!   first brings the local copy up to the newest version
//!   ([`Repl::ensure_fresh`], which may wait up to `freshness_timeout`),
//!   and its first change enters the file into the durable window and
//!   marks the session's version written.
//! * Every change that went through is then recorded for replication
//!   ([`Repl::record`]) while the lock is still held: lock, quorum, local
//!   write, mark dirty. When the session's last reference goes, its
//!   changes become a journal entry ([`Repl::end_session`]).
//!
//! No quorum: `EROFS`. Locked by another node for longer than `lock_wait`:
//! `EBUSY`.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::sync::{Arc, Mutex};

use fuser::Errno;

use crate::fs::{Mutation, NodeRef, OpGuard, WriteHooks};
use crate::repl::core::Repl;

use super::key::Key;
use super::manager::{FileCtx, LockManager};

pub struct ClusterHooks {
    locks: LockManager,
    repl: Arc<Repl>,
    /// Write locks per inode: one key per open writable handle.
    by_ino: Mutex<HashMap<u64, Vec<Key>>>,
}

/// The node's key, or None if it was deleted (an unlinked file is no
/// longer shared, so it needs no lock).
fn key_of(node: NodeRef<'_>) -> Result<Option<Key>, Errno> {
    match node.path() {
        Ok(p) => Ok(Some(Key::from_path(&p))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        // The kernel cannot name paths longer than PATH_MAX (with the
        // backing directory prefix). Lock the root instead: it covers the
        // file, at the cost of excluding every other node meanwhile.
        Err(e) if e.raw_os_error() == Some(libc::ENAMETOOLONG) => {
            log::debug!(
                "locks: inode {} has a path too long to name; locking /",
                node.ino()
            );
            Ok(Some(Key::new(b"")))
        }
        Err(e) => {
            log::warn!("locks: cannot resolve inode {}: {e}", node.ino());
            Err(Errno::EIO)
        }
    }
}

fn child(parent: NodeRef<'_>, name: &OsStr) -> Result<Option<Key>, Errno> {
    Ok(key_of(parent)?.map(|p| p.child(name)))
}

impl ClusterHooks {
    pub fn new(locks: LockManager, repl: Arc<Repl>) -> ClusterHooks {
        ClusterHooks {
            locks,
            repl,
            by_ino: Mutex::new(HashMap::new()),
        }
    }

    fn by_ino(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Vec<Key>>> {
        self.by_ino.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Locks `keys` until the returned guard drops.
    fn op_lock(&self, keys: Vec<Option<Key>>) -> Result<OpGuard, Errno> {
        self.op_lock_with(keys, false)
    }

    /// `fail_on_held`: EBUSY at once if another node holds a conflicting
    /// lock, instead of waiting up to `lock_wait`.
    fn op_lock_with(&self, keys: Vec<Option<Key>>, fail_on_held: bool) -> Result<OpGuard, Errno> {
        // A fixed order, so two renames in opposite directions don't wait
        // for each other.
        let mut keys: Vec<Key> = keys.into_iter().flatten().collect();
        keys.sort();
        keys.dedup();
        let mut taken: Vec<Key> = Vec::new();
        for key in keys {
            let res = self
                .locks
                .acquire_file(&key, None, fail_on_held)
                .map(|_| ());
            if let Err(e) = res {
                for k in &taken {
                    release(&self.locks, &self.repl, k);
                }
                log::debug!("locks: {key}: {e}");
                return Err(e.errno());
            }
            taken.push(key);
        }
        Ok(self.guard(taken))
    }

    fn guard(&self, taken: Vec<Key>) -> OpGuard {
        if taken.is_empty() {
            return OpGuard::none();
        }
        let (locks, repl) = (self.locks.clone(), Arc::clone(&self.repl));
        OpGuard::new(move || {
            for k in &taken {
                release(&locks, &repl, k);
            }
        })
    }

    /// The replication id of `node` if it is a regular file.
    fn file_of(&self, node: NodeRef<'_>) -> Option<crate::net::wire::FileId> {
        let st = node.stat().ok()?;
        if rustix::fs::FileType::from_raw_mode(st.st_mode) != rustix::fs::FileType::RegularFile {
            return None;
        }
        let path = node.path().ok()?;
        Some(self.repl.id_for(&crate::repl::core::path_bytes(&path)))
    }

    /// Takes a reference to the lock on `key` for `node`'s content; a new
    /// session first makes the local copy fresh.
    fn acquire_content(&self, key: &Key, node: NodeRef<'_>) -> Result<(), Errno> {
        let file = self.file_of(node);
        let ctx = file.map(|f| FileCtx {
            file: f,
            local: self.repl.local_version(&f),
        });
        let acquired = self.locks.acquire_file(key, ctx, false).map_err(|e| {
            log::debug!("locks: {key}: {e}");
            e.errno()
        })?;
        let (true, Some(f)) = (acquired.new_session, file) else {
            return Ok(());
        };
        // The kernel looked the inode up before the lock was held: the
        // previous holder's changes, applied by the handoff, may have
        // removed it or put another file at the path. ESTALE makes the
        // kernel look the path up again and retry once.
        if self.file_of(node) != file {
            log::debug!("locks: {key}: another file is there after the handoff (ESTALE)");
            self.repl.session_opened(f);
            release(&self.locks, &self.repl, key);
            self.locks.ready(key);
            return Err(Errno::ESTALE);
        }
        self.repl.session_opened(f);
        let fresh = self.repl.ensure_fresh(f, &acquired.known);
        if let Err(e) = fresh {
            release(&self.locks, &self.repl, key);
            self.locks.ready(key);
            return Err(match e {
                crate::repl::fresh::NotFresh::Stale => Errno::EBUSY,
                crate::repl::fresh::NotFresh::Removed => Errno::ENOENT,
            });
        }
        self.locks.ready(key);
        Ok(())
    }

    /// The lock on `key` covers a change to its file: the session's
    /// version is written.
    fn begin(&self, key: &Key) -> Result<(), Errno> {
        if let Some((file, version)) = self.locks.session(key) {
            self.repl.begin_change(file, version).map_err(|e| {
                log::error!("replication: cannot record {file} as changing: {e}");
                Errno::EIO
            })?;
            self.locks.mark_written(key);
        }
        Ok(())
    }

    fn content_change(&self, node: NodeRef<'_>) -> Result<OpGuard, Errno> {
        let held = self.by_ino().get(&node.ino()).cloned();
        match held {
            Some(keys) if !keys.is_empty() => {
                for key in &keys {
                    self.locks.ensure(key).map_err(|e| {
                        log::debug!("locks: {key}: {e}");
                        e.errno()
                    })?;
                }
                self.begin(&keys[0])?;
                Ok(OpGuard::none())
            }
            _ => {
                let Some(key) = key_of(node)? else {
                    return Ok(OpGuard::none());
                };
                self.acquire_content(&key, node)?;
                let guard = self.guard(vec![key.clone()]);
                self.begin(&key)?;
                Ok(guard)
            }
        }
    }

    /// Removing the last link of a regular file is a versioned change (it
    /// leaves a tombstone): its lock is a session like a write's.
    fn remove_session(&self, parent: NodeRef<'_>, name: &OsStr) -> Result<Option<OpGuard>, Errno> {
        let Some(key) = child(parent, name)? else {
            return Ok(None);
        };
        let Ok(st) = parent.stat_child(name) else {
            return Ok(None);
        };
        if rustix::fs::FileType::from_raw_mode(st.st_mode) != rustix::fs::FileType::RegularFile
            || st.st_nlink != 1
        {
            return Ok(None);
        }
        let Ok(dir) = parent.path() else {
            return Ok(None);
        };
        let file = self
            .repl
            .id_for(&crate::repl::core::path_bytes(&dir.join(name)));
        let ctx = FileCtx {
            file,
            local: self.repl.local_version(&file),
        };
        let acquired = self
            .locks
            .acquire_file(&key, Some(ctx), false)
            .map_err(|e| {
                log::debug!("locks: {key}: {e}");
                e.errno()
            })?;
        if !acquired.new_session {
            // Another reference's session (fresh already) does the remove.
            let guard = self.guard(vec![key.clone()]);
            self.begin(&key)?;
            return Ok(Some(guard));
        }
        self.repl.session_opened(file);
        // The kernel looked the name up before the lock was held: the
        // handoff may have put another file there (see acquire_content).
        let now = self
            .repl
            .id_for(&crate::repl::core::path_bytes(&dir.join(name)));
        if now != file {
            log::debug!("locks: {key}: another file is there after the handoff (ESTALE)");
            release(&self.locks, &self.repl, &key);
            self.locks.ready(&key);
            return Err(Errno::ESTALE);
        }
        // The tombstone descends from the newest version the lock knows
        // of, without fetching its content.
        let local = self.repl.local_version(&file);
        let superseded = crate::repl::version::target(&acquired.known)
            .newest
            .filter(|n| local.is_none_or(|l| *n > l));
        let Some((_, version)) = self.locks.session(&key) else {
            self.locks.ready(&key);
            return Ok(Some(self.guard(vec![key.clone()])));
        };
        let begun = self.repl.begin_remove(file, version, superseded);
        self.locks.ready(&key);
        if let Err(e) = begun {
            log::error!("replication: cannot record {file} as changing: {e}");
            release(&self.locks, &self.repl, &key);
            return Err(Errno::EIO);
        }
        let (locks, repl) = (self.locks.clone(), Arc::clone(&self.repl));
        Ok(Some(OpGuard::new(move || {
            // Recorded (the remove ended the session): the version exists.
            if !repl.in_session(&file) {
                locks.mark_written(&key);
            }
            release(&locks, &repl, &key);
        })))
    }

    /// Moves write locks under `from` to the same place under `to`.
    fn rekey(&self, from: &Key, to: &Key) {
        let mut by_ino = self.by_ino();
        for keys in by_ino.values_mut() {
            for key in keys.iter_mut() {
                let Some(new) = key.rebase(from, to) else {
                    continue;
                };
                match self.locks.acquire(&new) {
                    Ok(()) => {
                        // The session continues under the new key.
                        self.locks.move_session(key, &new);
                        release(&self.locks, &self.repl, key);
                        *key = new;
                    }
                    Err(e) => log::warn!(
                        "locks: {key} was renamed to {new} but locking the new path failed ({e}); \
                         writes will fail until it is reopened"
                    ),
                }
            }
        }
    }
}

impl WriteHooks for ClusterHooks {
    fn before(&self, op: &Mutation<'_>) -> Result<OpGuard, Errno> {
        if self.repl.is_rebuilding() {
            return Err(Errno::EROFS);
        }
        self.repl.rejoin_gate();
        match *op {
            Mutation::Write { node, .. }
            | Mutation::Allocate { node, .. }
            | Mutation::SetAttr { node, .. }
            | Mutation::Xattr { node, .. } => self.content_change(node),
            Mutation::Remove {
                parent,
                name,
                is_dir: false,
            } => match self.remove_session(parent, name)? {
                Some(guard) => Ok(guard),
                None => self.op_lock(vec![child(parent, name)?]),
            },
            Mutation::Create { parent, name, .. } | Mutation::Remove { parent, name, .. } => {
                self.op_lock(vec![child(parent, name)?])
            }
            Mutation::Link {
                new_parent,
                new_name,
                ..
            } => self.op_lock(vec![child(new_parent, new_name)?]),
            Mutation::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => self.op_lock_with(
                vec![child(parent, name)?, child(new_parent, new_name)?],
                true,
            ),
        }
    }

    fn failed(&self, op: &Mutation<'_>) {
        // A data change may have been partly applied: record it anyway.
        if matches!(
            op,
            Mutation::Write { .. } | Mutation::Allocate { .. } | Mutation::SetAttr { .. }
        ) {
            self.repl.record(op);
        }
    }

    fn after(&self, op: &Mutation<'_>) {
        self.repl.record(op);
        if let Mutation::Rename {
            parent,
            name,
            new_parent,
            new_name,
            ..
        } = *op
        {
            // The parents still exist: we hold locks under them.
            if let (Ok(Some(from)), Ok(Some(to))) =
                (child(parent, name), child(new_parent, new_name))
            {
                self.rekey(&from, &to);
            }
        }
    }

    fn open_write(&self, node: NodeRef<'_>) -> Result<(), Errno> {
        if self.repl.is_rebuilding() {
            return Err(Errno::EROFS);
        }
        self.repl.rejoin_gate();
        let Some(key) = key_of(node)? else {
            // The inode has no name any more: removed (by a peer's entry,
            // after the kernel looked it up). Opening it would write to a
            // file nobody else sees.
            return Err(Errno::ENOENT);
        };
        self.acquire_content(&key, node)?;
        self.by_ino().entry(node.ino()).or_default().push(key);
        Ok(())
    }

    fn open_read(&self, node: NodeRef<'_>) -> Result<(), Errno> {
        if !self.repl.is_rebuilding() {
            return Ok(());
        }
        // Rebuilding: a file not copied yet is copied first.
        let Some(file) = self.file_of(node) else {
            return Ok(());
        };
        if self.repl.fetch_missing(file, None) {
            Ok(())
        } else {
            log::warn!("rebuild: no peer could provide {file} yet");
            Err(Errno::EIO)
        }
    }

    fn release_write(&self, node: NodeRef<'_>) {
        let key = {
            let mut by_ino = self.by_ino();
            let Some(keys) = by_ino.get_mut(&node.ino()) else {
                return;
            };
            let key = keys.pop();
            if keys.is_empty() {
                by_ino.remove(&node.ino());
            }
            key
        };
        if let Some(key) = key {
            release(&self.locks, &self.repl, &key);
        }
    }
}

/// Drops a reference to `key`; the last one ends the session (its
/// changes go into the journal) before the lock is released.
fn release(locks: &LockManager, repl: &Repl, key: &Key) {
    let file = locks.last_reference(key);
    if let Some(file) = file {
        repl.end_session(file);
    }
    locks.release(key);
    if let Some(file) = file {
        repl.session_closed(file);
    }
}
