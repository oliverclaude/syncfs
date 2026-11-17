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
//! * Every change that went through is then marked dirty
//!   (`crate::dirty`), while the lock is still held: lock, quorum, local
//!   write, mark dirty.
//!
//! No quorum: `EROFS`. Locked by another node for longer than `lock_wait`:
//! `EBUSY`.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::sync::{Arc, Mutex};

use fuser::Errno;

use crate::dirty::DirtyTracker;
use crate::fs::{Mutation, NodeRef, OpGuard, WriteHooks};

use super::key::Key;
use super::manager::LockManager;

pub struct ClusterHooks {
    locks: LockManager,
    dirty: Arc<DirtyTracker>,
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
    pub fn new(locks: LockManager, dirty: Arc<DirtyTracker>) -> ClusterHooks {
        ClusterHooks {
            locks,
            dirty,
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
            let res = if fail_on_held {
                self.locks.acquire_unless_held(&key)
            } else {
                self.locks.acquire(&key)
            };
            if let Err(e) = res {
                for k in &taken {
                    self.locks.release(k);
                }
                log::debug!("locks: {key}: {e}");
                return Err(e.errno());
            }
            taken.push(key);
        }
        if taken.is_empty() {
            return Ok(OpGuard::none());
        }
        let locks = self.locks.clone();
        Ok(OpGuard::new(move || {
            for k in &taken {
                locks.release(k);
            }
        }))
    }

    fn content_change(&self, node: NodeRef<'_>) -> Result<OpGuard, Errno> {
        let held = self.by_ino().get(&node.ino()).cloned();
        match held {
            Some(keys) if !keys.is_empty() => {
                for key in keys {
                    self.locks.ensure(&key).map_err(|e| {
                        log::debug!("locks: {key}: {e}");
                        e.errno()
                    })?;
                }
                Ok(OpGuard::none())
            }
            _ => self.op_lock(vec![key_of(node)?]),
        }
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
                        self.locks.release(key);
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
        match *op {
            Mutation::Write { node, .. }
            | Mutation::Allocate { node, .. }
            | Mutation::SetAttr { node, .. }
            | Mutation::Xattr { node, .. } => self.content_change(node),
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
        self.dirty.record_failed(op);
    }

    fn after(&self, op: &Mutation<'_>) {
        self.dirty.record(op);
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
        let Some(key) = key_of(node)? else {
            return Ok(());
        };
        self.locks.acquire(&key).map_err(|e| {
            log::debug!("locks: open {key} for writing: {e}");
            e.errno()
        })?;
        self.by_ino().entry(node.ino()).or_default().push(key);
        Ok(())
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
            self.locks.release(&key);
        }
    }
}
