//! Marking local changes dirty (step 6 of the write path).
//!
//! A cluster node's write path is: take the lock, check the quorum, change
//! the backing directory, then record what changed here. Replication
//! (step 7) drains this record; until then it only grows, and it lives in
//! memory, so a restart forgets it.
//!
//! * Data writes and `fallocate` mark the blocks (`block_size`) they touch.
//! * `truncate` records the new size and forgets marks past it.
//! * chmod, chown, utimens and xattr changes mark the file's metadata.
//! * create, link, unlink, rmdir and rename are counted as namespace
//!   changes (the metadata journal of step 7 will carry them).
//!
//! Files are keyed by inode number, so renaming a dirty file keeps its
//! marks.

mod ranges;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::json;

use crate::fs::{AttrChange, Mutation};
use crate::status::{FnProvider, Provider};

pub use ranges::Ranges;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FileDirty {
    /// Dirty block numbers.
    pub blocks: Ranges,
    /// Size set by the last truncate, if any.
    pub size: Option<u64>,
    /// Mode, owner, times or xattrs changed.
    pub meta: bool,
}

#[derive(Debug, Default)]
struct Inner {
    files: HashMap<u64, FileDirty>,
    namespace_changes: u64,
    marked_writes: u64,
}

pub struct DirtyTracker {
    block_size: u64,
    inner: Mutex<Inner>,
}

impl DirtyTracker {
    pub fn new(block_size: u64) -> DirtyTracker {
        assert!(block_size > 0);
        DirtyTracker {
            block_size,
            inner: Mutex::new(Inner::default()),
        }
    }

    fn inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Records a change that was applied (or partly applied: a failed write
    /// may still have changed some bytes, so data changes are recorded
    /// either way, see [`DirtyTracker::record_failed`]).
    pub fn record(&self, op: &Mutation<'_>) {
        let mut g = self.inner();
        match *op {
            Mutation::Write { node, offset, len }
            | Mutation::Allocate {
                node, offset, len, ..
            } => {
                g.marked_writes += 1;
                let blocks = self.blocks(offset, len);
                let f = g.files.entry(node.ino()).or_default();
                if let Some((a, b)) = blocks {
                    f.blocks.insert(a, b);
                }
            }
            Mutation::SetAttr { node, change } => {
                let AttrChange {
                    mode,
                    uid,
                    gid,
                    size,
                    times,
                } = change;
                let f = g.files.entry(node.ino()).or_default();
                if let Some(size) = size {
                    f.size = Some(size);
                    f.blocks.truncate(size.div_ceil(self.block_size));
                }
                if mode.is_some() || uid.is_some() || gid.is_some() || times {
                    f.meta = true;
                }
            }
            Mutation::Xattr { node, .. } => g.files.entry(node.ino()).or_default().meta = true,
            Mutation::Create { .. }
            | Mutation::Link { .. }
            | Mutation::Remove { .. }
            | Mutation::Rename { .. } => g.namespace_changes += 1,
        }
    }

    /// An operation failed after the hooks allowed it. Data changes may have
    /// been partly applied, so they are recorded; others are not.
    pub fn record_failed(&self, op: &Mutation<'_>) {
        if matches!(
            op,
            Mutation::Write { .. } | Mutation::Allocate { .. } | Mutation::SetAttr { .. }
        ) {
            self.record(op);
        }
    }

    /// Block range `[first, end)` covering bytes `[offset, offset + len)`.
    fn blocks(&self, offset: u64, len: u64) -> Option<(u64, u64)> {
        if len == 0 {
            return None;
        }
        let end = offset.saturating_add(len);
        Some((offset / self.block_size, end.div_ceil(self.block_size)))
    }

    /// The marks on inode `ino`.
    pub fn file(&self, ino: u64) -> Option<FileDirty> {
        self.inner().files.get(&ino).cloned()
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let g = self.inner();
        let blocks: u64 = g.files.values().map(|f| f.blocks.len()).sum();
        json!({
            "implemented": "dirty marks only, in memory; replication is step 7",
            "block_size": self.block_size,
            "dirty_files": g.files.len(),
            "dirty_blocks": blocks,
            "dirty_bytes_max": blocks.saturating_mul(self.block_size),
            "truncated_files": g.files.values().filter(|f| f.size.is_some()).count(),
            "meta_dirty_files": g.files.values().filter(|f| f.meta).count(),
            "namespace_changes": g.namespace_changes,
            "marked_writes": g.marked_writes,
            "journal_queue": g.namespace_changes,
            "persisted": false,
            "in_sync": g.files.is_empty() && g.namespace_changes == 0,
        })
    }

    /// The `replication` status provider.
    pub fn provider(self: &Arc<Self>) -> Arc<dyn Provider> {
        let me = Arc::clone(self);
        Arc::new(FnProvider::new("replication", move || me.snapshot()))
    }
}
