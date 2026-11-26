//! Conflict copies. A node's change that another copy was written without
//! (a peer fell back to an older copy while this node was away) is kept as
//! `name.syncfs-conflict-<node>-<UTC time>` before the newer copy replaces
//! it. The conflict copy is a new file and replicates like one.

use std::io;
use std::time::SystemTime;

use super::net::ApplyError;
use crate::config::NodeId;
use crate::net::wire::{Attrs, Entry, FileId, NodeKind, Version};

use super::backing::attrs_of;
use super::core::Repl;
use super::files::diverged;

/// `path.syncfs-conflict-<me>-<YYYYMMDDTHHMMSSZ>`.
pub fn conflict_name(path: &[u8], me: NodeId, at: SystemTime) -> Vec<u8> {
    let ts: String = humantime::format_rfc3339_seconds(at)
        .to_string()
        .chars()
        .filter(|c| *c != '-' && *c != ':')
        .collect();
    let mut out = path.to_vec();
    out.extend_from_slice(format!(".syncfs-conflict-{me}-{ts}").as_bytes());
    out
}

/// A deterministic conflict path for a file set aside because two
/// FileIds met at `path`. The same version always maps to the same
/// name, so every node that applies the same rule keeps the loser
/// at the same path (issue #2).
pub fn versioned_conflict_name(path: &[u8], v: Version) -> Vec<u8> {
    let mut out = path.to_vec();
    out.extend_from_slice(format!(".syncfs-conflict-{}-{}-{}", v.node, v.seq, v.disk).as_bytes());
    out
}

/// A missing version is older than any real one (seq starts at 1).
fn version_or_none(v: Option<Version>) -> Version {
    v.filter(|v| v.seq > 0).unwrap_or(Version {
        seq: 0,
        node: 0,
        disk: 0,
    })
}

impl Repl {
    /// Keeps the local copy of `id` aside if replacing it with a copy at
    /// `incoming` (which went through `lineage`) would lose a change of
    /// this node's. The caller holds `id`. Returns whether it did.
    pub(crate) fn keep_if_diverged(
        &self,
        id: FileId,
        incoming: Version,
        lineage: &[Version],
    ) -> io::Result<bool> {
        let (local, path) = {
            let st = self.state();
            (
                st.index.version(&id),
                st.index.path_of(&id).map(<[u8]>::to_vec),
            )
        };
        let (Some(local), Some(path)) = (local, path) else {
            return Ok(false);
        };
        if !diverged(Some(local), self.me, self.disk, incoming, lineage) {
            return Ok(false);
        }
        self.conflict_copy(id, &path, local, incoming)?;
        Ok(true)
    }

    fn conflict_copy(
        &self,
        id: FileId,
        path: &[u8],
        local: Version,
        incoming: Version,
    ) -> io::Result<()> {
        let mut name = conflict_name(path, self.me, SystemTime::now());
        let base = name.clone();
        let mut n = 1;
        while self.backing.exists(&name) {
            n += 1;
            name = base.clone();
            name.extend_from_slice(format!("-{n}").as_bytes());
        }
        self.backing.copy_file(path, &name)?;
        let stat = self.backing.stat(&name)?;
        let new = FileId::random();
        let version = Version {
            seq: 1,
            node: self.me,
            disk: self.disk,
        };
        let mut st = self.state();
        st.index.insert(&name, new);
        st.index.set_version(new, version);
        // The original's marks described the change that is now in the
        // copy; the original becomes the incoming copy.
        st.dirty.forget(id);
        let blocks = (stat.st_size as u64).div_ceil(self.block_size);
        st.dirty.mark_all(new, version, blocks);
        let attrs = attrs_of(&stat);
        self.append(
            &mut st,
            Entry::Create {
                id: new,
                path: name.clone(),
                kind: NodeKind::File,
                attrs,
                rdev: 0,
                target: Vec::new(),
            },
        );
        let seq = self.append(
            &mut st,
            Entry::Data {
                id: new,
                path: name.clone(),
                version,
                size: stat.st_size as u64,
                attrs,
                lineage: Vec::new(),
            },
        );
        st.awaiting.insert(seq, (new, version));
        st.holders
            .entry((new, version))
            .or_default()
            .insert(self.me);
        st.counters.conflict_copies += 1;
        drop(st);
        if let Some(s) = self.stats.get() {
            s.conflict_copy();
        }
        self.flusher.flush_now()?;
        self.cv.notify_all();
        log::warn!(
            "replication: {} (version {} from node {}) was replaced by version {} from node {} \
             that does not include it; this node's copy is kept as {}",
            String::from_utf8_lossy(path),
            local.seq,
            local.node,
            incoming.seq,
            incoming.node,
            String::from_utf8_lossy(&name)
        );
        Ok(())
    }

    /// The newest known version of `id`: already in the index, or a
    /// Data/Attr still queued in any inbox (the Create entry itself
    /// carries no version).
    fn version_for(&self, id: FileId) -> Option<Version> {
        if let Some(v) = self.state().index.version(&id).filter(|v| v.seq > 0) {
            return Some(v);
        }
        let inbox = super::core::lock(&self.inbox);
        let mut found = None;
        for ib in inbox.values() {
            for q in ib.queue.values() {
                let v = match &q.entry {
                    Entry::Data {
                        id: fid, version, ..
                    } if *fid == id && version.seq > 0 => Some(*version),
                    Entry::Attr {
                        id: fid, version, ..
                    } if *fid == id && version.seq > 0 => Some(*version),
                    _ => None,
                };
                if let Some(v) = v {
                    found = Some(found.map_or(v, |f: Version| f.max(v)));
                }
            }
        }
        found
    }

    /// Moves `id` from `path` to the deterministic conflict name for
    /// `version`, keeping the same FileId and its version.
    fn sideline_as_conflict(
        &self,
        id: FileId,
        path: &[u8],
        version: Version,
    ) -> io::Result<Vec<u8>> {
        let name = versioned_conflict_name(path, version);
        if self.state().index.get(&name) == Some(id) && self.backing.exists(&name) {
            if self.state().index.get(path) == Some(id) {
                if let Err(e) = self.backing.remove(path, false) {
                    if e.kind() != io::ErrorKind::NotFound {
                        return Err(e);
                    }
                }
                self.invalidate_entry(path);
                let mut st = self.state();
                st.index.remove_path(path);
            }
            return Ok(name);
        }
        if self.backing.exists(path) {
            self.backing.copy_file(path, &name)?;
            if let Err(e) = self.backing.remove(path, false) {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(e);
                }
            }
        }
        self.invalidate_entry(path);
        self.invalidate_entry(&name);
        let mut st = self.state();
        // Add the conflict path first so remove_path is not the last link
        // (that would drop the FileRec and its version).
        st.index.insert(&name, id);
        if st.index.get(path) == Some(id) {
            st.index.remove_path(path);
        }
        drop(st);
        self.cv.notify_all();
        Ok(name)
    }

    /// `path` already names another file: pick a winner by Version (seq,
    /// then node, then disk; FileId breaks a tie) and keep the loser as
    /// a conflict copy with the same FileId, so later Data finds it and
    /// every node converges on the same names (issue #2).
    ///
    /// Does not skip: skipping would still advance `applied` and leave
    /// two FileIds at one path. Retries until the incoming version is
    /// known.
    pub(crate) fn apply_create_at_taken_path(
        &self,
        id: FileId,
        path: &[u8],
        kind: NodeKind,
        attrs: &Attrs,
        rdev: u64,
        target: &[u8],
    ) -> Result<bool, ApplyError> {
        let local_id = self.state().index.get(path);
        let Some(local_id) = local_id else {
            return Err(ApplyError::Retry(io::Error::other(
                "path exists but is not indexed; waiting",
            )));
        };
        if local_id == id {
            return Ok(false);
        }
        let Some(incoming_ver) = self.version_for(id) else {
            return Err(ApplyError::Retry(io::Error::other(
                "waiting for a version to compare with the file at this path",
            )));
        };
        let local_ver = version_or_none(self.state().index.version(&local_id));
        if (incoming_ver, id) > (local_ver, local_id) {
            self.sideline_as_conflict(local_id, path, local_ver)
                .map_err(ApplyError::Retry)?;
            self.backing
                .create(path, kind, attrs, rdev, target)
                .map_err(ApplyError::Retry)?;
            let mut st = self.state();
            st.index.insert(path, id);
            drop(st);
            self.cv.notify_all();
            Ok(true)
        } else {
            let name = versioned_conflict_name(path, incoming_ver);
            if self.state().index.get(&name) == Some(id) && self.backing.exists(&name) {
                return Ok(false);
            }
            if !self.backing.exists(&name) {
                self.backing
                    .create(&name, kind, attrs, rdev, target)
                    .map_err(ApplyError::Retry)?;
            }
            let mut st = self.state();
            st.index.insert(&name, id);
            drop(st);
            self.cv.notify_all();
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_names() {
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_790_000_000);
        assert_eq!(
            conflict_name(b"d/f.txt", 3, t),
            b"d/f.txt.syncfs-conflict-3-20260921T141320Z".to_vec()
        );
        assert_eq!(
            versioned_conflict_name(
                b"f",
                Version {
                    seq: 1,
                    node: 2,
                    disk: 7,
                },
            ),
            b"f.syncfs-conflict-2-1-7".to_vec()
        );
    }
}
