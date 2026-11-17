//! Conflict copies. A node's change that another copy was written without
//! (a peer fell back to an older copy while this node was away) is kept as
//! `name.syncfs-conflict-<node>-<UTC time>` before the newer copy replaces
//! it. The conflict copy is a new file and replicates like one.

use std::io;
use std::time::SystemTime;

use crate::config::NodeId;
use crate::net::wire::{Entry, FileId, NodeKind, Version};

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
    }
}
