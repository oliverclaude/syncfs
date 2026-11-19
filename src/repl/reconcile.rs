//! Reconciling the namespace after an unclean stop. Namespace changes made
//! in the crash window (up to one store flush before the stop) may be on
//! the disk without their journal entry. Before local changes are taken
//! (see `rejoin`), the node lists the tree of a data peer at a moment when
//! both have applied exactly the same entries (and the peer has every
//! entry of this node's journal), and compares:
//!
//! - a path only here is a change of this node's that was not recorded:
//!   it is sent as a new file (`Create`, and its data);
//! - a path only there was removed or renamed away here without a record:
//!   it is restored from the peer (nothing is lost; a lost rename leaves
//!   both names).
//!
//! If that moment does not come before the rejoin gate opens (no peer, or
//! constant changes), the comparison is skipped with a warning.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Weak;
use std::time::Duration;

use crate::config::NodeId;
use crate::net::wire::{Entry, FileId, NodeKind, TreeNode, Version};

use super::backing::{attrs_of, kind_of};
use super::core::{lock, Repl};
use super::fresh::Copy;

impl Repl {
    /// Whether this node and `source` (whose listing had `positions`)
    /// had applied the same entries.
    fn same_positions(&self, positions: &[(NodeId, u64)]) -> bool {
        let pos: BTreeMap<NodeId, u64> = positions.iter().copied().collect();
        let st = self.state();
        if pos.get(&self.me).copied().unwrap_or(0) < st.journal.durable()
            || st.journal.durable() < st.journal.last()
        {
            return false;
        }
        self.peers
            .iter()
            .all(|o| st.applied.get(o).copied().unwrap_or(0) == pos.get(o).copied().unwrap_or(0))
    }

    /// Compares the local tree with `source`'s `nodes` (see the module
    /// docs). Returns (paths sent, paths restored).
    fn reconcile_with(&self, source: NodeId, nodes: Vec<TreeNode>) -> io::Result<(u64, u64)> {
        let theirs: BTreeMap<Vec<u8>, TreeNode> =
            nodes.into_iter().map(|n| (n.path.clone(), n)).collect();
        let mine = self.backing.list()?;
        let mut sent = 0;
        let mut by_inode: HashMap<(u64, u64), (FileId, Vec<u8>)> = HashMap::new();
        for p in &mine {
            if theirs.contains_key(p) {
                continue;
            }
            self.publish(p, &mut by_inode)?;
            sent += 1;
        }
        let mut restored = 0;
        for (p, n) in &theirs {
            if self.backing.exists(p) {
                continue;
            }
            match self.restore(source, n) {
                Ok(()) => restored += 1,
                Err(e) => log::warn!(
                    "reconcile: restoring {} from node {source}: {e}",
                    String::from_utf8_lossy(p)
                ),
            }
        }
        self.flusher.flush_now()?;
        self.cv.notify_all();
        Ok((sent, restored))
    }

    /// Sends the local `path` (not known to peers) as a new file.
    fn publish(
        &self,
        path: &[u8],
        by_inode: &mut HashMap<(u64, u64), (FileId, Vec<u8>)>,
    ) -> io::Result<()> {
        let stat = self.backing.stat(path)?;
        let kind = kind_of(&stat);
        let attrs = attrs_of(&stat);
        log::warn!(
            "reconcile: {} was not recorded before the unclean stop; sending it",
            String::from_utf8_lossy(path)
        );
        let mut st = self.state();
        if kind == NodeKind::File && stat.st_nlink > 1 {
            if let Some((id, existing)) = by_inode.get(&(stat.st_dev, stat.st_ino)) {
                let id = *id;
                st.index.insert(path, id);
                let entry = Entry::Link {
                    id,
                    existing: existing.clone(),
                    path: path.to_vec(),
                };
                self.append(&mut st, entry);
                return Ok(());
            }
        }
        let id = FileId::random();
        by_inode.insert((stat.st_dev, stat.st_ino), (id, path.to_vec()));
        let target = if kind == NodeKind::Symlink {
            drop(st);
            let t = self.backing.readlink(path)?;
            st = self.state();
            t
        } else {
            Vec::new()
        };
        st.index.insert(path, id);
        self.append(
            &mut st,
            Entry::Create {
                id,
                path: path.to_vec(),
                kind,
                attrs,
                rdev: stat.st_rdev,
                target,
            },
        );
        if kind == NodeKind::File && stat.st_size > 0 {
            let version = Version {
                seq: 1,
                node: self.me,
                disk: self.disk,
            };
            st.index.set_version(id, version);
            let blocks = (stat.st_size as u64).div_ceil(self.block_size);
            st.dirty.mark_all(id, version, blocks);
            let seq = self.append(
                &mut st,
                Entry::Data {
                    id,
                    path: path.to_vec(),
                    version,
                    size: stat.st_size as u64,
                    attrs,
                    lineage: Vec::new(),
                },
            );
            st.awaiting.insert(seq, (id, version));
            st.holders.entry((id, version)).or_default().insert(self.me);
        }
        Ok(())
    }

    /// Makes `n` (only on `source`) here again.
    fn restore(&self, source: NodeId, n: &TreeNode) -> io::Result<()> {
        log::warn!(
            "reconcile: {} is on node {source} but not here (a change not recorded before the \
             unclean stop); restoring it",
            String::from_utf8_lossy(&n.path)
        );
        let other = self
            .state()
            .index
            .rec(&n.id)
            .and_then(|r| r.paths.iter().find(|p| self.backing.exists(p)).cloned());
        if let (NodeKind::File, Some(existing)) = (n.kind, other) {
            self.backing.link(&existing, &n.path)?;
            self.state().index.insert(&n.path, n.id);
            return Ok(());
        }
        self.backing
            .create(&n.path, n.kind, &n.meta.attrs, n.rdev, &n.target)?;
        {
            let mut st = self.state();
            st.index.insert(&n.path, n.id);
            if n.kind == NodeKind::File {
                st.index.forget_version(n.id);
            }
        }
        if n.kind == NodeKind::File {
            let _g = self.hold(n.id);
            self.copy_from(n.id, source, Copy::Rebuild)?;
        }
        Ok(())
    }
}

/// Runs the comparison once the node has caught up (see the module docs).
pub(crate) fn reconcile_loop(weak: Weak<Repl>) {
    let mut warned = false;
    loop {
        let Some(r) = weak.upgrade() else { return };
        if r.stopping() {
            return;
        }
        if r.caught_up() {
            let _g = lock(&r.reconcile_lock);
            if r.rejoined.load(Ordering::Acquire) {
                break;
            }
            for p in r.data_peers_up() {
                let tree = match r.fetch_tree(p) {
                    Ok(Some(t)) => t,
                    Ok(None) => continue,
                    Err(e) => {
                        log::debug!("reconcile: listing the tree of node {p}: {e}");
                        continue;
                    }
                };
                let (positions, nodes) = tree;
                if !r.same_positions(&positions) {
                    continue;
                }
                match r.reconcile_with(p, nodes) {
                    Ok((sent, restored)) => {
                        log::info!(
                            "reconcile: compared with node {p}: {sent} path(s) sent, {restored} \
                             restored"
                        );
                        let mut st = r.state();
                        st.counters.reconcile_sent += sent;
                        st.counters.reconcile_restored += restored;
                        drop(st);
                        r.reconciled.store(true, Ordering::Release);
                        r.cv.notify_all();
                        return;
                    }
                    // A partial pass is not a comparison: try again (it
                    // is idempotent) until the rejoin deadline.
                    Err(e) if !warned => {
                        warned = true;
                        log::warn!("reconcile: comparing with node {p}: {e}; retrying");
                    }
                    Err(e) => log::debug!("reconcile: comparing with node {p}: {e}; retrying"),
                }
            }
        } else if r.rejoined.load(Ordering::Acquire) {
            break;
        }
        drop(r);
        std::thread::sleep(Duration::from_millis(200));
    }
    log::warn!(
        "reconcile: the namespace was not compared with a peer after the unclean stop; changes \
         of its last moments may be missing on peers"
    );
}
