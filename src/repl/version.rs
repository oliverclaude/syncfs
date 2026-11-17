//! File versions: what they mean and how they are made. Everything about
//! version semantics lives here, so the rest of the code only moves
//! `Version`s and `VersionInfo`s around.
//!
//! * One version per **lock session** that changes the file: the holder
//!   picks it when it confirms the lock ([`next`]): one above everything
//!   it and the granting voters know of. A session that ends without a
//!   change withdraws it.
//! * A version is **committed** once a majority of the cluster (quorum,
//!   voter-only daemons counted) holds it completely.
//! * Every voter, daemons included, keeps a [`VersionTable`] and persists
//!   it; lock grants carry the voter's entry for the file, including the
//!   writer's disk id.
//! * Before writing, a new holder brings its copy up to the newest version
//!   the granting voters know ([`target`]); if only unreachable nodes have
//!   it, it waits up to `freshness_timeout`, then continues from the
//!   newest committed version it can reach.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::config::NodeId;
use crate::net::wire::{FileId, Outcome, Version, VersionInfo};

/// The version a session of node `me` on disk `disk` writes, given the
/// local copy's version and the voters' entries.
pub fn next<'a>(
    local: Option<Version>,
    known: impl IntoIterator<Item = &'a VersionInfo>,
    me: NodeId,
    disk: u64,
) -> Version {
    let mut top = local.map_or(0, |v| v.seq);
    for k in known {
        for v in [k.latest, k.committed].into_iter().flatten() {
            top = top.max(v.seq);
        }
    }
    Version {
        seq: top + 1,
        node: me,
        disk,
    }
}

/// Where a new holder has to get its copy from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Target {
    /// Newest version any granting voter knows of (None: nothing known).
    pub newest: Option<Version>,
    /// Nodes that have `newest` (the writer while it is pending).
    pub sources: Vec<NodeId>,
    /// Newest committed version and its holders: the fallback after
    /// `freshness_timeout`.
    pub committed: Option<(Version, Vec<NodeId>)>,
}

impl Target {
    /// Whether a copy at `local` is already fresh.
    pub fn satisfied_by(&self, local: Option<Version>) -> bool {
        self.newest.is_none_or(|n| local.is_some_and(|l| l >= n))
    }
}

/// Combines the grants' entries.
pub fn target<'a>(known: impl IntoIterator<Item = &'a VersionInfo>) -> Target {
    let mut t = Target::default();
    let mut sources: BTreeSet<NodeId> = BTreeSet::new();
    let mut committed: Option<(Version, BTreeSet<NodeId>)> = None;
    for k in known {
        if let Some(l) = k.latest {
            let who: Vec<NodeId> = if k.pending {
                vec![l.node]
            } else {
                k.holders.clone()
            };
            match t.newest {
                Some(n) if n > l => {}
                Some(n) if n == l => sources.extend(who),
                _ => {
                    t.newest = Some(l);
                    sources = who.into_iter().collect();
                }
            }
        }
        if let Some(c) = k.committed {
            match &mut committed {
                Some((v, _)) if *v > c => {}
                Some((v, h)) if *v == c => h.extend(k.committed_holders.iter().copied()),
                _ => committed = Some((c, k.committed_holders.iter().copied().collect())),
            }
        }
    }
    t.sources = sources.into_iter().collect();
    t.committed = committed.map(|(v, h)| (v, h.into_iter().collect()));
    t
}

/// A voter's entry for one file, plus what to fall back to when an
/// announced version is withdrawn.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub info: VersionInfo,
    /// `latest`, `pending` and `holders` before the last announcement.
    before: Option<(Option<Version>, bool, Vec<NodeId>)>,
}

/// A voter's versions table. Pure; persisted by `crate::repl::store`.
#[derive(Debug, Default)]
pub struct VersionTable {
    quorum: usize,
    files: BTreeMap<FileId, Record>,
    /// Entries changed since the last [`take_changed`](Self::take_changed).
    changed: BTreeSet<FileId>,
}

impl VersionTable {
    pub fn new(quorum: usize) -> VersionTable {
        VersionTable {
            quorum,
            ..Default::default()
        }
    }

    pub fn load(&mut self, file: FileId, r: Record) {
        self.files.insert(file, r);
    }

    pub fn get(&self, file: &FileId) -> Option<&VersionInfo> {
        self.files.get(file).map(|r| &r.info)
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// The entries changed since the last call, with their current value.
    pub fn take_changed(&mut self) -> Vec<(FileId, Option<Record>)> {
        std::mem::take(&mut self.changed)
            .into_iter()
            .map(|f| (f, self.files.get(&f).cloned()))
            .collect()
    }

    fn update(&mut self, file: FileId, f: impl FnOnce(&mut Record)) {
        let r = self.files.entry(file).or_default();
        let old = r.clone();
        f(r);
        if *r != old {
            self.changed.insert(file);
        }
    }

    fn commit_check(quorum: usize, i: &mut VersionInfo) {
        if !i.pending && i.holders.len() >= quorum && i.latest > i.committed {
            i.committed = i.latest;
            i.committed_holders = i.holders.clone();
        }
    }

    /// A holder confirmed a lock and announced `version`.
    pub fn on_confirm(&mut self, file: FileId, version: Version) {
        self.update(file, |r| {
            if r.info.latest.is_some_and(|l| l >= version) {
                return; // a repeated confirm (late grant, re-acquire)
            }
            r.before = Some((r.info.latest, r.info.pending, r.info.holders.clone()));
            r.info.latest = Some(version);
            r.info.pending = true;
            r.info.holders.clear();
        });
    }

    /// The session ended.
    pub fn on_release(&mut self, o: Outcome) {
        let quorum = self.quorum;
        self.update(o.file, |r| {
            if r.info.latest != Some(o.version) || !r.info.pending {
                return;
            }
            if o.written {
                r.info.pending = false;
                if !r.info.holders.contains(&o.version.node) {
                    r.info.holders.push(o.version.node);
                }
                r.before = None;
                Self::commit_check(quorum, &mut r.info);
            } else if let Some((latest, pending, holders)) = r.before.take() {
                r.info.latest = latest;
                r.info.pending = pending;
                r.info.holders = holders;
            }
        });
    }

    /// `holders` have `version` completely.
    pub fn on_holders(&mut self, file: FileId, version: Version, holders: &[NodeId]) {
        let quorum = self.quorum;
        self.update(file, |r| {
            let i = &mut r.info;
            if i.latest.is_none_or(|l| version > l) {
                i.latest = Some(version);
                i.pending = false;
                i.holders.clear();
                r.before = None;
            }
            if i.latest == Some(version) {
                // The writer has written it if others hold it.
                i.pending = false;
                for h in holders {
                    if !i.holders.contains(h) {
                        i.holders.push(*h);
                    }
                }
                i.holders.sort_unstable();
                Self::commit_check(quorum, i);
            } else if holders.len() >= quorum && i.committed.is_none_or(|c| version > c) {
                i.committed = Some(version);
                i.committed_holders = holders.to_vec();
            }
        });
    }

    /// Counts for the status files.
    pub fn summary(&self) -> serde_json::Value {
        let pending = self.files.values().filter(|r| r.info.pending).count();
        let committed = self
            .files
            .values()
            .filter(|r| r.info.latest.is_some() && r.info.latest == r.info.committed)
            .count();
        serde_json::json!({
            "files": self.files.len(),
            "pending": pending,
            "latest_committed": committed,
        })
    }
}

/// A version table loaded from the store and saved by `flusher`.
pub fn persisted(
    quorum: usize,
    flusher: &crate::repl::store::Flusher,
) -> std::io::Result<std::sync::Arc<std::sync::Mutex<VersionTable>>> {
    use crate::repl::store::{decode, encode, Table};
    let mut t = VersionTable::new(quorum);
    for (k, v) in flusher.store().load(Table::Versions)? {
        let Ok(id) = <[u8; 16]>::try_from(k.as_slice()) else {
            continue;
        };
        t.load(FileId(id), decode(&v)?);
    }
    let t = std::sync::Arc::new(std::sync::Mutex::new(t));
    let weak = std::sync::Arc::downgrade(&t);
    flusher.add_source(Box::new(move || {
        let Some(t) = weak.upgrade() else {
            return Default::default();
        };
        let changed = t.lock().unwrap_or_else(|e| e.into_inner()).take_changed();
        changed
            .into_iter()
            .map(|(f, r)| (Table::Versions, f.0.to_vec(), r.map(|r| encode(&r))))
            .collect::<Vec<_>>()
            .into()
    }));
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(seq: u64, node: NodeId) -> Version {
        Version {
            seq,
            node,
            disk: node as u64 * 10,
        }
    }
    const F: FileId = FileId([1; 16]);

    #[test]
    fn next_is_above_everything_known() {
        let k = VersionInfo {
            latest: Some(v(7, 2)),
            committed: Some(v(5, 1)),
            ..Default::default()
        };
        assert_eq!(next(Some(v(3, 1)), [&k], 1, 10), v(8, 1));
        assert_eq!(next(None, [], 3, 30), v(1, 3));
        assert_eq!(next(Some(v(9, 3)), [&k], 1, 10).seq, 10);
    }

    #[test]
    fn session_lifecycle_and_commit() {
        let mut t = VersionTable::new(2);
        t.on_confirm(F, v(1, 1));
        let i = t.get(&F).unwrap();
        assert!(i.pending && i.latest == Some(v(1, 1)) && i.holders.is_empty());
        t.on_release(Outcome {
            file: F,
            version: v(1, 1),
            written: true,
        });
        let i = t.get(&F).unwrap();
        assert!(!i.pending && i.holders == [1] && i.committed.is_none());
        t.on_holders(F, v(1, 1), &[1, 2]);
        let i = t.get(&F).unwrap();
        assert_eq!(i.committed, Some(v(1, 1)));
        assert_eq!(i.committed_holders, [1, 2]);
        // An unused session is withdrawn.
        t.on_confirm(F, v(2, 3));
        t.on_release(Outcome {
            file: F,
            version: v(2, 3),
            written: false,
        });
        assert_eq!(t.get(&F).unwrap().latest, Some(v(1, 1)));
        assert_eq!(t.get(&F).unwrap().holders, [1, 2]);
        assert!(!t.take_changed().is_empty());
        assert!(t.take_changed().is_empty());
    }

    #[test]
    fn crashed_writer_stays_pending_and_target_waits_for_it() {
        let mut a = VersionTable::new(2);
        a.on_confirm(F, v(1, 1));
        a.on_release(Outcome {
            file: F,
            version: v(1, 1),
            written: true,
        });
        a.on_holders(F, v(1, 1), &[1, 2]);
        a.on_confirm(F, v(2, 3)); // node 3 crashes mid-session
        let b = VersionTable::new(2); // a voter that knows nothing
        let t = target([a.get(&F).unwrap(), &VersionInfo::default()]);
        assert_eq!(t.newest, Some(v(2, 3)));
        assert_eq!(t.sources, [3]);
        assert_eq!(t.committed, Some((v(1, 1), vec![1, 2])));
        assert!(!t.satisfied_by(Some(v(1, 1))));
        assert!(t.satisfied_by(Some(v(2, 3))));
        assert!(target(b.get(&F)).satisfied_by(None));
    }

    #[test]
    fn holders_from_a_missed_session_move_latest_on() {
        let mut t = VersionTable::new(2);
        t.on_holders(F, v(4, 2), &[2]);
        let i = t.get(&F).unwrap();
        assert_eq!(i.latest, Some(v(4, 2)));
        assert!(!i.pending && i.committed.is_none());
        t.on_holders(F, v(4, 2), &[1]);
        assert_eq!(t.get(&F).unwrap().committed, Some(v(4, 2)));
        // Older news only moves `committed`.
        t.on_holders(F, v(3, 1), &[1, 3]);
        assert_eq!(t.get(&F).unwrap().committed, Some(v(4, 2)));
    }
}
