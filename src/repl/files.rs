//! The file index: cluster-wide [`FileId`]s for the local backing files,
//! by path, with the version each local copy holds. Persisted in the
//! store (keyed by id); renames move the paths, never the id.
//!
//! Paths are relative to the backing directory, as bytes, without a
//! leading slash; the root is the empty path.
//!
//! Ids are random for files created through syncfs. A file that was
//! already there gets an id derived from its path when first seen
//! ([`FileId::adopted`]), so nodes that start with the same tree agree.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::net::wire::{FileId, Version};

use super::store::{decode, encode, Op, Table};

impl FileId {
    /// The id of a pre-existing file first seen at `path`.
    pub fn adopted(path: &[u8]) -> FileId {
        let mut h = blake3::Hasher::new();
        h.update(b"syncfs adopted file\0");
        h.update(path);
        let mut id = [0; 16];
        id.copy_from_slice(&h.finalize().as_bytes()[..16]);
        FileId(id)
    }

    /// A new random id.
    pub fn random() -> FileId {
        let mut id = [0; 16];
        if rustix::rand::getrandom(&mut id, rustix::rand::GetRandomFlags::empty()).is_err() {
            // Fall back to hashing the time and the process.
            let mut h = blake3::Hasher::new();
            let t = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            h.update(&t.as_nanos().to_le_bytes());
            h.update(&std::process::id().to_le_bytes());
            h.update(&(&id as *const _ as usize).to_le_bytes());
            id.copy_from_slice(&h.finalize().as_bytes()[..16]);
        }
        FileId(id)
    }
}

/// Regular files with more than one link already in the backing
/// directory `root`, as `(paths, sorted)` per inode. Nodes that start with
/// the same tree give such a file the id of its first path, so its links
/// stay one file everywhere. Does not cross into other filesystems.
pub fn scan_hard_links(root: &std::path::Path) -> std::io::Result<Vec<Vec<Vec<u8>>>> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;
    let dev = std::fs::symlink_metadata(root)?.dev();
    let mut by_inode: HashMap<u64, Vec<Vec<u8>>> = HashMap::new();
    let mut stack: Vec<Vec<u8>> = vec![Vec::new()];
    while let Some(rel) = stack.pop() {
        let dir = root.join(std::ffi::OsStr::from_bytes(&rel));
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name();
            if rel.is_empty() && name == crate::fs::virt::NAME {
                continue;
            }
            let mut p = rel.clone();
            if !p.is_empty() {
                p.push(b'/');
            }
            p.extend_from_slice(name.as_bytes());
            // `file_type` does not follow symlinks: a symlink is neither
            // a directory to descend into nor a name of the file.
            let Ok(t) = e.file_type() else { continue };
            if !t.is_dir() && !t.is_file() {
                continue;
            }
            let Ok(md) = e.metadata() else { continue };
            if md.dev() != dev {
                continue;
            }
            if t.is_dir() {
                stack.push(p);
            } else if md.nlink() > 1 {
                by_inode.entry(md.ino()).or_default().push(p);
            }
        }
    }
    let mut out: Vec<Vec<Vec<u8>>> = by_inode
        .into_values()
        .filter(|v| v.len() > 1)
        .map(|mut v| {
            v.sort();
            v
        })
        .collect();
    out.sort();
    Ok(out)
}

/// Is `p` equal to `prefix` or below it?
pub fn under(p: &[u8], prefix: &[u8]) -> bool {
    prefix.is_empty()
        || p == prefix
        || (p.starts_with(prefix) && p.get(prefix.len()) == Some(&b'/'))
}

/// `p` moved from below `from` to below `to`.
fn rebase(p: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    let mut out = to.to_vec();
    out.extend_from_slice(&p[from.len()..]);
    out
}

/// How many earlier versions a copy remembers.
pub const LINEAGE: usize = 16;

/// How long a removed file's record (its tombstone) is kept.
pub const TOMBSTONE_KEEP: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 3600);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRec {
    /// Every path of the file (several for hard links), sorted. Empty for
    /// a tombstone.
    pub paths: Vec<Vec<u8>>,
    /// The version the local copy holds (a tombstone: the remove's).
    pub version: Option<Version>,
    /// Earlier versions this copy went through, oldest first (at most
    /// [`LINEAGE`]).
    pub lineage: Vec<Version>,
    /// Removed (a tombstone) at this time (seconds since the epoch).
    pub removed_at: Option<u64>,
}

/// The record format before lineage and tombstones.
#[derive(Deserialize)]
struct FileRecV0 {
    paths: Vec<Vec<u8>>,
    version: Option<Version>,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether `local`, a version this node (`me` on `disk`) made, is missing
/// from a copy at `incoming` that went through `lineage`: that copy was
/// written without this node's change (after a fallback), so the change
/// must be kept aside before the copy replaces it.
pub fn diverged(
    local: Option<Version>,
    me: crate::config::NodeId,
    disk: u64,
    incoming: Version,
    lineage: &[Version],
) -> bool {
    let Some(l) = local else { return false };
    l.seq > 0
        && l.node == me
        && l.disk == disk
        && l < incoming
        && !lineage.contains(&l)
        // Older than everything a full lineage remembers: assume it is in.
        && !(lineage.len() >= LINEAGE && lineage.first().is_some_and(|f| l < *f))
}

/// Whether a copy at `local` can take `incoming`'s changed blocks: it is
/// that version already or one `incoming` went through. Otherwise the
/// whole file is compared.
pub fn same_base(local: Option<Version>, incoming: Version, lineage: &[Version]) -> bool {
    match local {
        None => true,
        Some(l) => {
            l == incoming
                || lineage.contains(&l)
                || (lineage.len() >= LINEAGE && lineage.first().is_some_and(|f| l < *f))
        }
    }
}

#[derive(Debug, Default)]
pub struct Index {
    by_path: BTreeMap<Vec<u8>, FileId>,
    files: HashMap<FileId, FileRec>,
    changed: BTreeSet<FileId>,
}

impl Index {
    pub fn load(rows: Vec<(Vec<u8>, Vec<u8>)>) -> std::io::Result<Index> {
        let mut ix = Index::default();
        for (k, v) in rows {
            let Ok(id) = <[u8; 16]>::try_from(k.as_slice()) else {
                continue;
            };
            let rec: FileRec = match decode(&v) {
                Ok(r) => r,
                Err(_) => {
                    let old: FileRecV0 = decode(&v)?;
                    FileRec {
                        paths: old.paths,
                        version: old.version,
                        ..FileRec::default()
                    }
                }
            };
            for p in &rec.paths {
                ix.by_path.insert(p.clone(), FileId(id));
            }
            ix.files.insert(FileId(id), rec);
        }
        Ok(ix)
    }

    /// Live files (not tombstones).
    pub fn len(&self) -> usize {
        self.files().count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, path: &[u8]) -> Option<FileId> {
        self.by_path.get(path).copied()
    }

    /// A live file's record (not a tombstone's).
    pub fn rec(&self, id: &FileId) -> Option<&FileRec> {
        self.files.get(id).filter(|r| !r.paths.is_empty())
    }

    /// A current path of `id`.
    pub fn path_of(&self, id: &FileId) -> Option<&[u8]> {
        self.files.get(id)?.paths.first().map(Vec::as_slice)
    }

    pub fn version(&self, id: &FileId) -> Option<Version> {
        self.files.get(id)?.version
    }

    /// Earlier versions of the local copy (oldest first).
    pub fn lineage(&self, id: &FileId) -> Vec<Version> {
        self.files
            .get(id)
            .map(|r| r.lineage.clone())
            .unwrap_or_default()
    }

    /// The local copy is now at `v`, a change on top of what it was.
    pub fn set_version(&mut self, id: FileId, v: Version) {
        if let Some(r) = self.files.get_mut(&id) {
            if r.version.is_none_or(|old| v > old) {
                if let Some(old) = r.version {
                    r.lineage.push(old);
                    let excess = r.lineage.len().saturating_sub(LINEAGE);
                    r.lineage.drain(..excess);
                }
                r.version = Some(v);
                self.changed.insert(id);
            }
        }
    }

    /// The local copy is now a copy of another node's `v`, which went
    /// through `lineage`.
    pub fn take_version(&mut self, id: FileId, v: Version, lineage: &[Version]) {
        if let Some(r) = self.files.get_mut(&id) {
            if r.version.is_none_or(|old| v > old) {
                r.version = Some(v);
                r.lineage = lineage[lineage.len().saturating_sub(LINEAGE)..].to_vec();
                self.changed.insert(id);
            }
        }
    }

    /// Forgets which version the local copy of `id` is (it is not to be
    /// trusted: a rebuild compares it with a peer's in full).
    pub fn forget_version(&mut self, id: FileId) {
        if let Some(r) = self.files.get_mut(&id) {
            if r.version.is_some() || !r.lineage.is_empty() {
                r.version = None;
                r.lineage.clear();
                self.changed.insert(id);
            }
        }
    }

    /// Every indexed path.
    pub fn paths(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.by_path.keys().map(Vec::as_slice)
    }

    /// The version `id` was removed at, if it is a tombstone.
    pub fn tombstone(&self, id: &FileId) -> Option<Version> {
        let r = self.files.get(id)?;
        r.removed_at.and(r.version)
    }

    /// `id`'s last path is gone (removed at `v`): keeps its record as a
    /// tombstone.
    pub fn bury(&mut self, id: FileId, v: Version) {
        let r = self.files.entry(id).or_default();
        for p in std::mem::take(&mut r.paths) {
            self.by_path.remove(&p);
        }
        if let Some(old) = r.version.filter(|old| *old < v) {
            r.lineage.push(old);
            let excess = r.lineage.len().saturating_sub(LINEAGE);
            r.lineage.drain(..excess);
        }
        if r.version.is_none_or(|old| old < v) {
            r.version = Some(v);
        }
        r.removed_at = Some(now_secs());
        self.changed.insert(id);
    }

    /// Drops tombstones older than [`TOMBSTONE_KEEP`].
    pub fn expire_tombstones(&mut self) -> usize {
        let cut = now_secs().saturating_sub(TOMBSTONE_KEEP.as_secs());
        let old: Vec<FileId> = self
            .files
            .iter()
            .filter(|(_, r)| r.paths.is_empty() && r.removed_at.is_some_and(|t| t < cut))
            .map(|(id, _)| *id)
            .collect();
        for id in &old {
            self.files.remove(id);
            self.changed.insert(*id);
        }
        old.len()
    }

    /// Every live file: id, record.
    pub fn files(&self) -> impl Iterator<Item = (&FileId, &FileRec)> {
        self.files.iter().filter(|(_, r)| !r.paths.is_empty())
    }

    /// The id at `path`, adopting the file there if it has none.
    pub fn id_at(&mut self, path: &[u8]) -> FileId {
        if let Some(id) = self.get(path) {
            return id;
        }
        let id = FileId::adopted(path);
        self.insert(path, id);
        id
    }

    /// `path` now names `id` (whatever it named before loses it).
    pub fn insert(&mut self, path: &[u8], id: FileId) {
        if self.get(path) == Some(id) {
            return;
        }
        self.remove_path(path);
        self.by_path.insert(path.to_vec(), id);
        let r = self.files.entry(id).or_default();
        // A tombstone comes back to life.
        r.removed_at = None;
        if let Err(i) = r.paths.binary_search_by(|p| p.as_slice().cmp(path)) {
            r.paths.insert(i, path.to_vec());
        }
        self.changed.insert(id);
    }

    /// Forgets `path` (one link of its file); returns the id it named.
    pub fn remove_path(&mut self, path: &[u8]) -> Option<FileId> {
        let id = self.by_path.remove(path)?;
        if let Some(r) = self.files.get_mut(&id) {
            r.paths.retain(|p| p != path);
            if r.paths.is_empty() && r.removed_at.is_none() {
                self.files.remove(&id);
            }
        }
        self.changed.insert(id);
        Some(id)
    }

    /// Every indexed path at or below `prefix`.
    fn subtree(&self, prefix: &[u8]) -> Vec<(Vec<u8>, FileId)> {
        self.by_path
            .range(prefix.to_vec()..)
            .take_while(|(p, _)| p.starts_with(prefix))
            .filter(|(p, _)| under(p, prefix))
            .map(|(p, id)| (p.clone(), *id))
            .collect()
    }

    /// `from` (and everything below) was renamed to `to`, replacing
    /// whatever was there.
    pub fn rename(&mut self, from: &[u8], to: &[u8]) {
        if from == to {
            return;
        }
        let moving = self.subtree(from);
        let versions = self.versions_of(&moving);
        for (p, _) in self.subtree(to) {
            self.remove_path(&p);
        }
        for (p, _) in &moving {
            self.remove_path(p);
        }
        for (p, id) in moving {
            self.insert(&rebase(&p, from, to), id);
        }
        self.restore_versions(versions);
    }

    /// The versions of the files at `paths` (which are about to be
    /// removed and inserted again).
    fn versions_of(&self, paths: &[(Vec<u8>, FileId)]) -> Vec<(FileId, Version)> {
        paths
            .iter()
            .filter_map(|(_, id)| Some((*id, self.version(id)?)))
            .collect()
    }

    fn restore_versions(&mut self, versions: Vec<(FileId, Version)>) {
        for (id, v) in versions {
            self.set_version(id, v);
        }
    }

    /// `a` and `b` swapped places (`RENAME_EXCHANGE`).
    pub fn exchange(&mut self, a: &[u8], b: &[u8]) {
        let sa = self.subtree(a);
        let sb = self.subtree(b);
        let mut versions = self.versions_of(&sa);
        versions.extend(self.versions_of(&sb));
        for (p, _) in sa.iter().chain(&sb) {
            self.remove_path(p);
        }
        for (p, id) in sa {
            self.insert(&rebase(&p, a, b), id);
        }
        for (p, id) in sb {
            self.insert(&rebase(&p, b, a), id);
        }
        self.restore_versions(versions);
    }

    /// Gives every path in `links` (one file's hard links, sorted) the id
    /// adopted from the first, unless a path already has an id other than
    /// its own adopted one (it was created or renamed through syncfs).
    pub fn adopt_links(&mut self, links: &[Vec<u8>]) -> bool {
        let Some(first) = links.first() else {
            return false;
        };
        let id = FileId::adopted(first);
        let mut changed = false;
        for p in links {
            match self.get(p) {
                Some(known) if known == id => {}
                Some(known) if known != FileId::adopted(p) => {}
                _ => {
                    let v = self.get(p).and_then(|k| self.version(&k));
                    self.insert(p, id);
                    if let Some(v) = v {
                        self.set_version(id, v);
                    }
                    changed = true;
                }
            }
        }
        changed
    }

    /// Store operations for everything changed since the last call.
    pub fn take_changed(&mut self) -> Vec<Op> {
        std::mem::take(&mut self.changed)
            .into_iter()
            .map(|id| (Table::Files, id.0.to_vec(), self.files.get(&id).map(encode)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> FileId {
        FileId([n; 16])
    }

    fn ver(seq: u64, node: crate::config::NodeId) -> Version {
        Version {
            seq,
            node,
            disk: node as u64,
        }
    }

    /// A symlink to a hard-linked file, or to a directory holding one, is
    /// not taken for another link of it (issue #1).
    #[test]
    fn hard_link_scan_skips_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir(root.join("d")).unwrap();
        std::fs::write(root.join("d/a"), b"x").unwrap();
        std::fs::hard_link(root.join("d/a"), root.join("d/b")).unwrap();
        std::os::unix::fs::symlink("d/a", root.join("s")).unwrap();
        std::os::unix::fs::symlink("d", root.join("sd")).unwrap();
        let want = vec![vec![b"d/a".to_vec(), b"d/b".to_vec()]];
        assert_eq!(scan_hard_links(root).unwrap(), want);
    }

    #[test]
    fn lineage_and_divergence() {
        let mut ix = Index::default();
        ix.insert(b"f", id(1));
        ix.set_version(id(1), ver(1, 2));
        ix.set_version(id(1), ver(2, 1));
        assert_eq!(ix.lineage(&id(1)), vec![ver(1, 2)]);
        // A copy of node 2's version 3 that went through 1 and 2.
        ix.take_version(id(1), ver(3, 2), &[ver(1, 2), ver(2, 1)]);
        assert_eq!(ix.lineage(&id(1)), vec![ver(1, 2), ver(2, 1)]);
        for n in 4..40 {
            ix.set_version(id(1), ver(n, 1));
        }
        assert_eq!(ix.lineage(&id(1)).len(), LINEAGE);

        // Node 1's version 5 is in the incoming lineage: no conflict.
        let l = [ver(4, 2), ver(5, 1)];
        assert!(!diverged(Some(ver(5, 1)), 1, 1, ver(6, 2), &l));
        // Written over without it: a conflict, and not the same base.
        let l = [ver(4, 2)];
        assert!(diverged(Some(ver(5, 1)), 1, 1, ver(6, 2), &l));
        assert!(!same_base(Some(ver(5, 1)), ver(6, 2), &l));
        // Another node's version, or another disk's: no conflict here (but
        // still the whole file is compared).
        assert!(!diverged(Some(ver(5, 3)), 1, 1, ver(6, 2), &l));
        assert!(!diverged(Some(ver(5, 1)), 1, 9, ver(6, 2), &l));
        assert!(!same_base(Some(ver(5, 3)), ver(6, 2), &l));
        // The same version (an interim entry) or a newer local one.
        assert!(!diverged(Some(ver(6, 2)), 2, 2, ver(6, 2), &l));
        assert!(!diverged(Some(ver(7, 1)), 1, 1, ver(6, 2), &l));
        assert!(same_base(None, ver(6, 2), &l));
        // Older than a full lineage remembers.
        let full: Vec<Version> = (10..10 + LINEAGE as u64).map(|n| ver(n, 2)).collect();
        assert!(!diverged(Some(ver(3, 1)), 1, 1, ver(40, 2), &full));
        assert!(same_base(Some(ver(3, 1)), ver(40, 2), &full));
    }

    #[test]
    fn tombstones_are_kept_and_come_back() {
        let mut ix = Index::default();
        ix.insert(b"f", id(1));
        ix.set_version(id(1), ver(1, 1));
        ix.bury(id(1), ver(2, 1));
        assert_eq!(ix.get(b"f"), None);
        assert!(ix.rec(&id(1)).is_none());
        assert_eq!(ix.tombstone(&id(1)), Some(ver(2, 1)));
        assert_eq!(ix.version(&id(1)), Some(ver(2, 1)));
        assert_eq!(ix.lineage(&id(1)), vec![ver(1, 1)]);
        assert_eq!(ix.len(), 0);
        // Persisted and loaded again.
        let rows: Vec<(Vec<u8>, Vec<u8>)> = ix
            .take_changed()
            .into_iter()
            .filter_map(|(_, k, v)| Some((k, v?)))
            .collect();
        let mut ix = Index::load(rows).unwrap();
        assert_eq!(ix.tombstone(&id(1)), Some(ver(2, 1)));
        assert_eq!(ix.expire_tombstones(), 0);
        // A newer write brings it back.
        ix.insert(b"f", id(1));
        ix.take_version(id(1), ver(3, 2), &[ver(1, 1), ver(2, 1)]);
        assert_eq!(ix.tombstone(&id(1)), None);
        assert_eq!(ix.path_of(&id(1)), Some(&b"f"[..]));
    }

    #[test]
    fn old_records_load() {
        #[derive(Serialize)]
        struct V0 {
            paths: Vec<Vec<u8>>,
            version: Option<Version>,
        }
        let v0 = encode(&V0 {
            paths: vec![b"f".to_vec()],
            version: Some(ver(1, 1)),
        });
        let ix = Index::load(vec![(id(1).0.to_vec(), v0)]).unwrap();
        assert_eq!(ix.version(&id(1)), Some(ver(1, 1)));
        assert_eq!(ix.get(b"f"), Some(id(1)));
    }

    #[test]
    fn adopted_ids_are_stable_and_distinct() {
        assert_eq!(FileId::adopted(b"a/b"), FileId::adopted(b"a/b"));
        assert_ne!(FileId::adopted(b"a/b"), FileId::adopted(b"a/c"));
        assert_ne!(FileId::random(), FileId::random());
    }

    #[test]
    fn rename_moves_subtrees_and_keeps_ids() {
        let mut ix = Index::default();
        ix.insert(b"d", id(1));
        ix.insert(b"d/f", id(2));
        ix.insert(b"d/g/h", id(3));
        ix.insert(b"dd", id(4));
        ix.insert(b"e", id(5));
        ix.rename(b"d", b"e");
        assert_eq!(ix.get(b"e"), Some(id(1)));
        assert_eq!(ix.get(b"e/f"), Some(id(2)));
        assert_eq!(ix.get(b"e/g/h"), Some(id(3)));
        assert_eq!(ix.get(b"dd"), Some(id(4)));
        assert_eq!(ix.get(b"d/f"), None);
        assert!(ix.rec(&id(5)).is_none(), "the replaced file is gone");
        ix.exchange(b"e/f", b"dd");
        assert_eq!(ix.get(b"dd"), Some(id(2)));
        assert_eq!(ix.get(b"e/f"), Some(id(4)));
    }

    #[test]
    fn versions_survive_renames() {
        let v = |seq| Version {
            seq,
            node: 1,
            disk: 9,
        };
        let mut ix = Index::default();
        ix.insert(b"d/f", id(1));
        ix.insert(b"g", id(2));
        ix.set_version(id(1), v(3));
        ix.set_version(id(2), v(4));
        ix.rename(b"d", b"e");
        assert_eq!(ix.version(&id(1)), Some(v(3)));
        ix.exchange(b"e/f", b"g");
        assert_eq!(ix.version(&id(1)), Some(v(3)));
        assert_eq!(ix.version(&id(2)), Some(v(4)));
        assert_eq!(ix.get(b"g"), Some(id(1)));
    }

    #[test]
    fn hard_links_and_persistence() {
        let mut ix = Index::default();
        ix.insert(b"a", id(1));
        ix.insert(b"b", id(1));
        ix.set_version(
            id(1),
            Version {
                seq: 3,
                node: 1,
                disk: 9,
            },
        );
        assert_eq!(
            ix.rec(&id(1)).unwrap().paths,
            [b"a".to_vec(), b"b".to_vec()]
        );
        ix.remove_path(b"a");
        assert_eq!(ix.path_of(&id(1)), Some(&b"b"[..]));
        let ops = ix.take_changed();
        let rows: Vec<_> = ops
            .into_iter()
            .filter_map(|(_, k, v)| v.map(|v| (k, v)))
            .collect();
        let back = Index::load(rows).unwrap();
        assert_eq!(back.get(b"b"), Some(id(1)));
        assert_eq!(back.version(&id(1)).unwrap().seq, 3);
        assert_eq!(back.get(b"a"), None);
    }

    #[test]
    fn pre_existing_hard_links_share_the_first_paths_id() {
        let dir = tempfile::tempdir().unwrap();
        let r = dir.path();
        std::fs::create_dir_all(r.join("d/e")).unwrap();
        std::fs::create_dir_all(r.join(".syncfs")).unwrap();
        std::fs::write(r.join("d/e/x"), b"1").unwrap();
        std::fs::hard_link(r.join("d/e/x"), r.join("a")).unwrap();
        std::fs::hard_link(r.join("d/e/x"), r.join(".syncfs/hidden")).unwrap();
        std::fs::write(r.join("single"), b"2").unwrap();
        let links = scan_hard_links(r).unwrap();
        assert_eq!(links, vec![vec![b"a".to_vec(), b"d/e/x".to_vec()]]);
        let mut ix = Index::default();
        // Seen before by path (step 7 adopted each link on its own).
        ix.id_at(b"d/e/x");
        ix.insert(b"other", id(9));
        assert!(ix.adopt_links(&links[0]));
        let want = FileId::adopted(b"a");
        assert_eq!(ix.get(b"a"), Some(want));
        assert_eq!(ix.get(b"d/e/x"), Some(want));
        assert_eq!(ix.rec(&want).unwrap().paths.len(), 2);
        assert!(!ix.adopt_links(&links[0]), "idempotent");
        // A path with a syncfs-made id keeps it.
        ix.insert(b"b", id(7));
        assert!(!ix.adopt_links(&[b"b".to_vec()]));
        assert_eq!(ix.get(b"b"), Some(id(7)));
    }

    #[test]
    fn under_matches_whole_components() {
        assert!(under(b"a/b", b"a"));
        assert!(under(b"a", b"a"));
        assert!(!under(b"ab", b"a"));
        assert!(under(b"x", b""));
    }
}
