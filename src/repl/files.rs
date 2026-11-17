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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRec {
    /// Every path of the file (several for hard links), sorted.
    pub paths: Vec<Vec<u8>>,
    /// The version the local copy holds.
    pub version: Option<Version>,
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
            let rec: FileRec = decode(&v)?;
            for p in &rec.paths {
                ix.by_path.insert(p.clone(), FileId(id));
            }
            ix.files.insert(FileId(id), rec);
        }
        Ok(ix)
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub fn get(&self, path: &[u8]) -> Option<FileId> {
        self.by_path.get(path).copied()
    }

    pub fn rec(&self, id: &FileId) -> Option<&FileRec> {
        self.files.get(id)
    }

    /// A current path of `id`.
    pub fn path_of(&self, id: &FileId) -> Option<&[u8]> {
        self.files.get(id)?.paths.first().map(Vec::as_slice)
    }

    pub fn version(&self, id: &FileId) -> Option<Version> {
        self.files.get(id)?.version
    }

    pub fn set_version(&mut self, id: FileId, v: Version) {
        if let Some(r) = self.files.get_mut(&id) {
            if r.version.is_none_or(|old| v > old) {
                r.version = Some(v);
                self.changed.insert(id);
            }
        }
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
            if r.paths.is_empty() {
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
        for (p, _) in self.subtree(to) {
            self.remove_path(&p);
        }
        for (p, _) in &moving {
            self.remove_path(p);
        }
        for (p, id) in moving {
            self.insert(&rebase(&p, from, to), id);
        }
    }

    /// `a` and `b` swapped places (`RENAME_EXCHANGE`).
    pub fn exchange(&mut self, a: &[u8], b: &[u8]) {
        let sa = self.subtree(a);
        let sb = self.subtree(b);
        for (p, _) in sa.iter().chain(&sb) {
            self.remove_path(p);
        }
        for (p, id) in sa {
            self.insert(&rebase(&p, a, b), id);
        }
        for (p, id) in sb {
            self.insert(&rebase(&p, b, a), id);
        }
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
    fn under_matches_whole_components() {
        assert!(under(b"a/b", b"a"));
        assert!(under(b"a", b"a"));
        assert!(!under(b"ab", b"a"));
        assert!(under(b"x", b""));
    }
}
