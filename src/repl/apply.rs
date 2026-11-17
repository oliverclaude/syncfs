//! Applying a peer's journal entries to the backing directory.

use std::io;

use crate::config::NodeId;
use crate::net::wire::{Entry, FileId, Message, PullMode, Version};

use super::core::Repl;
use super::net::ApplyError;

fn skip(e: io::Error) -> ApplyError {
    ApplyError::Skip(e)
}

/// Ok(false): nothing to do (already there, or superseded).
type Applied = Result<bool, ApplyError>;

impl Repl {
    /// Where `id` lives locally: its indexed path, or `hint` if that path
    /// is not taken by another file.
    fn local_path(&self, id: Option<FileId>, hint: &[u8]) -> Option<Vec<u8>> {
        let st = self.state();
        if let Some(p) = id.and_then(|i| st.index.path_of(&i)) {
            return Some(p.to_vec());
        }
        match (st.index.get(hint), id) {
            (Some(other), Some(id)) if other != id => None,
            _ => Some(hint.to_vec()),
        }
    }

    /// Whether the local copy of `id` is newer than `version`: the entry
    /// is superseded. (Equal versions still apply: a session sends interim
    /// entries with its version.)
    fn superseded(&self, id: &FileId, version: Version) -> bool {
        self.state().index.version(id).is_some_and(|l| l > version)
    }

    /// Gives a file this node never touched (no index record yet) the
    /// entry's id at `path`.
    fn bind(&self, id: FileId, path: &[u8]) {
        let mut st = self.state();
        if st.index.path_of(&id).is_none() {
            st.index.insert(path, id);
        }
    }

    /// The local copy at `path` is now `version` of `id`, a copy of the
    /// origin's, which went through `lineage`.
    fn record_version(&self, id: FileId, path: &[u8], version: Version, lineage: &[Version]) {
        self.bind(id, path);
        self.state().index.take_version(id, version, lineage);
    }

    /// Whether the local copy of `id` is a base `version`'s changed blocks
    /// apply to (see [`same_base`](super::files::same_base)).
    fn has_base(&self, id: &FileId, version: Version, lineage: &[Version]) -> bool {
        super::files::same_base(self.state().index.version(id), version, lineage)
    }

    /// Makes the local copy of `id` (bound at its path) a whole copy of
    /// `source`'s, which must be at least `version` (keeping a diverged
    /// local change aside first).
    fn copy_whole(&self, source: NodeId, id: FileId, version: Version) -> Applied {
        match self.copy_from(id, source, super::fresh::Copy::Relayed) {
            Ok(Some(v)) if v >= version => Ok(true),
            Ok(v) => Err(ApplyError::Retry(io::Error::other(format!(
                "node {source} has {v:?} of {id}, not {version:?}"
            )))),
            // Gone there; a later entry says what happened.
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(ApplyError::Retry(e)),
        }
    }

    fn discard(&self, origin: NodeId, file: FileId, version: Version) {
        // Fire and forget: the answer has no listener.
        self.net.send(
            origin,
            Message::BlockPull {
                req: 0,
                file,
                mode: PullMode::Discard(version),
            },
        );
    }

    /// Applies `origin`'s `entry`, which came from `via` (the origin, or a
    /// node forwarding it: then data comes from `via` too).
    pub(crate) fn apply(&self, origin: NodeId, via: NodeId, entry: &Entry) -> Applied {
        match entry {
            Entry::Create {
                id,
                path,
                kind,
                attrs,
                rdev,
                target,
            } => {
                if let Ok(st) = self.backing.stat(path) {
                    let known = self.state().index.get(path);
                    if known == Some(*id) {
                        return Ok(false);
                    }
                    if super::backing::kind_of(&st) == *kind && known.is_none() {
                        // Created here too, before the index knew it.
                        self.state().index.insert(path, *id);
                        return Ok(false);
                    }
                    return Err(skip(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "another file has this path here",
                    )));
                }
                self.backing
                    .create(path, *kind, attrs, *rdev, target)
                    .map_err(skip)?;
                self.state().index.insert(path, *id);
                Ok(true)
            }
            Entry::Link { id, existing, path } => {
                let Some(src) = self.local_path(Some(*id), existing) else {
                    return Err(skip(io::Error::from(io::ErrorKind::NotFound)));
                };
                if self.state().index.get(path) == Some(*id) {
                    return Ok(false);
                }
                self.backing.link(&src, path).map_err(skip)?;
                self.state().index.insert(path, *id);
                Ok(true)
            }
            Entry::Remove { id, path, dir, .. } => {
                let known = self.state().index.get(path);
                if let (Some(id), Some(k)) = (id, known) {
                    if *id != k {
                        // The path names another file here now.
                        return Ok(false);
                    }
                }
                let res = self.backing.remove(path, *dir);
                let mut st = self.state();
                match res {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(skip(e)),
                }
                if let Some(gone) = st.index.remove_path(path) {
                    if st.index.rec(&gone).is_none() {
                        st.dirty.forget(gone);
                    }
                }
                Ok(true)
            }
            Entry::Rename {
                id,
                from,
                to,
                flags,
            } => {
                let known = self.state().index.get(from);
                if let (Some(id), Some(k)) = (id, known) {
                    if *id != k {
                        return Ok(false);
                    }
                }
                self.backing.rename(from, to, *flags).map_err(skip)?;
                let mut st = self.state();
                let replaced = st.index.get(to);
                if *flags & libc::RENAME_EXCHANGE != 0 {
                    st.index.exchange(from, to);
                } else {
                    st.index.rename(from, to);
                    if let Some(r) = replaced.filter(|r| Some(*r) != known) {
                        if st.index.rec(&r).is_none() {
                            st.dirty.forget(r);
                        }
                    }
                }
                Ok(true)
            }
            Entry::Attr {
                id,
                path,
                version,
                attrs,
                lineage,
            } => {
                let _g = self.hold(*id);
                if self.superseded(id, *version) {
                    return Ok(false);
                }
                let Some(p) = self.local_path(Some(*id), path) else {
                    return Ok(false);
                };
                if version.seq > 0 && !self.has_base(id, *version, lineage) {
                    // The local content is not what the version changed
                    // the attributes of.
                    self.bind(*id, &p);
                    return self.copy_whole(via, *id, *version);
                }
                self.backing.set_attrs(&p, attrs).map_err(skip)?;
                if version.seq > 0 {
                    self.record_version(*id, &p, *version, lineage);
                }
                self.invalidate(&p);
                Ok(true)
            }
            Entry::Xattr {
                id,
                path,
                name,
                value,
            } => {
                let Some(p) = self.local_path(Some(*id), path) else {
                    return Ok(false);
                };
                self.backing
                    .set_xattr(&p, name, value.as_deref())
                    .map_err(skip)?;
                Ok(true)
            }
            Entry::Data {
                id,
                path,
                version,
                attrs,
                lineage,
                ..
            } if via == origin => self.apply_data(origin, *id, path, *version, attrs, lineage),
            Entry::Data {
                id, path, version, ..
            } => self.apply_data_via(via, *id, path, *version),
        }
    }

    /// A forwarded `Data` entry: the origin's dirty marks are out of reach,
    /// so the content comes from the relay by hash comparison (the relay
    /// applied the entry, so its copy is at least that version). The
    /// origin drops its marks when this node acknowledges the session's
    /// final entry.
    fn apply_data_via(&self, via: NodeId, id: FileId, hint: &[u8], version: Version) -> Applied {
        let _g = self.hold(id);
        if self.superseded(&id, version) {
            return Ok(false);
        }
        let Some(path) = self.local_path(Some(id), hint) else {
            return Ok(false);
        };
        self.bind(id, &path);
        self.copy_whole(via, id, version)
    }

    fn apply_data(
        &self,
        origin: NodeId,
        id: FileId,
        hint: &[u8],
        version: Version,
        attrs: &crate::net::wire::Attrs,
        lineage: &[Version],
    ) -> Applied {
        let _g = self.hold(id);
        if self.superseded(&id, version) {
            self.discard(origin, id, version);
            return Ok(false);
        }
        let Some(path) = self.local_path(Some(id), hint) else {
            self.discard(origin, id, version);
            return Ok(false);
        };
        if !self.has_base(&id, version, lineage) {
            // The local copy is not one the origin's changed blocks apply
            // to (the origin wrote over a copy without this one's last
            // changes): compare the whole file.
            self.bind(id, &path);
            return self.copy_whole(origin, id, version);
        }
        let out = match self.backing.open_write(&path) {
            Ok(f) => f,
            Err(e) => {
                self.discard(origin, id, version);
                return Err(skip(e));
            }
        };
        let mut restart = true;
        let size = loop {
            let pulled = self
                .pull(origin, id, PullMode::Dirty { restart }, &out)
                .map_err(ApplyError::Retry)?;
            restart = false;
            if !pulled.found {
                // Gone at the origin; a later entry says what happened.
                return Ok(false);
            }
            if !pulled.more {
                break pulled.size;
            }
        };
        out.set_len(size).map_err(ApplyError::Retry)?;
        out.sync_all().map_err(ApplyError::Retry)?;
        drop(out);
        self.backing.set_attrs(&path, attrs).map_err(skip)?;
        self.record_version(id, &path, version, lineage);
        self.invalidate(&path);
        Ok(true)
    }
}
