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

    pub(crate) fn apply(&self, origin: NodeId, entry: &Entry) -> Applied {
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
            Entry::Remove { id, path, dir } => {
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
            } => {
                if self.superseded(id, *version) {
                    return Ok(false);
                }
                let Some(p) = self.local_path(Some(*id), path) else {
                    return Ok(false);
                };
                let _g = self.hold(*id);
                self.backing.set_attrs(&p, attrs).map_err(skip)?;
                self.state().index.set_version(*id, *version);
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
                ..
            } => self.apply_data(origin, *id, path, *version, attrs),
        }
    }

    fn apply_data(
        &self,
        origin: NodeId,
        id: FileId,
        hint: &[u8],
        version: Version,
        attrs: &crate::net::wire::Attrs,
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
        self.state().index.set_version(id, version);
        self.invalidate(&path);
        Ok(true)
    }
}
