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
            // insert may drop another FileId's last link at `path`.
            self.displace_last_link(&mut st, path);
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
        // A file a rebuild has not copied yet has no base.
        !self.rebuild_missing(id)
            && super::files::same_base(self.state().index.version(id), version, lineage)
    }

    /// Makes the local copy of `id` (bound at its path) a whole copy of
    /// `source`'s, which must be at least `version` (keeping a diverged
    /// local change aside first).
    fn copy_whole(&self, source: NodeId, id: FileId, version: Version) -> Applied {
        match self.copy_from(id, source, super::fresh::Copy::Relayed) {
            Ok(Some(v)) if v >= version => Ok(true),
            // Gone there, rebuilding, or still on an older copy: another
            // data peer may have the bytes (issue #2).
            Ok(_) => self.fetch_elsewhere(source, id, version),
            Err(e)
                if e.kind() == io::ErrorKind::NotFound || e.kind() == io::ErrorKind::WouldBlock =>
            {
                self.fetch_elsewhere(source, id, version)
            }
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
        if let Some((id, _)) = super::core::versioned(entry) {
            if self.state().locked_here.contains_key(&id) {
                // This node's session on the file started after the entry's
                // ended (the lock is exclusive): apply it after.
                return Err(ApplyError::Retry(io::Error::other(
                    "the file is locked here; waiting for the session to end",
                )));
            }
        }
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
                {
                    let mut st = self.state();
                    self.displace_last_link(&mut st, path);
                    st.index.insert(path, *id);
                }
                self.cv.notify_all();
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
                {
                    let mut st = self.state();
                    self.displace_last_link(&mut st, path);
                    st.index.insert(path, *id);
                }
                self.cv.notify_all();
                Ok(true)
            }
            Entry::Remove {
                id: Some(id),
                path,
                dir: false,
                version: Some(version),
                lineage,
            } => {
                let _g = self.hold(*id);
                if self.superseded(id, *version) || self.state().index.tombstone(id).is_some() {
                    // Written again here since (or removed already).
                    return Ok(false);
                }
                let here = self.state().index.get(path);
                match here {
                    Some(k) if k != *id => {}
                    _ => self.bind(*id, path),
                }
                self.remove_local(*id, *version, lineage).map_err(skip)?;
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
                self.invalidate_entry(path);
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
                let (moved, replaced) = (self.backing.stat(from).ok(), self.backing.stat(to).ok());
                self.backing.rename(from, to, *flags).map_err(skip)?;
                self.invalidate_entry(from);
                self.invalidate_entry(to);
                // The kernel may still reach both files under their old
                // names; the next open, unlink or rename there gets ESTALE.
                self.note_moved(from, moved.as_ref());
                self.note_moved(to, replaced.as_ref());
                let mut st = self.state();
                if *flags & libc::RENAME_EXCHANGE != 0 {
                    st.index.exchange(from, to);
                } else {
                    // After backing.rename succeeded: dest's last link is
                    // already overwritten on disk. Journal a Remove so a
                    // peer with an earlier Data for that FileId can skip
                    // instead of stalling (issue #2). RENAME_EXCHANGE
                    // keeps both files. displace holds only `&mut State`
                    // (no re-lock).
                    self.displace_last_link(&mut st, to);
                    let replaced = st.index.get(to);
                    st.index.rename(from, to);
                    if let Some(r) = replaced.filter(|r| Some(*r) != known) {
                        if st.index.rec(&r).is_none() {
                            st.dirty.forget(r);
                        }
                    }
                }
                self.cv.notify_all();
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
        if !self.has_base(&id, version, lineage) || self.session_data_is_final(origin, id, version)
        {
            // Interim Data stays on the dirty-block path below. The
            // session-final entry must match the origin's whole file:
            // a dirty pull only fetches blocks the origin rewrote, so a
            // stale or torn prefix would otherwise be recorded as this
            // version (issue #2).
            self.bind(id, &path);
            return self.copy_whole(origin, id, version);
        }
        if !self.backing.exists(&path) && self.state().index.tombstone(&id).is_some() {
            // Removed here and written again at the origin.
            self.bind(id, &path);
            self.backing
                .create(&path, crate::net::wire::NodeKind::File, attrs, 0, &[])
                .map_err(skip)?;
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
                // Gone at the origin (removed, or the path cannot be
                // opened). Another data peer may still have the bytes.
                self.bind(id, &path);
                return self.fetch_elsewhere(origin, id, version);
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

    /// The origin (or relay) of this data has no bytes for `id`. Try every
    /// other connected data peer. If none have at least `version`, skip
    /// only when a later Remove or newer Data for `id` is already queued;
    /// otherwise retry, so the entry is not marked applied (issue #2).
    fn fetch_elsewhere(&self, skip: NodeId, id: FileId, version: Version) -> Applied {
        for p in self.data_peers_up() {
            if p == skip {
                continue;
            }
            match self.copy_from(id, p, super::fresh::Copy::Relayed) {
                Ok(Some(v)) if v >= version => {
                    log::info!(
                        "replication: {id} version {version:?} was gone at node {skip};                          fetched from node {p}"
                    );
                    return Ok(true);
                }
                Ok(_) => continue,
                Err(e)
                    if e.kind() == io::ErrorKind::NotFound
                        || e.kind() == io::ErrorKind::WouldBlock =>
                {
                    continue;
                }
                Err(e) => return Err(ApplyError::Retry(e)),
            }
        }
        if self.later_replace_known(id, version) {
            return Ok(false);
        }
        Err(ApplyError::Retry(io::Error::other(format!(
            "no connected node has version {version:?} of {id}"
        ))))
    }

    /// Whether this Data is the last one we have seen for `id` at
    /// `version` from `origin`. A later same-version Data still queued
    /// means this entry is interim; the dirty-block path applies. The
    /// current entry has already been popped from the inbox.
    fn session_data_is_final(&self, origin: NodeId, id: FileId, version: Version) -> bool {
        let inbox = super::core::lock(&self.inbox);
        !inbox.get(&origin).is_some_and(|ib| {
            ib.queue.values().any(|q| {
                matches!(
                    &q.entry,
                    Entry::Data {
                        id: fid,
                        version: v,
                        ..
                    } if *fid == id && *v == version
                )
            })
        })
    }

    /// A later Remove or a newer Data for `id` is already in the inbox.
    /// The current entry has been popped; anything still queued comes
    /// after it, or is from another origin.
    fn later_replace_known(&self, id: FileId, version: Version) -> bool {
        let inbox = super::core::lock(&self.inbox);
        inbox.values().any(|ib| {
            ib.queue.values().any(|q| match &q.entry {
                Entry::Remove { id: Some(fid), .. } if *fid == id => true,
                Entry::Data {
                    id: fid,
                    version: v,
                    ..
                } if *fid == id && *v > version => true,
                _ => false,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::config::{resolve, Config, Environment, Mode, PartialConfig, Peer};
    use crate::lock::{LockManager, Settings};
    use crate::net::Network;
    use crate::repl::core::Repl;
    use crate::repl::store::{Flusher, Store};

    fn cfg(port: u16, state: &std::path::Path) -> Config {
        let p = PartialConfig {
            node_id: Some(1),
            mode: Some(Mode::Daemon),
            cluster_name: Some("test".into()),
            listen: Some(format!("127.0.0.1:{port}").parse().unwrap()),
            peers: Some(vec![Peer {
                id: 1,
                address: format!("127.0.0.1:{port}"),
            }]),
            state_dir: Some(state.to_path_buf()),
            heartbeat_interval: Some("100ms".parse().unwrap()),
            lock_lease_timeout: Some("1s".parse().unwrap()),
            lock_wait: Some("1s".parse().unwrap()),
            ..Default::default()
        };
        let env = Environment {
            is_root: false,
            home: Some("/tmp".into()),
            xdg_state_home: None,
            cwd: "/".into(),
        };
        resolve(p, &env).unwrap().config
    }

    struct Harness {
        repl: Arc<Repl>,
        backing: tempfile::TempDir,
        _state: tempfile::TempDir,
        net: Network,
        locks: LockManager,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.repl.shutdown();
            self.locks.shutdown();
            self.net.shutdown();
        }
    }

    fn start() -> Harness {
        let state = tempfile::tempdir().unwrap();
        let backing = tempfile::tempdir().unwrap();
        let sock = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = sock.local_addr().unwrap().port();
        drop(sock);
        let cfg = cfg(port, state.path());
        let store = Store::open(state.path()).unwrap();
        let flusher = Flusher::new(Arc::new(store));
        let net = Network::start(&cfg).unwrap();
        let locks = LockManager::start(Settings::from_config(&cfg), net.clone());
        let repl = Repl::start(
            &cfg,
            backing.path(),
            1,
            Arc::clone(&flusher),
            net.clone(),
            locks.clone(),
            None,
        )
        .unwrap();
        Harness {
            repl,
            backing,
            _state: state,
            net,
            locks,
        }
    }

    fn ver(seq: u64) -> Version {
        Version {
            seq,
            node: 1,
            disk: 1,
        }
    }

    /// Applying a peer's Rename over a last link must bury that FileId
    /// and journal a local Remove; otherwise a later Data for it stalls
    /// (issue #2).
    #[test]
    fn apply_rename_over_journals_a_remove_for_the_displaced_file() {
        let h = start();
        let x = FileId([0x11; 16]);
        let y = FileId([0x22; 16]);
        std::fs::write(h.backing.path().join("f"), b"old\n").unwrap();
        std::fs::write(h.backing.path().join("g"), b"new\n").unwrap();
        {
            let mut st = h.repl.state();
            st.index.insert(b"f", x);
            st.index.take_version(x, ver(1), &[]);
            st.index.insert(b"g", y);
        }
        assert!(
            matches!(
                h.repl.apply(
                    2,
                    2,
                    &Entry::Rename {
                        id: Some(y),
                        from: b"g".to_vec(),
                        to: b"f".to_vec(),
                        flags: 0,
                    },
                ),
                Ok(true)
            ),
            "rename should apply"
        );
        assert_eq!(std::fs::read(h.backing.path().join("f")).unwrap(), b"new\n");
        assert!(!h.backing.path().join("g").exists());
        let st = h.repl.state();
        assert_eq!(st.index.get(b"f"), Some(y));
        assert!(
            st.index.tombstone(&x).is_some(),
            "the displaced FileId must be buried, not dropped"
        );
        assert!(
            st.journal.entries().any(|(_, rec)| {
                matches!(
                    &rec.entry,
                    Entry::Remove { id: Some(id), .. } if *id == x
                )
            }),
            "apply must journal a Remove for the displaced last link"
        );
    }

    /// A versioned Remove is skipped once the FileId is already a
    /// tombstone, so Removes journaled by several nodes for the same
    /// last-link drop stay idempotent (issue #2).
    #[test]
    fn apply_of_duplicate_removes_is_idempotent() {
        let h = start();
        let x = FileId([0x33; 16]);
        let v = ver(2);
        {
            let mut st = h.repl.state();
            st.index.bury(x, v);
        }
        let rem = Entry::Remove {
            id: Some(x),
            path: b"f".to_vec(),
            dir: false,
            version: Some(v),
            lineage: Vec::new(),
        };
        assert!(
            matches!(h.repl.apply(2, 2, &rem), Ok(false)),
            "first remove"
        );
        assert!(
            matches!(h.repl.apply(3, 3, &rem), Ok(false)),
            "duplicate remove"
        );
        assert_eq!(h.repl.state().index.tombstone(&x), Some(v));
    }
}
