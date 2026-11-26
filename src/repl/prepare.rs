//! Journal a local namespace change durably before the backing syscall,
//! and recover if a crash or a failed syscall leaves the journal ahead
//! of the tree (issue #2).
//!
//! Create, rename and unlink append first, `flush_now`, then issue the
//! syscall. A failed syscall gets a compensating journal entry and an
//! index revert; sequence numbers are never reused once durable. On
//! unclean start, an inflight op is redone if any peer has acknowledged
//! it, and compensated otherwise (or if redo fails). A syscall that
//! already completed (crash after success, marker still set) is left
//! alone. A peer may have applied an unacked entry, so this can still
//! undo or complete in the opposite case (the stale-ack caveat).

use std::io;

use fuser::FileType;
use serde::{Deserialize, Serialize};

use crate::net::wire::{Attrs, Entry, FileId, NodeKind, Version};

use super::backing::attrs_of;
use super::core::Repl;
use super::store::{decode, Table};

pub const INFLIGHT_KEY: &[u8] = b"inflight";

/// A displaced dest kept so a compensating undelete can restore it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Displaced {
    pub id: FileId,
    pub path: Vec<u8>,
    pub version: Option<Version>,
    pub lineage: Vec<Version>,
    pub kind: NodeKind,
    pub attrs: Attrs,
    pub rdev: u64,
    pub target: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InflightOp {
    Create {
        id: FileId,
        path: Vec<u8>,
        kind: NodeKind,
        attrs: Attrs,
        rdev: u64,
        target: Vec<u8>,
        displaced: Option<Displaced>,
    },
    Rename {
        id: Option<FileId>,
        from: Vec<u8>,
        to: Vec<u8>,
        flags: u32,
        displaced: Option<Displaced>,
    },
    Remove {
        id: Option<FileId>,
        path: Vec<u8>,
        dir: bool,
        version: Option<Version>,
        lineage: Vec<Version>,
        kind: NodeKind,
        attrs: Attrs,
        rdev: u64,
        target: Vec<u8>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Inflight {
    pub seq: u64,
    pub op: InflightOp,
}

pub fn node_kind(kind: FileType) -> NodeKind {
    match kind {
        FileType::Directory => NodeKind::Dir,
        FileType::Symlink => NodeKind::Symlink,
        FileType::NamedPipe => NodeKind::Fifo,
        FileType::Socket => NodeKind::Socket,
        FileType::CharDevice => NodeKind::CharDevice,
        FileType::BlockDevice => NodeKind::BlockDevice,
        _ => NodeKind::File,
    }
}

pub fn intended_attrs(mode: u32, uid: u32, gid: u32) -> Attrs {
    Attrs {
        mode,
        uid,
        gid,
        mtime: (0, 0),
        atime: (0, 0),
    }
}

impl Repl {
    pub fn load_inflight(store: &super::store::Store) -> io::Result<Option<Inflight>> {
        match store.get(Table::Meta, INFLIGHT_KEY)? {
            Some(v) => Ok(Some(decode(&v)?)),
            None => Ok(None),
        }
    }

    /// Durable journal + index for a create, rename or unlink, before the
    /// backing syscall.
    pub fn prepare(&self, op: &crate::fs::Mutation<'_>) -> io::Result<()> {
        use super::core::{join, path_bytes};
        use crate::fs::Mutation;
        match *op {
            Mutation::Create {
                parent,
                name,
                kind,
                mode,
                uid,
                gid,
                rdev,
                target,
            } => {
                let path = join(&path_bytes(&parent.path()?), name);
                self.prepare_create(
                    path,
                    node_kind(kind),
                    intended_attrs(mode, uid, gid),
                    rdev,
                    target,
                )
            }
            Mutation::Rename {
                parent,
                name,
                new_parent,
                new_name,
                flags,
            } => {
                let from = join(&path_bytes(&parent.path()?), name);
                let to = join(&path_bytes(&new_parent.path()?), new_name);
                self.prepare_rename(from, to, flags)
            }
            Mutation::Remove {
                parent,
                name,
                is_dir,
            } => {
                let path = join(&path_bytes(&parent.path()?), name);
                self.prepare_remove(path, is_dir)
            }
            _ => Ok(()),
        }
    }

    pub fn confirm(&self, op: &crate::fs::Mutation<'_>) -> io::Result<()> {
        use super::core::{join, path_bytes};
        use crate::fs::Mutation;
        if let Mutation::Create { parent, name, .. } = *op {
            let path = join(&path_bytes(&parent.path()?), name);
            self.confirm_create(&path)?;
        }
        self.clear_inflight()
    }

    pub fn abort(&self, op: &crate::fs::Mutation<'_>) {
        if let Err(e) = self.compensate_inflight() {
            log::warn!("replication: cannot compensate a failed namespace change ({op:?}): {e}");
            self.state().counters.unrecorded_changes += 1;
        }
    }

    pub fn prepare_create(
        &self,
        path: Vec<u8>,
        kind: NodeKind,
        attrs: Attrs,
        rdev: u64,
        target: &[u8],
    ) -> io::Result<()> {
        self.finish_leftover_inflight()?;
        let id = FileId::random();
        let mut st = self.state();
        let displaced = snapshot_displaced(&self.backing, &st, &path);
        if displaced.is_some() {
            self.displace_last_link(&mut st, &path);
        }
        st.index.insert(&path, id);
        let seq = self.append(
            &mut st,
            Entry::Create {
                id,
                path: path.clone(),
                kind,
                attrs: attrs.clone(),
                rdev,
                target: target.to_vec(),
            },
        );
        st.inflight = Some(Inflight {
            seq,
            op: InflightOp::Create {
                id,
                path,
                kind,
                attrs,
                rdev,
                target: target.to_vec(),
                displaced,
            },
        });
        st.inflight_changed = true;
        drop(st);
        self.flush_or_retract()
    }

    pub fn prepare_rename(&self, from: Vec<u8>, to: Vec<u8>, flags: u32) -> io::Result<()> {
        self.finish_leftover_inflight()?;
        let mut st = self.state();
        let id = st.index.get(&from);
        let exchange = flags & libc::RENAME_EXCHANGE != 0;
        let displaced = if exchange {
            None
        } else {
            let dest = st.index.get(&to);
            if dest.filter(|r| Some(*r) != id).is_some() {
                snapshot_displaced(&self.backing, &st, &to)
            } else {
                None
            }
        };
        if exchange {
            st.index.exchange(&from, &to);
        } else {
            if displaced.is_some() {
                self.displace_last_link(&mut st, &to);
            }
            st.index.rename(&from, &to);
            if let Some(r) = st.index.get(&to).or(id) {
                let _ = r;
            }
            if let Some(d) = displaced.as_ref() {
                if st.index.rec(&d.id).is_none() {
                    st.dirty.forget(d.id);
                    st.sessions.remove(&d.id);
                }
            }
        }
        let seq = self.append(
            &mut st,
            Entry::Rename {
                id,
                from: from.clone(),
                to: to.clone(),
                flags,
            },
        );
        st.inflight = Some(Inflight {
            seq,
            op: InflightOp::Rename {
                id,
                from,
                to,
                flags,
                displaced,
            },
        });
        st.inflight_changed = true;
        drop(st);
        self.flush_or_retract()
    }

    pub fn prepare_remove(&self, path: Vec<u8>, dir: bool) -> io::Result<()> {
        self.finish_leftover_inflight()?;
        let snap = {
            let st = self.state();
            snapshot_displaced(&self.backing, &st, &path)
        };
        let mut st = self.state();
        let buried = st.index.get(&path).and_then(|id| {
            let last = st.index.rec(&id).is_some_and(|r| r.paths.len() == 1);
            let x = st.sessions.get(&id)?;
            (last && !dir).then_some((id, x.version, x.superseded))
        });
        let (id, version, lineage, seq) = if let Some((id, version, superseded)) = buried {
            st.index.bury(id, version);
            if let Some(v) = superseded.filter(|v| *v < version) {
                st.index.note_lineage(id, v);
            }
            st.dirty.forget(id);
            st.sessions.remove(&id);
            st.window_drop.insert(id);
            let lineage = st.index.lineage(&id);
            let seq = self.append(
                &mut st,
                Entry::Remove {
                    id: Some(id),
                    path: path.clone(),
                    dir: false,
                    version: Some(version),
                    lineage: lineage.clone(),
                },
            );
            st.awaiting.insert(seq, (id, version));
            st.holders.entry((id, version)).or_default().insert(self.me);
            (Some(id), Some(version), lineage, seq)
        } else {
            let id = st.index.remove_path(&path);
            if let Some(id) = id {
                if st.index.rec(&id).is_none() {
                    st.dirty.forget(id);
                    st.sessions.remove(&id);
                }
            }
            let seq = self.append(
                &mut st,
                Entry::Remove {
                    id,
                    path: path.clone(),
                    dir,
                    version: None,
                    lineage: Vec::new(),
                },
            );
            (id, None, Vec::new(), seq)
        };
        let (kind, attrs, rdev, target) = snap
            .as_ref()
            .map(|s| (s.kind, s.attrs.clone(), s.rdev, s.target.clone()))
            .unwrap_or((
                if dir { NodeKind::Dir } else { NodeKind::File },
                Attrs::default(),
                0,
                Vec::new(),
            ));
        st.inflight = Some(Inflight {
            seq,
            op: InflightOp::Remove {
                id,
                path,
                dir,
                version,
                lineage,
                kind,
                attrs,
                rdev,
                target,
            },
        });
        st.inflight_changed = true;
        drop(st);
        self.flush_or_retract()
    }

    fn confirm_create(&self, path: &[u8]) -> io::Result<()> {
        let stat = self.backing.stat(path)?;
        let mut st = self.state();
        let Some(id) = st.index.get(path) else {
            return Ok(());
        };
        let version = st.index.version(&id).unwrap_or(Version {
            seq: 0,
            node: self.me,
            disk: self.disk,
        });
        let lineage = st.index.lineage(&id);
        self.append(
            &mut st,
            Entry::Attr {
                id,
                path: path.to_vec(),
                version,
                attrs: attrs_of(&stat),
                lineage,
            },
        );
        drop(st);
        Ok(())
    }

    pub(crate) fn clear_inflight(&self) -> io::Result<()> {
        let mut st = self.state();
        if st.inflight.is_none() {
            return Ok(());
        }
        st.inflight = None;
        st.inflight_changed = true;
        drop(st);
        self.flusher.flush_now()
    }

    fn flush_or_retract(&self) -> io::Result<()> {
        self.cv.notify_all();
        match self.flusher.flush_now() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.revert_undurable();
                Err(e)
            }
        }
    }

    fn revert_undurable(&self) {
        let mut st = self.state();
        if let Some(inflight) = st.inflight.take() {
            st.inflight_changed = true;
            revert_index(&mut st, &inflight.op);
        }
        st.journal.retract_undurable();
    }

    fn finish_leftover_inflight(&self) -> io::Result<()> {
        if self.state().inflight.is_some() {
            self.recover_inflight()?;
        }
        Ok(())
    }

    /// Redo or compensate a durable inflight namespace change left by a
    /// crash between the journal flush and the syscall (or after a
    /// successful syscall, before the marker was cleared).
    pub fn recover_inflight(&self) -> io::Result<()> {
        let Some(inflight) = self.state().inflight.clone() else {
            return Ok(());
        };
        if backing_matches(&self.backing, &inflight.op) {
            self.confirm_if_create(&inflight.op)?;
            return self.clear_inflight();
        }
        let acked = self.any_peer_acked(inflight.seq);
        if acked {
            if let Err(e) = self.redo_syscall(&inflight.op) {
                log::warn!(
                    "replication: redo of inflight journal seq {} failed ({e}); compensating",
                    inflight.seq
                );
                self.compensate_inflight()?;
            } else {
                self.confirm_if_create(&inflight.op)?;
                self.clear_inflight()?;
            }
        } else {
            self.compensate_inflight()?;
        }
        Ok(())
    }

    fn any_peer_acked(&self, seq: u64) -> bool {
        let st = self.state();
        let acked = st.journal.peers().any(|p| st.journal.acked(p) >= seq);
        acked
    }

    pub(crate) fn confirm_if_create(&self, op: &InflightOp) -> io::Result<()> {
        if let InflightOp::Create { path, .. } = op {
            self.confirm_create(path)?;
        }
        Ok(())
    }

    fn redo_syscall(&self, op: &InflightOp) -> io::Result<()> {
        match op {
            InflightOp::Create {
                path,
                kind,
                attrs,
                rdev,
                target,
                ..
            } => {
                if !self.backing.exists(path) {
                    self.backing
                        .create_intended(path, *kind, attrs, *rdev, target)?;
                }
            }
            InflightOp::Rename {
                from, to, flags, ..
            } => {
                if self.backing.exists(from) {
                    self.backing.rename(from, to, *flags)?;
                } else if !self.backing.exists(to) {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "rename source and dest are both missing",
                    ));
                }
            }
            InflightOp::Remove { path, dir, .. } => {
                if self.backing.exists(path) {
                    self.backing.remove(path, *dir)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn compensate_inflight(&self) -> io::Result<()> {
        let Some(inflight) = self.state().inflight.clone() else {
            return Ok(());
        };
        {
            let mut st = self.state();
            revert_index(&mut st, &inflight.op);
            match &inflight.op {
                InflightOp::Create { id, path, kind, .. } => {
                    let dir = *kind == NodeKind::Dir;
                    self.append(
                        &mut st,
                        Entry::Remove {
                            id: Some(*id),
                            path: path.clone(),
                            dir,
                            version: None,
                            lineage: Vec::new(),
                        },
                    );
                    if let Some(d) = displaced_of(&inflight.op) {
                        self.undelete(&mut st, d)?;
                    }
                }
                InflightOp::Rename { id, from, to, .. } => {
                    self.append(
                        &mut st,
                        Entry::Rename {
                            id: *id,
                            from: to.clone(),
                            to: from.clone(),
                            flags: 0,
                        },
                    );
                    if let Some(d) = displaced_of(&inflight.op) {
                        self.undelete(&mut st, d)?;
                    }
                }
                InflightOp::Remove { .. } => {
                    if let Some(d) = snapshot_as_displaced(&inflight.op) {
                        self.undelete(&mut st, &d)?;
                    }
                }
            }
            st.inflight = None;
            st.inflight_changed = true;
            drop(st);
        }
        self.cv.notify_all();
        self.flusher.flush_now()
    }

    fn undelete(&self, st: &mut super::core::State, d: &Displaced) -> io::Result<()> {
        st.index.insert(&d.path, d.id);
        if let Some(v) = d.version {
            st.index.set_version(d.id, v);
        }
        self.append(
            st,
            Entry::Create {
                id: d.id,
                path: d.path.clone(),
                kind: d.kind,
                attrs: d.attrs.clone(),
                rdev: d.rdev,
                target: d.target.clone(),
            },
        );
        if d.kind != NodeKind::File && d.kind != NodeKind::Dir {
            return Ok(());
        }
        if d.kind == NodeKind::Dir {
            return Ok(());
        }
        let size = self
            .backing
            .stat(&d.path)
            .map(|s| s.st_size as u64)
            .unwrap_or(0);
        if size == 0 {
            return Ok(());
        }
        let version = d.version.filter(|v| v.seq > 0).unwrap_or(Version {
            seq: 1,
            node: self.me,
            disk: self.disk,
        });
        if d.version.is_none() {
            st.index.set_version(d.id, version);
        }
        let blocks = size.div_ceil(self.block_size);
        st.dirty.mark_all(d.id, version, blocks);
        let seq = self.append(
            st,
            Entry::Data {
                id: d.id,
                path: d.path.clone(),
                version,
                size,
                attrs: d.attrs.clone(),
                lineage: d.lineage.clone(),
            },
        );
        st.awaiting.insert(seq, (d.id, version));
        st.holders
            .entry((d.id, version))
            .or_default()
            .insert(self.me);
        Ok(())
    }
}

fn displaced_of(op: &InflightOp) -> Option<&Displaced> {
    match op {
        InflightOp::Create { displaced, .. } | InflightOp::Rename { displaced, .. } => {
            displaced.as_ref()
        }
        InflightOp::Remove { .. } => None,
    }
}

fn snapshot_as_displaced(op: &InflightOp) -> Option<Displaced> {
    match op {
        InflightOp::Remove {
            id: Some(id),
            path,
            version,
            lineage,
            kind,
            attrs,
            rdev,
            target,
            ..
        } => Some(Displaced {
            id: *id,
            path: path.clone(),
            version: *version,
            lineage: lineage.clone(),
            kind: *kind,
            attrs: attrs.clone(),
            rdev: *rdev,
            target: target.clone(),
        }),
        _ => None,
    }
}

fn snapshot_displaced(
    backing: &super::backing::Backing,
    st: &super::core::State,
    path: &[u8],
) -> Option<Displaced> {
    let id = st.index.get(path)?;
    let rec = st.index.rec(&id)?;
    let version = rec
        .version
        .or_else(|| st.sessions.get(&id).map(|s| s.version));
    let lineage = st.index.lineage(&id);
    let stat = backing.stat(path).ok()?;
    let kind = super::backing::kind_of(&stat);
    let target = if kind == NodeKind::Symlink {
        // Best-effort; empty if it cannot be read.
        backing.readlink(path).unwrap_or_default()
    } else {
        Vec::new()
    };
    Some(Displaced {
        id,
        path: path.to_vec(),
        version,
        lineage,
        kind,
        attrs: attrs_of(&stat),
        rdev: stat.st_rdev,
        target,
    })
}

fn revert_index(st: &mut super::core::State, op: &InflightOp) {
    match op {
        InflightOp::Create {
            id,
            path,
            displaced,
            ..
        } => {
            if st.index.get(path) == Some(*id) {
                st.index.remove_path(path);
            }
            if let Some(d) = displaced {
                st.index.insert(&d.path, d.id);
                if let Some(v) = d.version {
                    st.index.set_version(d.id, v);
                }
            }
        }
        InflightOp::Rename {
            from,
            to,
            flags,
            displaced,
            id,
            ..
        } => {
            if *flags & libc::RENAME_EXCHANGE != 0 {
                st.index.exchange(to, from);
            } else {
                st.index.rename(to, from);
                if let Some(d) = displaced {
                    st.index.insert(&d.path, d.id);
                    if let Some(v) = d.version {
                        st.index.set_version(d.id, v);
                    }
                } else if let Some(id) = id {
                    let _ = id;
                }
            }
        }
        InflightOp::Remove {
            id: Some(id),
            path,
            version,
            ..
        } => {
            st.index.insert(path, *id);
            if let Some(v) = version {
                st.index.set_version(*id, *v);
            }
        }
        InflightOp::Remove { id: None, .. } => {}
    }
}

fn backing_matches(backing: &super::backing::Backing, op: &InflightOp) -> bool {
    match op {
        InflightOp::Create { path, .. } => backing.exists(path),
        InflightOp::Rename {
            from, to, flags, ..
        } => {
            if *flags & libc::RENAME_EXCHANGE != 0 {
                backing.exists(to) && backing.exists(from)
            } else {
                backing.exists(to) && !backing.exists(from)
            }
        }
        InflightOp::Remove { path, .. } => !backing.exists(path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::config::{resolve, Config, Environment, Mode, PartialConfig, Peer};
    use crate::lock::{LockManager, Settings};
    use crate::net::Network;
    use crate::repl::store::{Flusher, Store};

    fn cfg(port: u16, state: &std::path::Path) -> Config {
        let p = PartialConfig {
            node_id: Some(1),
            mode: Some(Mode::Daemon),
            cluster_name: Some("test".into()),
            listen: Some(format!("127.0.0.1:{port}").parse().unwrap()),
            peers: Some(vec![
                Peer {
                    id: 1,
                    address: format!("127.0.0.1:{port}"),
                },
                Peer {
                    id: 2,
                    address: "127.0.0.1:1".into(),
                },
            ]),
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
        repl: Option<Arc<Repl>>,
        backing: Option<tempfile::TempDir>,
        state: Option<tempfile::TempDir>,
        net: Option<Network>,
        locks: Option<LockManager>,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.stop_services();
        }
    }

    impl Harness {
        fn repl(&self) -> &Repl {
            self.repl.as_ref().unwrap()
        }

        fn backing_path(&self) -> &std::path::Path {
            self.backing.as_ref().unwrap().path()
        }

        fn stop_services(&mut self) {
            if let Some(r) = self.repl.take() {
                r.shutdown();
            }
            if let Some(l) = self.locks.take() {
                l.shutdown();
            }
            if let Some(n) = self.net.take() {
                n.shutdown();
            }
        }

        fn restart(mut self) -> Harness {
            self.stop_services();
            let backing = self.backing.take().unwrap();
            let state = self.state.take().unwrap();
            start_at(state, backing)
        }
    }

    fn start() -> Harness {
        start_at(tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap())
    }

    fn start_at(state: tempfile::TempDir, backing: tempfile::TempDir) -> Harness {
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
            repl: Some(repl),
            backing: Some(backing),
            state: Some(state),
            net: Some(net),
            locks: Some(locks),
        }
    }

    fn attrs() -> Attrs {
        intended_attrs(
            0o644,
            rustix::process::geteuid().as_raw(),
            rustix::process::getegid().as_raw(),
        )
    }

    fn journal_entries(h: &Harness) -> Vec<Entry> {
        h.repl()
            .state()
            .journal
            .entries()
            .map(|(_, r)| r.entry.clone())
            .collect()
    }

    fn inflight_seq(h: &Harness) -> u64 {
        h.repl().state().inflight.as_ref().unwrap().seq
    }

    fn ack_peer(h: &Harness, seq: u64) {
        {
            let mut st = h.repl().state();
            st.journal.ack(2, seq);
        }
        h.repl().flusher.flush_now().unwrap();
    }

    fn create_and_confirm(h: &Harness, path: &[u8], contents: &[u8]) {
        h.repl()
            .prepare_create(path.to_vec(), NodeKind::File, attrs(), 0, b"")
            .unwrap();
        let file = h.backing_path().join(std::str::from_utf8(path).unwrap());
        if let Some(parent) = file.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&file, contents).unwrap();
        let op = h.repl().state().inflight.as_ref().unwrap().op.clone();
        h.repl().confirm_if_create(&op).unwrap();
        h.repl().clear_inflight().unwrap();
    }

    #[test]
    fn create_is_journaled_durably_before_the_backing_file_exists() {
        let h = start();
        h.repl()
            .prepare_create(b"f".to_vec(), NodeKind::File, attrs(), 0, b"")
            .unwrap();
        assert!(!h.repl().backing.exists(b"f"));
        assert!(!h.backing_path().join("f").exists());
        let entries = journal_entries(&h);
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, Entry::Create { path, .. } if path == b"f")),
            "missing durable Create: {entries:?}"
        );
        assert!(h.repl().state().inflight.is_some());
        assert!(h.repl().state().journal.durable() >= 1);
    }

    #[test]
    fn successful_create_appends_attr_from_the_real_stat() {
        let h = start();
        create_and_confirm(&h, b"f", b"");
        let entries = journal_entries(&h);
        let attr = entries.iter().find_map(|e| match e {
            Entry::Attr { path, attrs, .. } if path == b"f" => Some(attrs),
            _ => None,
        });
        let attr = attr.expect(&format!("missing Attr after create: {entries:?}"));
        assert_ne!(attr.mtime, (0, 0), "origin times should come from stat");
        assert!(h.repl().state().inflight.is_none());
    }

    #[test]
    fn syscall_failure_after_create_journals_a_compensate() {
        let h = start();
        h.repl()
            .prepare_create(b"f".to_vec(), NodeKind::File, attrs(), 0, b"")
            .unwrap();
        let seq = inflight_seq(&h);
        h.repl().compensate_inflight().unwrap();
        assert!(h.repl().state().index.get(b"f").is_none());
        let entries = journal_entries(&h);
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, Entry::Remove { path, .. } if path == b"f")),
            "missing compensating Remove: {entries:?}"
        );
        assert!(h.repl().state().inflight.is_none());
        // The Create's sequence number was not reused.
        assert!(h.repl().state().journal.last() > seq);
    }

    #[test]
    fn syscall_failure_after_rename_journals_the_inverse() {
        let h = start();
        create_and_confirm(&h, b"a", b"x");
        let id = h.repl().state().index.get(b"a").unwrap();
        h.repl()
            .prepare_rename(b"a".to_vec(), b"b".to_vec(), 0)
            .unwrap();
        assert_eq!(h.repl().state().index.get(b"b"), Some(id));
        assert!(!h.backing_path().join("b").exists());
        h.repl().compensate_inflight().unwrap();
        assert_eq!(h.repl().state().index.get(b"a"), Some(id));
        assert!(h.repl().state().index.get(b"b").is_none());
        let entries = journal_entries(&h);
        assert!(
            entries.iter().any(|e| matches!(
                e,
                Entry::Rename { from, to, .. } if from == b"b" && to == b"a"
            )),
            "missing inverse Rename: {entries:?}"
        );
    }

    #[test]
    fn syscall_failure_after_unlink_journals_an_undelete() {
        let h = start();
        create_and_confirm(&h, b"f", b"hello");
        let id = h.repl().state().index.get(b"f").unwrap();
        h.repl().prepare_remove(b"f".to_vec(), false).unwrap();
        assert!(h.repl().state().index.get(b"f").is_none());
        h.repl().compensate_inflight().unwrap();
        assert_eq!(h.repl().state().index.get(b"f"), Some(id));
        let entries = journal_entries(&h);
        assert!(
            entries.iter().any(
                |e| matches!(e, Entry::Create { path, id: i, .. } if path == b"f" && *i == id)
            ),
            "missing undelete Create: {entries:?}"
        );
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, Entry::Data { path, id: i, .. } if path == b"f" && *i == id)),
            "missing undelete Data: {entries:?}"
        );
    }

    #[test]
    fn recover_compensates_when_no_peer_acked() {
        let h = start();
        h.repl()
            .prepare_create(b"f".to_vec(), NodeKind::File, attrs(), 0, b"")
            .unwrap();
        let seq = inflight_seq(&h);
        let h = h.restart();
        assert!(h.repl().state().inflight.is_none());
        assert!(h.repl().state().index.get(b"f").is_none());
        assert!(!h.backing_path().join("f").exists());
        let entries = journal_entries(&h);
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, Entry::Remove { path, .. } if path == b"f")),
            "expected compensate on restart with no ack: {entries:?}"
        );
        assert!(h.repl().state().journal.last() > seq);
    }

    #[test]
    fn recover_redos_when_a_peer_acked() {
        let h = start();
        h.repl()
            .prepare_create(b"f".to_vec(), NodeKind::File, attrs(), 0, b"")
            .unwrap();
        let seq = inflight_seq(&h);
        ack_peer(&h, seq);
        let h = h.restart();
        assert!(h.repl().state().inflight.is_none());
        assert!(h.repl().state().index.get(b"f").is_some());
        assert!(h.backing_path().join("f").exists());
        let entries = journal_entries(&h);
        assert!(
            !entries
                .iter()
                .any(|e| matches!(e, Entry::Remove { path, .. } if path == b"f")),
            "redo should not compensate: {entries:?}"
        );
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, Entry::Attr { path, .. } if path == b"f")),
            "redo should append Attr: {entries:?}"
        );
    }

    #[test]
    fn recover_compensates_if_redo_fails() {
        let h = start();
        h.repl()
            .prepare_create(b"missing/f".to_vec(), NodeKind::File, attrs(), 0, b"")
            .unwrap();
        let seq = inflight_seq(&h);
        ack_peer(&h, seq);
        let h = h.restart();
        assert!(h.repl().state().inflight.is_none());
        assert!(h.repl().state().index.get(b"missing/f").is_none());
        let entries = journal_entries(&h);
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, Entry::Remove { path, .. } if path == b"missing/f")),
            "redo failure should compensate: {entries:?}"
        );
    }

    #[test]
    fn recover_is_idempotent_if_the_syscall_already_completed() {
        let h = start();
        create_and_confirm(&h, b"a", b"x");
        let id = h.repl().state().index.get(b"a").unwrap();
        h.repl()
            .prepare_rename(b"a".to_vec(), b"b".to_vec(), 0)
            .unwrap();
        h.repl().backing.rename(b"a", b"b", 0).unwrap();
        let h = h.restart();
        assert!(h.repl().state().inflight.is_none());
        assert_eq!(h.repl().state().index.get(b"b"), Some(id));
        assert!(h.backing_path().join("b").exists());
        assert!(!h.backing_path().join("a").exists());
        let entries = journal_entries(&h);
        assert!(
            !entries.iter().any(|e| matches!(
                e,
                Entry::Rename { from, to, .. } if from == b"b" && to == b"a"
            )),
            "already-done rename must not be compensated: {entries:?}"
        );
    }
}
