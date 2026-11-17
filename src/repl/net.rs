//! Replication over the network: sending this node's journal, applying
//! peers' journals, and serving and pulling blocks and block hashes.

use std::collections::BTreeMap;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::mpsc;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use crate::config::NodeId;
use crate::net::wire::{Attrs, BlockHash, FileId, FileMeta, Message, Presence, PullMode, Version};

use super::backing::{hash_block, Backing};
use super::core::{lock, Repl};
use super::journal::Record;
use super::ranges::Ranges;
use super::relay::Queued;

/// Unacknowledged journal entries in flight per peer.
const SEND_WINDOW: u64 = 256;
/// Resend from the last acknowledged entry after this long without one.
const RESEND_AFTER: Duration = Duration::from_secs(10);
/// How long to wait for one answer from a peer.
pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// How often a waiting request checks that its connection is still there.
const RPC_POLL: Duration = Duration::from_millis(100);
/// Data per block pull and per hash reply.
const PULL_BYTES: u64 = 4 << 20;
const HASH_BYTES: u64 = 256 << 20;

/// How often positions go to each data peer, and how often a node asks
/// a relay for the entries of an origin it cannot reach.
const POSITIONS_EVERY: Duration = Duration::from_secs(1);
const RELAY_POLL: Duration = Duration::from_millis(300);
/// Entries per origin in one answer to `CatchUpRequest`.
const RELAY_BATCH: usize = 256;

pub(crate) fn start(repl: &Arc<Repl>) {
    let weak = Arc::downgrade(repl);
    repl.net.add_handler(
        Message::is_repl,
        Arc::new(move |from, msg| {
            if let Some(r) = weak.upgrade() {
                r.receive(from, msg);
            }
        }),
    );
    let mut threads = lock(&repl.threads);
    for &p in &repl.peers {
        let weak = Arc::downgrade(repl);
        threads.push(
            std::thread::Builder::new()
                .name(format!("syncfs-repl-tx-{p}"))
                .spawn(move || send_loop(weak, p))
                .expect("spawning a replication sender"),
        );
    }
    let weak = Arc::downgrade(repl);
    threads.push(
        std::thread::Builder::new()
            .name("syncfs-repl-relay".into())
            .spawn(move || relay_loop(weak))
            .expect("spawning the relay puller"),
    );
    let weak = Arc::downgrade(repl);
    threads.push(
        std::thread::Builder::new()
            .name("syncfs-repl-suspects".into())
            .spawn(move || super::fresh::suspect_loop(weak))
            .expect("spawning the suspect checker"),
    );
}

/// What a send loop last gossiped: the session, when, and the positions.
type PositionsSent = Option<(u64, Instant, Vec<(NodeId, u64)>)>;

fn send_loop(weak: Weak<Repl>, peer: NodeId) {
    let mut session = 0u64;
    let mut sent = 0u64;
    let mut last_acked = 0u64;
    let mut last_progress = Instant::now();
    let mut positions_sent: PositionsSent = None;
    let mut dropped = false;
    loop {
        let Some(r) = weak.upgrade() else { return };
        if r.stopping() {
            return;
        }
        let snap = r.net.snapshot();
        let role = snap
            .peers
            .iter()
            .find(|p| p.id == peer)
            .and_then(|p| p.role);
        if role.is_some_and(|ro| !ro.data) {
            if !dropped {
                log::info!("replication: node {peer} stores no data; not replicating to it");
                dropped = true;
                let mut st = r.state();
                st.dirty.drop_peer(peer);
                st.journal.drop_peer(peer);
                st.relay.drop_peer(peer);
            }
            let st = r.state();
            let _ = r.cv.wait_timeout(st, Duration::from_millis(500));
            continue;
        }
        if dropped && role.is_some() {
            // Restarted with a backing directory (it rebuilds from peers).
            log::info!("replication: node {peer} stores data now; replicating to it");
            dropped = false;
            r.add_data_peer(peer);
        }
        let s = r.net.session_of(peer);
        if s != session {
            // New connection (or none): resend from the last acknowledged
            // entry; pulls in flight on the old one are lost.
            session = s;
            let mut st = r.state();
            st.dirty.restore(peer);
            sent = st.journal.acked(peer);
        }
        if s != 0 && role.is_some() {
            // Positions: on a new connection, on a change, and now and then.
            let pos = r.positions();
            let due = match &positions_sent {
                None => true,
                Some((sess, at, last)) => {
                    *sess != s
                        || (*last != pos && at.elapsed() >= POSITIONS_EVERY)
                        || at.elapsed() >= POSITIONS_EVERY * 5
                }
            };
            if due {
                r.net.send(
                    peer,
                    Message::Positions {
                        applied: pos.clone(),
                    },
                );
                positions_sent = Some((s, Instant::now(), pos));
            }
        }
        if s != 0 {
            let st = r.state();
            let acked = st.journal.acked(peer);
            if acked != last_acked {
                last_acked = acked;
                last_progress = Instant::now();
            }
            sent = sent.max(acked);
            if sent > acked && last_progress.elapsed() > RESEND_AFTER {
                sent = acked;
                last_progress = Instant::now();
            }
            let room = SEND_WINDOW.saturating_sub(sent - acked) as usize;
            let batch = st.journal.sendable(sent, room);
            drop(st);
            for (seq, rec) in batch {
                if !r.net.send(
                    peer,
                    Message::JournalEntry {
                        origin: r.me,
                        seq,
                        deps: rec.deps,
                        entry: rec.entry,
                    },
                ) {
                    break;
                }
                sent = seq;
            }
        }
        let st = r.state();
        let _ = r.cv.wait_timeout(st, Duration::from_millis(100));
    }
}

impl Repl {
    /// Replicates to `peer` from now on (it may have stored no data so far).
    pub(crate) fn add_data_peer(&self, peer: NodeId) {
        let mut st = self.state();
        st.dirty.add_peer(peer);
        st.journal.add_peer(peer);
        st.relay.add_peer(peer);
    }

    /// What this node tells peers in `Positions`.
    pub(crate) fn positions(&self) -> Vec<(NodeId, u64)> {
        let st = self.state();
        let mut p: Vec<(NodeId, u64)> = st.applied_durable.iter().map(|(o, s)| (*o, *s)).collect();
        p.push((self.me, st.journal.durable()));
        p
    }

    pub(crate) fn receive(self: &Arc<Self>, from: NodeId, msg: Message) {
        match msg {
            Message::JournalEntry {
                origin,
                seq,
                deps,
                entry,
            } if origin != self.me && self.peers.contains(&origin) => {
                // From the origin, or forwarded by `from`.
                let mut inbox = lock(&self.inbox);
                let ib = inbox.entry(origin).or_default();
                ib.insert(
                    origin,
                    seq,
                    Queued {
                        deps,
                        entry,
                        via: from,
                    },
                );
                if !ib.worker {
                    ib.worker = true;
                    let weak = Arc::downgrade(self);
                    let spawned = std::thread::Builder::new()
                        .name(format!("syncfs-repl-rx-{origin}"))
                        .spawn(move || apply_loop(weak, origin));
                    match spawned {
                        Ok(h) => lock(&self.threads).push(h),
                        Err(e) => {
                            ib.worker = false;
                            log::error!("replication: cannot start applier for node {origin}: {e}");
                        }
                    }
                }
                drop(inbox);
                self.cv.notify_all();
            }
            Message::Positions { applied } => {
                self.state().relay.learn(from, &applied);
                self.cv.notify_all();
            }
            Message::CatchUpRequest { req, .. } if self.is_rebuilding() => {
                // Its entries may be ahead of its data: not a relay yet.
                self.net.send(from, Message::CatchUpDone { req });
            }
            Message::TreeRequest { req, after } => {
                let me = Arc::clone(self);
                let _ = std::thread::Builder::new()
                    .name("syncfs-repl-tree".into())
                    .spawn(move || me.serve_tree(from, req, after));
            }
            Message::CatchUpRequest { req, since } => {
                let batches: Vec<(NodeId, Vec<(u64, Record)>)> = {
                    let st = self.state();
                    since
                        .iter()
                        .filter(|(o, _)| *o != self.me && *o != from)
                        .map(|&(o, after)| (o, st.relay.serve(o, after, RELAY_BATCH)))
                        .collect()
                };
                let mut n = 0;
                for (origin, entries) in batches {
                    for (seq, rec) in entries {
                        n += 1;
                        self.net.send(
                            from,
                            Message::JournalEntry {
                                origin,
                                seq,
                                deps: rec.deps,
                                entry: rec.entry,
                            },
                        );
                    }
                }
                self.state().counters.entries_forwarded += n;
                self.net.send(from, Message::CatchUpDone { req });
            }
            Message::CatchUpDone { .. } => {}
            Message::JournalAck { origin, seq } if origin == self.me => self.on_ack(from, seq),
            Message::BlockPull { req, file, mode } => {
                let me = Arc::clone(self);
                let _ = std::thread::Builder::new()
                    .name("syncfs-repl-serve".into())
                    .spawn(move || me.serve_pull(from, req, file, mode));
            }
            Message::HashRequest {
                req,
                file,
                from: first,
                block_size,
            } => {
                let me = Arc::clone(self);
                let _ = std::thread::Builder::new()
                    .name("syncfs-repl-hash".into())
                    .spawn(move || me.serve_hashes(from, req, file, first, block_size));
            }
            Message::BlockData { req, .. }
            | Message::BlockDone { req, .. }
            | Message::HashReply { req, .. }
            | Message::TreeReply { req, .. } => {
                let tx = lock(&self.rpc).get(&req).cloned();
                if let Some(tx) = tx {
                    let _ = tx.send(msg);
                }
            }
            other => log::debug!("replication: unexpected {} from node {from}", other.kind()),
        }
    }

    fn on_ack(&self, peer: NodeId, seq: u64) {
        let mut announce = Vec::new();
        {
            let mut st = self.state();
            let st = &mut *st;
            let done = super::core::on_journal_ack(
                &mut st.journal,
                &mut st.dirty,
                &st.awaiting,
                peer,
                seq,
            );
            let peers: Vec<NodeId> = st.journal.peers().collect();
            for (s, id, v) in done {
                let h = st.holders.entry((id, v)).or_default();
                if h.insert(peer) {
                    announce.push((id, v, h.iter().copied().collect::<Vec<_>>()));
                }
                if peers.iter().all(|p| h.contains(p)) {
                    st.awaiting.remove(&s);
                    st.holders.remove(&(id, v));
                }
            }
        }
        for (id, v, h) in announce {
            self.announce_holders(id, v, h);
        }
        self.cv.notify_all();
    }

    /// Sends `msg` (which carries `req`) to `peer` and returns the
    /// receiver for the answers.
    pub(crate) fn call(&self, peer: NodeId, req: u64, msg: Message) -> io::Result<Rpc<'_>> {
        let (tx, rx) = mpsc::channel();
        lock(&self.rpc).insert(req, tx);
        let rpc = Rpc {
            repl: self,
            req,
            rx,
            peer,
            session: self.net.session_of(peer),
        };
        if !self.net.send(peer, msg) {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                format!("node {peer} is not connected"),
            ));
        }
        Ok(rpc)
    }

    pub(crate) fn req(&self) -> u64 {
        self.next_req
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn pull_max_blocks(&self) -> u64 {
        (PULL_BYTES / self.block_size).max(1)
    }

    fn serve_pull(&self, peer: NodeId, req: u64, file: FileId, mode: PullMode) {
        // A rebuilding node has nothing to serve of a file it lacks.
        let lacking = self.rebuild_missing(&file);
        let (blocks, more, path, version) = {
            let mut st = self.state();
            let path = st
                .index
                .path_of(&file)
                .map(<[u8]>::to_vec)
                .filter(|_| !lacking);
            let version = st.index.version(&file);
            match mode {
                PullMode::Discard(v) => {
                    st.dirty.discard(peer, file, v);
                    (Ranges::default(), false, path, version)
                }
                PullMode::Dirty { restart } => {
                    if restart {
                        st.dirty.restore(peer);
                    }
                    if path.is_none() {
                        st.dirty.forget(file);
                    }
                    let (r, more) = st.dirty.take_for_pull(peer, file, self.pull_max_blocks());
                    (r, more, path, version)
                }
                PullMode::Ranges(list) => {
                    let mut r: Ranges = list.into_iter().collect();
                    let r = r.take_front(self.pull_max_blocks());
                    (r, false, path, version)
                }
            }
        };
        let mut size = 0;
        let mut count = 0u64;
        let found = match path.as_deref().map(|p| self.backing.open_read(p)) {
            Some(Ok(f)) => {
                size = f.metadata().map(|m| m.len()).unwrap_or(0);
                let mut sent = 0u64;
                for n in blocks.iter().flat_map(|(a, b)| a..b) {
                    let data = match Backing::read_block(&f, n, self.block_size) {
                        Ok(d) => d,
                        Err(e) => {
                            log::warn!("replication: reading block {n} of {file}: {e}");
                            break;
                        }
                    };
                    sent += data.len() as u64;
                    count += 1;
                    self.net.send(
                        peer,
                        Message::BlockData {
                            req,
                            block: n,
                            hash: hash_block(&data),
                            data,
                        },
                    );
                }
                let mut st = self.state();
                st.counters.blocks_sent += count;
                st.counters.bytes_sent += sent;
                true
            }
            _ => false,
        };
        self.net.send(
            peer,
            Message::BlockDone {
                req,
                blocks: count,
                found,
                size,
                version,
                more,
            },
        );
    }

    fn hash_max(&self, block_size: u64) -> u64 {
        (HASH_BYTES / block_size.max(1)).clamp(1, 65536)
    }

    fn serve_hashes(&self, peer: NodeId, req: u64, file: FileId, first: u64, block_size: u64) {
        let (path, version, lineage, removed) = {
            let st = self.state();
            (
                st.index.path_of(&file).map(<[u8]>::to_vec),
                st.index.version(&file),
                st.index.lineage(&file),
                st.index.tombstone(&file).is_some(),
            )
        };
        let reply =
            |presence, size, attrs, hashes: Vec<BlockHash>, more, xattrs| Message::HashReply {
                req,
                presence,
                version,
                meta: FileMeta { size, attrs },
                from: first,
                hashes,
                more,
                xattrs,
                lineage: lineage.clone(),
            };
        if self.rebuild_missing(&file) {
            let msg = reply(
                Presence::Missing,
                0,
                Attrs::default(),
                Vec::new(),
                false,
                Vec::new(),
            );
            self.net.send(peer, msg);
            return;
        }
        let msg = (|| {
            let path = path?;
            let st = self.backing.stat(&path).ok()?;
            let attrs = super::backing::attrs_of(&st);
            let size = st.st_size as u64;
            if block_size != self.block_size {
                log::warn!("replication: node {peer} uses another block_size; cannot compare");
                return None;
            }
            let total = size.div_ceil(block_size);
            let f = self.backing.open_read(&path).ok()?;
            let end = total.min(first.saturating_add(self.hash_max(block_size)));
            let mut hashes = Vec::new();
            for n in first..end {
                hashes.push(hash_block(&Backing::read_block(&f, n, block_size).ok()?));
            }
            let xattrs = if first == 0 {
                match self.backing.xattrs(&path) {
                    Ok(x) => x,
                    Err(e) => {
                        log::warn!("replication: reading xattrs of {file}: {e}");
                        return None;
                    }
                }
            } else {
                Vec::new()
            };
            Some(reply(
                Presence::Found,
                size,
                attrs,
                hashes,
                end < total,
                xattrs,
            ))
        })()
        .unwrap_or_else(|| {
            let presence = if removed {
                Presence::Removed
            } else {
                Presence::Absent
            };
            reply(presence, 0, Attrs::default(), Vec::new(), false, Vec::new())
        });
        self.net.send(peer, msg);
    }

    /// Asks `peer` for every block hash of `file`.
    pub(crate) fn remote_hashes(&self, peer: NodeId, file: FileId) -> io::Result<RemoteFile> {
        let mut out = RemoteFile::default();
        loop {
            let req = self.req();
            let rpc = self.call(
                peer,
                req,
                Message::HashRequest {
                    req,
                    file,
                    from: out.hashes.len() as u64,
                    block_size: self.block_size,
                },
            )?;
            match rpc.recv()? {
                Message::HashReply {
                    presence,
                    version,
                    meta,
                    hashes,
                    more,
                    xattrs,
                    lineage,
                    ..
                } => {
                    let why = match presence {
                        Presence::Found => None,
                        Presence::Removed => {
                            out.version = version;
                            out.lineage = lineage;
                            out.removed = true;
                            return Ok(out);
                        }
                        Presence::Missing => {
                            // Not a "gone": ask again or elsewhere.
                            return Err(io::Error::new(
                                io::ErrorKind::WouldBlock,
                                super::fresh::Missing,
                            ));
                        }
                        Presence::Absent => Some("does not have"),
                    };
                    if let Some(why) = why {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("node {peer} {why} {file}"),
                        ));
                    }
                    out.version = version;
                    out.lineage = lineage;
                    out.size = meta.size;
                    out.attrs = meta.attrs;
                    if out.hashes.is_empty() {
                        out.xattrs = xattrs;
                    }
                    out.hashes.extend(hashes);
                    if !more {
                        return Ok(out);
                    }
                }
                other => return Err(io::Error::other(format!("unexpected {}", other.kind()))),
            }
        }
    }

    /// Pulls blocks of `file` from `peer` into `out`. Returns the peer's
    /// size, version and whether it has more dirty blocks for us.
    pub(crate) fn pull(
        &self,
        peer: NodeId,
        file: FileId,
        mode: PullMode,
        out: &std::fs::File,
    ) -> io::Result<Pulled> {
        let req = self.req();
        let rpc = self.call(peer, req, Message::BlockPull { req, file, mode })?;
        let mut bytes = 0u64;
        let mut blocks = 0u64;
        // The end, once it came; blocks may still follow it (see
        // `Message::BlockDone`).
        let mut done: Option<(u64, Pulled)> = None;
        loop {
            if let Some((want, _)) = &done {
                if blocks >= *want {
                    break;
                }
            }
            match rpc.recv()? {
                Message::BlockData {
                    block, hash, data, ..
                } => {
                    if hash_block(&data) != hash {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("block {block} of {file} from node {peer}: checksum mismatch"),
                        ));
                    }
                    out.write_all_at(&data, block * self.block_size)?;
                    bytes += data.len() as u64;
                    blocks += 1;
                }
                Message::BlockDone {
                    blocks: want,
                    found,
                    size,
                    version,
                    more,
                    ..
                } => {
                    done = Some((
                        want,
                        Pulled {
                            found,
                            size,
                            version,
                            more,
                        },
                    ));
                }
                other => return Err(io::Error::other(format!("unexpected {}", other.kind()))),
            }
        }
        let mut st = self.state();
        st.counters.blocks_received += blocks;
        st.counters.bytes_received += bytes;
        Ok(done.expect("loop ends with the answer").1)
    }
}

#[derive(Default, Debug)]
pub(crate) struct RemoteFile {
    pub version: Option<Version>,
    pub size: u64,
    pub attrs: Attrs,
    pub hashes: Vec<BlockHash>,
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    pub lineage: Vec<Version>,
    /// The file was removed at `version` there.
    pub removed: bool,
}

pub(crate) struct Pulled {
    pub found: bool,
    pub size: u64,
    #[allow(dead_code)]
    pub version: Option<Version>,
    pub more: bool,
}

/// Answers to one request; unregisters on drop.
pub(crate) struct Rpc<'a> {
    repl: &'a Repl,
    req: u64,
    rx: mpsc::Receiver<Message>,
    peer: NodeId,
    /// The connection the request went out on: answers cannot come once
    /// it is gone.
    session: u64,
}

impl Rpc<'_> {
    pub fn recv(&self) -> io::Result<Message> {
        let deadline = Instant::now() + RPC_TIMEOUT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.rx.recv_timeout(left.min(RPC_POLL)) {
                Ok(m) => return Ok(m),
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
            if self.repl.net.session_of(self.peer) != self.session {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    format!("node {} went away", self.peer),
                ));
            }
            if left.is_zero() {
                break;
            }
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "no answer from peer",
        ))
    }
}

impl Drop for Rpc<'_> {
    fn drop(&mut self) {
        lock(&self.repl.rpc).remove(&self.req);
    }
}

/// Why applying an entry did not complete.
pub(crate) enum ApplyError {
    /// Try again later (peer unreachable, timeout).
    Retry(io::Error),
    /// Give up on this entry.
    Skip(io::Error),
}

fn apply_loop(weak: Weak<Repl>, origin: NodeId) {
    let mut backoff = Duration::from_millis(50);
    loop {
        let Some(r) = weak.upgrade() else { return };
        if r.stopping() {
            return;
        }
        if r.rebuild_holds_entries() {
            // The tree is being made; its positions say where to go on.
            let st = r.state();
            let _ = r.cv.wait_timeout(st, Duration::from_millis(200));
            continue;
        }
        // The next entry to apply, if it and its dependencies are here.
        let next = {
            let (applied, all) = {
                let st = r.state();
                (
                    st.applied.get(&origin).copied().unwrap_or(0),
                    st.applied.clone(),
                )
            };
            lock(&r.inbox)
                .entry(origin)
                .or_default()
                .next(r.me, applied, &all)
        };
        let Some((seq, q)) = next else {
            // Acknowledge again whatever is applied (the ack may be lost).
            let applied = r.state().applied.get(&origin).copied().unwrap_or(0);
            if applied > 0 {
                r.net.send(
                    origin,
                    Message::JournalAck {
                        origin,
                        seq: applied,
                    },
                );
            }
            let st = r.state();
            let _ = r.cv.wait_timeout(st, Duration::from_millis(200));
            continue;
        };
        let res = r.apply(origin, q.via, &q.entry);
        let done = |r: &Repl, ok: bool| {
            let mut st = r.state();
            if ok {
                st.counters.entries_applied += 1;
            }
            if q.via != origin {
                st.counters.entries_relayed += 1;
            }
            st.applied.insert(origin, seq);
            st.applied_changed = true;
            st.relay.keep(
                origin,
                seq,
                Record {
                    deps: q.deps.clone(),
                    entry: q.entry.clone(),
                },
            );
            drop(st);
            // Durable before acknowledging.
            if r.flusher.flush_now().is_ok() {
                let mut st = r.state();
                let d = st.applied_durable.entry(origin).or_default();
                *d = (*d).max(seq);
                drop(st);
                r.net.send(origin, Message::JournalAck { origin, seq });
            }
            r.cv.notify_all();
        };
        match res {
            Ok(applied) => {
                backoff = Duration::from_millis(50);
                if !applied {
                    r.state().counters.entries_skipped += 1;
                }
                done(&r, applied);
            }
            Err(ApplyError::Skip(e)) => {
                log::warn!(
                    "replication: skipping entry {seq} from node {origin} ({:?}): {e}",
                    q.entry
                );
                r.state().counters.apply_errors += 1;
                done(&r, false);
            }
            Err(ApplyError::Retry(e)) => {
                log::debug!("replication: entry {seq} from node {origin}: {e}; retrying");
                // A copy whose sender went away is dropped: the origin
                // resends on its next connection, and meanwhile the relay
                // loop asks a peer that has the entry.
                if r.data_peers_up().contains(&q.via) {
                    lock(&r.inbox)
                        .entry(origin)
                        .or_default()
                        .insert(origin, seq, q);
                }
                drop(r);
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        }
    }
}

/// Asks relays for the entries of origins this node cannot reach.
fn relay_loop(weak: Weak<Repl>) {
    loop {
        let Some(r) = weak.upgrade() else { return };
        if r.stopping() {
            return;
        }
        let up = r.data_peers_up();
        let mut ask: BTreeMap<NodeId, Vec<(NodeId, u64)>> = BTreeMap::new();
        {
            let st = r.state();
            let inbox = lock(&r.inbox);
            for &o in &r.peers {
                if up.contains(&o) {
                    continue;
                }
                let applied = st.applied.get(&o).copied().unwrap_or(0);
                if inbox.get(&o).is_some_and(|ib| ib.has(applied + 1)) {
                    continue;
                }
                let best = up
                    .iter()
                    .map(|&p| (st.relay.position(p, o), p))
                    .max()
                    .filter(|(pos, _)| *pos > applied);
                if let Some((_, p)) = best {
                    ask.entry(p).or_default().push((o, applied));
                }
            }
        }
        for (p, since) in ask {
            log::debug!("replication: asking node {p} for entries of {since:?}");
            let req = r.req();
            r.net.send(p, Message::CatchUpRequest { req, since });
        }
        let st = r.state();
        let _ = r.cv.wait_timeout(st, RELAY_POLL);
    }
}
