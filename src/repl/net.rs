//! Replication over the network: sending this node's journal, applying
//! peers' journals, and serving and pulling blocks and block hashes.

use std::collections::BTreeMap;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::mpsc;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use crate::config::NodeId;
use crate::net::wire::{Attrs, BlockHash, Entry, FileId, Message, PullMode, Version};

use super::backing::{hash_block, Backing};
use super::core::{lock, Repl};
use super::ranges::Ranges;

/// Unacknowledged journal entries in flight per peer.
const SEND_WINDOW: u64 = 256;
/// Resend from the last acknowledged entry after this long without one.
const RESEND_AFTER: Duration = Duration::from_secs(10);
/// How long to wait for one answer from a peer.
pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// Data per block pull and per hash reply.
const PULL_BYTES: u64 = 4 << 20;
const HASH_BYTES: u64 = 256 << 20;

/// Journal entries received from one origin, not applied yet.
#[derive(Default)]
pub(crate) struct Inbox {
    pub queue: BTreeMap<u64, (Vec<(NodeId, u64)>, Entry)>,
    pub worker: bool,
}

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
            .name("syncfs-repl-suspects".into())
            .spawn(move || super::fresh::suspect_loop(weak))
            .expect("spawning the suspect checker"),
    );
}

fn send_loop(weak: Weak<Repl>, peer: NodeId) {
    let mut session = 0u64;
    let mut sent = 0u64;
    let mut last_acked = 0u64;
    let mut last_progress = Instant::now();
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
            log::info!("replication: node {peer} stores no data; not replicating to it");
            let mut st = r.state();
            st.dirty.drop_peer(peer);
            st.journal.drop_peer(peer);
            return;
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
    pub(crate) fn receive(self: &Arc<Self>, from: NodeId, msg: Message) {
        match msg {
            Message::JournalEntry {
                origin,
                seq,
                deps,
                entry,
            } if origin == from => {
                let mut inbox = lock(&self.inbox);
                let ib = inbox.entry(from).or_default();
                ib.queue.insert(seq, (deps, entry));
                if !ib.worker {
                    ib.worker = true;
                    let weak = Arc::downgrade(self);
                    let spawned = std::thread::Builder::new()
                        .name(format!("syncfs-repl-rx-{from}"))
                        .spawn(move || apply_loop(weak, from));
                    match spawned {
                        Ok(h) => lock(&self.threads).push(h),
                        Err(e) => {
                            ib.worker = false;
                            log::error!("replication: cannot start applier for node {from}: {e}");
                        }
                    }
                }
                drop(inbox);
                self.cv.notify_all();
            }
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
            | Message::HashReply { req, .. } => {
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
            st.journal.ack(peer, seq);
            st.dirty.acked(peer);
            let peers: Vec<NodeId> = st.journal.peers().collect();
            let done: Vec<(u64, FileId, Version)> = st
                .awaiting
                .range(..=seq)
                .map(|(s, (id, v))| (*s, *id, *v))
                .collect();
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
        let (blocks, more, path, version) = {
            let mut st = self.state();
            let path = st.index.path_of(&file).map(<[u8]>::to_vec);
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
                st.counters.blocks_sent += blocks.len();
                st.counters.bytes_sent += sent;
                true
            }
            _ => false,
        };
        self.net.send(
            peer,
            Message::BlockDone {
                req,
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
        let (path, version) = {
            let st = self.state();
            (
                st.index.path_of(&file).map(<[u8]>::to_vec),
                st.index.version(&file),
            )
        };
        let reply = |found, size, attrs, hashes: Vec<BlockHash>, more, xattrs| Message::HashReply {
            req,
            found,
            version,
            size,
            attrs,
            from: first,
            hashes,
            more,
            xattrs,
        };
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
            Some(reply(true, size, attrs, hashes, end < total, xattrs))
        })()
        .unwrap_or_else(|| reply(false, 0, Attrs::default(), Vec::new(), false, Vec::new()));
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
                    found,
                    version,
                    size,
                    attrs,
                    hashes,
                    more,
                    xattrs,
                    ..
                } => {
                    if !found {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("node {peer} does not have {file}"),
                        ));
                    }
                    out.version = version;
                    out.size = size;
                    out.attrs = attrs;
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
        loop {
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
                    found,
                    size,
                    version,
                    more,
                    ..
                } => {
                    let mut st = self.state();
                    st.counters.blocks_received += blocks;
                    st.counters.bytes_received += bytes;
                    return Ok(Pulled {
                        found,
                        size,
                        version,
                        more,
                    });
                }
                other => return Err(io::Error::other(format!("unexpected {}", other.kind()))),
            }
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct RemoteFile {
    pub version: Option<Version>,
    pub size: u64,
    pub attrs: Attrs,
    pub hashes: Vec<BlockHash>,
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
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
}

impl Rpc<'_> {
    pub fn recv(&self) -> io::Result<Message> {
        self.rx
            .recv_timeout(RPC_TIMEOUT)
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no answer from peer"))
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
        // The next entry to apply, if it and its dependencies are here.
        let next = {
            let applied = r.state().applied.get(&origin).copied().unwrap_or(0);
            let mut inbox = lock(&r.inbox);
            let ib = inbox.entry(origin).or_default();
            // Duplicates (resent after a reconnect).
            while ib
                .queue
                .first_key_value()
                .is_some_and(|(s, _)| *s <= applied)
            {
                ib.queue.pop_first();
            }
            match ib.queue.first_key_value() {
                Some((&s, (deps, _))) if s == applied + 1 => {
                    let st = r.state();
                    let ready = deps
                        .iter()
                        .all(|(n, k)| *n == r.me || st.applied.get(n).copied().unwrap_or(0) >= *k);
                    drop(st);
                    ready.then(|| ib.queue.pop_first().expect("checked"))
                }
                _ => None,
            }
        };
        let Some((seq, (deps, entry))) = next else {
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
        match r.apply(origin, &entry) {
            Ok(applied) => {
                backoff = Duration::from_millis(50);
                let mut st = r.state();
                if applied {
                    st.counters.entries_applied += 1;
                } else {
                    st.counters.entries_skipped += 1;
                }
                st.applied.insert(origin, seq);
                st.applied_changed = true;
                drop(st);
                // Durable before acknowledging.
                if r.flusher.flush_now().is_ok() {
                    r.net.send(origin, Message::JournalAck { origin, seq });
                }
                r.cv.notify_all();
            }
            Err(ApplyError::Skip(e)) => {
                log::warn!("replication: skipping entry {seq} from node {origin} ({entry:?}): {e}");
                let mut st = r.state();
                st.counters.apply_errors += 1;
                st.applied.insert(origin, seq);
                st.applied_changed = true;
                drop(st);
                if r.flusher.flush_now().is_ok() {
                    r.net.send(origin, Message::JournalAck { origin, seq });
                }
            }
            Err(ApplyError::Retry(e)) => {
                log::debug!("replication: entry {seq} from node {origin}: {e}; retrying");
                lock(&r.inbox)
                    .entry(origin)
                    .or_default()
                    .queue
                    .insert(seq, (deps, entry));
                drop(r);
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        }
    }
}
