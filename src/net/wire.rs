//! The node-to-node wire format.
//!
//! A connection starts with an 8-byte preamble in each direction: the magic
//! `SYNCFS` and the protocol version as a big-endian `u16`. The preamble
//! never changes, so any two versions can tell that they disagree. After it
//! come frames:
//!
//! ```text
//! +----------------------+------------------------------+
//! | length: u32, BE      | payload: postcard(Message)   |
//! +----------------------+------------------------------+
//! ```
//!
//! `length` counts the payload only, must be at least 1 and at most
//! [`MAX_FRAME_LEN`] ([`HANDSHAKE_MAX_FRAME_LEN`] before the handshake
//! completes). The payload must decode to exactly one [`Message`] with no
//! bytes left over. Any violation closes the connection.
//!
//! [`Message`] variants are identified by their position, so variants and
//! fields are only ever appended within a protocol version; anything else
//! bumps [`PROTOCOL_VERSION`].

use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::NodeId;

pub const MAGIC: [u8; 6] = *b"SYNCFS";
pub const PROTOCOL_VERSION: u16 = 5;
pub const PREAMBLE_LEN: usize = 8;
/// Largest payload: a 16 MiB block (the largest `block_size`) plus headroom.
pub const MAX_FRAME_LEN: u32 = 32 << 20;
/// Largest payload accepted before the peer is authenticated.
pub const HANDSHAKE_MAX_FRAME_LEN: u32 = 64 << 10;

#[derive(Debug, Error)]
pub enum WireError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("not a syncfs peer (bad magic)")]
    BadMagic,
    #[error("frame of {0} bytes exceeds the limit of {1}")]
    TooLarge(u32, u32),
    #[error("empty frame")]
    Empty,
    #[error("undecodable frame: {0}")]
    Decode(#[from] postcard::Error),
    #[error("{0} trailing bytes after the message")]
    Trailing(usize),
}

impl WireError {
    /// The peer closed the connection cleanly between frames.
    pub fn is_eof(&self) -> bool {
        matches!(self, WireError::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof)
    }
}

pub fn preamble() -> [u8; PREAMBLE_LEN] {
    let mut p = [0; PREAMBLE_LEN];
    p[..6].copy_from_slice(&MAGIC);
    p[6..].copy_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    p
}

/// Checks the magic and returns the peer's protocol version.
pub fn parse_preamble(p: &[u8; PREAMBLE_LEN]) -> Result<u16, WireError> {
    if p[..6] != MAGIC {
        return Err(WireError::BadMagic);
    }
    Ok(u16::from_be_bytes([p[6], p[7]]))
}

/// Encodes `msg` as one frame (length prefix included).
pub fn encode(msg: &Message) -> Vec<u8> {
    let mut out = vec![0; 4];
    out = postcard::to_extend(msg, out).expect("serializing to a Vec cannot fail");
    let len = (out.len() - 4) as u32;
    out[..4].copy_from_slice(&len.to_be_bytes());
    out
}

/// Decodes one frame payload (without the length prefix).
pub fn decode_payload(payload: &[u8]) -> Result<Message, WireError> {
    if payload.is_empty() {
        return Err(WireError::Empty);
    }
    let (msg, rest) = postcard::take_from_bytes(payload)?;
    if !rest.is_empty() {
        return Err(WireError::Trailing(rest.len()));
    }
    Ok(msg)
}

fn check_len(len: u32, max: u32) -> Result<usize, WireError> {
    match len {
        0 => Err(WireError::Empty),
        n if n > max => Err(WireError::TooLarge(n, max)),
        n => Ok(n as usize),
    }
}

pub fn write_frame(w: &mut impl Write, msg: &Message) -> io::Result<()> {
    w.write_all(&encode(msg))
}

/// Reads one frame of at most `max` payload bytes.
pub fn read_frame(r: &mut impl Read, max: u32) -> Result<Message, WireError> {
    let mut len = [0; 4];
    r.read_exact(&mut len)?;
    let n = check_len(u32::from_be_bytes(len), max)?;
    let mut payload = vec![0; n];
    r.read_exact(&mut payload)?;
    decode_payload(&payload)
}

/// Incremental frame decoder for byte streams that arrive in pieces.
#[derive(Debug)]
pub struct FrameDecoder {
    buf: Vec<u8>,
    max: u32,
}

impl FrameDecoder {
    pub fn new(max: u32) -> Self {
        Self {
            buf: Vec::new(),
            max,
        }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// The next complete frame, `None` if more bytes are needed. After an
    /// error the stream is unusable.
    pub fn next_frame(&mut self) -> Result<Option<Message>, WireError> {
        let Some(len) = self.buf.get(..4) else {
            return Ok(None);
        };
        let n = check_len(
            u32::from_be_bytes(len.try_into().expect("4 bytes")),
            self.max,
        )?;
        if self.buf.len() < 4 + n {
            return Ok(None);
        }
        let msg = decode_payload(&self.buf[4..4 + n]);
        self.buf.drain(..4 + n);
        msg.map(Some)
    }
}

/// Everything nodes say to each other.
///
/// Lock messages: `key` is the path relative to the mount root; `req`
/// identifies one attempt of the requesting node, and replies echo it.
/// They also carry file versions (see `crate::repl::version`): the request
/// names the file, grants say what the voter knows about its versions,
/// the confirm announces the version the holder's session will write, and
/// the release says whether it did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Message {
    /// First frame from the dialing node.
    Hello(Hello),
    /// Accepting node's answer to an acceptable `Hello`.
    Welcome(Welcome),
    /// Accepting node refuses the connection; it closes right after.
    Reject(Reject),
    /// Sent every `heartbeat_interval` in both directions.
    Heartbeat(Heartbeat),
    /// The sender is shutting down; the receiver marks it down at once.
    Goodbye {
        reason: String,
    },

    // Whole-file locks (see `crate::lock`).
    /// Phase 1: asks a voter for a provisional grant.
    LockRequest {
        req: u64,
        key: Vec<u8>,
        ballot: Ballot,
        /// The file at `key`, if it exists on the requester.
        file: Option<FileId>,
    },
    /// Phase 1 reply: provisional grant.
    LockGrant {
        req: u64,
        ballot: Ballot,
        /// What this voter knows about the requested file's versions.
        known: Option<VersionInfo>,
    },
    /// Refusal of a request, confirmation or renewal; also sent unasked
    /// when an older ballot preempts a provisional grant.
    LockDeny {
        req: u64,
        ballot: Ballot,
        reason: DenyReason,
        holder: Option<NodeId>,
    },
    /// Phase 2: turns the provisional grant into a lease.
    LockConfirm {
        req: u64,
        key: Vec<u8>,
        ballot: Ballot,
        lease_ms: u64,
        /// The version this lock session will write, if it writes.
        version: Option<(FileId, Version)>,
    },
    /// Phase 2 reply: the lease started when the voter received the confirm.
    LockConfirmed {
        req: u64,
        ballot: Ballot,
    },
    /// Extends a confirmed lease by `lease_ms` from receipt.
    LockRenew {
        req: u64,
        key: Vec<u8>,
        ballot: Ballot,
        lease_ms: u64,
    },
    LockRenewed {
        req: u64,
        ballot: Ballot,
    },
    /// Drops whatever grant `ballot` has for `key`.
    LockRelease {
        key: Vec<u8>,
        ballot: Ballot,
        /// Whether the announced version was written.
        outcome: Option<Outcome>,
    },

    // Asynchronous replication (see `crate::repl`).
    /// One entry of `origin`'s metadata journal. `deps` are the journal
    /// positions of other origins the sender had applied when it made the
    /// entry; receivers apply it only after those.
    JournalEntry {
        origin: NodeId,
        seq: u64,
        deps: Vec<(NodeId, u64)>,
        entry: Entry,
    },
    /// Everything of `origin`'s journal up to `seq` is applied and durable.
    JournalAck {
        origin: NodeId,
        seq: u64,
    },
    /// Asks for blocks of `file`, or drops marks (see [`PullMode`]).
    BlockPull {
        req: u64,
        file: FileId,
        mode: PullMode,
    },
    /// One block (shorter at the end of the file; empty past it).
    BlockData {
        req: u64,
        block: u64,
        hash: BlockHash,
        data: Vec<u8>,
    },
    /// End of the answer to a `BlockPull`. `more`: dirty blocks are left,
    /// pull again. `blocks`: how many `BlockData` came before it (they may
    /// arrive after it when the bulk lane opened during the answer).
    BlockDone {
        req: u64,
        blocks: u64,
        found: bool,
        size: u64,
        version: Option<Version>,
        more: bool,
    },
    /// Asks for the block hashes of `file` from block `from` on.
    HashRequest {
        req: u64,
        file: FileId,
        from: u64,
        block_size: u64,
    },
    HashReply {
        req: u64,
        found: bool,
        version: Option<Version>,
        size: u64,
        attrs: Attrs,
        from: u64,
        hashes: Vec<BlockHash>,
        /// More hashes follow (ask again from `from + hashes.len()`).
        more: bool,
        /// The file's extended attributes, sorted by name (in the first
        /// reply only, `from == 0`).
        xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// How far the sender has durably applied each origin's journal (its
    /// own journal under its own id). Sent to data peers about once a
    /// second.
    Positions {
        applied: Vec<(NodeId, u64)>,
    },
    /// The nodes known to hold `version` of `file` completely.
    VersionHolders {
        file: FileId,
        version: Version,
        holders: Vec<NodeId>,
    },

    /// Asks a node for the entries of other origins it keeps for
    /// forwarding (`crate::repl::relay`): for each `(origin, after)`, the
    /// entries after `after`, as `JournalEntry` messages (with the
    /// original `origin`), then `CatchUpDone`.
    CatchUpRequest {
        req: u64,
        since: Vec<(NodeId, u64)>,
    },
    CatchUpDone {
        req: u64,
    },

    // Step 5 addendum: skipping the start-up grace on a cold start.
    /// "Could you be relying on a lease I granted before I restarted?"
    LockFreshQuery {
        round: u64,
    },
    /// `fresh`: no lock acquired since this process started, and no
    /// attempt in progress that counts a vote from the asker.
    LockFreshReply {
        round: u64,
        fresh: bool,
    },
}

impl Message {
    /// Lock and version messages (handled by `crate::lock`).
    pub fn is_lock(&self) -> bool {
        matches!(
            self,
            Message::LockRequest { .. }
                | Message::LockGrant { .. }
                | Message::LockDeny { .. }
                | Message::LockConfirm { .. }
                | Message::LockConfirmed { .. }
                | Message::LockRenew { .. }
                | Message::LockRenewed { .. }
                | Message::LockRelease { .. }
                | Message::LockFreshQuery { .. }
                | Message::LockFreshReply { .. }
                | Message::VersionHolders { .. }
        )
    }

    /// Large or numerous messages that queue behind control messages.
    pub fn is_bulk(&self) -> bool {
        matches!(
            self,
            Message::JournalEntry { .. }
                | Message::BlockData { .. }
                | Message::BlockDone { .. }
                | Message::HashReply { .. }
        )
    }

    /// Replication messages (handled by `crate::repl`).
    pub fn is_repl(&self) -> bool {
        matches!(
            self,
            Message::JournalEntry { .. }
                | Message::JournalAck { .. }
                | Message::BlockPull { .. }
                | Message::BlockData { .. }
                | Message::BlockDone { .. }
                | Message::HashRequest { .. }
                | Message::HashReply { .. }
                | Message::Positions { .. }
                | Message::CatchUpRequest { .. }
                | Message::CatchUpDone { .. }
        )
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Message::Hello(_) => "hello",
            Message::Welcome(_) => "welcome",
            Message::Reject(_) => "reject",
            Message::Heartbeat(_) => "heartbeat",
            Message::Goodbye { .. } => "goodbye",
            Message::LockRequest { .. } => "lock-request",
            Message::LockGrant { .. } => "lock-grant",
            Message::LockDeny { .. } => "lock-deny",
            Message::LockConfirm { .. } => "lock-confirm",
            Message::LockConfirmed { .. } => "lock-confirmed",
            Message::LockRenew { .. } => "lock-renew",
            Message::LockRenewed { .. } => "lock-renewed",
            Message::LockRelease { .. } => "lock-release",
            Message::JournalEntry { .. } => "journal-entry",
            Message::JournalAck { .. } => "journal-ack",
            Message::BlockPull { .. } => "block-pull",
            Message::BlockData { .. } => "block-data",
            Message::BlockDone { .. } => "block-done",
            Message::HashRequest { .. } => "hash-request",
            Message::HashReply { .. } => "hash-reply",
            Message::Positions { .. } => "positions",
            Message::VersionHolders { .. } => "version-holders",
            Message::CatchUpRequest { .. } => "catch-up-request",
            Message::CatchUpDone { .. } => "catch-up-done",
            Message::LockFreshQuery { .. } => "lock-fresh-query",
            Message::LockFreshReply { .. } => "lock-fresh-reply",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Hello {
    pub node_id: NodeId,
    pub cluster_name: String,
    /// Sorted node ids of the sender's `peers`.
    pub members: Vec<NodeId>,
    /// Random per process start; a change means the peer restarted.
    pub incarnation: u64,
    /// `CARGO_PKG_VERSION` of the sender, for diagnostics.
    pub software: String,
    pub role: Role,
    /// Which connection of the pair this is.
    pub lane: Lane,
}

/// The connections between two nodes. Every pair has a main connection;
/// two data nodes also open a bulk lane for journal entries, blocks and
/// hash replies, so transfers never queue in front of locks and
/// heartbeats. The bulk lane belongs to the main connection with the same
/// incarnations and lives and dies with it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Lane {
    #[default]
    Main,
    Bulk,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Welcome {
    pub node_id: NodeId,
    pub incarnation: u64,
    pub software: String,
    /// Sorted ids of the acceptor's members, so the dialer can compare too.
    pub members: Vec<NodeId>,
    pub role: Role,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Reject {
    pub reason: RejectReason,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum RejectReason {
    /// Different `cluster_name`.
    ClusterName,
    /// Different set of node ids in `peers`. No longer sent (protocol 3
    /// accepts such peers with a warning); kept so variants keep their
    /// positions.
    Membership,
    /// The dialer's node id is not a member.
    UnknownNode,
    /// The dialer claims the acceptor's own node id.
    SameNodeId,
    /// Only the lower node id dials; a higher one dialed.
    WrongDirection,
}

/// What a node stores.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Role {
    /// Has a backing directory (a mount), as opposed to a voter-only
    /// daemon.
    pub data: bool,
    /// Identity of the backing directory's disk (0 for daemons).
    pub disk: u64,
}

/// Cluster-wide identity of a file, directory or other node, kept across
/// renames.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct FileId(pub [u8; 16]);

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// What a `BlockPull` asks for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum PullMode {
    /// The blocks the asker is missing according to the answering node's
    /// dirty bitmap. `restart`: the asker starts applying an entry, so
    /// blocks sent to it earlier and not acknowledged were lost (a failed
    /// attempt) and are marked again first.
    Dirty { restart: bool },
    /// These block ranges.
    Ranges(Vec<(u64, u64)>),
    /// The asker already has this version: drop its marks up to it.
    Discard(Version),
}

/// BLAKE3 of a block, truncated.
pub type BlockHash = [u8; 16];

/// One version of a file's contents and attributes. Ordered by `seq`, then
/// `node`. See `crate::repl::version` for how versions are made.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Version {
    pub seq: u64,
    pub node: NodeId,
    /// The writer's disk id.
    pub disk: u64,
}

/// What a voter knows about one file.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct VersionInfo {
    /// Newest version announced; `pending` while its writer's session may
    /// still be writing it (or ended without saying).
    pub latest: Option<Version>,
    pub pending: bool,
    /// Nodes known to hold `latest` completely.
    pub holders: Vec<NodeId>,
    /// Newest version known to be on a quorum of nodes, and those nodes.
    pub committed: Option<Version>,
    pub committed_holders: Vec<NodeId>,
}

/// End of a lock session, for the version table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Outcome {
    pub file: FileId,
    pub version: Version,
    pub written: bool,
}

/// Attributes carried by journal entries and hash replies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Attrs {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: (i64, u32),
    pub atime: (i64, u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum NodeKind {
    File,
    Dir,
    Symlink,
    Fifo,
    Socket,
    CharDevice,
    BlockDevice,
}

/// One change in a metadata journal. Paths are relative to the mount
/// root, as the origin saw them when it made the change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Entry {
    Create {
        id: FileId,
        path: Vec<u8>,
        kind: NodeKind,
        attrs: Attrs,
        rdev: u64,
        /// Symlink target.
        target: Vec<u8>,
    },
    /// New hard link `path` to the file at `existing`.
    Link {
        id: FileId,
        existing: Vec<u8>,
        path: Vec<u8>,
    },
    Remove {
        id: Option<FileId>,
        path: Vec<u8>,
        dir: bool,
    },
    Rename {
        id: Option<FileId>,
        from: Vec<u8>,
        to: Vec<u8>,
        flags: u32,
    },
    /// Mode, owner and times after a change.
    Attr {
        id: FileId,
        path: Vec<u8>,
        version: Version,
        attrs: Attrs,
    },
    Xattr {
        id: FileId,
        path: Vec<u8>,
        name: Vec<u8>,
        value: Option<Vec<u8>>,
    },
    /// Contents changed: pull the dirty blocks from the origin.
    Data {
        id: FileId,
        path: Vec<u8>,
        version: Version,
        size: u64,
        attrs: Attrs,
    },
}

/// Orders lock attempts: lower is older and wins conflicts. `counter` is a
/// Lamport clock, `node` breaks ties.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Ballot {
    pub counter: u64,
    pub node: NodeId,
}

impl std::fmt::Display for Ballot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.counter, self.node)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum DenyReason {
    /// Another node holds a confirmed lease on a conflicting key.
    Held,
    /// An older attempt has a provisional grant on a conflicting key.
    Older,
    /// This attempt's provisional grant was taken over by an older one.
    Preempted,
    /// The voter restarted less than one lease ago and grants nothing yet.
    Quarantine,
    /// The requested lease is longer than this voter's
    /// `lock_lease_timeout` (the nodes are configured differently).
    Lease,
    /// Confirm or renew for a grant the voter does not have.
    NotHeld,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Heartbeat {
    pub seq: u64,
    /// Node ids the sender currently sees up, itself included.
    pub up: Vec<NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn hello() -> Message {
        Message::Hello(Hello {
            node_id: 2,
            cluster_name: "syncfs".into(),
            members: vec![1, 2, 3],
            incarnation: 0xdead_beef,
            software: "0.1.0".into(),
            role: Role::default(),
            lane: Lane::Main,
        })
    }

    #[test]
    fn preamble_roundtrip() {
        assert_eq!(parse_preamble(&preamble()).unwrap(), PROTOCOL_VERSION);
        assert!(matches!(
            parse_preamble(b"HTTP/1.1"),
            Err(WireError::BadMagic)
        ));
    }

    #[test]
    fn frame_layout_is_stable() {
        // Variant 3 (Heartbeat), seq 1, up [1, 2]: pins the encoding.
        let f = encode(&Message::Heartbeat(Heartbeat {
            seq: 1,
            up: vec![1, 2],
        }));
        assert_eq!(f, [0, 0, 0, 5, 3, 1, 2, 1, 2]);
    }

    #[test]
    fn rejects_bad_frames() {
        let mut r: &[u8] = &[0, 0, 0, 0];
        assert!(matches!(
            read_frame(&mut r, MAX_FRAME_LEN),
            Err(WireError::Empty)
        ));
        let mut r: &[u8] = &[0xff, 0, 0, 0];
        assert!(matches!(
            read_frame(&mut r, MAX_FRAME_LEN),
            Err(WireError::TooLarge(..))
        ));
        let mut big = encode(&hello());
        big.splice(..4, (HANDSHAKE_MAX_FRAME_LEN + 1).to_be_bytes());
        assert!(matches!(
            read_frame(&mut big.as_slice(), HANDSHAKE_MAX_FRAME_LEN),
            Err(WireError::TooLarge(..))
        ));
        let mut f = encode(&hello());
        f.push(0);
        let n = (f.len() - 4) as u32;
        f.splice(..4, n.to_be_bytes());
        assert!(matches!(
            read_frame(&mut f.as_slice(), MAX_FRAME_LEN),
            Err(WireError::Trailing(1))
        ));
        let mut r: &[u8] = &[0, 0, 0, 1, 200];
        assert!(matches!(
            read_frame(&mut r, MAX_FRAME_LEN),
            Err(WireError::Decode(_))
        ));
        let mut r: &[u8] = &[0, 0, 0, 9, 3];
        assert!(read_frame(&mut r, MAX_FRAME_LEN).unwrap_err().is_eof());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        #[test]
        fn roundtrip(msg: Message) {
            let f = encode(&msg);
            let back = read_frame(&mut f.as_slice(), MAX_FRAME_LEN).unwrap();
            prop_assert_eq!(back, msg);
        }

        #[test]
        fn split_stream_roundtrip(msgs: Vec<Message>, cuts in proptest::collection::vec(any::<usize>(), 0..8)) {
            let stream: Vec<u8> = msgs.iter().flat_map(encode).collect();
            let mut points: Vec<usize> = cuts.iter().map(|c| c % (stream.len() + 1)).collect();
            points.push(0);
            points.push(stream.len());
            points.sort_unstable();
            let mut d = FrameDecoder::new(MAX_FRAME_LEN);
            let mut out = Vec::new();
            for w in points.windows(2) {
                d.push(&stream[w[0]..w[1]]);
                while let Some(m) = d.next_frame().unwrap() {
                    out.push(m);
                }
            }
            prop_assert_eq!(out, msgs);
        }

        #[test]
        fn garbage_never_panics(bytes: Vec<u8>) {
            let _ = decode_payload(&bytes);
            let _ = read_frame(&mut bytes.as_slice(), HANDSHAKE_MAX_FRAME_LEN);
            let mut d = FrameDecoder::new(HANDSHAKE_MAX_FRAME_LEN);
            d.push(&bytes);
            while let Ok(Some(_)) = d.next_frame() {}
        }

        #[test]
        fn mutated_frames_never_panic(msg: Message, flips in proptest::collection::vec((any::<usize>(), any::<u8>()), 1..6), cut: usize) {
            let mut f = encode(&msg);
            for (i, b) in flips {
                let i = i % f.len();
                f[i] ^= b;
            }
            f.truncate(cut % (f.len() + 1));
            if let Ok(m) = read_frame(&mut f.as_slice(), MAX_FRAME_LEN) {
                // Whatever decodes must re-encode to a valid frame.
                let again = encode(&m);
                prop_assert_eq!(read_frame(&mut again.as_slice(), MAX_FRAME_LEN).unwrap(), m);
            }
        }
    }
}
