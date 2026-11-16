//! Connection setup: preamble exchange, `Hello` / `Welcome` / `Reject`.
//!
//! Exactly one connection exists per pair of nodes, opened by the node with
//! the lower id. The dialer sends `Hello`; the acceptor checks it with
//! [`Identity::check_hello`] and answers `Welcome` or `Reject` (then
//! closes). The dialer checks that the node that answered is the one it
//! meant to reach.

use std::io::{Read, Write};
use std::net::TcpStream;

use thiserror::Error;

use super::wire::{
    self, Hello, Message, Reject, RejectReason, Welcome, WireError, HANDSHAKE_MAX_FRAME_LEN,
    PREAMBLE_LEN, PROTOCOL_VERSION,
};
use crate::config::{Config, NodeId};

/// Who this node is, as presented to peers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub node_id: NodeId,
    pub cluster_name: String,
    /// Sorted ids of all members, this node included.
    pub members: Vec<NodeId>,
    pub incarnation: u64,
}

impl Identity {
    pub fn from_config(cfg: &Config, incarnation: u64) -> Self {
        let mut members: Vec<NodeId> = cfg.peers.iter().map(|p| p.id).collect();
        if members.is_empty() {
            members.push(cfg.node_id);
        }
        members.sort_unstable();
        members.dedup();
        Self {
            node_id: cfg.node_id,
            cluster_name: cfg.cluster_name.clone(),
            members,
            incarnation,
        }
    }

    pub fn hello(&self) -> Hello {
        Hello {
            node_id: self.node_id,
            cluster_name: self.cluster_name.clone(),
            members: self.members.clone(),
            incarnation: self.incarnation,
            software: env!("CARGO_PKG_VERSION").into(),
        }
    }

    pub fn welcome(&self) -> Welcome {
        Welcome {
            node_id: self.node_id,
            incarnation: self.incarnation,
            software: env!("CARGO_PKG_VERSION").into(),
        }
    }

    /// Whether this node dials `peer` (lower id dials higher).
    pub fn dials(&self, peer: NodeId) -> bool {
        self.node_id < peer
    }

    /// Decides whether to accept a connection that sent `h`.
    pub fn check_hello(&self, h: &Hello) -> Result<(), Reject> {
        let reject = |reason, detail: String| Err(Reject { reason, detail });
        if h.cluster_name != self.cluster_name {
            return reject(
                RejectReason::ClusterName,
                format!(
                    "node {} is in cluster {:?}, node {} in {:?}",
                    h.node_id, h.cluster_name, self.node_id, self.cluster_name
                ),
            );
        }
        if h.node_id == self.node_id {
            return reject(
                RejectReason::SameNodeId,
                format!("both nodes have node_id {}", h.node_id),
            );
        }
        if !self.members.contains(&h.node_id) {
            return reject(
                RejectReason::UnknownNode,
                format!(
                    "node {} is not in node {}'s peers {:?}",
                    h.node_id, self.node_id, self.members
                ),
            );
        }
        let mut theirs = h.members.clone();
        theirs.sort_unstable();
        theirs.dedup();
        if theirs != self.members {
            return reject(
                RejectReason::Membership,
                format!(
                    "node {} has peers {:?}, node {} has {:?}",
                    h.node_id, theirs, self.node_id, self.members
                ),
            );
        }
        if !Identity::dials_to(h.node_id, self.node_id) {
            return reject(
                RejectReason::WrongDirection,
                format!(
                    "node {} dialed node {}; only the lower id dials",
                    h.node_id, self.node_id
                ),
            );
        }
        Ok(())
    }

    fn dials_to(from: NodeId, to: NodeId) -> bool {
        from < to
    }
}

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("{0}")]
    Wire(#[from] WireError),
    #[error("peer speaks protocol version {theirs}, this node {ours}")]
    Version { ours: u16, theirs: u16 },
    #[error("rejected by peer ({:?}): {}", .0.reason, .0.detail)]
    Rejected(Reject),
    #[error("refused ({:?}): {}", .0.reason, .0.detail)]
    Refused(Reject),
    #[error("expected node {expected} at this address, node {got} answered")]
    WrongNode { expected: NodeId, got: NodeId },
    #[error("unexpected {0} during handshake")]
    Unexpected(&'static str),
}

impl From<std::io::Error> for HandshakeError {
    fn from(e: std::io::Error) -> Self {
        HandshakeError::Wire(e.into())
    }
}

/// Sends our preamble and checks the peer's.
pub fn exchange_preamble(s: &mut TcpStream) -> Result<(), HandshakeError> {
    s.write_all(&wire::preamble())?;
    let mut p = [0; PREAMBLE_LEN];
    s.read_exact(&mut p)?;
    let theirs = wire::parse_preamble(&p)?;
    if theirs != PROTOCOL_VERSION {
        return Err(HandshakeError::Version {
            ours: PROTOCOL_VERSION,
            theirs,
        });
    }
    Ok(())
}

/// Dialer side. Returns the peer's `Welcome`.
pub fn dial(s: &mut TcpStream, me: &Identity, expect: NodeId) -> Result<Welcome, HandshakeError> {
    exchange_preamble(s)?;
    wire::write_frame(s, &Message::Hello(me.hello()))?;
    match wire::read_frame(s, HANDSHAKE_MAX_FRAME_LEN)? {
        Message::Welcome(w) if w.node_id == expect => Ok(w),
        Message::Welcome(w) => Err(HandshakeError::WrongNode {
            expected: expect,
            got: w.node_id,
        }),
        Message::Reject(r) => Err(HandshakeError::Rejected(r)),
        other => Err(HandshakeError::Unexpected(other.kind())),
    }
}

/// Acceptor side. Returns the accepted `Hello`.
pub fn accept(s: &mut TcpStream, me: &Identity) -> Result<Hello, HandshakeError> {
    exchange_preamble(s)?;
    let h = match wire::read_frame(s, HANDSHAKE_MAX_FRAME_LEN)? {
        Message::Hello(h) => h,
        other => return Err(HandshakeError::Unexpected(other.kind())),
    };
    if let Err(r) = me.check_hello(&h) {
        let _ = wire::write_frame(s, &Message::Reject(r.clone()));
        return Err(HandshakeError::Refused(r));
    }
    wire::write_frame(s, &Message::Welcome(me.welcome()))?;
    Ok(h)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn me() -> Identity {
        Identity {
            node_id: 2,
            cluster_name: "c".into(),
            members: vec![1, 2, 3],
            incarnation: 1,
        }
    }

    fn hello(node_id: NodeId) -> Hello {
        Hello {
            node_id,
            cluster_name: "c".into(),
            members: vec![3, 1, 2],
            incarnation: 9,
            software: "x".into(),
        }
    }

    fn reason(h: Hello) -> Option<RejectReason> {
        me().check_hello(&h).err().map(|r| r.reason)
    }

    #[test]
    fn hello_checks() {
        assert_eq!(reason(hello(1)), None);
        let mut h = hello(1);
        h.cluster_name = "other".into();
        assert_eq!(reason(h), Some(RejectReason::ClusterName));
        assert_eq!(reason(hello(2)), Some(RejectReason::SameNodeId));
        assert_eq!(reason(hello(7)), Some(RejectReason::UnknownNode));
        let mut h = hello(1);
        h.members = vec![1, 2, 3, 4];
        assert_eq!(reason(h), Some(RejectReason::Membership));
        assert_eq!(reason(hello(3)), Some(RejectReason::WrongDirection));
    }
}
