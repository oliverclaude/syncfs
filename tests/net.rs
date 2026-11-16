//! Membership and handshake, in process over localhost TCP.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use syncfs::config::{resolve, Config, Environment, Mode, NodeId, PartialConfig, Peer};
use syncfs::net::wire::{
    self, Hello, Message, RejectReason, HANDSHAKE_MAX_FRAME_LEN, MAGIC, PREAMBLE_LEN,
};
use syncfs::net::{Network, PeerState, Snapshot};

const WAIT: Duration = Duration::from_secs(10);

fn free_ports(n: usize) -> Vec<u16> {
    let ls: Vec<_> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter().map(|l| l.local_addr().unwrap().port()).collect()
}

fn config(id: NodeId, ports: &[(NodeId, u16)], listen: u16, cluster: &str) -> Config {
    let p = PartialConfig {
        cluster_name: Some(cluster.into()),
        node_id: Some(id),
        mode: Some(Mode::Daemon),
        listen: Some(format!("127.0.0.1:{listen}").parse().unwrap()),
        peers: Some(
            ports
                .iter()
                .map(|&(id, port)| Peer {
                    id,
                    address: format!("127.0.0.1:{port}"),
                })
                .collect(),
        ),
        state_dir: Some("/tmp/syncfs-net-test".into()),
        heartbeat_interval: Some("100ms".parse().unwrap()),
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

/// Configs for nodes 1..=n on fresh ports.
fn cluster(n: usize) -> Vec<Config> {
    let ports = free_ports(n);
    let members: Vec<(NodeId, u16)> = (1..=n as NodeId).zip(ports.iter().copied()).collect();
    members
        .iter()
        .map(|&(id, port)| config(id, &members, port, "test"))
        .collect()
}

fn all_up(n: usize) -> impl Fn(&Snapshot) -> bool {
    move |s| s.reachable == n && s.have_quorum
}

#[test]
fn three_nodes_converge_leave_and_rejoin() {
    let cfgs = cluster(3);
    let nets: Vec<Network> = cfgs.iter().map(|c| Network::start(c).unwrap()).collect();
    for n in &nets {
        assert!(n.wait_until(WAIT, all_up(3)), "{:#?}", n.snapshot());
    }
    let s = nets[0].snapshot();
    assert_eq!((s.cluster_size, s.quorum), (3, 2));
    // Heartbeats carry each peer's view.
    assert!(nets[0].wait_until(WAIT, |s| s.peers.iter().all(|p| p.sees == [1, 2, 3])));
    let inc2 = s.peers[1].incarnation;

    // Graceful leave: peers notice at once (Goodbye), not after a timeout.
    let t = Instant::now();
    nets[1].shutdown();
    for n in [&nets[0], &nets[2]] {
        assert!(n.wait_until(WAIT, |s| s.state_of(2) == Some(PeerState::Down)));
        let p = n.snapshot().peers[1].clone();
        assert_eq!(p.state, PeerState::Down);
        assert!(n.snapshot().have_quorum, "2 of 3 is still a quorum");
    }
    assert!(
        t.elapsed() < Duration::from_millis(1500),
        "{:?}",
        t.elapsed()
    );

    // Node 2 comes back as a new process (new incarnation) on the same port.
    let back = Network::start(&cfgs[1]).unwrap();
    for n in [&nets[0], &nets[2], &back] {
        assert!(n.wait_until(WAIT, all_up(3)), "{:#?}", n.snapshot());
    }
    let p = nets[0].snapshot().peers[1].clone();
    assert_eq!(p.connects, 2);
    assert_ne!(p.incarnation, inc2);
    assert!(p.last_error.is_none());

    // Losing two of three loses quorum.
    nets[2].shutdown();
    back.shutdown();
    assert!(nets[0].wait_until(WAIT, |s| s.reachable == 1 && !s.have_quorum));
    nets[0].shutdown();
}

#[test]
fn standalone_node_has_quorum_without_network() {
    let mut c = cluster(1).remove(0);
    c.peers.clear();
    let n = Network::start(&c).unwrap();
    assert!(n.listen_addr().is_none());
    let s = n.snapshot();
    assert_eq!((s.cluster_size, s.quorum, s.reachable), (1, 1, 1));
    assert!(s.have_quorum);
}

#[test]
fn mismatched_cluster_name_is_rejected() {
    let ports = free_ports(2);
    let members = [(1, ports[0]), (2, ports[1])];
    let a = Network::start(&config(1, &members, ports[0], "alpha")).unwrap();
    let b = Network::start(&config(2, &members, ports[1], "beta")).unwrap();
    assert!(a.wait_until(WAIT, |s| s.state_of(2) == Some(PeerState::Rejected)));
    let p = a.snapshot().peers[1].clone();
    let err = p.last_error.unwrap();
    assert!(err.contains("ClusterName"), "{err}");
    assert_eq!(b.snapshot().state_of(1), Some(PeerState::Unknown));
    assert!(!a.snapshot().have_quorum);
    a.shutdown();
    b.shutdown();
}

#[test]
fn mismatched_membership_is_rejected() {
    let ports = free_ports(3);
    let three = [(1, ports[0]), (2, ports[1]), (3, ports[2])];
    let a = Network::start(&config(1, &three[..2], ports[0], "c")).unwrap();
    let b = Network::start(&config(2, &three, ports[1], "c")).unwrap();
    assert!(a.wait_until(WAIT, |s| s.state_of(2) == Some(PeerState::Rejected)));
    let err = a.snapshot().peers[1].last_error.clone().unwrap();
    assert!(
        err.contains("Membership") && err.contains("[1, 2, 3]"),
        "{err}"
    );
    a.shutdown();
    b.shutdown();
}

#[test]
fn wrong_node_at_address_is_rejected() {
    // Node 1 expects node 2 at port B, but node 3 listens there.
    let ports = free_ports(3);
    let n1 = [(1, ports[0]), (2, ports[1]), (3, ports[2])];
    let n3 = [(1, ports[0]), (2, ports[2]), (3, ports[1])];
    let a = Network::start(&config(1, &n1, ports[0], "c")).unwrap();
    let c = Network::start(&config(3, &n3, ports[1], "c")).unwrap();
    assert!(a.wait_until(WAIT, |s| s.state_of(2) == Some(PeerState::Rejected)));
    let err = a.snapshot().peers[1].last_error.clone().unwrap();
    assert!(
        err.contains("expected node 2") && err.contains("node 3 answered"),
        "{err}"
    );
    a.shutdown();
    c.shutdown();
}

/// A raw client speaking to node 2 of {1, 2, 3}.
struct Raw(TcpStream);

impl Raw {
    fn connect(port: u16) -> Raw {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        Raw(s)
    }

    fn their_preamble(&mut self) -> u16 {
        let mut p = [0; PREAMBLE_LEN];
        self.0.read_exact(&mut p).unwrap();
        wire::parse_preamble(&p).unwrap()
    }

    fn send_preamble(&mut self, version: u16) {
        let mut p = MAGIC.to_vec();
        p.extend_from_slice(&version.to_be_bytes());
        self.0.write_all(&p).unwrap();
    }

    fn closed(&mut self) -> bool {
        let mut b = [0; 64];
        loop {
            match self.0.read(&mut b) {
                Ok(0) => return true,
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return true,
                Err(_) => return false,
            }
        }
    }

    fn hello(&mut self, h: Hello) -> Message {
        self.send_preamble(wire::PROTOCOL_VERSION);
        assert_eq!(self.their_preamble(), wire::PROTOCOL_VERSION);
        wire::write_frame(&mut self.0, &Message::Hello(h)).unwrap();
        wire::read_frame(&mut self.0, HANDSHAKE_MAX_FRAME_LEN).unwrap()
    }
}

fn hello(node_id: NodeId) -> Hello {
    Hello {
        node_id,
        cluster_name: "test".into(),
        members: vec![1, 2, 3],
        incarnation: 42,
        software: "raw".into(),
    }
}

#[test]
fn handshake_rejection_cases() {
    let cfgs = cluster(3);
    let port = cfgs[1].listen.port();
    let node2 = Network::start(&cfgs[1]).unwrap();

    let reject = |h: Hello| match Raw::connect(port).hello(h) {
        Message::Reject(r) => r.reason,
        other => panic!("expected a reject, got {other:?}"),
    };
    let mut h = hello(1);
    h.cluster_name = "other".into();
    assert_eq!(reject(h), RejectReason::ClusterName);
    let mut h = hello(1);
    h.members = vec![1, 2];
    assert_eq!(reject(h), RejectReason::Membership);
    assert_eq!(reject(hello(9)), RejectReason::UnknownNode);
    assert_eq!(reject(hello(2)), RejectReason::SameNodeId);
    assert_eq!(reject(hello(3)), RejectReason::WrongDirection);

    // Not a syncfs peer: closed without a reply frame.
    let mut r = Raw::connect(port);
    r.0.write_all(b"GET / HTTP/1.1\r\n\r\n").unwrap();
    assert_eq!(r.their_preamble(), wire::PROTOCOL_VERSION);
    assert!(r.closed());
    // Other protocol version: closed after the preambles.
    let mut r = Raw::connect(port);
    r.send_preamble(99);
    r.their_preamble();
    assert!(r.closed());
    // Oversized first frame: closed.
    let mut r = Raw::connect(port);
    r.send_preamble(wire::PROTOCOL_VERSION);
    r.their_preamble();
    r.0.write_all(&(HANDSHAKE_MAX_FRAME_LEN + 1).to_be_bytes())
        .unwrap();
    assert!(r.closed());
    // Garbage frame: closed.
    let mut r = Raw::connect(port);
    r.send_preamble(wire::PROTOCOL_VERSION);
    r.their_preamble();
    r.0.write_all(&[0, 0, 0, 3, 0xff, 0xff, 0xff]).unwrap();
    assert!(r.closed());
    // None of that disturbed the node.
    assert_eq!(node2.snapshot().state_of(1), Some(PeerState::Unknown));

    // A valid hello is welcomed and the peer shows up, then goes down.
    let mut r = Raw::connect(port);
    match r.hello(hello(1)) {
        Message::Welcome(w) => assert_eq!(w.node_id, 2),
        other => panic!("{other:?}"),
    }
    assert!(node2.wait_until(WAIT, |s| s.state_of(1) == Some(PeerState::Up)));
    // node 2 heartbeats us.
    match wire::read_frame(&mut r.0, wire::MAX_FRAME_LEN).unwrap() {
        Message::Heartbeat(h) => assert!(h.up.contains(&1) && h.up.contains(&2)),
        other => panic!("{other:?}"),
    }
    // Silence: node 2 times us out after 3 heartbeat intervals.
    let t = Instant::now();
    assert!(node2.wait_until(WAIT, |s| s.state_of(1) == Some(PeerState::Down)));
    assert!(
        t.elapsed() >= Duration::from_millis(200),
        "{:?}",
        t.elapsed()
    );
    let err = node2.snapshot().peers[0].last_error.clone().unwrap();
    assert_eq!(err, "heartbeat timeout");
    node2.shutdown();
}

fn raise_nofile() {
    unsafe {
        let mut l = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        libc::getrlimit(libc::RLIMIT_NOFILE, &mut l);
        l.rlim_cur = l.rlim_max;
        libc::setrlimit(libc::RLIMIT_NOFILE, &l);
    }
}

fn full_mesh(n: usize) {
    raise_nofile();
    let cfgs = cluster(n);
    let t = Instant::now();
    let nets: Vec<Network> = cfgs.iter().map(|c| Network::start(c).unwrap()).collect();
    for net in &nets {
        assert!(
            net.wait_until(Duration::from_secs(60), all_up(n)),
            "{:#?}",
            net.snapshot()
        );
    }
    eprintln!("{n}-node full mesh converged in {:?}", t.elapsed());
    nets[n - 1].shutdown();
    for net in &nets[..n - 1] {
        assert!(net.wait_until(WAIT, all_up(n - 1)));
    }
    for net in &nets {
        net.shutdown();
    }
}

/// 25 nodes in one process: 300 connections, ~900 threads. (99 nodes do
/// not fit in one process: ~12,000 threads exceed `vm.max_map_count`; see
/// `cluster::daemons_99_nodes` for the real thing, one process per node.)
#[test]
fn full_mesh_25() {
    full_mesh(25);
}
