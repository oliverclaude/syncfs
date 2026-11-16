//! Basic configuration value types.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Numeric identifier of a cluster node. Must be non-zero and unique within
/// the cluster.
pub type NodeId = u16;

/// What this process does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Mount the filesystem, store data in `backing_dir`, and vote.
    #[default]
    Mount,
    /// Voter-only daemon: no backing directory and no mount; it only takes
    /// part in quorum and lock voting.
    Daemon,
}

impl FromStr for Mode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "mount" => Ok(Self::Mount),
            "daemon" => Ok(Self::Daemon),
            _ => Err(format!("invalid mode {s:?} (expected mount or daemon)")),
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Mount => "mount",
            Self::Daemon => "daemon",
        })
    }
}

/// Log verbosity.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    clap::ValueEnum,
)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Ok(Self::Error),
            "warn" | "warning" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            _ => Err(format!(
                "invalid log level {s:?} (expected error, warn, info, debug or trace)"
            )),
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        })
    }
}

/// One member of the cluster. The peer list is the full membership and
/// includes this node itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub id: NodeId,
    /// `host:port`, where host is a DNS name, an IPv4 address, or an IPv6
    /// address in brackets (`[fd00::1]:7420`).
    pub address: String,
}

impl FromStr for Peer {
    type Err = String;

    /// Parses `ID@HOST:PORT`, e.g. `2@10.0.0.2:7420`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (id, address) = s
            .trim()
            .split_once('@')
            .ok_or_else(|| format!("invalid peer {s:?} (expected ID@HOST:PORT)"))?;
        let id = id
            .parse()
            .map_err(|_| format!("invalid peer id {id:?} in {s:?} (expected 1..=65535)"))?;
        Ok(Self {
            id,
            address: address.to_string(),
        })
    }
}

/// Checks that `address` is `host:port` with a non-empty host and a
/// non-zero port. Returns a human-readable reason on failure.
pub fn check_host_port(address: &str) -> Result<(), String> {
    let (host, port) = address.rsplit_once(':').ok_or("missing :PORT")?;
    match port.parse::<u16>() {
        Ok(0) | Err(_) => return Err(format!("invalid port {port:?}")),
        Ok(_) => {}
    }
    if host.is_empty() {
        return Err("empty host".into());
    }
    if let Some(inner) = host.strip_prefix('[') {
        let inner = inner
            .strip_suffix(']')
            .ok_or("unclosed [ in IPv6 address")?;
        inner
            .parse::<std::net::Ipv6Addr>()
            .map_err(|_| format!("invalid IPv6 address {inner:?}"))?;
    } else if host.contains(':') {
        return Err("IPv6 addresses must be written in brackets, e.g. [fd00::1]:7420".into());
    } else if host.contains(|c: char| c.is_whitespace() || c == '/' || c == '@') {
        return Err(format!("invalid host {host:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_and_level_parse() {
        assert_eq!("Daemon".parse::<Mode>().unwrap(), Mode::Daemon);
        assert!("voter".parse::<Mode>().is_err());
        assert_eq!("warning".parse::<LogLevel>().unwrap(), LogLevel::Warn);
        assert!("loud".parse::<LogLevel>().is_err());
    }

    #[test]
    fn peer_parse() {
        let p: Peer = "2@10.0.0.2:7420".parse().unwrap();
        assert_eq!(
            p,
            Peer {
                id: 2,
                address: "10.0.0.2:7420".into()
            }
        );
        assert!("10.0.0.2:7420".parse::<Peer>().is_err());
        assert!("x@10.0.0.2:7420".parse::<Peer>().is_err());
        assert!("70000@h:1".parse::<Peer>().is_err());
    }

    #[test]
    fn host_port_check() {
        for ok in [
            "10.0.0.1:7420",
            "node-a.example:1",
            "[fd00::1]:7420",
            "[::]:9",
        ] {
            assert!(check_host_port(ok).is_ok(), "{ok}");
        }
        for bad in [
            "10.0.0.1",
            "10.0.0.1:0",
            "10.0.0.1:99999",
            ":7420",
            "fd00::1:7420",
            "[fd00::1:7420",
            "[zz::1]:7420",
            "a b:7420",
        ] {
            assert!(check_host_port(bad).is_err(), "{bad}");
        }
    }
}
