//! Command-line interface. Usage follows the FUSE helper convention
//! (`syncfs BACKING_DIR MOUNTPOINT -o opts`) so `mount(8)` and fstab can
//! call it directly.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::Parser;

use crate::config::{
    absolutize, ByteSize, HumanDuration, LogLevel, Mode, NodeId, PartialConfig, Peer, Sources,
};

#[derive(Debug, Parser)]
#[command(
    name = "syncfs",
    version,
    about = "Replicating FUSE filesystem",
    after_help = "Precedence: built-in defaults < YAML config file < -o options < command-line flags.\n\
                  Example: syncfs /srv/data /mnt/data -o node_id=2,allow_other --print-config"
)]
pub struct Cli {
    /// Local backing directory (this node's source of truth). Mount mode only.
    #[arg(value_name = "BACKING_DIR", conflicts_with = "backing_dir")]
    pub backing_dir_arg: Option<PathBuf>,

    /// Where to mount the filesystem. Mount mode only.
    #[arg(value_name = "MOUNTPOINT", conflicts_with = "mountpoint")]
    pub mountpoint_arg: Option<PathBuf>,

    /// YAML config file [default: /etc/syncfs/syncfs.yaml, skipped if missing]
    #[arg(short, long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Mount options, comma-separated (key=value,flag,noflag). Repeatable.
    #[arg(short = 'o', value_name = "OPTIONS")]
    pub options: Vec<String>,

    /// This node's id (1..=65535) [default: 1]
    #[arg(long, value_name = "ID")]
    pub node_id: Option<NodeId>,

    /// mount: store data and mount; daemon: voter-only, no data [default: mount]
    #[arg(long, value_enum)]
    pub mode: Option<Mode>,

    /// Address for node-to-node traffic [default: 0.0.0.0:7420]
    #[arg(long, value_name = "IP:PORT")]
    pub listen: Option<SocketAddr>,

    /// Cluster member, this node included. Repeatable; replaces the config file's list.
    #[arg(long = "peer", value_name = "ID@HOST:PORT")]
    pub peers: Vec<Peer>,

    /// Same as the BACKING_DIR argument.
    #[arg(long, value_name = "PATH")]
    pub backing_dir: Option<PathBuf>,

    /// Same as the MOUNTPOINT argument.
    #[arg(long, value_name = "PATH")]
    pub mountpoint: Option<PathBuf>,

    /// Dirty bitmaps and journal [default: /var/lib/syncfs as root, else $XDG_STATE_HOME/syncfs]
    #[arg(long, value_name = "PATH")]
    pub state_dir: Option<PathBuf>,

    /// Lock lease; a dead holder's lock expires after this [default: 30s]
    #[arg(long, value_name = "DURATION")]
    pub lock_lease_timeout: Option<HumanDuration>,

    /// Peer heartbeat interval [default: 1s]
    #[arg(long, value_name = "DURATION")]
    pub heartbeat_interval: Option<HumanDuration>,

    /// Dirty-bitmap block size, power of two, 4K..16M [default: 64K]
    #[arg(long, value_name = "SIZE")]
    pub block_size: Option<ByteSize>,

    /// Debug mode: verbose logging (log level debug unless set)
    #[arg(short, long)]
    pub debug: bool,

    /// Let other users access the mount (needs user_allow_other in /etc/fuse.conf for non-root)
    #[arg(long)]
    pub allow_other: bool,

    /// Let root access the mount
    #[arg(long)]
    pub allow_root: bool,

    /// error, warn, info, debug or trace [default: info]
    #[arg(long, value_enum)]
    pub log_level: Option<LogLevel>,

    /// Stay in the foreground instead of daemonizing (implied by --debug)
    #[arg(short, long)]
    pub foreground: bool,

    /// Print the effective configuration as YAML and exit
    #[arg(long)]
    pub print_config: bool,
}

impl Cli {
    /// Converts the arguments into configuration sources. Relative paths
    /// are resolved against `cwd`.
    pub fn sources(&self, cwd: &Path) -> Sources {
        let abs = |p: &Option<PathBuf>| p.as_deref().map(|p| absolutize(p, cwd));
        let flag = |b: bool| b.then_some(true);
        Sources {
            cli: PartialConfig {
                node_id: self.node_id,
                mode: self.mode,
                listen: self.listen,
                peers: (!self.peers.is_empty()).then(|| self.peers.clone()),
                backing_dir: abs(&self.backing_dir_arg).or_else(|| abs(&self.backing_dir)),
                mountpoint: abs(&self.mountpoint_arg).or_else(|| abs(&self.mountpoint)),
                state_dir: abs(&self.state_dir),
                lock_lease_timeout: self.lock_lease_timeout,
                heartbeat_interval: self.heartbeat_interval,
                block_size: self.block_size,
                debug: flag(self.debug),
                allow_other: flag(self.allow_other),
                allow_root: flag(self.allow_root),
                log_level: self.log_level,
                foreground: flag(self.foreground),
            },
            cli_config_file: abs(&self.config),
            mount_options: self.options.clone(),
            sloppy: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn sources(args: &[&str]) -> Sources {
        let cli =
            Cli::try_parse_from(std::iter::once("syncfs").chain(args.iter().copied())).unwrap();
        cli.sources(Path::new("/home/u"))
    }

    #[test]
    fn clap_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn positional_fuse_helper_style() {
        let s = sources(&[
            "data",
            "/mnt/x",
            "-o",
            "node_id=2,debug",
            "-o",
            "allow_other",
        ]);
        assert_eq!(s.cli.backing_dir, Some("/home/u/data".into()));
        assert_eq!(s.cli.mountpoint, Some("/mnt/x".into()));
        assert_eq!(s.mount_options, vec!["node_id=2,debug", "allow_other"]);
        // Flags not given stay unset, so lower layers can supply them.
        assert_eq!(s.cli.debug, None);
        assert_eq!(s.cli.node_id, None);
        assert_eq!(s.cli.peers, None);
    }

    #[test]
    fn all_flags() {
        let s = sources(&[
            "--config",
            "c.yaml",
            "--node-id",
            "3",
            "--mode",
            "daemon",
            "--listen",
            "[::]:7500",
            "--peer",
            "1@a:1",
            "--peer",
            "3@b:1",
            "--backing-dir",
            "/b",
            "--mountpoint",
            "/m",
            "--state-dir",
            "/s",
            "--lock-lease-timeout",
            "1m",
            "--heartbeat-interval",
            "2s",
            "--block-size",
            "1M",
            "-d",
            "--allow-root",
            "--log-level",
            "trace",
        ]);
        assert_eq!(s.cli_config_file, Some("/home/u/c.yaml".into()));
        let c = s.cli;
        assert_eq!(c.node_id, Some(3));
        assert_eq!(c.mode, Some(Mode::Daemon));
        assert_eq!(c.listen, Some("[::]:7500".parse().unwrap()));
        assert_eq!(c.peers.unwrap().len(), 2);
        assert_eq!(c.backing_dir, Some("/b".into()));
        assert_eq!(c.mountpoint, Some("/m".into()));
        assert_eq!(c.state_dir, Some("/s".into()));
        assert_eq!(c.lock_lease_timeout, Some(HumanDuration::from_secs(60)));
        assert_eq!(c.heartbeat_interval, Some(HumanDuration::from_secs(2)));
        assert_eq!(c.block_size, Some(ByteSize(1 << 20)));
        assert_eq!(c.debug, Some(true));
        assert_eq!(c.allow_root, Some(true));
        assert_eq!(c.allow_other, None);
        assert_eq!(c.log_level, Some(LogLevel::Trace));
    }

    #[test]
    fn rejects_bad_values_and_conflicts() {
        for args in [
            &["--node-id", "x"][..],
            &["--peer", "nope"],
            &["--mode", "voter"],
            &["--block-size", "big"],
            &["/a", "--backing-dir", "/b"],
            &["/a", "/m", "--mountpoint", "/n"],
        ] {
            let argv = std::iter::once("syncfs").chain(args.iter().copied());
            assert!(Cli::try_parse_from(argv).is_err(), "{args:?}");
        }
    }
}
