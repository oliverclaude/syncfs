//! The disk marker: `<backing_dir>/.syncfs/disk` names the cluster, the
//! node and the disk id of the backing directory's contents. It sits under
//! the name of the virtual `.syncfs` directory, so the mount hides it.
//!
//! Step 8 turns a missing marker on a node with replication state into
//! the disk-loss rebuild; for now a missing marker gets a new disk id.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::config::Config;

pub const FILE: &str = "disk";

pub fn path(backing: &Path) -> PathBuf {
    backing.join(crate::fs::virt::NAME).join(FILE)
}

/// Whether `<backing>/.syncfs` holds nothing but the marker (so the
/// virtual directory hides nothing of the user's).
pub fn only_marker(backing: &Path) -> bool {
    let dir = backing.join(crate::fs::virt::NAME);
    match std::fs::symlink_metadata(&dir) {
        Ok(md) if md.is_dir() => std::fs::read_dir(&dir).is_ok_and(|entries| {
            entries
                .flatten()
                .all(|e| e.file_name() == FILE || e.file_name() == "disk.tmp")
        }),
        _ => false,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Marker {
    pub cluster: String,
    pub node: u32,
    pub disk: u64,
}

impl Marker {
    fn render(&self) -> String {
        format!(
            "# syncfs disk marker; do not edit or copy to another node\ncluster = {}\nnode = {}\ndisk = {:016x}\n",
            self.cluster, self.node, self.disk
        )
    }

    fn parse(s: &str) -> Option<Marker> {
        let mut cluster = None;
        let mut node = None;
        let mut disk = None;
        for line in s.lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            match k.trim() {
                "cluster" => cluster = Some(v.trim().to_string()),
                "node" => node = v.trim().parse().ok(),
                "disk" => disk = u64::from_str_radix(v.trim(), 16).ok(),
                _ => {}
            }
        }
        Some(Marker {
            cluster: cluster?,
            node: node?,
            disk: disk?,
        })
    }
}

/// Reads the marker, creating it with a new disk id if missing. Refuses a
/// marker of another cluster or node.
pub fn load_or_create(cfg: &Config, backing: &Path) -> anyhow::Result<u64> {
    let p = path(backing);
    match std::fs::read_to_string(&p) {
        Ok(s) => {
            let m = Marker::parse(&s)
                .ok_or_else(|| anyhow::anyhow!("{} is not a valid disk marker", p.display()))?;
            if m.cluster != cfg.cluster_name || m.node != u32::from(cfg.node_id) {
                anyhow::bail!(
                    "{} belongs to node {} of cluster {:?}, not node {} of {:?}",
                    p.display(),
                    m.node,
                    m.cluster,
                    cfg.node_id,
                    cfg.cluster_name
                );
            }
            Ok(m.disk)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let mut disk = 0;
            while disk == 0 {
                let mut b = [0u8; 8];
                rustix::rand::getrandom(&mut b, rustix::rand::GetRandomFlags::empty())?;
                disk = u64::from_le_bytes(b);
            }
            let m = Marker {
                cluster: cfg.cluster_name.clone(),
                node: u32::from(cfg.node_id),
                disk,
            };
            let dir = p.parent().expect("marker has a parent");
            std::fs::create_dir_all(dir)?;
            let tmp = dir.join("disk.tmp");
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(m.render().as_bytes())?;
            f.sync_all()?;
            std::fs::rename(&tmp, &p)?;
            std::fs::File::open(dir)?.sync_all()?;
            log::info!("new disk id {disk:016x} in {}", p.display());
            Ok(disk)
        }
        Err(e) => Err(anyhow::Error::from(e).context(format!("reading {}", p.display()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let m = Marker {
            cluster: "c".into(),
            node: 3,
            disk: 0xabc,
        };
        assert_eq!(Marker::parse(&m.render()), Some(m));
    }
}
