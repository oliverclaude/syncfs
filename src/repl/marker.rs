//! The disk marker: `<backing_dir>/.syncfs/disk` names the cluster, the
//! node and the disk id of the backing directory's contents. It sits under
//! the name of the virtual `.syncfs` directory, so the mount hides it.
//!
//! A missing marker on a node with replication state means the disk was
//! lost: the node rebuilds from its peers (`crate::repl::rebuild`). The
//! marker says `state = rebuilding` until that is done, so an interrupted
//! rebuild resumes.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::config::Config;

pub const FILE: &str = "disk";

pub fn path(backing: &Path) -> PathBuf {
    backing.join(crate::fs::virt::NAME).join(FILE)
}

/// Where a rebuild moves local files its source does not have.
pub const ORPHANS: &str = "orphans";

/// Whether `<backing>/.syncfs` holds nothing but syncfs's own files (so
/// the virtual directory hides nothing of the user's).
pub fn only_marker(backing: &Path) -> bool {
    let dir = backing.join(crate::fs::virt::NAME);
    match std::fs::symlink_metadata(&dir) {
        Ok(md) if md.is_dir() => std::fs::read_dir(&dir).is_ok_and(|entries| {
            entries.flatten().all(|e| {
                e.file_name() == FILE || e.file_name() == "disk.tmp" || e.file_name() == ORPHANS
            })
        }),
        _ => false,
    }
}

/// Whether the backing directory holds nothing but syncfs's own files.
pub fn backing_empty(backing: &Path) -> bool {
    std::fs::read_dir(backing).is_ok_and(|entries| {
        entries
            .flatten()
            .all(|e| e.file_name() == crate::fs::virt::NAME)
    }) && (!backing.join(crate::fs::virt::NAME).exists() || only_marker(backing))
}

#[derive(Debug, PartialEq, Eq)]
pub struct Marker {
    pub cluster: String,
    pub node: u32,
    pub disk: u64,
    /// A rebuild from peers is not finished.
    pub rebuilding: bool,
}

impl Marker {
    fn render(&self) -> String {
        format!(
            "# syncfs disk marker; do not edit or copy to another node\ncluster = {}\nnode = {}\ndisk = {:016x}\n{}",
            self.cluster,
            self.node,
            self.disk,
            if self.rebuilding {
                "state = rebuilding\n"
            } else {
                ""
            }
        )
    }

    fn parse(s: &str) -> Option<Marker> {
        let mut cluster = None;
        let mut node = None;
        let mut disk = None;
        let mut rebuilding = false;
        for line in s.lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            match k.trim() {
                "cluster" => cluster = Some(v.trim().to_string()),
                "node" => node = v.trim().parse().ok(),
                "disk" => disk = u64::from_str_radix(v.trim(), 16).ok(),
                "state" => rebuilding = v.trim() == "rebuilding",
                _ => {}
            }
        }
        Some(Marker {
            cluster: cluster?,
            node: node?,
            disk: disk?,
            rebuilding,
        })
    }
}

/// Reads the marker (`None` if there is none). Refuses a marker of
/// another cluster or node.
pub fn load(cfg: &Config, backing: &Path) -> anyhow::Result<Option<Marker>> {
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
            Ok(Some(m))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::Error::from(e).context(format!("reading {}", p.display()))),
    }
}

/// A marker for this node with a new random disk id.
pub fn fresh(cfg: &Config, rebuilding: bool) -> io::Result<Marker> {
    let mut disk = 0;
    while disk == 0 {
        let mut b = [0u8; 8];
        rustix::rand::getrandom(&mut b, rustix::rand::GetRandomFlags::empty())?;
        disk = u64::from_le_bytes(b);
    }
    Ok(Marker {
        cluster: cfg.cluster_name.clone(),
        node: u32::from(cfg.node_id),
        disk,
        rebuilding,
    })
}

/// Writes `m` durably (replacing any marker).
pub fn store(backing: &Path, m: &Marker) -> io::Result<()> {
    let p = path(backing);
    let dir = p.parent().expect("marker has a parent");
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join("disk.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(m.render().as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, &p)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
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
            rebuilding: false,
        };
        assert_eq!(Marker::parse(&m.render()), Some(m));
        let m = Marker {
            rebuilding: true,
            ..Marker::parse("cluster = c\nnode = 3\ndisk = abc\n").unwrap()
        };
        assert_eq!(Marker::parse(&m.render()), Some(m));
    }
}
