//! Direct access to the backing directory by relative path, for changes
//! that arrive from peers and for serving blocks. Paths from peers are
//! checked ([`check`]) and resolved strictly beneath the backing directory
//! without following symlinks, so a peer cannot reach outside it.

use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::path::Path;

use rustix::fs::{AtFlags, FileType, Mode, OFlags, ResolveFlags, Stat, Timespec, Timestamps};

use crate::net::wire::{Attrs, BlockHash, NodeKind};

pub fn hash_block(data: &[u8]) -> BlockHash {
    let mut h = [0; 16];
    h.copy_from_slice(&blake3::hash(data).as_bytes()[..16]);
    h
}

/// Rejects paths that are absolute, contain `.`/`..`/empty components or
/// name the virtual `.syncfs` directory.
pub fn check(path: &[u8]) -> io::Result<()> {
    let bad = || io::Error::new(io::ErrorKind::InvalidInput, "bad path from peer");
    if path.is_empty() {
        return Ok(());
    }
    if path.contains(&0) {
        return Err(bad());
    }
    for (i, c) in path.split(|b| *b == b'/').enumerate() {
        if c.is_empty()
            || c == b"."
            || c == b".."
            || (i == 0 && c == crate::fs::virt::NAME.as_bytes())
        {
            return Err(bad());
        }
    }
    Ok(())
}

pub fn attrs_of(st: &Stat) -> Attrs {
    Attrs {
        mode: st.st_mode & 0o7777,
        uid: st.st_uid,
        gid: st.st_gid,
        mtime: (st.st_mtime, st.st_mtime_nsec as u32),
        atime: (st.st_atime, st.st_atime_nsec as u32),
    }
}

pub fn kind_of(st: &Stat) -> NodeKind {
    match FileType::from_raw_mode(st.st_mode) {
        FileType::Directory => NodeKind::Dir,
        FileType::Symlink => NodeKind::Symlink,
        FileType::Fifo => NodeKind::Fifo,
        FileType::Socket => NodeKind::Socket,
        FileType::CharacterDevice => NodeKind::CharDevice,
        FileType::BlockDevice => NodeKind::BlockDevice,
        _ => NodeKind::File,
    }
}

fn ts((sec, nsec): (i64, u32)) -> Timespec {
    Timespec {
        tv_sec: sec,
        tv_nsec: nsec.into(),
    }
}

pub struct Backing {
    root: OwnedFd,
    as_root: bool,
}

impl Backing {
    pub fn open(root: &Path) -> io::Result<Backing> {
        let root = rustix::fs::open(
            root,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        Ok(Backing {
            root,
            as_root: rustix::process::geteuid().is_root(),
        })
    }

    /// The directory containing `path` and the last component. The root
    /// itself is `(root, ".")`.
    fn parent<'p>(&self, path: &'p [u8]) -> io::Result<(OwnedFd, &'p OsStr)> {
        check(path)?;
        let (dir, name): (&[u8], &[u8]) = match path.iter().rposition(|b| *b == b'/') {
            Some(i) => (&path[..i], &path[i + 1..]),
            None => (b"", path),
        };
        let dir = if dir.is_empty() { &b"."[..] } else { dir };
        let fd = rustix::fs::openat2(
            &self.root,
            OsStr::from_bytes(dir),
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )?;
        let name = if name.is_empty() { &b"."[..] } else { name };
        Ok((fd, OsStr::from_bytes(name)))
    }

    pub fn stat(&self, path: &[u8]) -> io::Result<Stat> {
        let (d, n) = self.parent(path)?;
        Ok(rustix::fs::statat(&d, n, AtFlags::SYMLINK_NOFOLLOW)?)
    }

    pub fn exists(&self, path: &[u8]) -> bool {
        self.stat(path).is_ok()
    }

    pub fn create(
        &self,
        path: &[u8],
        kind: NodeKind,
        attrs: &Attrs,
        rdev: u64,
        target: &[u8],
    ) -> io::Result<()> {
        let (d, n) = self.parent(path)?;
        let mode = Mode::from_raw_mode(attrs.mode & 0o7777);
        match kind {
            NodeKind::Dir => rustix::fs::mkdirat(&d, n, mode)?,
            NodeKind::Symlink => rustix::fs::symlinkat(OsStr::from_bytes(target), &d, n)?,
            NodeKind::File => {
                rustix::fs::openat(
                    &d,
                    n,
                    OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::CLOEXEC,
                    mode,
                )?;
            }
            NodeKind::Fifo | NodeKind::Socket | NodeKind::CharDevice | NodeKind::BlockDevice => {
                let ft = match kind {
                    NodeKind::Fifo => FileType::Fifo,
                    NodeKind::Socket => FileType::Socket,
                    NodeKind::CharDevice => FileType::CharacterDevice,
                    _ => FileType::BlockDevice,
                };
                rustix::fs::mknodat(&d, n, ft, mode, rdev)?;
            }
        }
        self.set_attrs_at(&d, n, kind, attrs)
    }

    pub fn link(&self, existing: &[u8], path: &[u8]) -> io::Result<()> {
        let (d1, n1) = self.parent(existing)?;
        let (d2, n2) = self.parent(path)?;
        Ok(rustix::fs::linkat(&d1, n1, &d2, n2, AtFlags::empty())?)
    }

    pub fn remove(&self, path: &[u8], dir: bool) -> io::Result<()> {
        let (d, n) = self.parent(path)?;
        let flags = if dir {
            AtFlags::REMOVEDIR
        } else {
            AtFlags::empty()
        };
        Ok(rustix::fs::unlinkat(&d, n, flags)?)
    }

    pub fn rename(&self, from: &[u8], to: &[u8], flags: u32) -> io::Result<()> {
        let (d1, n1) = self.parent(from)?;
        let (d2, n2) = self.parent(to)?;
        Ok(rustix::fs::renameat_with(
            &d1,
            n1,
            &d2,
            n2,
            rustix::fs::RenameFlags::from_bits_retain(flags),
        )?)
    }

    fn set_attrs_at(&self, d: &OwnedFd, n: &OsStr, kind: NodeKind, a: &Attrs) -> io::Result<()> {
        if self.as_root {
            let r = rustix::fs::chownat(
                d,
                n,
                Some(rustix::fs::Uid::from_raw(a.uid)),
                Some(rustix::fs::Gid::from_raw(a.gid)),
                AtFlags::SYMLINK_NOFOLLOW,
            );
            if let Err(e) = r {
                log::debug!("replication: chown {n:?}: {e}");
            }
        }
        if kind != NodeKind::Symlink {
            // After chown, which clears set-id bits.
            rustix::fs::chmodat(d, n, Mode::from_raw_mode(a.mode & 0o7777), AtFlags::empty())?;
        }
        rustix::fs::utimensat(
            d,
            n,
            &Timestamps {
                last_access: ts(a.atime),
                last_modification: ts(a.mtime),
            },
            AtFlags::SYMLINK_NOFOLLOW,
        )?;
        Ok(())
    }

    pub fn set_attrs(&self, path: &[u8], a: &Attrs) -> io::Result<()> {
        let (d, n) = self.parent(path)?;
        let st = rustix::fs::statat(&d, n, AtFlags::SYMLINK_NOFOLLOW)?;
        self.set_attrs_at(&d, n, kind_of(&st), a)
    }

    pub fn set_xattr(&self, path: &[u8], name: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        let (d, n) = self.parent(path)?;
        // Through an O_PATH descriptor of the file itself (not a symlink:
        // user xattrs are not allowed on those).
        let fd = rustix::fs::openat(
            &d,
            n,
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let st = rustix::fs::fstat(&fd)?;
        if FileType::from_raw_mode(st.st_mode) == FileType::Symlink {
            return Ok(());
        }
        let p = format!("/proc/self/fd/{}", std::os::fd::AsRawFd::as_raw_fd(&fd));
        let name = OsStr::from_bytes(name);
        match value {
            Some(v) => rustix::fs::setxattr(p.as_str(), name, v, rustix::fs::XattrFlags::empty())?,
            None => match rustix::fs::removexattr(p.as_str(), name) {
                Err(rustix::io::Errno::NODATA) => {}
                r => r?,
            },
        }
        Ok(())
    }

    /// Opens a regular file for reading (never following a symlink).
    pub fn open_read(&self, path: &[u8]) -> io::Result<File> {
        let (d, n) = self.parent(path)?;
        let fd = rustix::fs::openat(
            &d,
            n,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        Ok(File::from(fd))
    }

    /// Opens a regular file for writing, even if its mode forbids it to
    /// the owner (the mode is restored by the caller's next `set_attrs`).
    pub fn open_write(&self, path: &[u8]) -> io::Result<File> {
        let (d, n) = self.parent(path)?;
        let open = || {
            rustix::fs::openat(
                &d,
                n,
                OFlags::WRONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
        };
        let fd = match open() {
            Err(rustix::io::Errno::ACCESS) if !self.as_root => {
                let st = rustix::fs::statat(&d, n, AtFlags::SYMLINK_NOFOLLOW)?;
                rustix::fs::chmodat(
                    &d,
                    n,
                    Mode::from_raw_mode((st.st_mode & 0o7777) | 0o200),
                    AtFlags::empty(),
                )?;
                open()?
            }
            r => r?,
        };
        Ok(File::from(fd))
    }

    /// Block `n` of `file` (shorter at the end, empty past it).
    pub fn read_block(file: &File, n: u64, size: u64) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; size as usize];
        let mut got = 0;
        while got < buf.len() {
            match file.read_at(&mut buf[got..], n * size + got as u64) {
                Ok(0) => break,
                Ok(k) => got += k,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        buf.truncate(got);
        Ok(buf)
    }

    pub fn root_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.root.as_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_from_peers_are_checked() {
        for ok in [&b""[..], b"a", b"a/b", b"..a", b"a/.syncfs"] {
            assert!(check(ok).is_ok(), "{ok:?}");
        }
        for bad in [
            &b"/a"[..],
            b"a//b",
            b"a/..",
            b"../x",
            b"./a",
            b".syncfs",
            b".syncfs/x",
            b"a/\0",
        ] {
            assert!(check(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn creates_and_refuses_symlink_escapes() {
        let dir = tempfile::tempdir().unwrap();
        let b = Backing::open(dir.path()).unwrap();
        let a = Attrs {
            mode: 0o640,
            mtime: (1_000_000, 5),
            atime: (1_000_000, 5),
            ..Default::default()
        };
        b.create(b"d", NodeKind::Dir, &Attrs { mode: 0o755, ..a }, 0, b"")
            .unwrap();
        b.create(b"d/f", NodeKind::File, &a, 0, b"").unwrap();
        let st = b.stat(b"d/f").unwrap();
        assert_eq!(st.st_mode & 0o777, 0o640);
        assert_eq!(st.st_mtime, 1_000_000);
        b.create(b"out", NodeKind::Symlink, &a, 0, b"/").unwrap();
        assert!(b.create(b"out/etc_x", NodeKind::File, &a, 0, b"").is_err());
        assert!(b.open_read(b"out").is_err(), "symlinks are not followed");
        b.rename(b"d/f", b"d/g", 0).unwrap();
        b.link(b"d/g", b"h").unwrap();
        b.remove(b"d/g", false).unwrap();
        assert!(b.exists(b"h"));
    }
}
