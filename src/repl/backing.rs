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
    path: std::path::PathBuf,
}

impl Backing {
    pub fn open(root: &Path) -> io::Result<Backing> {
        let path = std::fs::canonicalize(root)?;
        let root = rustix::fs::open(
            root,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        Ok(Backing {
            root,
            as_root: rustix::process::geteuid().is_root(),
            path,
        })
    }

    /// The backing directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn readlink(&self, path: &[u8]) -> io::Result<Vec<u8>> {
        let (d, n) = self.parent(path)?;
        Ok(rustix::fs::readlinkat(&d, n, Vec::new())?.into_bytes())
    }

    /// Every path below the root except the root's `.syncfs`, sorted
    /// (a directory comes before what is in it). Does not follow symlinks
    /// or cross into other filesystems.
    pub fn list(&self) -> io::Result<Vec<Vec<u8>>> {
        use std::os::unix::fs::MetadataExt;
        let dev = std::fs::symlink_metadata(&self.path)?.dev();
        let mut out = Vec::new();
        let mut stack: Vec<Vec<u8>> = vec![Vec::new()];
        while let Some(rel) = stack.pop() {
            let dir = self.path.join(OsStr::from_bytes(&rel));
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let name = e.file_name();
                if rel.is_empty() && name == crate::fs::virt::NAME {
                    continue;
                }
                let mut p = rel.clone();
                if !p.is_empty() {
                    p.push(b'/');
                }
                p.extend_from_slice(name.as_bytes());
                // `file_type` does not follow a symlink: one to a
                // directory is an entry, not a subtree.
                if e.file_type().is_ok_and(|t| t.is_dir())
                    && e.metadata().is_ok_and(|md| md.dev() == dev)
                {
                    stack.push(p.clone());
                }
                out.push(p);
            }
        }
        out.sort();
        Ok(out)
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

    /// Moves `path` (a peer-style path, checked) to `into/path`, where
    /// `into` is a directory below the root's `.syncfs` (e.g. the rebuild's
    /// orphans), creating the directories on the way. Every directory is
    /// opened beneath the root without following symlinks, so a symlink
    /// in either path cannot move anything outside the backing directory.
    pub fn move_aside(&self, path: &[u8], into: &[u8]) -> io::Result<()> {
        let (from_dir, from_name) = self.parent(path)?;
        let mut target: Vec<u8> = into.to_vec();
        if let Some(i) = path.iter().rposition(|b| *b == b'/') {
            target.push(b'/');
            target.extend_from_slice(&path[..i]);
        }
        let mut dir = rustix::fs::openat(
            &self.root,
            ".",
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        for c in target.split(|b| *b == b'/') {
            if c.is_empty() || c == b"." || c == b".." || c.contains(&0) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "bad path"));
            }
            let c = OsStr::from_bytes(c);
            match rustix::fs::mkdirat(&dir, c, Mode::from_raw_mode(0o700)) {
                Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                Err(e) => return Err(e.into()),
            }
            dir = rustix::fs::openat2(
                &dir,
                c,
                OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
                ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
            )?;
        }
        Ok(rustix::fs::renameat(&from_dir, from_name, &dir, from_name)?)
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
        let (d, n) = self.create_node(path, kind, attrs, rdev, target)?;
        self.set_attrs_at(&d, &n, kind, attrs)
    }

    /// Like [`create`], but does not apply placeholder times. Used when
    /// redoing a local Create that was journaled with intended metadata
    /// before the inode existed.
    pub fn create_intended(
        &self,
        path: &[u8],
        kind: NodeKind,
        attrs: &Attrs,
        rdev: u64,
        target: &[u8],
    ) -> io::Result<()> {
        let (d, n) = self.create_node(path, kind, attrs, rdev, target)?;
        if self.as_root {
            let r = rustix::fs::chownat(
                &d,
                &n,
                Some(rustix::fs::Uid::from_raw(attrs.uid)),
                Some(rustix::fs::Gid::from_raw(attrs.gid)),
                AtFlags::SYMLINK_NOFOLLOW,
            );
            if let Err(e) = r {
                log::debug!("replication: chown {n:?}: {e}");
            }
        }
        if kind != NodeKind::Symlink {
            rustix::fs::chmodat(
                &d,
                &n,
                Mode::from_raw_mode(attrs.mode & 0o7777),
                AtFlags::empty(),
            )?;
        }
        Ok(())
    }

    fn create_node(
        &self,
        path: &[u8],
        kind: NodeKind,
        attrs: &Attrs,
        rdev: u64,
        target: &[u8],
    ) -> io::Result<(OwnedFd, std::ffi::OsString)> {
        let (d, n) = self.parent(path)?;
        let n = n.to_os_string();
        let mode = Mode::from_raw_mode(attrs.mode & 0o7777);
        match kind {
            NodeKind::Dir => rustix::fs::mkdirat(&d, &n, mode)?,
            NodeKind::Symlink => rustix::fs::symlinkat(OsStr::from_bytes(target), &d, &n)?,
            NodeKind::File => {
                rustix::fs::openat(
                    &d,
                    &n,
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
                rustix::fs::mknodat(&d, &n, ft, mode, rdev)?;
            }
        }
        Ok((d, n))
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

    /// An O_PATH descriptor of the file itself and its `/proc` path, for
    /// xattr calls; None for a symlink (user xattrs are not allowed on
    /// those).
    fn xattr_target(&self, path: &[u8]) -> io::Result<Option<(OwnedFd, String)>> {
        let (d, n) = self.parent(path)?;
        let fd = rustix::fs::openat(
            &d,
            n,
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let st = rustix::fs::fstat(&fd)?;
        if FileType::from_raw_mode(st.st_mode) == FileType::Symlink {
            return Ok(None);
        }
        let p = format!("/proc/self/fd/{}", std::os::fd::AsRawFd::as_raw_fd(&fd));
        Ok(Some((fd, p)))
    }

    pub fn set_xattr(&self, path: &[u8], name: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        let Some((_fd, p)) = self.xattr_target(path)? else {
            return Ok(());
        };
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

    /// Every extended attribute of the file at `path` with its value,
    /// sorted by name (none for a symlink).
    pub fn xattrs(&self, path: &[u8]) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let Some((_fd, p)) = self.xattr_target(path)? else {
            return Ok(Vec::new());
        };
        let p = p.as_str();
        let names = loop {
            let len = rustix::fs::listxattr_query(p)?;
            let mut buf = vec![0u8; len];
            match rustix::fs::listxattr(p, &mut buf[..]) {
                Ok(n) => {
                    buf.truncate(n);
                    break buf;
                }
                // Grew in between.
                Err(rustix::io::Errno::RANGE) => continue,
                Err(e) => return Err(e.into()),
            }
        };
        let mut out = Vec::new();
        for name in names.split(|b| *b == 0).filter(|n| !n.is_empty()) {
            let os = OsStr::from_bytes(name);
            let value = loop {
                let len = match rustix::fs::getxattr_query(p, os) {
                    Ok(n) => n,
                    Err(rustix::io::Errno::NODATA) => break None,
                    Err(e) => return Err(e.into()),
                };
                let mut buf = vec![0u8; len];
                match rustix::fs::getxattr(p, os, &mut buf[..]) {
                    Ok(n) => {
                        buf.truncate(n);
                        break Some(buf);
                    }
                    Err(rustix::io::Errno::RANGE) => continue,
                    Err(rustix::io::Errno::NODATA) => break None,
                    Err(e) => return Err(e.into()),
                }
            };
            if let Some(v) = value {
                out.push((name.to_vec(), v));
            }
        }
        out.sort();
        Ok(out)
    }

    /// Makes the extended attributes of `path` exactly `want` (sorted by
    /// name). Returns how many were set or removed.
    pub fn sync_xattrs(&self, path: &[u8], want: &[(Vec<u8>, Vec<u8>)]) -> io::Result<usize> {
        let have = self.xattrs(path)?;
        let mut changed = 0;
        for (name, _) in &have {
            if want.binary_search_by(|(n, _)| n.cmp(name)).is_err() {
                self.set_xattr(path, name, None)?;
                changed += 1;
            }
        }
        for (name, value) in want {
            let same = have
                .binary_search_by(|(n, _)| n.cmp(name))
                .is_ok_and(|i| have[i].1 == *value);
            if !same {
                self.set_xattr(path, name, Some(value))?;
                changed += 1;
            }
        }
        Ok(changed)
    }

    /// Copies the regular file `from` to the new path `to` (contents,
    /// mode, owner, times and extended attributes).
    pub fn copy_file(&self, from: &[u8], to: &[u8]) -> io::Result<()> {
        let st = self.stat(from)?;
        let attrs = attrs_of(&st);
        self.create(to, NodeKind::File, &attrs, 0, &[])?;
        let mut src = self.open_read(from)?;
        let mut dst = self.open_write(to)?;
        std::io::copy(&mut src, &mut dst)?;
        dst.sync_all()?;
        drop(dst);
        self.sync_xattrs(to, &self.xattrs(from)?)?;
        self.set_attrs(to, &attrs)
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

    /// A symlink to a directory is one entry: the walk does not descend
    /// into its target (issue #1).
    #[test]
    fn list_does_not_follow_directory_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(root.join("d")).unwrap();
        std::fs::write(root.join("d/f"), b"x").unwrap();
        std::os::unix::fs::symlink("d", root.join("link")).unwrap();
        let outside = dir.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("secret"), b"x").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("out")).unwrap();
        let b = Backing::open(&root).unwrap();
        let want: Vec<Vec<u8>> = ["d", "d/f", "link", "out"]
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect();
        assert_eq!(b.list().unwrap(), want);
    }

    /// Moving a path aside never reaches through a symlink (issue #1).
    #[test]
    fn move_aside_stays_beneath_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(root.join("d")).unwrap();
        std::fs::write(root.join("d/f"), b"x").unwrap();
        let outside = dir.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("file"), b"x").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        let b = Backing::open(&root).unwrap();
        assert!(b.move_aside(b"link/file", b".syncfs/orphans/t").is_err());
        assert!(outside.join("file").exists());
        // An orphans directory replaced by a symlink is not followed.
        std::fs::create_dir(root.join(".syncfs")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join(".syncfs/orphans")).unwrap();
        assert!(b.move_aside(b"d/f", b".syncfs/orphans/t").is_err());
        assert!(!outside.join("t").exists());
        std::fs::remove_file(root.join(".syncfs/orphans")).unwrap();
        b.move_aside(b"d/f", b".syncfs/orphans/t").unwrap();
        assert!(root.join(".syncfs/orphans/t/d/f").exists());
        assert!(!root.join("d/f").exists());
        // The symlink itself can be moved aside (not what it points to).
        b.move_aside(b"link", b".syncfs/orphans/t").unwrap();
        assert!(outside.join("file").exists());
    }

    #[test]
    fn xattrs_are_listed_and_synced() {
        let dir = tempfile::tempdir().unwrap();
        let b = Backing::open(dir.path()).unwrap();
        b.create(
            b"f",
            NodeKind::File,
            &Attrs {
                mode: 0o644,
                ..Default::default()
            },
            0,
            b"",
        )
        .unwrap();
        if b.set_xattr(b"f", b"user.a", Some(b"1")).is_err() {
            eprintln!("no user xattrs on {}; skipping", dir.path().display());
            return;
        }
        b.set_xattr(b"f", b"user.b", Some(b"2")).unwrap();
        let want = vec![
            (b"user.b".to_vec(), b"two".to_vec()),
            (b"user.c".to_vec(), b"3".to_vec()),
        ];
        assert_eq!(b.sync_xattrs(b"f", &want).unwrap(), 3);
        assert_eq!(b.xattrs(b"f").unwrap(), want);
        assert_eq!(b.sync_xattrs(b"f", &want).unwrap(), 0);
        b.create(b"l", NodeKind::Symlink, &Attrs::default(), 0, b"f")
            .unwrap();
        assert!(b.xattrs(b"l").unwrap().is_empty());
    }
}
