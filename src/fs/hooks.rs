//! The boundary between the FUSE layer and the local write path.
//!
//! Every operation that changes the backing directory is described as a
//! [`Mutation`] and passed to [`WriteHooks::before`] before it touches the
//! disk and to [`WriteHooks::after`] once it succeeded. Opening a file for
//! writing and closing it again go through [`WriteHooks::open_write`] and
//! [`WriteHooks::release_write`].
//!
//! A standalone node uses [`LocalOnly`], which allows everything; a cluster
//! node uses `lock::ClusterHooks`: whole-file write locks and the quorum
//! check in `before`, replication records (`crate::repl`) in `after` and `failed`.
//! Step 7 replicates what was marked.

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};

use fuser::{Errno, FileType};

use super::inode::Inode;

/// A file or directory taking part in a mutation.
#[derive(Clone, Copy, Debug)]
pub struct NodeRef<'a> {
    inode: &'a Inode,
    root: &'a Path,
}

impl<'a> NodeRef<'a> {
    pub(crate) fn new(inode: &'a Inode, root: &'a Path) -> Self {
        Self { inode, root }
    }

    /// FUSE inode number.
    pub fn ino(&self) -> u64 {
        self.inode.ino
    }

    pub fn kind(&self) -> FileType {
        self.inode.kind
    }

    /// Current path relative to the backing directory (empty for the root),
    /// resolved on demand from the inode's descriptor. Fails with
    /// `NotFound` if the file has been deleted.
    pub fn path(&self) -> io::Result<PathBuf> {
        let target = std::fs::read_link(self.inode.proc_path())?;
        if target
            .as_os_str()
            .as_encoded_bytes()
            .ends_with(b" (deleted)")
        {
            return Err(io::ErrorKind::NotFound.into());
        }
        target
            .strip_prefix(self.root)
            .map(Path::to_path_buf)
            .map_err(|_| io::Error::other("file is outside the backing directory"))
    }

    /// The file's current attributes.
    pub fn stat(&self) -> io::Result<rustix::fs::Stat> {
        Ok(rustix::fs::statat(
            &self.inode.fd,
            "",
            rustix::fs::AtFlags::EMPTY_PATH | rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?)
    }

    /// Attributes of `name` in this directory (not following a symlink).
    pub fn stat_child(&self, name: &std::ffi::OsStr) -> io::Result<rustix::fs::Stat> {
        Ok(rustix::fs::statat(
            &self.inode.fd,
            name,
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?)
    }

    /// Target of the symlink `name` in this directory.
    pub fn readlink_child(&self, name: &std::ffi::OsStr) -> io::Result<Vec<u8>> {
        Ok(rustix::fs::readlinkat(&self.inode.fd, name, Vec::new())?.into_bytes())
    }

    /// Value of extended attribute `name` (None if it has none).
    pub fn xattr(&self, name: &std::ffi::OsStr) -> io::Result<Option<Vec<u8>>> {
        if self.inode.kind == FileType::Symlink {
            return Ok(None);
        }
        let target = self.inode.proc_path();
        let len = match rustix::fs::getxattr_query(target.as_str(), name) {
            Ok(n) => n,
            Err(rustix::io::Errno::NODATA) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut buf = vec![0u8; len];
        let n = rustix::fs::getxattr(target.as_str(), name, &mut buf[..])?;
        buf.truncate(n);
        Ok(Some(buf))
    }
}

/// Which attributes a `setattr` changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AttrChange {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub times: bool,
}

/// A change to the backing directory.
#[derive(Clone, Copy, Debug)]
pub enum Mutation<'a> {
    /// New file, directory, symlink or special file `name` in `parent`.
    Create {
        parent: NodeRef<'a>,
        name: &'a OsStr,
        kind: FileType,
    },
    /// New hard link to `node`.
    Link {
        node: NodeRef<'a>,
        new_parent: NodeRef<'a>,
        new_name: &'a OsStr,
    },
    /// `unlink` or `rmdir`.
    Remove {
        parent: NodeRef<'a>,
        name: &'a OsStr,
        is_dir: bool,
    },
    /// `rename`, with `RENAME_*` flags.
    Rename {
        parent: NodeRef<'a>,
        name: &'a OsStr,
        new_parent: NodeRef<'a>,
        new_name: &'a OsStr,
        flags: u32,
    },
    /// chmod, chown, truncate or utimens.
    SetAttr {
        node: NodeRef<'a>,
        change: AttrChange,
    },
    /// Data written to `[offset, offset + len)`.
    Write {
        node: NodeRef<'a>,
        offset: u64,
        len: u64,
    },
    /// `fallocate` (including punching holes).
    Allocate {
        node: NodeRef<'a>,
        offset: u64,
        len: u64,
        mode: i32,
    },
    /// Extended attribute set or removed.
    Xattr {
        node: NodeRef<'a>,
        name: &'a OsStr,
        removed: bool,
    },
}

/// Returned by [`WriteHooks::before`]; dropped once the operation is
/// over, whether it succeeded or failed (after [`WriteHooks::after`] on
/// success). Used to release locks taken for a single operation.
#[must_use]
#[derive(Default)]
pub struct OpGuard(Option<Box<dyn FnOnce() + Send>>);

impl OpGuard {
    pub fn none() -> OpGuard {
        OpGuard(None)
    }

    pub fn new(on_drop: impl FnOnce() + Send + 'static) -> OpGuard {
        OpGuard(Some(Box::new(on_drop)))
    }
}

impl Drop for OpGuard {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

/// Hooks around every local change. All methods have permissive defaults.
pub trait WriteHooks: Send + Sync + 'static {
    /// Called before `op` is applied. An error aborts the operation and is
    /// returned to the caller.
    fn before(&self, _op: &Mutation<'_>) -> Result<OpGuard, Errno> {
        Ok(OpGuard::none())
    }

    /// Called after `op` was applied successfully.
    fn after(&self, _op: &Mutation<'_>) {}

    /// Called when `op` was allowed by `before` but then failed. A data
    /// change may have been partly applied.
    fn failed(&self, _op: &Mutation<'_>) {}

    /// Called when a file is opened (or created) for writing.
    fn open_write(&self, _node: NodeRef<'_>) -> Result<(), Errno> {
        Ok(())
    }

    /// Called when a file opened for writing is released (last close).
    fn release_write(&self, _node: NodeRef<'_>) {}
}

/// Single-node behaviour: every change is allowed and nothing is tracked.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalOnly;

impl WriteHooks for LocalOnly {}

impl<T: WriteHooks + ?Sized> WriteHooks for std::sync::Arc<T> {
    fn before(&self, op: &Mutation<'_>) -> Result<OpGuard, Errno> {
        (**self).before(op)
    }

    fn after(&self, op: &Mutation<'_>) {
        (**self).after(op)
    }

    fn failed(&self, op: &Mutation<'_>) {
        (**self).failed(op)
    }

    fn open_write(&self, node: NodeRef<'_>) -> Result<(), Errno> {
        (**self).open_write(node)
    }

    fn release_write(&self, node: NodeRef<'_>) {
        (**self).release_write(node)
    }
}
