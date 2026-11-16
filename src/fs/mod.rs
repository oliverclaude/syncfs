//! The FUSE filesystem: a passthrough of the backing directory.
//!
//! [`SyncFs`] serves every request from the backing directory (see
//! [`inode`] for how inodes map to backing files). Every change goes through
//! the [`WriteHooks`] given at construction, which is where cluster locking,
//! quorum and replication plug in.

pub mod attr;
pub mod hooks;
pub mod inode;

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    Errno, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, KernelConfig,
    LockOwner, OpenAccMode, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLseek, ReplyOpen, ReplyStatfs, ReplyWrite,
    ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use rustix::fs::{AtFlags, Mode, OFlags, Timespec, Timestamps};

use attr::{key, to_attr};
pub use hooks::{AttrChange, LocalOnly, Mutation, NodeRef, WriteHooks};
use inode::{Inode, InodeTable, ROOT};

/// How long the kernel may cache attributes and directory entries. Kept
/// short because the backing directory can change underneath us.
pub const TTL: Duration = Duration::from_secs(1);

fn errno(e: rustix::io::Errno) -> Errno {
    Errno::from_i32(e.raw_os_error())
}

type Res<T> = Result<T, Errno>;

macro_rules! reply {
    ($reply:ident, $res:expr, |$v:pat_param| $ok:expr) => {
        match $res {
            Ok($v) => $ok,
            Err(e) => $reply.error(e),
        }
    };
}

#[derive(Debug)]
struct OpenFile {
    file: File,
    inode: Arc<Inode>,
    writable: bool,
}

#[derive(Debug)]
struct OpenDir {
    fd: OwnedFd,
    inode: Arc<Inode>,
    entries: Vec<(OsString, u64, FileType)>,
}

/// The passthrough filesystem.
pub struct SyncFs<H: WriteHooks = LocalOnly> {
    root: PathBuf,
    inodes: InodeTable,
    files: RwLock<HashMap<u64, Arc<OpenFile>>>,
    dirs: Mutex<HashMap<u64, Arc<Mutex<OpenDir>>>>,
    next_fh: AtomicU64,
    hooks: H,
    /// Running as root: new files are chowned to the requesting user.
    as_root: bool,
}

impl<H: WriteHooks> SyncFs<H> {
    /// Opens `backing_dir` (which must be a directory) as the root.
    pub fn new(backing_dir: &Path, hooks: H) -> std::io::Result<Self> {
        let root = std::fs::canonicalize(backing_dir)?;
        let fd = rustix::fs::open(
            &root,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let st = rustix::fs::statat(&fd, "", AtFlags::EMPTY_PATH)?;
        Ok(Self {
            root,
            inodes: InodeTable::new(fd, &st),
            files: RwLock::new(HashMap::new()),
            dirs: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            hooks,
            as_root: rustix::process::geteuid().is_root(),
        })
    }

    /// Canonical path of the backing directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Number of inodes the kernel currently holds.
    pub fn live_inodes(&self) -> usize {
        self.inodes.live_count()
    }

    fn node<'a>(&'a self, inode: &'a Inode) -> NodeRef<'a> {
        NodeRef::new(inode, &self.root)
    }

    fn inode(&self, ino: INodeNo) -> Res<Arc<Inode>> {
        self.inodes.get(ino.0)
    }

    fn stat(inode: &Inode) -> Res<rustix::fs::Stat> {
        rustix::fs::statat(
            &inode.fd,
            "",
            AtFlags::EMPTY_PATH | AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(errno)
    }

    fn attr(inode: &Inode) -> Res<fuser::FileAttr> {
        Ok(to_attr(inode.ino, &Self::stat(inode)?))
    }

    /// Looks up `name` in `parent`, counting one kernel lookup.
    fn do_lookup(&self, parent: &Inode, name: &OsStr) -> Res<(Arc<Inode>, fuser::FileAttr)> {
        if parent.ino == ROOT && name == ".." {
            let root = self.inodes.get(ROOT)?;
            let attr = Self::attr(&root)?;
            return Ok((root, attr));
        }
        let fd = rustix::fs::openat(
            &parent.fd,
            name,
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(errno)?;
        let st = rustix::fs::statat(&fd, "", AtFlags::EMPTY_PATH | AtFlags::SYMLINK_NOFOLLOW)
            .map_err(errno)?;
        let inode = self.inodes.lookup(fd, &st);
        Ok((Arc::clone(&inode), to_attr(inode.ino, &st)))
    }

    /// Gives a newly created `name` in `parent` to the requesting user when
    /// running as root. The group is left alone in set-group-ID
    /// directories, where the kernel already inherited the parent's group.
    fn chown_new(&self, req: &Request, parent: &Inode, name: &OsStr) -> Res<()> {
        if !self.as_root {
            return Ok(());
        }
        let pst = Self::stat(parent)?;
        let gid = (pst.st_mode & libc::S_ISGID == 0).then(|| rustix::fs::Gid::from_raw(req.gid()));
        rustix::fs::chownat(
            &parent.fd,
            name,
            Some(rustix::fs::Uid::from_raw(req.uid())),
            gid,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(errno)
    }

    /// Runs `apply` between the `before` and `after` hooks for `op`.
    fn mutate<T>(&self, op: Mutation<'_>, apply: impl FnOnce() -> Res<T>) -> Res<T> {
        self.hooks.before(&op)?;
        let v = apply()?;
        self.hooks.after(&op);
        Ok(v)
    }

    fn create_node(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        kind: FileType,
        make: impl FnOnce(&Inode) -> Res<()>,
    ) -> Res<fuser::FileAttr> {
        let p = self.inode(parent)?;
        let op = Mutation::Create {
            parent: self.node(&p),
            name,
            kind,
        };
        let (_, attr) = self.mutate(op, || {
            make(&p)?;
            self.chown_new(req, &p, name)?;
            self.do_lookup(&p, name)
        })?;
        Ok(attr)
    }

    fn new_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    fn file(&self, fh: FileHandle) -> Res<Arc<OpenFile>> {
        self.files
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&fh.0)
            .cloned()
            .ok_or(Errno::EBADF)
    }

    fn dir(&self, fh: FileHandle) -> Res<Arc<Mutex<OpenDir>>> {
        self.dirs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&fh.0)
            .cloned()
            .ok_or(Errno::EBADF)
    }

    /// Opens the backing file of `inode` for I/O with the caller's flags.
    fn open_backing(inode: &Inode, flags: i32) -> Res<File> {
        let strip = libc::O_CREAT | libc::O_EXCL | libc::O_NOCTTY | libc::O_NOFOLLOW;
        let flags = OFlags::from_bits_retain(((flags & !strip) | libc::O_CLOEXEC) as u32);
        let fd = rustix::fs::open(inode.proc_path(), flags, Mode::empty()).map_err(errno)?;
        Ok(File::from(fd))
    }

    fn register_file(&self, file: File, inode: Arc<Inode>, writable: bool) -> u64 {
        let fh = self.new_fh();
        self.files
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                fh,
                Arc::new(OpenFile {
                    file,
                    inode,
                    writable,
                }),
            );
        fh
    }

    #[allow(clippy::too_many_arguments)]
    fn do_setattr(
        &self,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        fh: Option<FileHandle>,
    ) -> Res<fuser::FileAttr> {
        let inode = self.inode(ino)?;
        let change = AttrChange {
            mode,
            uid,
            gid,
            size,
            times: atime.is_some() || mtime.is_some(),
        };
        let op = Mutation::SetAttr {
            node: self.node(&inode),
            change,
        };
        self.mutate(op, || {
            let is_link = inode.kind == FileType::Symlink;
            if let Some(size) = size {
                let open = fh.and_then(|fh| self.file(fh).ok()).filter(|f| f.writable);
                match open {
                    Some(f) => f.file.set_len(size).map_err(Errno::from)?,
                    None => Self::open_backing(&inode, libc::O_WRONLY)?
                        .set_len(size)
                        .map_err(Errno::from)?,
                }
            }
            if uid.is_some() || gid.is_some() {
                rustix::fs::chownat(
                    &inode.fd,
                    "",
                    uid.map(rustix::fs::Uid::from_raw),
                    gid.map(rustix::fs::Gid::from_raw),
                    AtFlags::EMPTY_PATH | AtFlags::SYMLINK_NOFOLLOW,
                )
                .map_err(errno)?;
            }
            if let Some(mode) = mode {
                if is_link {
                    return Err(Errno::EOPNOTSUPP);
                }
                rustix::fs::chmod(inode.proc_path(), Mode::from_raw_mode(mode & 0o7777))
                    .map_err(errno)?;
            }
            if atime.is_some() || mtime.is_some() {
                let times = Timestamps {
                    last_access: timespec(atime),
                    last_modification: timespec(mtime),
                };
                if is_link {
                    rustix::fs::utimensat(
                        &inode.fd,
                        "",
                        &times,
                        AtFlags::EMPTY_PATH | AtFlags::SYMLINK_NOFOLLOW,
                    )
                } else {
                    rustix::fs::utimensat(
                        rustix::fs::CWD,
                        inode.proc_path(),
                        &times,
                        AtFlags::empty(),
                    )
                }
                .map_err(errno)?;
            }
            Self::attr(&inode)
        })
    }

    fn read_dir_entries(&self, dir: &mut OpenDir) -> Res<()> {
        let mut d = rustix::fs::Dir::read_from(&dir.fd).map_err(errno)?;
        let dev = key(&Self::stat(&dir.inode)?).0;
        dir.entries.clear();
        for ent in &mut d {
            let ent = ent.map_err(errno)?;
            let name = OsStr::from_bytes(ent.file_name().to_bytes()).to_os_string();
            let ino = match name.as_bytes() {
                b"." => dir.inode.ino,
                b".." if dir.inode.ino == ROOT => ROOT,
                _ => self.inodes.number_for((dev, ent.ino())),
            };
            use rustix::fs::FileType as R;
            let kind = match ent.file_type() {
                R::Directory => FileType::Directory,
                R::Symlink => FileType::Symlink,
                R::Fifo => FileType::NamedPipe,
                R::Socket => FileType::Socket,
                R::CharacterDevice => FileType::CharDevice,
                R::BlockDevice => FileType::BlockDevice,
                R::RegularFile => FileType::RegularFile,
                R::Unknown => rustix::fs::statat(&dir.fd, &name, AtFlags::SYMLINK_NOFOLLOW)
                    .map(|st| attr::kind_from_mode(st.st_mode))
                    .unwrap_or(FileType::RegularFile),
            };
            dir.entries.push((name, ino, kind));
        }
        Ok(())
    }

    fn xattr_target(inode: &Inode) -> Res<String> {
        // Symlinks cannot be reached through /proc/self/fd without
        // following them; user xattrs are not allowed on them anyway.
        if inode.kind == FileType::Symlink {
            return Err(Errno::EPERM);
        }
        Ok(inode.proc_path())
    }
}

fn timespec(t: Option<TimeOrNow>) -> Timespec {
    match t {
        None => Timespec {
            tv_sec: 0,
            tv_nsec: rustix::fs::UTIME_OMIT,
        },
        Some(TimeOrNow::Now) => Timespec {
            tv_sec: 0,
            tv_nsec: rustix::fs::UTIME_NOW,
        },
        Some(TimeOrNow::SpecificTime(t)) => {
            let (sec, nsec) = match t.duration_since(UNIX_EPOCH) {
                Ok(d) => (d.as_secs() as i64, i64::from(d.subsec_nanos())),
                Err(e) => {
                    let d = e.duration();
                    let mut sec = -(d.as_secs() as i64);
                    let mut nsec = i64::from(d.subsec_nanos());
                    if nsec > 0 {
                        sec -= 1;
                        nsec = 1_000_000_000 - nsec;
                    }
                    (sec, nsec)
                }
            };
            Timespec {
                tv_sec: sec,
                tv_nsec: nsec,
            }
        }
    }
}

fn reply_xattr(reply: ReplyXattr, size: u32, data: &[u8]) {
    if size == 0 {
        reply.size(data.len() as u32);
    } else if data.len() > size as usize {
        reply.error(Errno::ERANGE);
    } else {
        reply.data(data);
    }
}

impl<H: WriteHooks> Filesystem for SyncFs<H> {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> std::io::Result<()> {
        log::info!("mounted {}", self.root.display());
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let res = self.inode(parent).and_then(|p| self.do_lookup(&p, name));
        reply!(reply, res, |(_, attr)| reply.entry(
            &TTL,
            &attr,
            Generation(0)
        ));
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        self.inodes.forget(ino.0, nlookup);
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let res = self.inode(ino).and_then(|i| Self::attr(&i));
        reply!(reply, res, |attr| reply.attr(&TTL, &attr));
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let res = self.do_setattr(ino, mode, uid, gid, size, atime, mtime, fh);
        reply!(reply, res, |attr| reply.attr(&TTL, &attr));
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let res = self
            .inode(ino)
            .and_then(|i| rustix::fs::readlinkat(&i.fd, "", Vec::new()).map_err(errno));
        reply!(reply, res, |target| reply.data(target.as_bytes()));
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let kind = attr::kind_from_mode(mode);
        let res = self.create_node(req, parent, name, kind, |p| {
            rustix::fs::mknodat(
                &p.fd,
                name,
                rustix::fs::FileType::from_raw_mode(mode),
                Mode::from_raw_mode(mode & !umask & 0o7777),
                u64::from(rdev),
            )
            .map_err(errno)
        });
        reply!(reply, res, |attr| reply.entry(&TTL, &attr, Generation(0)));
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let res = self.create_node(req, parent, name, FileType::Directory, |p| {
            rustix::fs::mkdirat(&p.fd, name, Mode::from_raw_mode(mode & !umask & 0o7777))
                .map_err(errno)
        });
        reply!(reply, res, |attr| reply.entry(&TTL, &attr, Generation(0)));
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let res = self.inode(parent).and_then(|p| {
            let op = Mutation::Remove {
                parent: self.node(&p),
                name,
                is_dir: false,
            };
            self.mutate(op, || {
                rustix::fs::unlinkat(&p.fd, name, AtFlags::empty()).map_err(errno)
            })
        });
        reply!(reply, res, |()| reply.ok());
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let res = self.inode(parent).and_then(|p| {
            let op = Mutation::Remove {
                parent: self.node(&p),
                name,
                is_dir: true,
            };
            self.mutate(op, || {
                rustix::fs::unlinkat(&p.fd, name, AtFlags::REMOVEDIR).map_err(errno)
            })
        });
        reply!(reply, res, |()| reply.ok());
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let res = self.create_node(req, parent, link_name, FileType::Symlink, |p| {
            rustix::fs::symlinkat(target, &p.fd, link_name).map_err(errno)
        });
        reply!(reply, res, |attr| reply.entry(&TTL, &attr, Generation(0)));
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let res = (|| {
            let p = self.inode(parent)?;
            let np = self.inode(newparent)?;
            let op = Mutation::Rename {
                parent: self.node(&p),
                name,
                new_parent: self.node(&np),
                new_name: newname,
                flags: flags.bits(),
            };
            self.mutate(op, || {
                rustix::fs::renameat_with(
                    &p.fd,
                    name,
                    &np.fd,
                    newname,
                    rustix::fs::RenameFlags::from_bits_retain(flags.bits()),
                )
                .map_err(errno)
            })
        })();
        reply!(reply, res, |()| reply.ok());
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let res = (|| {
            let i = self.inode(ino)?;
            let np = self.inode(newparent)?;
            let op = Mutation::Link {
                node: self.node(&i),
                new_parent: self.node(&np),
                new_name: newname,
            };
            self.mutate(op, || {
                // Following the /proc magic link needs no CAP_DAC_READ_SEARCH,
                // unlike AT_EMPTY_PATH.
                rustix::fs::linkat(
                    rustix::fs::CWD,
                    i.proc_path(),
                    &np.fd,
                    newname,
                    AtFlags::SYMLINK_FOLLOW,
                )
                .map_err(errno)?;
                self.do_lookup(&np, newname)
            })
        })();
        reply!(reply, res, |(_, attr)| reply.entry(
            &TTL,
            &attr,
            Generation(0)
        ));
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let res = (|| {
            let inode = self.inode(ino)?;
            let writable = flags.acc_mode() != OpenAccMode::O_RDONLY;
            if writable {
                self.hooks.open_write(self.node(&inode))?;
            }
            let opened = if flags.0 & libc::O_TRUNC != 0 {
                let op = Mutation::SetAttr {
                    node: self.node(&inode),
                    change: AttrChange {
                        size: Some(0),
                        ..Default::default()
                    },
                };
                self.mutate(op, || Self::open_backing(&inode, flags.0))
            } else {
                Self::open_backing(&inode, flags.0)
            };
            match opened {
                Ok(file) => Ok(self.register_file(file, inode, writable)),
                Err(e) => {
                    if writable {
                        self.hooks.release_write(self.node(&inode));
                    }
                    Err(e)
                }
            }
        })();
        reply!(reply, res, |fh| reply
            .opened(FileHandle(fh), FopenFlags::empty()));
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let res = self.file(fh).and_then(|f| {
            let mut buf = vec![0u8; size as usize];
            let mut filled = 0;
            while filled < buf.len() {
                match f.file.read_at(&mut buf[filled..], offset + filled as u64) {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(Errno::from(e)),
                }
            }
            buf.truncate(filled);
            Ok(buf)
        });
        reply!(reply, res, |data| reply.data(&data));
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let res = self.file(fh).and_then(|f| {
            let op = Mutation::Write {
                node: self.node(&f.inode),
                offset,
                len: data.len() as u64,
            };
            self.mutate(op, || {
                f.file.write_all_at(data, offset).map_err(Errno::from)
            })
        });
        reply!(reply, res, |()| reply.written(data.len() as u32));
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply!(reply, self.file(fh), |_| reply.ok());
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let removed = self
            .files
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&fh.0);
        if let Some(f) = removed {
            if f.writable {
                self.hooks.release_write(self.node(&f.inode));
            }
        }
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let res = self.file(fh).and_then(|f| {
            if datasync {
                f.file.sync_data()
            } else {
                f.file.sync_all()
            }
            .map_err(Errno::from)
        });
        reply!(reply, res, |()| reply.ok());
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let res = self.inode(ino).and_then(|inode| {
            let fd = rustix::fs::open(
                inode.proc_path(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno)?;
            let fh = self.new_fh();
            let dir = OpenDir {
                fd,
                inode,
                entries: Vec::new(),
            };
            self.dirs
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(fh, Arc::new(Mutex::new(dir)));
            Ok(fh)
        });
        reply!(reply, res, |fh| reply
            .opened(FileHandle(fh), FopenFlags::empty()));
    }

    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let dir = match self.dir(fh) {
            Ok(d) => d,
            Err(e) => return reply.error(e),
        };
        let mut dir = dir.lock().unwrap_or_else(|e| e.into_inner());
        // Offset 0 is the start (or a rewinddir): take a fresh snapshot.
        if offset == 0 {
            if let Err(e) = self.read_dir_entries(&mut dir) {
                return reply.error(e);
            }
        }
        for (i, (name, ino, kind)) in dir.entries.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(*ino), i as u64 + 1, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.dirs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&fh.0);
        reply.ok();
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let res = self.dir(fh).and_then(|d| {
            let d = d.lock().unwrap_or_else(|e| e.into_inner());
            rustix::fs::fsync(&d.fd).map_err(errno)
        });
        reply!(reply, res, |()| reply.ok());
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let res = self
            .inodes
            .get(ROOT)
            .and_then(|r| rustix::fs::fstatvfs(r.fd.as_fd()).map_err(errno));
        reply!(reply, res, |s| reply.statfs(
            s.f_blocks,
            s.f_bfree,
            s.f_bavail,
            s.f_files,
            s.f_ffree,
            s.f_bsize as u32,
            s.f_namemax as u32,
            s.f_frsize as u32,
        ));
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        let res = self.inode(ino).and_then(|i| {
            let target = Self::xattr_target(&i)?;
            let op = Mutation::Xattr {
                node: self.node(&i),
                name,
                removed: false,
            };
            self.mutate(op, || {
                rustix::fs::setxattr(
                    target.as_str(),
                    name,
                    value,
                    rustix::fs::XattrFlags::from_bits_retain(flags as u32),
                )
                .map_err(errno)
            })
        });
        reply!(reply, res, |()| reply.ok());
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let res = self.inode(ino).and_then(|i| {
            if i.kind == FileType::Symlink {
                return Err(Errno::NO_XATTR);
            }
            let target = i.proc_path();
            let len = rustix::fs::getxattr_query(target.as_str(), name).map_err(errno)?;
            let mut buf = vec![0u8; len];
            let n = rustix::fs::getxattr(target.as_str(), name, &mut buf[..]).map_err(errno)?;
            buf.truncate(n);
            Ok(buf)
        });
        reply!(reply, res, |data| reply_xattr(reply, size, &data));
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let res = self.inode(ino).and_then(|i| {
            if i.kind == FileType::Symlink {
                return Ok(Vec::new());
            }
            let target = i.proc_path();
            let len = rustix::fs::listxattr_query(target.as_str()).map_err(errno)?;
            let mut buf = vec![0u8; len];
            let n = rustix::fs::listxattr(target.as_str(), &mut buf[..]).map_err(errno)?;
            buf.truncate(n);
            Ok(buf)
        });
        reply!(reply, res, |data| reply_xattr(reply, size, &data));
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let res = self.inode(ino).and_then(|i| {
            let target = Self::xattr_target(&i)?;
            let op = Mutation::Xattr {
                node: self.node(&i),
                name,
                removed: true,
            };
            self.mutate(op, || {
                rustix::fs::removexattr(target.as_str(), name).map_err(errno)
            })
        });
        reply!(reply, res, |()| reply.ok());
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let res = (|| {
            let p = self.inode(parent)?;
            let writable = flags & libc::O_ACCMODE != libc::O_RDONLY;
            let op = Mutation::Create {
                parent: self.node(&p),
                name,
                kind: FileType::RegularFile,
            };
            let (inode, attr, file) = self.mutate(op, || {
                let oflags = OFlags::from_bits_retain(
                    ((flags & !libc::O_NOCTTY) | libc::O_CREAT | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                        as u32,
                );
                let fd = rustix::fs::openat(
                    &p.fd,
                    name,
                    oflags,
                    Mode::from_raw_mode(mode & !umask & 0o7777),
                )
                .map_err(errno)?;
                self.chown_new(req, &p, name)?;
                let (inode, attr) = self.do_lookup(&p, name)?;
                Ok((inode, attr, File::from(fd)))
            })?;
            if writable {
                if let Err(e) = self.hooks.open_write(self.node(&inode)) {
                    self.inodes.forget(inode.ino, 1);
                    return Err(e);
                }
            }
            let fh = self.register_file(file, inode, writable);
            Ok((attr, fh))
        })();
        reply!(reply, res, |(attr, fh)| reply.created(
            &TTL,
            &attr,
            Generation(0),
            FileHandle(fh),
            FopenFlags::empty()
        ));
    }

    fn fallocate(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let res = self.file(fh).and_then(|f| {
            let op = Mutation::Allocate {
                node: self.node(&f.inode),
                offset,
                len: length,
                mode,
            };
            self.mutate(op, || {
                rustix::fs::fallocate(
                    &f.file,
                    rustix::fs::FallocateFlags::from_bits_retain(mode as u32),
                    offset,
                    length,
                )
                .map_err(errno)
            })
        });
        reply!(reply, res, |()| reply.ok());
    }

    fn lseek(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        let res = self.file(fh).and_then(|f| {
            use std::os::fd::AsRawFd;
            // SAFETY: plain lseek on a descriptor we own; I/O uses pread and
            // pwrite, so moving the offset affects nothing else.
            let r = unsafe { libc::lseek(f.file.as_raw_fd(), offset, whence) };
            if r < 0 {
                Err(Errno::from(std::io::Error::last_os_error()))
            } else {
                Ok(r)
            }
        });
        reply!(reply, res, |off| reply.offset(off));
    }
}
