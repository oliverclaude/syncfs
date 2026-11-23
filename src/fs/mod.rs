//! The FUSE filesystem: a passthrough of the backing directory.
//!
//! [`SyncFs`] serves every request from the backing directory (see
//! [`inode`] for how inodes map to backing files). Every change goes through
//! the [`WriteHooks`] given at construction, which is where cluster locking,
//! quorum and replication plug in. The hidden `.syncfs` status directory
//! is served from memory by this same filesystem (see [`virt`]).

pub mod attr;
pub mod hooks;
pub mod inode;
pub mod virt;

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
pub use hooks::{AttrChange, LocalOnly, Mutation, NodeRef, OpGuard, WriteHooks};
use inode::{Inode, InodeTable, ROOT};

use crate::status::{FnProvider, Op, Provider, Registry, Stats};

/// How long the kernel may cache attributes and directory entries. Kept
/// short because the backing directory can change underneath us.
pub const TTL: Duration = Duration::from_secs(1);

fn errno(e: rustix::io::Errno) -> Errno {
    Errno::from_i32(e.raw_os_error())
}

type Res<T> = Result<T, Errno>;

/// Counts `$op`, then replies with `$ok` or the error (counting it).
macro_rules! reply {
    ($self:ident, $op:expr, $reply:ident, $res:expr, |$v:pat_param| $ok:expr) => {{
        $self.stats.call($op);
        match $res {
            Ok($v) => $ok,
            Err(e) => {
                $self.stats.error($op);
                $reply.error(e)
            }
        }
    }};
}

/// Serves a request for a `.syncfs` inode from memory: replies and returns
/// if `$ino` is virtual.
macro_rules! virtual_reply {
    ($self:ident, $ino:expr, $reply:ident, $res:expr, |$v:pat_param| $ok:expr) => {
        if virt::is_virtual($ino) {
            $self.stats.status_request();
            match $res {
                Ok($v) => $ok,
                Err(e) => $reply.error(e),
            }
            return;
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
    inodes: Arc<InodeTable>,
    files: RwLock<HashMap<u64, Arc<OpenFile>>>,
    dirs: Mutex<HashMap<u64, Arc<Mutex<OpenDir>>>>,
    next_fh: AtomicU64,
    hooks: H,
    /// Running as root: new files are chowned to the requesting user.
    as_root: bool,
    stats: Arc<Stats>,
    status: Arc<Registry>,
    /// Open `.syncfs` files: the snapshot rendered at open.
    status_files: RwLock<HashMap<u64, Arc<Vec<u8>>>>,
    /// The backing directory has its own `.syncfs`, which is hidden.
    shadowed: bool,
}

impl<H: WriteHooks> SyncFs<H> {
    /// Opens `backing_dir` (which must be a directory) as the root.
    /// `status` supplies the files of the `.syncfs` directory.
    pub fn new(backing_dir: &Path, hooks: H, status: Arc<Registry>) -> std::io::Result<Self> {
        let root = std::fs::canonicalize(backing_dir)?;
        let fd = rustix::fs::open(
            &root,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let st = rustix::fs::statat(&fd, "", AtFlags::EMPTY_PATH)?;
        // The disk marker (crate::repl::marker) lives there on purpose.
        let shadowed = rustix::fs::statat(&fd, virt::NAME, AtFlags::SYMLINK_NOFOLLOW).is_ok()
            && !crate::repl::marker::only_marker(&root);
        if shadowed {
            log::warn!(
                "{}/{} exists in the backing directory; it is hidden by the virtual {} directory",
                root.display(),
                virt::NAME,
                virt::NAME
            );
        }
        Ok(Self {
            root,
            inodes: Arc::new(InodeTable::new(fd, &st)),
            files: RwLock::new(HashMap::new()),
            dirs: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            hooks,
            as_root: rustix::process::geteuid().is_root(),
            stats: Arc::new(Stats::default()),
            status,
            status_files: RwLock::new(HashMap::new()),
            shadowed,
        })
    }

    /// The inode table, to map backing files to kernel inodes (for cache
    /// invalidation after replication changed a file).
    pub fn inode_table(&self) -> Arc<inode::InodeTable> {
        Arc::clone(&self.inodes)
    }

    /// Whether a real `.syncfs` in the backing directory is shadowed.
    pub fn shadows_backing_entry(&self) -> bool {
        self.shadowed
    }

    /// Operation counters.
    pub fn stats(&self) -> &Arc<Stats> {
        &self.stats
    }

    /// The `stats` provider: counters plus inode-table gauges.
    pub fn stats_provider(&self) -> Arc<dyn Provider> {
        let stats = Arc::clone(&self.stats);
        let inodes = Arc::clone(&self.inodes);
        Arc::new(FnProvider::new("stats", move || {
            stats.render(serde_json::Map::from_iter([
                ("live_inodes".to_string(), inodes.live_count().into()),
                (
                    "assigned_inodes".to_string(),
                    inodes.assigned_count().into(),
                ),
            ]))
        }))
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

    /// The backing inode. Requests that reach here with a `.syncfs` inode
    /// are changes (reads are served by `virtual_reply!` first): `EROFS`.
    fn inode(&self, ino: INodeNo) -> Res<Arc<Inode>> {
        if virt::is_virtual(ino.0) {
            return Err(Errno::EROFS);
        }
        self.inodes.get(ino.0)
    }

    /// Rejects changes to the name `.syncfs` in the root.
    fn guard(parent: INodeNo, name: &OsStr) -> Res<()> {
        if virt::is_entry(parent.0, name) {
            Err(Errno::EROFS)
        } else {
            Ok(())
        }
    }

    /// Owner of the virtual files: the owner of the backing root.
    fn virt_owner(&self) -> (u32, u32) {
        self.inodes
            .get(ROOT)
            .and_then(|r| Self::stat(&r))
            .map_or((0, 0), |st| (st.st_uid, st.st_gid))
    }

    fn virt_attr(&self, ino: u64) -> Res<fuser::FileAttr> {
        let owner = self.virt_owner();
        match virt::file_index(ino) {
            None => Ok(virt::dir_attr(owner)),
            Some(i) => {
                let data = self.status.render_file(i).ok_or(Errno::ENOENT)?;
                Ok(virt::file_attr(i, data.len() as u64, owner))
            }
        }
    }

    /// Opens a `.syncfs` file: renders a snapshot that every read serves.
    fn virt_open(&self, ino: u64, flags: OpenFlags) -> Res<u64> {
        let i = virt::file_index(ino).ok_or(Errno::EISDIR)?;
        if flags.acc_mode() != OpenAccMode::O_RDONLY || flags.0 & libc::O_TRUNC != 0 {
            return Err(Errno::EROFS);
        }
        let data = self.status.render_file(i).ok_or(Errno::ENOENT)?;
        let fh = self.new_fh();
        self.status_files
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(fh, Arc::new(data));
        Ok(fh)
    }

    fn status_file(&self, fh: FileHandle) -> Option<Arc<Vec<u8>>> {
        self.status_files
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&fh.0)
            .cloned()
    }

    fn virt_lookup(&self, parent: u64, name: &OsStr) -> Res<fuser::FileAttr> {
        if virt::file_index(parent).is_some() {
            return Err(Errno::ENOTDIR);
        }
        match name.as_bytes() {
            b"." => self.virt_attr(virt::DIR_INO),
            b".." => self.inode(INodeNo(ROOT)).and_then(|r| Self::attr(&r)),
            _ => {
                let names = self.status.file_names();
                let i = names
                    .iter()
                    .position(|n| OsStr::new(n) == name)
                    .ok_or(Errno::ENOENT)?;
                self.virt_attr(virt::file_ino(i))
            }
        }
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
        // The kernel's entry for the name is fresh now.
        self.inodes.take_moved_from(parent.ino, name);
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

    /// Runs `apply` between the `before` and `after` (or `failed`) hooks
    /// for `op`. The guard from `before` (an operation lock) is held until
    /// the change is recorded.
    fn mutate<T>(&self, op: Mutation<'_>, apply: impl FnOnce() -> Res<T>) -> Res<T> {
        let _guard = self.hooks.before(&op)?;
        match apply() {
            Ok(v) => {
                self.hooks.after(&op);
                Ok(v)
            }
            Err(e) => {
                self.hooks.failed(&op);
                Err(e)
            }
        }
    }

    fn create_node(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        kind: FileType,
        make: impl FnOnce(&Inode) -> Res<()>,
    ) -> Res<fuser::FileAttr> {
        Self::guard(parent, name)?;
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

    /// Opens an existing file with the caller's `flags` (taking its write
    /// lock if writable; `O_TRUNC` is a change of its own).
    fn open_inode(&self, inode: Arc<Inode>, flags: i32) -> Res<u64> {
        let writable = flags & libc::O_ACCMODE != libc::O_RDONLY;
        if writable {
            self.hooks.open_write(self.node(&inode))?;
        } else {
            self.hooks.open_read(self.node(&inode))?;
        }
        let opened = if flags & libc::O_TRUNC != 0 {
            let op = Mutation::SetAttr {
                node: self.node(&inode),
                change: AttrChange {
                    size: Some(0),
                    ..Default::default()
                },
            };
            self.mutate(op, || Self::open_backing(&inode, flags))
        } else {
            Self::open_backing(&inode, flags)
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
    }

    fn register_file(&self, file: File, inode: Arc<Inode>, writable: bool) -> u64 {
        let fh = self.new_fh();
        self.stats.file_opened(1);
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
            if virt::is_entry(dir.inode.ino, &name) {
                continue; // shadowed by the hidden virtual directory
            }
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
        if virt::is_entry(parent.0, name) {
            self.stats.status_request();
            return match self.virt_attr(virt::DIR_INO) {
                Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
                Err(e) => reply.error(e),
            };
        }
        virtual_reply!(
            self,
            parent.0,
            reply,
            self.virt_lookup(parent.0, name),
            |attr| { reply.entry(&TTL, &attr, Generation(0)) }
        );
        let res = self.inode(parent).and_then(|p| self.do_lookup(&p, name));
        reply!(self, Op::Lookup, reply, res, |(_, attr)| reply.entry(
            &TTL,
            &attr,
            Generation(0)
        ));
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        if virt::is_virtual(ino.0) {
            return;
        }
        self.stats.call(Op::Forget);
        self.inodes.forget(ino.0, nlookup);
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        virtual_reply!(self, ino.0, reply, self.virt_attr(ino.0), |attr| reply
            .attr(&TTL, &attr));
        let res = self.inode(ino).and_then(|i| Self::attr(&i));
        reply!(self, Op::Getattr, reply, res, |attr| reply
            .attr(&TTL, &attr));
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
        reply!(self, Op::Setattr, reply, res, |attr| reply
            .attr(&TTL, &attr));
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        virtual_reply!(self, ino.0, reply, Err::<(), _>(Errno::EINVAL), |()| reply
            .data(&[]));
        let res = self
            .inode(ino)
            .and_then(|i| rustix::fs::readlinkat(&i.fd, "", Vec::new()).map_err(errno));
        reply!(self, Op::Readlink, reply, res, |target| reply
            .data(target.as_bytes()));
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
        reply!(self, Op::Mknod, reply, res, |attr| reply.entry(
            &TTL,
            &attr,
            Generation(0)
        ));
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
        reply!(self, Op::Mkdir, reply, res, |attr| reply.entry(
            &TTL,
            &attr,
            Generation(0)
        ));
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let res = Self::guard(parent, name)
            .and_then(|()| self.inode(parent))
            .and_then(|p| {
                // As in open: the kernel's entry may still name the file a
                // peer renamed away.
                if self.inodes.take_moved_from(p.ino, name) {
                    log::debug!("unlink: {name:?} was renamed away by a peer (ESTALE)");
                    return Err(Errno::ESTALE);
                }
                let op = Mutation::Remove {
                    parent: self.node(&p),
                    name,
                    is_dir: false,
                };
                self.mutate(op, || {
                    rustix::fs::unlinkat(&p.fd, name, AtFlags::empty()).map_err(errno)
                })
            });
        reply!(self, Op::Unlink, reply, res, |()| reply.ok());
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let res = Self::guard(parent, name)
            .and_then(|()| self.inode(parent))
            .and_then(|p| {
                let op = Mutation::Remove {
                    parent: self.node(&p),
                    name,
                    is_dir: true,
                };
                self.mutate(op, || {
                    rustix::fs::unlinkat(&p.fd, name, AtFlags::REMOVEDIR).map_err(errno)
                })
            });
        reply!(self, Op::Rmdir, reply, res, |()| reply.ok());
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
        reply!(self, Op::Symlink, reply, res, |attr| reply.entry(
            &TTL,
            &attr,
            Generation(0)
        ));
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
            Self::guard(parent, name)?;
            Self::guard(newparent, newname)?;
            let p = self.inode(parent)?;
            let np = self.inode(newparent)?;
            // As in unlink: the kernel's entry may still name the file a
            // peer renamed away. Also after a lock wait, if the handoff
            // replaced the source (see `ClusterHooks::before`).
            if self.inodes.take_moved_from(p.ino, name)
                || ((flags.bits() & libc::RENAME_EXCHANGE) != 0
                    && self.inodes.take_moved_from(np.ino, newname))
            {
                log::debug!("rename: {name:?} was renamed away by a peer (ESTALE)");
                return Err(Errno::ESTALE);
            }
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
        reply!(self, Op::Rename, reply, res, |()| reply.ok());
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
            Self::guard(newparent, newname)?;
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
        reply!(self, Op::Link, reply, res, |(_, attr)| reply.entry(
            &TTL,
            &attr,
            Generation(0)
        ));
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        virtual_reply!(self, ino.0, reply, self.virt_open(ino.0, flags), |fh| reply
            .opened(FileHandle(fh), FopenFlags::FOPEN_DIRECT_IO));
        let res = self
            .inode(ino)
            .and_then(|inode| {
                // Replication renamed the file after the kernel looked it
                // up: the kernel may have reached it through its old name.
                // ESTALE makes it look the name up again and retry once.
                if self.inodes.take_moved(inode.ino) {
                    log::debug!("open: inode {} was renamed by a peer (ESTALE)", inode.ino);
                    return Err(Errno::ESTALE);
                }
                Ok(inode)
            })
            .and_then(|inode| self.open_inode(inode, flags.0));
        reply!(self, Op::Open, reply, res, |fh| reply
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
        let snapshot = self.status_file(fh);
        if let Some(data) = snapshot {
            self.stats.status_request();
            let start = (offset as usize).min(data.len());
            let end = start.saturating_add(size as usize).min(data.len());
            return reply.data(&data[start..end]);
        }
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
            self.stats.read_bytes(filled as u64);
            Ok(buf)
        });
        reply!(self, Op::Read, reply, res, |data| reply.data(&data));
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
            })?;
            self.stats.wrote_bytes(data.len() as u64);
            Ok(())
        });
        reply!(self, Op::Write, reply, res, |()| reply
            .written(data.len() as u32));
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        virtual_reply!(self, ino.0, reply, Ok::<(), Errno>(()), |()| reply.ok());
        reply!(self, Op::Flush, reply, self.file(fh), |_| reply.ok());
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if virt::is_virtual(ino.0) {
            self.stats.status_request();
            self.status_files
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&fh.0);
            return reply.ok();
        }
        self.stats.call(Op::Release);
        let removed = self
            .files
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&fh.0);
        if let Some(f) = removed {
            self.stats.file_opened(-1);
            if f.writable {
                self.hooks.release_write(self.node(&f.inode));
            }
        }
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        virtual_reply!(self, ino.0, reply, Ok::<(), Errno>(()), |()| reply.ok());
        let res = self.file(fh).and_then(|f| {
            if datasync {
                f.file.sync_data()
            } else {
                f.file.sync_all()
            }
            .map_err(Errno::from)
        });
        reply!(self, Op::Fsync, reply, res, |()| reply.ok());
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let res = if virt::file_index(ino.0).is_some() {
            Err(Errno::ENOTDIR)
        } else {
            Ok(self.new_fh())
        };
        virtual_reply!(self, ino.0, reply, res, |fh| reply
            .opened(FileHandle(fh), FopenFlags::empty()));
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
            self.stats.dir_opened(1);
            Ok(fh)
        });
        reply!(self, Op::Opendir, reply, res, |fh| reply
            .opened(FileHandle(fh), FopenFlags::empty()));
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        if virt::is_virtual(ino.0) {
            self.stats.status_request();
            let names = self.status.file_names();
            let entries = [
                (virt::DIR_INO, FileType::Directory, OsStr::new(".")),
                (ROOT, FileType::Directory, OsStr::new("..")),
            ]
            .into_iter()
            .chain(
                names
                    .iter()
                    .enumerate()
                    .map(|(i, n)| (virt::file_ino(i), FileType::RegularFile, OsStr::new(n))),
            );
            for (i, (ino, kind, name)) in entries.enumerate().skip(offset as usize) {
                if reply.add(INodeNo(ino), i as u64 + 1, kind, name) {
                    break;
                }
            }
            return reply.ok();
        }
        self.stats.call(Op::Readdir);
        let dir = match self.dir(fh) {
            Ok(d) => d,
            Err(e) => {
                self.stats.error(Op::Readdir);
                return reply.error(e);
            }
        };
        let mut dir = dir.lock().unwrap_or_else(|e| e.into_inner());
        // Offset 0 is the start (or a rewinddir): take a fresh snapshot.
        if offset == 0 {
            if let Err(e) = self.read_dir_entries(&mut dir) {
                self.stats.error(Op::Readdir);
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
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        if virt::is_virtual(ino.0) {
            self.stats.status_request();
            return reply.ok();
        }
        self.stats.call(Op::Releasedir);
        let removed = self
            .dirs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&fh.0);
        if removed.is_some() {
            self.stats.dir_opened(-1);
        }
        reply.ok();
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        virtual_reply!(self, ino.0, reply, Ok::<(), Errno>(()), |()| reply.ok());
        let res = self.dir(fh).and_then(|d| {
            let d = d.lock().unwrap_or_else(|e| e.into_inner());
            rustix::fs::fsync(&d.fd).map_err(errno)
        });
        reply!(self, Op::Fsyncdir, reply, res, |()| reply.ok());
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let res = self
            .inodes
            .get(ROOT)
            .and_then(|r| rustix::fs::fstatvfs(r.fd.as_fd()).map_err(errno));
        reply!(self, Op::Statfs, reply, res, |s| reply.statfs(
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
        reply!(self, Op::Setxattr, reply, res, |()| reply.ok());
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        virtual_reply!(self, ino.0, reply, Err::<(), _>(Errno::NO_XATTR), |()| {
            reply.size(0)
        });
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
        reply!(self, Op::Getxattr, reply, res, |data| reply_xattr(
            reply, size, &data
        ));
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        virtual_reply!(self, ino.0, reply, Ok::<(), Errno>(()), |()| reply_xattr(
            reply,
            size,
            &[]
        ));
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
        reply!(self, Op::Listxattr, reply, res, |data| reply_xattr(
            reply, size, &data
        ));
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
        reply!(self, Op::Removexattr, reply, res, |()| reply.ok());
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
            Self::guard(parent, name)?;
            let p = self.inode(parent)?;
            let writable = flags & libc::O_ACCMODE != libc::O_RDONLY;
            let op = Mutation::Create {
                parent: self.node(&p),
                name,
                kind: FileType::RegularFile,
            };
            let created = self.mutate(op, || {
                // Exclusive: if the name appeared since the kernel looked
                // (a peer's create, applied while we took the lock), the
                // file is opened as an existing one below instead.
                let oflags = OFlags::from_bits_retain(
                    ((flags & !libc::O_NOCTTY)
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_NOFOLLOW
                        | libc::O_CLOEXEC) as u32,
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
            });
            let (inode, attr, file) = match created {
                Err(e) if e == Errno::EEXIST && flags & libc::O_EXCL == 0 => {
                    let (inode, _) = self.do_lookup(&p, name)?;
                    let fh = match self.open_inode(Arc::clone(&inode), flags) {
                        Ok(fh) => fh,
                        Err(e) => {
                            self.inodes.forget(inode.ino, 1);
                            return Err(e);
                        }
                    };
                    return Ok((Self::attr(&inode)?, fh));
                }
                other => other?,
            };
            if writable {
                if let Err(e) = self.hooks.open_write(self.node(&inode)) {
                    self.inodes.forget(inode.ino, 1);
                    return Err(e);
                }
            }
            let fh = self.register_file(file, inode, writable);
            Ok((attr, fh))
        })();
        reply!(self, Op::Create, reply, res, |(attr, fh)| reply.created(
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
        reply!(self, Op::Fallocate, reply, res, |()| reply.ok());
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
        if let Some(data) = self.status_file(fh) {
            self.stats.status_request();
            let len = data.len() as i64;
            return match whence {
                libc::SEEK_DATA if offset < len => reply.offset(offset),
                libc::SEEK_HOLE if offset < len => reply.offset(len),
                libc::SEEK_DATA | libc::SEEK_HOLE => reply.error(Errno::ENXIO),
                _ => reply.error(Errno::EINVAL),
            };
        }
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
        reply!(self, Op::Lseek, reply, res, |off| reply.offset(off));
    }
}
