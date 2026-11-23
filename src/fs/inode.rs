//! The inode table.
//!
//! # Inode scheme
//!
//! Every inode the kernel knows about is backed by an `O_PATH` file
//! descriptor on the backing file, so operations follow the file itself
//! rather than a path: a rename in the backing directory (by us, by a peer
//! later on, or by an admin) cannot make an inode point at the wrong file.
//! All operations use `*at()` calls relative to these descriptors or
//! `/proc/self/fd/N`.
//!
//! FUSE inode numbers are our own: `1` is the root and every other backing
//! file gets the next number from a counter the first time it is seen. The
//! number is keyed by the backing file's identity `(st_dev, st_ino)`, so
//! hard links share one FUSE inode, files on different filesystems below
//! the backing directory never collide, and a file keeps its number for the
//! whole mount even after the kernel forgets it (so `stat` and `readdir`
//! agree). Numbers are never reused within a mount, so the generation is
//! always 0.
//!
//! The kernel's lookup count is tracked per inode; when `forget` drops it
//! to zero the descriptor is closed. Only the small number-assignment entry
//! (16 bytes of key plus the number) stays behind.

use std::collections::HashMap;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::{Arc, Mutex, MutexGuard};

use fuser::{Errno, FileType};
use rustix::fs::Stat;

use super::attr::{key, kind_from_mode};

/// At most this many names renamed away are remembered.
const MOVED_FROM_MAX: usize = 4096;

/// FUSE inode number of the root directory.
pub const ROOT: u64 = 1;

pub type Key = (u64, u64);

/// A live inode: a FUSE inode number bound to an `O_PATH` descriptor.
#[derive(Debug)]
pub struct Inode {
    pub ino: u64,
    pub fd: OwnedFd,
    pub kind: FileType,
    pub key: Key,
}

impl Inode {
    /// `/proc/self/fd/N`, which opens or changes the file itself even if it
    /// was renamed.
    pub fn proc_path(&self) -> String {
        format!("/proc/self/fd/{}", self.fd.as_raw_fd())
    }
}

#[derive(Debug)]
struct Live {
    inode: Arc<Inode>,
    nlookup: u64,
    /// Replication renamed the file since: the kernel may still reach it
    /// under its old name.
    moved: bool,
}

#[derive(Debug)]
struct Inner {
    live: HashMap<u64, Live>,
    numbers: HashMap<Key, u64>,
    next: u64,
    /// Names (directory inode, name) replication renamed a file away
    /// from, until the kernel looks them up again.
    moved_from: std::collections::HashSet<(u64, std::ffi::OsString)>,
}

#[derive(Debug)]
pub struct InodeTable {
    inner: Mutex<Inner>,
}

impl InodeTable {
    pub fn new(root_fd: OwnedFd, root_stat: &Stat) -> Self {
        let k = key(root_stat);
        let root = Arc::new(Inode {
            ino: ROOT,
            fd: root_fd,
            kind: FileType::Directory,
            key: k,
        });
        let mut live = HashMap::new();
        // The root is never forgotten.
        live.insert(
            ROOT,
            Live {
                inode: root,
                nlookup: u64::MAX / 2,
                moved: false,
            },
        );
        Self {
            inner: Mutex::new(Inner {
                live,
                numbers: HashMap::from([(k, ROOT)]),
                next: ROOT + 1,
                moved_from: std::collections::HashSet::new(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The live inode `ino`, or `ESTALE` if the kernel already forgot it.
    pub fn get(&self, ino: u64) -> Result<Arc<Inode>, Errno> {
        self.lock()
            .live
            .get(&ino)
            .map(|l| Arc::clone(&l.inode))
            .ok_or(Errno::ESTALE)
    }

    /// The number of the live inode for backing file `st`, if the kernel
    /// knows it.
    pub fn live_number(&self, st: &Stat) -> Option<u64> {
        let inner = self.lock();
        inner
            .numbers
            .get(&key(st))
            .copied()
            .filter(|n| inner.live.contains_key(n))
    }

    /// The inode number for a backing identity, assigning one if new.
    /// Used by `readdir`, which reports numbers without taking a lookup.
    pub fn number_for(&self, k: Key) -> u64 {
        let mut inner = self.lock();
        Self::number(&mut inner, k)
    }

    fn number(inner: &mut Inner, k: Key) -> u64 {
        if let Some(&n) = inner.numbers.get(&k) {
            return n;
        }
        let n = inner.next;
        inner.next += 1;
        inner.numbers.insert(k, n);
        n
    }

    /// Records one kernel lookup of the file `fd` (with stat data `st`)
    /// and returns its inode. If the file is already live, `fd` is dropped
    /// and the existing inode is reused.
    pub fn lookup(&self, fd: OwnedFd, st: &Stat) -> Arc<Inode> {
        let k = key(st);
        let mut inner = self.lock();
        let ino = Self::number(&mut inner, k);
        let live = inner.live.entry(ino).or_insert_with(|| Live {
            inode: Arc::new(Inode {
                ino,
                fd,
                #[allow(clippy::unnecessary_cast)]
                kind: kind_from_mode(st.st_mode as u32),
                key: k,
            }),
            nlookup: 0,
            moved: false,
        });
        live.nlookup += 1;
        Arc::clone(&live.inode)
    }

    /// Drops `n` kernel lookups of `ino`, closing it when none remain.
    pub fn forget(&self, ino: u64, n: u64) {
        let mut inner = self.lock();
        if let Some(live) = inner.live.get_mut(&ino) {
            live.nlookup = live.nlookup.saturating_sub(n);
            if live.nlookup == 0 && ino != ROOT {
                inner.live.remove(&ino);
            }
        }
    }

    /// Replication renamed the file `file` (if the kernel knows it) away
    /// from `name` in the directory `dir`. The kernel may still have the old
    /// name cached (its invalidation is asynchronous), so the next open of
    /// the file, the next unlink of the name, and the next rename of the
    /// name get `ESTALE`
    /// ([`InodeTable::take_moved`], [`InodeTable::take_moved_from`]).
    pub fn mark_moved(&self, file: Option<&Stat>, dir: &Stat, name: &std::ffi::OsStr) {
        let mut inner = self.lock();
        if let Some(n) = file.and_then(|st| inner.numbers.get(&key(st)).copied()) {
            if let Some(l) = inner.live.get_mut(&n) {
                l.moved = true;
            }
        }
        let Some(d) = inner.numbers.get(&key(dir)).copied() else {
            return;
        };
        if inner.live.contains_key(&d) {
            // Bounded: a dropped mark only loses one retry.
            if inner.moved_from.len() >= MOVED_FROM_MAX {
                inner.moved_from.clear();
            }
            inner.moved_from.insert((d, name.to_os_string()));
        }
    }

    /// Whether `ino` was renamed by replication since its last open (and
    /// clears it: the kernel retries an open once, after looking the name
    /// up again).
    pub fn take_moved(&self, ino: u64) -> bool {
        self.lock()
            .live
            .get_mut(&ino)
            .is_some_and(|l| std::mem::take(&mut l.moved))
    }

    /// Whether replication renamed a file away from `name` in `dir` since
    /// the kernel last looked it up (and clears it).
    pub fn take_moved_from(&self, dir: u64, name: &std::ffi::OsStr) -> bool {
        self.lock().moved_from.remove(&(dir, name.to_os_string()))
    }

    /// Number of inodes with an open descriptor.
    pub fn live_count(&self) -> usize {
        self.lock().live.len()
    }

    /// Number of inode numbers assigned so far.
    pub fn assigned_count(&self) -> usize {
        self.lock().numbers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustix::fs::{open, statat, AtFlags, Mode, OFlags, CWD};

    fn path_fd(p: &std::path::Path) -> (OwnedFd, Stat) {
        let fd = open(
            p,
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .unwrap();
        let st = statat(&fd, "", AtFlags::EMPTY_PATH | AtFlags::SYMLINK_NOFOLLOW).unwrap();
        (fd, st)
    }

    #[test]
    fn a_moved_file_and_name_get_one_estale_each() {
        let dir = std::env::temp_dir().join(format!("syncfs-inode-moved-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a"), b"x").unwrap();
        let (rfd, rst) = path_fd(&dir);
        let t = InodeTable::new(rfd, &rst);
        let (fa, sa) = path_fd(&dir.join("a"));
        let a = t.lookup(fa, &sa);
        let name = std::ffi::OsStr::new("a");
        t.mark_moved(Some(&sa), &rst, name);
        assert!(t.take_moved(a.ino));
        assert!(!t.take_moved(a.ino));
        assert!(t.take_moved_from(ROOT, name));
        assert!(!t.take_moved_from(ROOT, name));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn numbering_lookup_and_forget() {
        let dir = std::env::temp_dir().join(format!("syncfs-inode-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a"), b"x").unwrap();
        std::fs::hard_link(dir.join("a"), dir.join("b")).unwrap_or(());
        std::fs::write(dir.join("c"), b"y").unwrap();
        let _ = CWD;

        let (rfd, rst) = path_fd(&dir);
        let t = InodeTable::new(rfd, &rst);
        assert_eq!(t.get(ROOT).unwrap().ino, ROOT);

        let (fa, sa) = path_fd(&dir.join("a"));
        let a = t.lookup(fa, &sa);
        assert_eq!(a.ino, 2);
        // Hard link: same backing identity, same inode, second lookup.
        let (fb, sb) = path_fd(&dir.join("b"));
        assert_eq!(t.lookup(fb, &sb).ino, 2);
        let (fc, sc) = path_fd(&dir.join("c"));
        assert_eq!(t.lookup(fc, &sc).ino, 3);
        assert_eq!(t.live_count(), 3);

        t.forget(2, 1);
        assert!(t.get(2).is_ok());
        t.forget(2, 1);
        assert_eq!(t.get(2).unwrap_err(), Errno::ESTALE);
        // The number survives the forget.
        assert_eq!(t.number_for(key(&sa)), 2);
        let (fa, sa) = path_fd(&dir.join("a"));
        assert_eq!(t.lookup(fa, &sa).ino, 2);

        // The root is never forgotten.
        t.forget(ROOT, u64::MAX);
        assert!(t.get(ROOT).is_ok());
        assert_eq!(t.assigned_count(), 3);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
