//! The hidden, read-only `.syncfs` directory at the mount root.
//!
//! It lives only in memory: nothing is stored in the backing directory.
//! `lookup(root, ".syncfs")` always resolves here, so a real `.syncfs` in
//! the backing directory is shadowed (and a warning logged at mount). The
//! root listing omits it. Its files are the [`Registry`] entries, rendered
//! when opened and served with `FOPEN_DIRECT_IO`, so `cat` sees the whole
//! snapshot whatever size `stat` reported.
//!
//! Inode numbers: the directory is `1 << 63`, file `i` of
//! [`Registry::file_names`] is `(1 << 63) + 1 + i`. Backing inode numbers
//! come from a counter starting at 2 and never get near.
//!
//! Every change aimed at the directory or its files fails with `EROFS`.
//! Callers without permission (anyone but root, given modes 0555/0444) get
//! `EACCES` from the kernel's `default_permissions` check first.
//!
//! [`Registry`]: crate::status::Registry
//! [`Registry::file_names`]: crate::status::Registry::file_names

use std::ffi::OsStr;
use std::time::SystemTime;

use fuser::{FileAttr, FileType, INodeNo};

/// Name of the directory at the mount root.
pub const NAME: &str = ".syncfs";

/// Inode number of the directory.
pub const DIR_INO: u64 = 1 << 63;

pub fn is_virtual(ino: u64) -> bool {
    ino >= DIR_INO
}

/// Whether `name` in `parent` names the virtual directory.
pub fn is_entry(parent: u64, name: &OsStr) -> bool {
    parent == super::ROOT && name == NAME
}

pub fn file_ino(index: usize) -> u64 {
    DIR_INO + 1 + index as u64
}

pub fn file_index(ino: u64) -> Option<usize> {
    ino.checked_sub(DIR_INO + 1).map(|i| i as usize)
}

fn attr(ino: u64, kind: FileType, perm: u16, size: u64, owner: (u32, u32)) -> FileAttr {
    let now = SystemTime::now();
    FileAttr {
        ino: INodeNo(ino),
        size,
        blocks: size.div_ceil(512),
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind,
        perm,
        nlink: if kind == FileType::Directory { 2 } else { 1 },
        uid: owner.0,
        gid: owner.1,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

pub fn dir_attr(owner: (u32, u32)) -> FileAttr {
    attr(DIR_INO, FileType::Directory, 0o555, 0, owner)
}

pub fn file_attr(index: usize, size: u64, owner: (u32, u32)) -> FileAttr {
    attr(file_ino(index), FileType::RegularFile, 0o444, size, owner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbering() {
        assert!(is_virtual(DIR_INO));
        assert!(is_virtual(file_ino(3)));
        assert!(!is_virtual(super::super::ROOT));
        assert_eq!(file_index(file_ino(3)), Some(3));
        assert_eq!(file_index(DIR_INO), None);
        assert!(is_entry(1, OsStr::new(".syncfs")));
        assert!(!is_entry(2, OsStr::new(".syncfs")));
    }
}
