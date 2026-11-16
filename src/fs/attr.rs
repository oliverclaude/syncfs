//! Conversions from backing-file `stat` data to FUSE attributes.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{FileAttr, FileType, INodeNo};
use rustix::fs::Stat;

/// File type from the `S_IFMT` bits of a mode.
pub fn kind_from_mode(mode: u32) -> FileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFSOCK => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

fn time(secs: i64, nsecs: u64) -> SystemTime {
    let nsecs = Duration::from_nanos(nsecs.min(999_999_999));
    if secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(secs.unsigned_abs()) + nsecs
    } else {
        UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs()) + nsecs
    }
}

/// Builds the FUSE attributes for `st`, reporting `ino` as the inode number.
/// The `stat` field widths differ between x86_64 and aarch64, hence the
/// casts.
#[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
pub fn to_attr(ino: u64, st: &Stat) -> FileAttr {
    FileAttr {
        ino: INodeNo(ino),
        size: st.st_size.max(0) as u64,
        blocks: st.st_blocks as u64,
        atime: time(st.st_atime as i64, st.st_atime_nsec as u64),
        mtime: time(st.st_mtime as i64, st.st_mtime_nsec as u64),
        ctime: time(st.st_ctime as i64, st.st_ctime_nsec as u64),
        crtime: UNIX_EPOCH,
        kind: kind_from_mode(st.st_mode as u32),
        perm: (st.st_mode as u32 & 0o7777) as u16,
        nlink: st.st_nlink as u32,
        uid: st.st_uid as u32,
        gid: st.st_gid as u32,
        rdev: st.st_rdev as u32,
        blksize: st.st_blksize as u32,
        flags: 0,
    }
}

/// `(st_dev, st_ino)`: the identity of a backing file.
#[allow(clippy::unnecessary_cast)]
pub fn key(st: &Stat) -> (u64, u64) {
    (st.st_dev as u64, st.st_ino as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds() {
        assert_eq!(kind_from_mode(libc::S_IFDIR | 0o755), FileType::Directory);
        assert_eq!(kind_from_mode(libc::S_IFLNK), FileType::Symlink);
        assert_eq!(kind_from_mode(libc::S_IFREG | 0o644), FileType::RegularFile);
        assert_eq!(kind_from_mode(libc::S_IFIFO), FileType::NamedPipe);
    }

    #[test]
    fn times_before_and_after_epoch() {
        assert_eq!(time(1, 5), UNIX_EPOCH + Duration::new(1, 5));
        assert_eq!(time(-1, 0), UNIX_EPOCH - Duration::from_secs(1));
    }
}
