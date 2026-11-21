//! `mount.syncfs`: the mount(8) helper interface.
//!
//! mount(8) runs `/sbin/mount.syncfs SOURCE MOUNTPOINT [-sfnvrw] [-N ns]
//! [-o OPTIONS] [-t TYPE]` for fstab entries of type `syncfs`. SOURCE is the
//! backing directory; a SOURCE without a `/` (e.g. `syncfs`) is just a label
//! and the backing directory comes from the config file or `-o
//! backing_dir=`.
//!
//! `mount -o remount[,OPTIONS] MOUNTPOINT` runs the helper with `remount`
//! among the options; it then asks the running daemon to reload (see
//! [`crate::reload`]) instead of mounting. util-linux 2.41 (Debian 13)
//! passes:
//! - with an fstab entry: `mount.syncfs SOURCE MOUNTPOINT -o
//!   rw,FSTAB_OPTIONS,remount,NEW_OPTIONS`;
//! - without one: `mount.fuse.syncfs SOURCE MOUNTPOINT -o
//!   KERNEL_OPTIONS,remount,NEW_OPTIONS`, where KERNEL_OPTIONS is the
//!   kernel's view of the mount (`rw,nosuid,nodev,relatime,user_id=0,
//!   group_id=0,default_permissions,allow_other`);
//!
//! plus `-f` for `mount -f` and `-s` for `mount -s` (util-linux 2.38,
//! Debian 12, passes the same). Options the kernel already shows for the
//! mount, and FUSE's own (`user_id=` ...), are dropped
//! ([`remount_options`]): they describe the mount as it is and mean
//! nothing to syncfs. A remount never calls mount(2), so the kernel mount
//! is left exactly as it is.

use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::bail;

/// What mount(8) asked for, translated to `syncfs` arguments.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct HelperArgs {
    /// Arguments for the normal `syncfs` command line (without argv0).
    pub args: Vec<OsString>,
    /// `-s`: tolerate unknown options.
    pub sloppy: bool,
    /// `-f`: check everything but do not mount.
    pub fake: bool,
    /// `-v`.
    pub verbose: bool,
    /// `remount` was among the options: the mountpoint to reload.
    pub remount: Option<PathBuf>,
    /// All `-o` groups as given (for [`remount_options`]).
    pub raw_options: Vec<String>,
}

/// Parses the helper command line (without argv0).
pub fn parse(argv: impl IntoIterator<Item = OsString>) -> anyhow::Result<HelperArgs> {
    let mut out = HelperArgs::default();
    let mut positional: Vec<OsString> = Vec::new();
    let mut options: Vec<OsString> = Vec::new();
    let mut it = argv.into_iter();
    while let Some(arg) = it.next() {
        let s = arg.to_string_lossy().into_owned();
        if s == "-o" || s == "-t" || s == "-N" {
            let Some(v) = it.next() else {
                bail!("{s} needs a value");
            };
            match s.as_str() {
                "-o" => options.push(v),
                "-t" => {}
                _ => bail!("mount namespaces (-N) are not supported"),
            }
        } else if let Some(v) = s.strip_prefix("-o").filter(|v| !v.is_empty()) {
            options.push(v.into());
        } else if s.starts_with('-') && s.len() > 1 {
            for c in s[1..].chars() {
                match c {
                    's' => out.sloppy = true,
                    'f' => out.fake = true,
                    'v' => out.verbose = true,
                    'n' | 'w' => {}
                    'r' => options.push("ro".into()),
                    other => bail!("unknown mount helper flag -{other}"),
                }
            }
        } else {
            positional.push(arg);
        }
    }
    let [source, mountpoint] = <[OsString; 2]>::try_from(positional).map_err(|p| {
        anyhow::anyhow!(
            "usage: mount.syncfs SOURCE MOUNTPOINT [-o OPTIONS] (got {} arguments)",
            p.len()
        )
    })?;
    out.raw_options = options
        .iter()
        .map(|o| o.to_string_lossy().into_owned())
        .collect();
    if out
        .raw_options
        .iter()
        .flat_map(|g| g.split(','))
        .any(|o| o.trim() == "remount")
    {
        out.remount = Some(PathBuf::from(&mountpoint));
    }
    if source.as_encoded_bytes().contains(&b'/') {
        out.args.push(source);
        out.args.push(mountpoint);
    } else {
        out.args.push("--mountpoint".into());
        out.args.push(mountpoint);
    }
    for o in options {
        out.args.push("-o".into());
        out.args.push(o);
    }
    Ok(out)
}

/// Options of the kernel's FUSE mount itself, which mount(8) may pass back
/// on a remount.
const FUSE_KERNEL_OPTIONS: &[&str] = &[
    "user_id=",
    "group_id=",
    "rootmode=",
    "fd=",
    "max_read=",
    "blksize=",
    "default_permissions",
];

/// The options a remount asks for: all but `remount` itself, the options
/// that `current` (the kernel's options for the mount) shows, and FUSE's
/// own. Returned as one comma-separated group (empty if none).
///
/// The kernel's view is dropped wherever it appears, as mount(8) versions
/// differ in where they put it; asking for what the mount already has
/// changes nothing anyway.
pub fn remount_options(groups: &[String], current: &HashSet<String>) -> Vec<String> {
    let mut keep = Vec::new();
    for o in groups.iter().flat_map(|g| g.split(',')).map(str::trim) {
        let fuse_own = FUSE_KERNEL_OPTIONS
            .iter()
            .any(|f| o == *f || (f.ends_with('=') && o.starts_with(f)));
        if o.is_empty() || o == "remount" || fuse_own || current.contains(o) {
            continue;
        }
        keep.push(o);
    }
    if keep.is_empty() {
        Vec::new()
    } else {
        vec![keep.join(",")]
    }
}

/// The kernel's options for the mount at `mountpoint` (per-mount and
/// superblock options, from `/proc/self/mountinfo`), empty if it is not
/// mounted.
pub fn kernel_options_of(mountpoint: &Path) -> HashSet<String> {
    let info = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    kernel_options_in(&info, mountpoint)
}

fn kernel_options_in(mountinfo: &str, mountpoint: &Path) -> HashSet<String> {
    let want = mountpoint.to_string_lossy();
    let mut found = HashSet::new();
    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split(' ').collect();
        if fields.len() < 6 || unescape(fields[4]) != want {
            continue;
        }
        // The last match is the top of any stack of mounts.
        found.clear();
        found.extend(fields[5].split(',').map(String::from));
        if let Some(sep) = fields.iter().position(|f| *f == "-") {
            if let Some(sup) = fields.get(sep + 3) {
                found.extend(sup.split(',').map(String::from));
            }
        }
    }
    found
}

/// Undoes mountinfo's octal escapes (`\040` for a space).
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let octal = b.get(i + 1..i + 4).filter(|d| {
            d.iter()
                .all(|c| (b'0'..=b'3').contains(&d[0]) && (b'0'..=b'7').contains(c))
        });
        if let (b'\\', Some(d)) = (b[i], octal) {
            out.push((d[0] - b'0') * 64 + (d[1] - b'0') * 8 + (d[2] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// True if the program was invoked as a mount helper.
pub fn invoked_as_helper(argv0: &std::ffi::OsStr) -> bool {
    std::path::Path::new(argv0)
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n == "mount.syncfs" || n == "mount.fuse.syncfs")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(args: &[&str]) -> anyhow::Result<HelperArgs> {
        parse(args.iter().map(OsString::from))
    }

    fn strs(h: &HelperArgs) -> Vec<String> {
        h.args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn typical_fstab_invocation() {
        let h = p(&[
            "/srv/data",
            "/mnt/sync",
            "-o",
            "rw,node_id=1,_netdev,allow_other",
        ])
        .unwrap();
        assert_eq!(
            strs(&h),
            [
                "/srv/data",
                "/mnt/sync",
                "-o",
                "rw,node_id=1,_netdev,allow_other"
            ]
        );
        assert!(!h.sloppy && !h.fake);
    }

    #[test]
    fn flags_and_type() {
        let h = p(&[
            "-sfnv",
            "/srv/data",
            "/mnt/sync",
            "-t",
            "syncfs",
            "-onode_id=2",
        ])
        .unwrap();
        assert!(h.sloppy && h.fake && h.verbose);
        assert_eq!(strs(&h), ["/srv/data", "/mnt/sync", "-o", "node_id=2"]);
    }

    #[test]
    fn label_source_uses_config_backing_dir() {
        let h = p(&["syncfs", "/mnt/sync", "-o", "config=/etc/x.yaml"]).unwrap();
        assert_eq!(
            strs(&h),
            ["--mountpoint", "/mnt/sync", "-o", "config=/etc/x.yaml"]
        );
    }

    #[test]
    fn remount_is_detected() {
        let h = p(&["/srv/data", "/mnt/sync", "-o", "rw,remount,lock_wait=5s"]).unwrap();
        assert_eq!(h.remount, Some(PathBuf::from("/mnt/sync")));
        assert_eq!(h.raw_options, ["rw,remount,lock_wait=5s"]);
        let h = p(&["/srv/data", "/mnt/sync", "-o", "rw,lock_wait=5s"]).unwrap();
        assert_eq!(h.remount, None);
    }

    fn set(s: &[&str]) -> HashSet<String> {
        s.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn remount_options_drop_the_kernel_view() {
        let kernel = set(&[
            "rw",
            "nosuid",
            "nodev",
            "relatime",
            "user_id=0",
            "group_id=0",
            "default_permissions",
            "allow_other",
        ]);
        // No fstab entry: the kernel's options, then remount, then new.
        let groups = ["rw,nosuid,nodev,relatime,user_id=0,group_id=0,\
                       default_permissions,allow_other,remount,lock_wait=20s"
            .replace(' ', "")];
        assert_eq!(remount_options(&groups, &kernel), ["lock_wait=20s"]);
        // An fstab entry: its options, then remount, then new.
        let groups = [
            "rw,state_dir=/s,lock_wait=5s,allow_other,_netdev,remount,lock_wait=20s".to_string(),
        ];
        assert_eq!(
            remount_options(&groups, &kernel),
            ["state_dir=/s,lock_wait=5s,_netdev,lock_wait=20s"]
        );
        // The kernel's view after remount, or not found in mountinfo at
        // all: FUSE's own options still go.
        let groups =
            ["remount,lock_wait=1s,rw,user_id=0,group_id=0,default_permissions".to_string()];
        assert_eq!(remount_options(&groups, &kernel), ["lock_wait=1s"]);
        assert_eq!(
            remount_options(&groups, &HashSet::new()),
            ["lock_wait=1s,rw"]
        );
        // What the mount does not have yet is kept (and judged by the
        // daemon).
        let groups = ["rw,remount,allow_root,noexec".to_string()];
        assert_eq!(remount_options(&groups, &kernel), ["allow_root,noexec"]);
        let groups = ["rw,nosuid,remount".to_string()];
        assert!(remount_options(&groups, &kernel).is_empty());
    }

    #[test]
    fn kernel_options_from_mountinfo() {
        let info = "\
36 25 0:32 / /mnt/a rw,relatime shared:1 - ext4 /dev/sda1 rw
40 25 0:45 / /mnt/my\\040sync rw,nosuid,nodev,relatime shared:2 - fuse.syncfs /srv/b rw,user_id=0,group_id=0,default_permissions,allow_other
41 25 0:46 / /mnt/other rw shared:3 - tmpfs tmpfs rw
";
        let got = kernel_options_in(info, Path::new("/mnt/my sync"));
        assert_eq!(
            got,
            set(&[
                "rw",
                "nosuid",
                "nodev",
                "relatime",
                "user_id=0",
                "group_id=0",
                "default_permissions",
                "allow_other"
            ])
        );
        assert!(kernel_options_in(info, Path::new("/mnt/none")).is_empty());
    }

    #[test]
    fn errors() {
        assert!(p(&["/srv/data"]).is_err());
        assert!(p(&["/a", "/b", "/c"]).is_err());
        assert!(p(&["/a", "/b", "-o"]).is_err());
        assert!(p(&["/a", "/b", "-N", "ns"]).is_err());
        assert!(p(&["/a", "/b", "-x"]).is_err());
        let h = p(&["-r", "/a", "/b"]).unwrap();
        assert_eq!(strs(&h), ["/a", "/b", "-o", "ro"]);
    }

    #[test]
    fn argv0_detection() {
        assert!(invoked_as_helper("/sbin/mount.syncfs".as_ref()));
        assert!(invoked_as_helper("mount.fuse.syncfs".as_ref()));
        assert!(!invoked_as_helper("/usr/bin/syncfs".as_ref()));
    }
}
