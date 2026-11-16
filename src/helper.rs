//! `mount.syncfs`: the mount(8) helper interface.
//!
//! mount(8) runs `/sbin/mount.syncfs SOURCE MOUNTPOINT [-sfnvrw] [-N ns]
//! [-o OPTIONS] [-t TYPE]` for fstab entries of type `syncfs`. SOURCE is the
//! backing directory; a SOURCE without a `/` (e.g. `syncfs`) is just a label
//! and the backing directory comes from the config file or `-o
//! backing_dir=`.

use std::ffi::OsString;

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
