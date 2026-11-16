//! Configuration: YAML file, `-o` mount options and CLI flags, merged with
//! the precedence built-in defaults < YAML file < `-o` options < CLI flags.

mod partial;
mod types;
mod units;

pub use partial::PartialConfig;
pub use types::{check_host_port, LogLevel, Mode, NodeId, Peer};
pub use units::{ByteSize, HumanDuration};

mod options;
pub use options::{parse_mount_options, MountOptions, OptionError};

use std::path::{Component, Path, PathBuf};

/// Makes `p` absolute against `cwd` and normalizes `.` and `..` lexically
/// (without touching the filesystem, so the path need not exist yet).
pub fn absolutize(p: &Path, cwd: &Path) -> PathBuf {
    let joined = if p.is_absolute() {
        p.to_path_buf()
    } else {
        cwd.join(p)
    };
    let mut out = PathBuf::new();
    for c in joined.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}
