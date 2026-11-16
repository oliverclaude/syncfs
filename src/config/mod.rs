//! Configuration: YAML file, `-o` mount options and CLI flags, merged with
//! the precedence built-in defaults < YAML file < `-o` options < CLI flags.

mod partial;
mod types;
mod units;

pub use partial::PartialConfig;
pub use types::{check_host_port, LogLevel, Mode, NodeId, Peer};
pub use units::{ByteSize, HumanDuration};
