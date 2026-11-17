//! Cluster-wide whole-file write locks. See the README ("Write locks") for
//! the algorithm and its safety argument.

pub mod attempt;
pub mod key;
#[cfg(test)]
mod model;
pub mod voter;

pub use key::Key;
pub mod manager;

pub use manager::{Acquired, FileCtx, LockError, LockManager, Settings};
pub mod hooks;

pub use hooks::ClusterHooks;
