//! syncfs: a replicating FUSE filesystem.
//!
//! See the README for the overall design and build plan.
pub mod cli;
pub mod config;
pub mod fs;
pub mod helper;
pub mod logging;
pub mod mount;
pub mod net;
pub mod status;
