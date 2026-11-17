//! Asynchronous replication: file identities, versions, the persisted
//! dirty-block bitmaps and metadata journal, block transfer and the
//! freshness check.

pub mod backing;
pub mod dirty;
pub mod files;
pub mod journal;
pub mod marker;
pub mod ranges;
pub mod store;
pub mod version;
