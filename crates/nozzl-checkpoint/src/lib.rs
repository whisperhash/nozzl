//! Per-(relay, kind) checkpoint state persistence.
//!
//! See `docs/crates/nozzl-checkpoint.md` and `docs/design.md` §5.D8 for
//! the contract and rationale. The on-disk JSON file is the source of truth
//! for crash recovery; an in-memory copy guarded by a `tokio::sync::RwLock`
//! services hot-path reads and writes, and a single dedicated background task
//! flushes dirty state to disk on a configurable interval.
//!
//! Atomic flushes use the standard tempfile-then-rename pattern: write to a
//! same-directory tempfile, fsync, rename to the target, fsync the parent.
//! Readers will only ever see either the previous file or the next one — never
//! a half-written truncation.

mod error;
mod state;
mod store;

pub use error::CheckpointError;
pub use state::{BackfillState, SCHEMA_VERSION};
pub use store::{CheckpointStore, FileCheckpointStore};
