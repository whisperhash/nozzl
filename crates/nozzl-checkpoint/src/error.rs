//! Error types for the checkpoint store.

use std::path::PathBuf;

/// Errors that can occur loading, persisting, or shutting down a checkpoint store.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// I/O failure reading or writing the checkpoint file.
    #[error("checkpoint I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The on-disk checkpoint file could not be parsed as JSON. The file is
    /// **not** auto-recovered — operators must inspect and repair it.
    #[error("checkpoint file at {path} is corrupt: {source}")]
    Corrupt {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    /// The on-disk file declared a `schema_version` we don't understand.
    #[error(
        "checkpoint file at {path} declares unsupported schema_version {found} (expected {expected})"
    )]
    UnsupportedSchema {
        path: PathBuf,
        found: u32,
        expected: u32,
    },

    /// Failure serializing in-memory state to JSON. Should not happen in
    /// practice — the structures are all serde-friendly — but surfaced rather
    /// than panicked on for robustness.
    #[error("failed to serialize checkpoint state: {0}")]
    Serialize(#[source] serde_json::Error),
}
