//! Chunked Parquet archive, path encoding, and event write API.
//!
//! See `docs/crates/nozzl-archive.md` for the contract and
//! `docs/design.md` §3.D3 / §6 for the storage layout and d-tag encoding.
//!
//! # Overview
//!
//! - [`Archive`] is the write-only trait; [`ChunkedArchive`] is the default
//!   implementation. It routes each event to a per-`(class, kind, shard)`
//!   writer task that owns a local-disk WAL (the durability boundary — an
//!   event is only acknowledged after fsync) plus an in-memory Arrow batch
//!   builder, then publishes Parquet chunks on size/row/idle rolls.
//! - [`build_archive`] constructs a `ChunkedArchive` from a [`StorageConfig`].
//! - [`event_path`] is a pure function deriving the archive path of an event.
//!
//! The submodules [`wal`], [`parquet_writer`], [`chunk_writer`], [`demux`],
//! and [`reader`] back this stack: WAL framing, Arrow→Parquet encoding,
//! per-shard writer-task lifecycle, the demux that fans events out to those
//! tasks, and a small reader used by tests (production reads go through
//! `DuckDB` / Athena / polars over the published chunk tree).

#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    // Panics in this crate are internal-invariant violations (poisoned locks,
    // payload-size sanity checks). Per-function `# Panics` sections would be noise.
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

mod archive;
pub mod chunk_writer;
mod config;
pub mod demux;
mod error;
mod parquet_writer;
mod path_enc;
pub mod reader;
pub mod wal;

pub use chunk_writer::ShardFlushOutcome;
pub use demux::{ChunkedArchive, FlushControl, FlushSummary, KindResidentUsage, ShardError};

pub use archive::{Archive, build_archive, build_object_store};
pub use config::{ChunkWriterConfig, LocalConfig, S3Config, StorageBackend, StorageConfig};
pub use error::{ArchiveError, PutOutcome, SkipReason};
pub use path_enc::{
    chunk_path, class_dir_segment, date_from_unix_secs, encode_d_tag, event_path,
    parse_class_segment, parse_kind_segment, wal_path, PubkeyShard,
};
