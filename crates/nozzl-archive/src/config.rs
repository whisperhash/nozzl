//! Storage backend configuration consumed by `build_archive`.
//!
//! This crate intentionally owns these structs (rather than reusing
//! `nozzl-config`) so the archive remains usable in isolation — for tests,
//! tools, or callers that configure storage programmatically without going
//! through TOML.

use std::path::PathBuf;

/// Top-level storage configuration. Exactly one of `local` / `s3` should be
/// populated, matching `backend`. `build_archive` enforces this.
#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub local: Option<LocalConfig>,
    pub s3: Option<S3Config>,
}

/// Which backend to instantiate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageBackend {
    Local,
    S3,
}

/// Local-filesystem backend settings.
#[derive(Clone, Debug)]
pub struct LocalConfig {
    /// Filesystem directory under which the archive lives. Must already exist.
    pub root: PathBuf,
}

/// S3 backend settings. Credentials are NOT carried here — the standard AWS
/// credential chain is consulted at request time.
#[derive(Clone, Debug)]
pub struct S3Config {
    pub bucket: String,
    /// Optional prefix applied to every object key. May be empty.
    pub prefix: String,
    pub region: String,
}

/// Chunk-roll thresholds and WAL policy for the chunked archive.
///
/// Defaults match the storage plan:
/// - `chunk_max_size_bytes`: 64 MiB upper bound on the in-memory builder.
///   This is an estimate based on uncompressed string lengths; the on-disk
///   Parquet chunk will typically be 3-5x smaller after zstd.
/// - `chunk_max_rows`: 50,000 rows.
/// - `chunk_max_buffer_seconds`: 3600 (1 h). Roll if the in-memory buffer has
///   been non-empty for this long — measured from the first push into the
///   buffer (the first event since the last successful flush), not from the
///   last arrival. Bounds buffered-row age regardless of arrival rate.
/// - `chunk_buffer_age_jitter`: 0.10. Per-writer fraction subtracted from
///   `chunk_max_buffer_seconds` so the age deadlines of writers that started
///   together do not all come due in the same second - see
///   #57 (Restart-synchronized buffer-age flush burst).
/// - `chunk_flush_concurrency`: 32. Process-wide cap on chunk encodes + PUTs
///   in flight, across every roll trigger.
/// - `wal_fsync_batch_ms`: 0 (fsync per event). >0 enables batched fsync
///   over the given window — trades a small loss window for higher throughput.
/// - `parquet_zstd_level`: 9. ZSTD level (1..=22) for published chunks. Lossless
///   at every level (verified by the round-trip tests in `parquet_writer`), so
///   it only affects on-disk size / encode CPU, never data fidelity.
#[derive(Clone, Copy, Debug)]
pub struct ChunkWriterConfig {
    pub chunk_max_size_bytes: usize,
    pub chunk_max_rows: usize,
    pub chunk_max_buffer_seconds: u64,
    /// Fraction of `chunk_max_buffer_seconds` to spread buffer-age deadlines
    /// over, per writer. See
    /// [`jittered_buffer_budget`](crate::chunk_writer::jittered_buffer_budget)
    /// for the mechanism and the arithmetic behind the 0.10 default. Clamped
    /// to `0.0..=0.5`; `0.0` disables jitter (every writer uses the full
    /// budget, the behaviour before #57 (Restart-synchronized buffer-age
    /// flush burst)).
    pub chunk_buffer_age_jitter: f64,
    /// Process-wide ceiling on how many writers may be inside the
    /// encode-then-PUT step of a flush at once, for *every* roll trigger. See
    /// [`ChunkedArchive::flush_permits`](crate::demux::ChunkedArchive) for why
    /// one shared bound rather than a shutdown-only one. Clamped to `>= 1`.
    pub chunk_flush_concurrency: usize,
    pub wal_fsync_batch_ms: u64,
    pub parquet_zstd_level: i32,
}

impl Default for ChunkWriterConfig {
    fn default() -> Self {
        Self {
            chunk_max_size_bytes: 64 * 1024 * 1024,
            chunk_max_rows: 50_000,
            chunk_max_buffer_seconds: 3600,
            chunk_buffer_age_jitter: 0.10,
            chunk_flush_concurrency: 32,
            wal_fsync_batch_ms: 0,
            parquet_zstd_level: 9,
        }
    }
}
