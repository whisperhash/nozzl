# nozzl-archive - crate brief

## Purpose

The write-side of the archive: takes a `nostr::Event`, demuxes it to a per-(class, kind, shard) writer task that buffers rows into Apache Arrow record batches and rolls them out as **chunked Parquet files** under a hive-partitioned directory tree. A local-disk WAL fronts the columnar buffer so events survive a crash before they reach Parquet. Provides a backend-agnostic `Archive` trait (`ChunkedArchive` is the concrete implementation) so callers don't see the difference between LocalFS and S3.

## Dependencies

crates.io:
- `arrow` / `arrow-array` / `arrow-schema` (universal columnar schema)
- `parquet` (writer with bloom-filter and zstd support)
- `object_store` (S3 backend; local writes go through `tokio::fs` directly so we can fsync)
- `ulid` (chunk filenames)
- `nostr` (for `Event`)
- `ciborium` (length-prefixed CBOR records in the WAL)
- `async-trait`
- `tracing`
- `thiserror`
- `tokio` (with `sync`, `fs`)

sister crates:
- `nozzl-types` (for `EventClass`, `AddressCoord`, `RelayUrl`)

## Public API contract

```rust
#[async_trait::async_trait]
pub trait Archive: Send + Sync {
    /// Demux the event to its per-(class, kind, shard) writer task. Returns
    /// once the event has been accepted by that task's bounded mpsc; the
    /// WAL fsync happens asynchronously inside the writer task.
    async fn put_event(&self, e: &nostr::Event, slug: &str) -> Result<PutOutcome, ArchiveError>;

    /// Replay any pre-existing WAL files into fresh chunk buffers. Called
    /// by the binary at startup, before the event consumer task spawns.
    async fn recover_from_wal(&self, kinds: &[KindEntry]) -> Result<(), ArchiveError>;

    /// Flush every per-shard buffer to a final Parquet chunk and truncate
    /// the WAL. Required for clean exits.
    async fn shutdown(&self) -> Result<(), ArchiveError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutOutcome {
    /// Accepted by the per-shard writer task's mpsc. The WAL fsync and
    /// Parquet roll happen asynchronously after this returns; a WAL-append
    /// failure inside the writer task drops the event silently.
    Written,
    Skipped { reason: SkipReason },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkipReason {
    /// The event's kind is not replaceable or addressable.
    NotReplaceableNorAddressable,
}

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("wal io: {0}")]
    Wal(#[from] std::io::Error),
    #[error("invalid storage config: {0}")]
    Config(String),
}

/// Build a concrete Archive (currently `ChunkedArchive`) from a config.
pub fn build_archive(cfg: &StorageConfig, runtime: &RuntimeConfig) -> Result<Arc<dyn Archive>, ArchiveError>;

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub local: Option<LocalConfig>,
    pub s3: Option<S3Config>,
}
#[derive(Clone, Copy, Debug)]
pub enum StorageBackend { Local, S3 }
#[derive(Clone, Debug)]
pub struct LocalConfig { pub root: PathBuf }
#[derive(Clone, Debug)]
pub struct S3Config { pub bucket: String, pub prefix: String, pub region: String }

/// Public for testing — also available so other code can compute the path
/// without hitting storage. Pure function; no I/O.
pub fn event_path(e: &nostr::Event) -> Option<object_store::path::Path>;
```

## Path derivation (must match design.md §3.D3 and §6 exactly)

For an event:
1. `AddressCoord::from_event(e)?` — if `None`, return `Skipped { NotReplaceableNorAddressable }`.
2. The caller passes a `slug: &str` (the validated slug from `kinds.toml`).
3. Branch (chunk file path; the `events-{ulid}.parquet` filename is assigned by the writer task at roll time, not by `event_path`):
   - `Replaceable { kind, pubkey }` → `c=replaceable/k={kind}/p={shard}/dt={YYYY-MM-DD}/events-{ulid}.parquet`
   - `Addressable { kind, pubkey, d }` → `c=addressable/k={kind}/p={shard}/dt={YYYY-MM-DD}/events-{ulid}.parquet` (the `d` value is stored as a column, not a path segment)
4. `shard = pubkey_hex[0..1]` — a single 1-hex-char shard (16 buckets per kind). Caps the per-kind chunk-writer fanout at a manageable level while preserving useful pubkey locality. Rationale and scale math are in [../design.md §3.D3 → Pubkey sharding](../design.md#pubkey-sharding-pshard).
5. `dt={YYYY-MM-DD}` is the UTC calendar day derived from the event's `created_at`. Pre-1970 / >9999 timestamps clamp to `dt=1970-01-01`.
6. `pubkey_hex` is 64-char lowercase hex (stored as a column in the row, not part of the path).
7. The `d` tag is stored in the `d_tag` column (empty string when absent). Length / encoding fallbacks no longer apply — arbitrary bytes go straight into the column.

## Acceptance criteria

1. `event_path` is deterministic — same event always yields the same `(class, kind, shard, dt)` directory prefix. (The trailing `events-{ulid}.parquet` filename is decided at chunk-roll time and is not part of the pure derivation.)
2. `put_event` returns `Written` once the event has been accepted by the per-shard writer task's bounded mpsc (capacity 1024). The WAL fsync and Parquet roll happen asynchronously inside the writer task. `AlreadyExists` is no longer a normal-path outcome under chunked storage; duplicates are filtered upstream (relay-pool dedup + per-shard buffer dedup) and would only surface on cross-process replay scenarios.
3. The WAL is the durability boundary, but it is *not* synchronous with `put_event`: a saturated shard backpressures the caller via the bounded channel, but a WAL-append failure inside the writer task is logged and the event is dropped (already counted as `events_written_total`). Parquet roll happens asynchronously after WAL fsync. Tightening `put_event` to await the WAL fsync would require an ack channel from the writer task; tracked separately.
4. Events live as **columnar rows in Parquet**, not as raw JSON files. The universal Arrow schema is `id` (binary[32]), `pubkey` (binary[32]), `kind` (uint16), `created_at` (int64 unix-seconds), `content` (utf8), `tags` (list<list<utf8>>), `sig` (binary[64]), `d_tag` (utf8, empty when absent), `ingested_at` (timestamp[ns, UTC]). Files are zstd-compressed, sorted by `(pubkey ASC, created_at DESC)`, and carry bloom filters on `id` and `pubkey`.
5. Chunks roll on whichever boundary is hit first: 64 MiB on disk, 50 000 rows, or `chunk_max_buffer_seconds` (default 1 h) since the first push into an empty buffer. All three are configurable. The buffer-age clock is the *age of the oldest unflushed event*, not the time since the last arrival — so it fires unconditionally under sustained traffic.
6. Path derivation is independent of any global state. `event_path` is a pure function.
7. The `_state/` prefix used by checkpoints and the `_wal/` prefix used by the write-ahead log are **not** consumed by Parquet readers. Production reads under `replaceable/` and `addressable/` only.

## Crash recovery and shutdown

- **WAL.** Each per-(class, kind, shard) writer owns a file under `{root}/_wal/c={class}-k={kind}-p={shard}.wal`. Events are appended (length-prefixed CBOR record per event) and fsynced inside the writer task — *after* `put_event` returns, not before. (`wal_fsync_batch_ms` batching is supported by the WAL primitive but unused by the live writer loop today.) When a chunk rolls successfully, the WAL is truncated.
- **`ChunkedArchive::recover_from_wal(&kinds)`** is called at startup, before the binary spawns its event consumer. For each WAL file under `_wal/`, it replays records into a fresh chunk buffer (which will be flushed once the writer task is up). Idempotent: replaying twice yields the same Parquet rows because the chunk-row dedup tracks `id`s.
- **`ChunkedArchive::shutdown(&self)`** drains every writer task: each one flushes its current buffer to a final `events-{ulid}.parquet`, fsyncs the directory, and truncates its WAL. Required for clean exits; the binary calls it during graceful shutdown with a 120-second budget.

## Reads

Production query workloads (DuckDB, Athena, Polars, Spark, etc.) read the directory tree directly via hive partitioning — `replaceable/k=*/p=*/dt=*/events-*.parquet`. There is no metastore. The crate ships a small `reader.rs` helper used by the integration tests to scan a chunk and assert row contents; it is **not** intended as a production read path.

## Testing

Unit:
- `event_path` for replaceable: kind 0, 3, 10002 each with a fixed pubkey/event_id/created_at — assert exact directory prefix (`c=replaceable/k={kind}/p={shard}/dt={YYYY-MM-DD}/`).
- `event_path` for addressable: kind 30023 with various pubkeys and `created_at` values — assert the directory prefix and that the `d` tag does **not** appear in the path.
- `event_path` returns `None` for kind 1 (regular), kind 22500 (ephemeral), kind 999 (regular).
- Shard derivation: every pubkey hex prefix `0..f` maps to its expected `p={shard}` segment.
- `dt=` clamping: pre-1970 and >9999 `created_at` values clamp to `dt=1970-01-01`.

Integration (`tests/`):
- Against `tempfile::TempDir`-backed `LocalFileSystem`: write 1 000 events spread across kinds, call `shutdown()`, then use `reader.rs` (or DuckDB if available in CI) to scan the resulting chunks and assert every `id` is present exactly once, and rows are sorted by `(pubkey ASC, created_at DESC)` within each chunk.
- Listing test: write 3 versions of one replaceable event (same pubkey, same kind, increasing `created_at`); shutdown; assert all three rows land in the same `(p={shard}, dt=…)` partition.
- WAL crash-recovery: write 500 events, **drop** the archive without calling `shutdown` (simulates SIGKILL after WAL fsync but before chunk roll), build a new `ChunkedArchive` on the same root, call `recover_from_wal`, shutdown cleanly, and verify all 500 events appear in Parquet.
- Roll-boundary tests: drive a single shard past `chunk_max_rows` and verify a new `events-{ulid}.parquet` is started; same for `chunk_max_size_bytes` (force with large `content` payloads); same for `chunk_max_buffer_seconds` under *sustained* traffic — advance virtual time and confirm a roll lands without any quiet window (this is the test that pins the §3.3 fix: an idle-since-last-event trigger would never fire here).
- Flush-failure recovery: wrap the test `ObjectStore` in a fault-injector that fails the first N `put_opts` calls, drive a roll, assert the in-memory builder, `batch_ids` dedup set, and `buffer_started_at` clock are all preserved across the failure, then let the next trigger publish successfully and verify the retry chunk contains every row from the failed batch.
- Schema fidelity: round-trip an event through write → Parquet read; assert every field — including `tags` nested list and `d_tag` — matches.

## Out of scope

- Production read path. The crate writes Parquet; query workloads point DuckDB / Athena / polars at the directory tree directly. The `reader.rs` helper is test-only.
- Compaction or chunk rewriting. Today's chunks are immutable once rolled; a future compactor crate may merge small chunks within a `(class, kind, shard, dt)` partition.
- Layout migrations.

## Notes

The `Archive` trait is intentionally narrow (`put_event` + `recover_from_wal` + `shutdown`) so test doubles stay tiny. The real implementation lives in `chunk_writer.rs` (per-shard writer task), `demux.rs` (the (class, kind, shard) → mpsc routing), `parquet_writer.rs` (Arrow → Parquet roll, including bloom filters and sort), `wal.rs` (append-only durability), and `reader.rs` (test scanner). `object_store` is still used for the eventual S3 backend; for the local backend the writer task writes to the filesystem directly so it can fsync the file and its parent directory.
