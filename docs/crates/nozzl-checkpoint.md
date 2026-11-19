# nozzl-checkpoint - crate brief

## Purpose

Persists per-`(relay, kind)` checkpoint state — the `since` timestamp for forward-subscription resumption, the backfill state machine, and the `until_cursor` for resumable backfills. Owns the single JSON file at `{archive_root}/_state/checkpoints.json` (path configurable).

The schema and rationale are defined in [../design.md §5.D8](../design.md#d8--checkpointing).

## Dependencies

crates.io:
- `serde` (with `derive`)
- `serde_json`
- `tokio` (with `sync`, `fs`, `time`)
- `async-trait`
- `tracing`
- `thiserror`
- `tempfile` (for atomic write)

sister crates:
- `nozzl-types` (for `RelayUrl`, `EventId`, `Timestamp` — define `Timestamp = i64` in `nozzl-types` if not already there).

## Public API contract

```rust
#[async_trait::async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Newest observed created_at for (relay, kind), or None if never seen.
    async fn since(&self, relay: &RelayUrl, kind: u16) -> Option<i64>;

    /// Snapshot of every observed (relay, kind, since) triple. Coordinates
    /// with no observation are omitted. Used by the orchestrator's gauge
    /// loop to drive `checkpoint_lag_seconds`.
    async fn since_snapshot(&self) -> Vec<(RelayUrl, u16, i64)>;

    /// Record an observation. Updates `since` if `created_at` is newer than the
    /// current value; updates `last_event_id`. Idempotent — safe to call from
    /// multiple writer tasks concurrently.
    async fn observe(
        &self,
        relay: &RelayUrl,
        kind: u16,
        created_at: i64,
        event_id: &EventId,
    );

    async fn backfill_state(&self, relay: &RelayUrl, kind: u16) -> BackfillState;
    async fn record_backfill(&self, relay: &RelayUrl, kind: u16, state: BackfillState);

    /// Force an immediate flush (otherwise flushes happen on the configured interval).
    async fn flush(&self) -> Result<(), CheckpointError>;
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BackfillState {
    Pending,
    InProgress { until_cursor: i64 },
    Complete,
}

pub struct FileCheckpointStore { /* … */ }

impl FileCheckpointStore {
    /// Loads existing state from `path`, or starts empty if not present.
    /// Spawns a background flush task that fires every `flush_interval`.
    pub async fn open(path: PathBuf, flush_interval: Duration)
        -> Result<Arc<Self>, CheckpointError>;

    /// Stop the background flush task and flush one last time.
    pub async fn shutdown(self: Arc<Self>) -> Result<(), CheckpointError>;
}

impl CheckpointStore for FileCheckpointStore { /* … */ }

#[derive(Debug, thiserror::Error)]
pub enum CheckpointError { /* IO, parse, schema-mismatch, etc. */ }
```

## On-disk format (schema_version: 1)

Match this exactly. Backwards-compat decisions are explicit changes to `schema_version`.

```json
{
  "schema_version": 1,
  "updated_at": "2026-05-01T19:42:11Z",
  "relays": {
    "wss://relay.damus.io": {
      "kinds": {
        "0":     { "since": 1714579200, "last_event_id": "abc…", "backfill": { "state": "complete" } },
        "10002": { "since": 1714579150, "last_event_id": "ghi…", "backfill": { "state": "in_progress", "until_cursor": 1700000000 } }
      }
    }
  }
}
```

## Acceptance criteria

1. `since` returns the maximum observed `created_at` for the given `(relay, kind)`.
2. `since_snapshot` returns one entry per `(relay, kind)` whose `since` is set; coordinates that have only ever had `record_backfill` calls (and thus `since: None`) are omitted.
3. `observe` updates `since` only when the new `created_at` is strictly greater than the current value. Older or equal observations don't regress state.
4. `observe` is safe to call concurrently from multiple Tokio tasks. Internal locking strategy: a single in-memory state guarded by `tokio::sync::RwLock` is sufficient at v1 scale.
5. **Atomic flush.** Write to `{path}.tmp.{nonce}`, fsync, rename to `{path}`. Never leave a half-written `checkpoints.json`. Verified by a crash-during-write test that kills the flush mid-stream and confirms the previous file is intact.
6. On startup, if the file is missing, treat as empty and begin flushing on the first `observe` after a state mutation.
7. On startup, if the file is corrupted (parse fails), `open` returns `CheckpointError`. Do not auto-recover or auto-truncate; fail loud.
8. On startup, `schema_version` mismatch returns `CheckpointError::UnsupportedSchema`.
9. Background flush task fires every `flush_interval`. If state is unchanged since last flush, no I/O happens (no-op flush).
10. `shutdown` flushes once more and stops the background task cleanly. Idempotent.

## Testing

Unit:
- File-format round-trip (serialize → deserialize → serialize equality).
- `observe` doesn't regress `since` when given older timestamps.
- `since_snapshot` returns observed coordinates only; entries from `record_backfill` alone are excluded.
- Concurrent `observe` from many tasks: final state matches the maximum across all observations (including via `since_snapshot`).
- `BackfillState` serde shape matches the documented JSON.

Integration (`tests/`):
- `open` on a missing file: starts empty, first observation persists across reload.
- `open` on a corrupted file: returns error.
- `open` with `schema_version=2`: returns `UnsupportedSchema`.
- Atomic write under crash: spawn a process, kill it mid-flush, verify the file is either fully old or fully new — never truncated. (Use `std::process::Command` to exec a small helper binary if needed, or simulate by injecting a fault on rename.)
- Flush interval: assert no I/O during idle periods.

## Out of scope

- Anything beyond a local file. AWS-specific checkpoint storage (DynamoDB, versioned object) is a future consideration.
- Compaction / GC of stale (relay, kind) entries when relays are removed from config. Removed relays' state stays in the file harmlessly; cleanup is a v2 concern.
