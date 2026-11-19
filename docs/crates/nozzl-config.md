# nozzl-config - crate brief

## Purpose

Loads and watches the three TOML configuration files (`nozzl.toml`, `relays.toml`, `kinds.toml`). Emits structured config snapshots to the rest of the system, including diffs on hot-reload, so the acquirer can apply changes without a restart.

The schema and hot-reload semantics are defined in [../design.md §5.D9](../design.md#d9--configuration-and-hot-reload).

## Dependencies

crates.io:
- `serde` (with `derive`)
- `toml`
- `notify` (file watcher, cross-platform)
- `tracing`
- `thiserror`
- `tokio` (with at minimum `sync`, `fs`)
- `url` (for relay URL validation — match `nozzl-types::RelayUrl` if convenient, but keep this crate independent in v1)

sister crates: none in v1. (We keep `nozzl-config` independent of `nozzl-types` so the config layer is reusable. The binary crate maps `config::RelayUrl` → `nozzl_types::RelayUrl` at startup.)

## Public API contract

```rust
/// The complete, resolved configuration: nozzl.toml + the contents of the
/// relays.toml and kinds.toml files it points to.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub acquirer: AcquirerConfig,
    pub checkpoint: CheckpointConfig,
    pub runtime: RuntimeConfig,
    pub observability: ObservabilityConfig,
    pub sources: SourcesConfig,
    pub relays: Vec<RelayEntry>,    // resolved from relays.toml
    pub kinds: Vec<KindEntry>,      // resolved from kinds.toml
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct StorageConfig {
    pub backend: StorageBackend,    // "local" | "s3"
    pub root: Option<PathBuf>,      // local
    pub bucket: Option<String>,     // s3
    pub prefix: Option<String>,     // s3
    pub region: Option<String>,     // s3
}
#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend { Local, S3 }

#[derive(Clone, Debug, serde::Deserialize)]
pub struct AcquirerConfig {
    pub backfill_safety_margin_seconds: u64,         // default 60
    pub backfill_default_limit: u32,                 // default 500
    pub backfill_empty_pages_to_complete: u32,       // default 8
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct CheckpointConfig {
    pub path: PathBuf,
    pub flush_interval_seconds: u64,            // default 30
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct RuntimeConfig {
    /// Bound on the in-process event channel between the acquirer and the
    /// archive consumer task.
    pub channel_capacity: usize,                    // default 1024

    /// Roll a chunk file once its in-memory Arrow buffer reaches this many
    /// uncompressed bytes. Default 64 MiB.
    pub chunk_max_size_bytes: usize,                // default 67_108_864

    /// Roll a chunk file once its row count hits this threshold.
    pub chunk_max_rows: usize,                      // default 50_000

    /// Roll a chunk file this many seconds after the first push into an
    /// otherwise empty in-memory buffer. Clock is set on the first push
    /// since the last successful flush and cleared on flush — so the
    /// trigger fires unconditionally under sustained traffic. Default 1 h.
    pub chunk_max_buffer_seconds: u64,                  // default 3600

    /// Capacity of the `nostr-relay-pool` MemoryDatabase LRU used for
    /// upstream dedup across all relays. ~600 MB ceiling at default.
    pub pool_dedup_max_events: usize,               // default 5_000_000

    /// Coalesce WAL fsyncs across multiple events arriving within this
    /// window. `0` (the default) means fsync every event individually for
    /// strongest durability. Increase to amortize fsync cost on slow
    /// disks at the cost of a small loss-on-crash window.
    pub wal_fsync_batch_ms: u64,                    // default 0
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ObservabilityConfig {
    pub metrics_addr: SocketAddr,
    pub log_format: LogFormat,                  // "json" | "pretty"
    pub log_level: String,
}
#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat { Json, Pretty }

#[derive(Clone, Debug, serde::Deserialize)]
pub struct SourcesConfig {
    pub relays_path: PathBuf,
    pub kinds_path: PathBuf,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct RelayEntry {
    pub url: String,                            // validated downstream
    /// Optional per-relay kind allowlist. When present, only the intersection
    /// of this list with `kinds.toml` is requested from this relay. When
    /// absent, every configured kind is requested. Validated at load: each
    /// entry must be a kind in `kinds.toml`, no duplicates, non-empty (use
    /// field absence to mean "all kinds"). Changes to this list for an
    /// already-running relay are restart-required; `warn_on_restart_required`
    /// in the binary surfaces the change to the operator.
    #[serde(default)]
    pub only_kinds: Option<Vec<u16>>,
}

#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, Hash)]
pub struct KindEntry {
    pub k: u16,
    pub slug: String,    // required; validated `^[a-z0-9][a-z0-9-]*$`, ≤ 63 bytes
}

/// Parse and validate the three files starting from the path of nozzl.toml.
pub fn load(nozzl_path: &Path) -> Result<Config, ConfigError>;

/// Watch the three files and stream new Config snapshots whenever any of
/// them changes. Bad reloads (parse error, validation failure) are logged
/// at error level and the previous Config is retained — no error is sent
/// over the channel; the consumer always sees a valid config.
pub struct ConfigWatcher {
    rx: mpsc::Receiver<Arc<Config>>,
    _handles: Vec<JoinHandle<()>>,
}

impl ConfigWatcher {
    pub async fn next(&mut self) -> Option<Arc<Config>>;
}

pub fn watch(nozzl_path: &Path) -> Result<(Arc<Config>, ConfigWatcher), ConfigError>;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError { /* file not found, parse error, validation, IO */ }
```

## Acceptance criteria

1. `load(nozzl_path)` parses `nozzl.toml`, then resolves `sources.relays_path` and `sources.kinds_path` (relative paths resolved against the dir of `nozzl_path`) and parses those.
2. Validation: at least one relay; at least one kind; `kind.k` in valid Nostr range; `storage.backend == Local` requires `storage.root`; `storage.backend == S3` requires `bucket`, `prefix`, `region`.
3. `watch` returns the initial config and a watcher. Subsequent file edits emit new `Arc<Config>` snapshots over the channel.
4. **Bad reload** (parse error in any file) is logged at `error` level and the previous config is retained. The watcher does **not** propagate the error to the consumer.
5. Debouncing: file editors often emit multiple `notify` events per save (e.g. write-truncate-write). Debounce all events from any of the three files into a single reload after a 250 ms quiet period.
6. The watcher survives one of the watched files being deleted and recreated (atomic-rename editors).
7. All field defaults match the values shown in [../design.md §5.D9](../design.md#d9--configuration-and-hot-reload).

## Testing

Unit:
- Schema parse round-trips for valid `nozzl.toml`, `relays.toml`, `kinds.toml`.
- Each error path (missing field, wrong type, invalid kind range, empty list) yields a useful `ConfigError`.
- Default values applied correctly when fields are absent.

Integration (`tests/`):
- Write three valid files to a `tempfile::TempDir`, call `watch`, mutate one file, assert a new `Config` arrives.
- Write an invalid file edit, assert no config emission and the prior config is still authoritative.
- Atomic-rename test: `mv tmp.new relays.toml`. Watcher should still pick it up.
- Debounce test: rapid-fire writes within 250 ms should yield exactly one reload.

## Out of scope

- The actual application of config changes to running subsystems. That belongs to the consumer (the binary crate's reload-handler routes changes to acquirer, observability, etc.).
- AWS-specific credential handling. Credentials never enter this config; the AWS SDK reads them from its standard chain.
- Hot-reloadability of fields that aren't designed to be hot-reloadable (storage backend, chunk roll thresholds, `pool_dedup_max_events`). Consumers detect those changes and log "restart required."

## Notes on hot-reload diff

Producing a structured "diff" between two configs is tempting but the consumers (acquirer, etc.) need different views of the same data. Keep this crate simple: emit full `Arc<Config>` snapshots; let consumers diff against their last-applied snapshot. Cheap (`Arc::ptr_eq` short-circuits if unchanged).
