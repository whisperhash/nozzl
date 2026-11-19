//! Strongly-typed schema for `nozzl.toml`, `relays.toml`, `kinds.toml`.
//!
//! Defaults match `docs/design.md §5.D9`. Validation is performed by
//! [`Config::validate`] after deserialization.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::ConfigError;

/// The complete, resolved configuration: `nozzl.toml` + the contents of the
/// `relays.toml` and `kinds.toml` files it points to.
///
/// `relays` and `kinds` are resolved by [`crate::load`]; they are not
/// present in `nozzl.toml` itself.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    #[serde(default)]
    pub acquirer: AcquirerConfig,
    pub checkpoint: CheckpointConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub storage_metrics: StorageMetricsConfig,
    pub sources: SourcesConfig,

    /// Resolved from `relays.toml`.
    #[serde(skip)]
    pub relays: Vec<RelayEntry>,

    /// Resolved from `kinds.toml`.
    #[serde(skip)]
    pub kinds: Vec<KindEntry>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub root: Option<PathBuf>,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub region: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    Local,
    S3,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AcquirerConfig {
    #[serde(default = "defaults::backfill_safety_margin_seconds")]
    pub backfill_safety_margin_seconds: u64,
    #[serde(default = "defaults::backfill_default_limit")]
    pub backfill_default_limit: u32,
    /// Number of consecutive zero-event backfill pages required before a
    /// relay's history is declared complete. Larger values tolerate
    /// transient empty bands (rate-limit hiccups, brief shard misses,
    /// per-kind retention edges) at the cost of one extra round trip per
    /// step. Reset to 0 by any non-empty page.
    #[serde(default = "defaults::backfill_empty_pages_to_complete")]
    pub backfill_empty_pages_to_complete: u32,
    /// How long a forward subscription may go without delivering any event
    /// before the acquirer fires a subscription-dispatch liveness probe at
    /// the relay (an NDK-style one-shot `REQ` whose EOSE / EVENT / CLOSED
    /// proves the relay's subscription-dispatch path is working). The
    /// probe replaces the prior asymmetric "relay alive but sub silent"
    /// re-issue watchdog so the failure mode is detected regardless of
    /// whether the relay's non-event traffic is also silent (the case the
    /// old gate missed). Default
    /// 60 s; set to 0 to disable the probe entirely (kill switch).
    #[serde(default = "defaults::forward_silence_threshold_seconds")]
    pub forward_silence_threshold_seconds: u64,
    /// How long to wait for an issued dispatch-liveness probe to settle
    /// (EOSE / EVENT / CLOSED for the probe `sub_id`) before declaring
    /// the relay's subscription-dispatch dead and forcing a reconnect.
    /// Default 5 s (matches NDK's `keepalive.ts`).
    #[serde(default = "defaults::probe_timeout_seconds")]
    pub probe_timeout_seconds: u64,
    /// Minimum spacing between successive dispatch-liveness probe attempts
    /// against the same relay, anchored on issue time (not settlement).
    /// Prevents a tight probe-timeout → reconnect → re-probe loop and
    /// caps probe rate at `~ 3600 / probe_min_interval_seconds` per hour
    /// per relay. Default 60 s.
    #[serde(default = "defaults::probe_min_interval_seconds")]
    pub probe_min_interval_seconds: u64,
}

impl Default for AcquirerConfig {
    fn default() -> Self {
        Self {
            backfill_safety_margin_seconds: defaults::backfill_safety_margin_seconds(),
            backfill_default_limit: defaults::backfill_default_limit(),
            backfill_empty_pages_to_complete: defaults::backfill_empty_pages_to_complete(),
            forward_silence_threshold_seconds: defaults::forward_silence_threshold_seconds(),
            probe_timeout_seconds: defaults::probe_timeout_seconds(),
            probe_min_interval_seconds: defaults::probe_min_interval_seconds(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CheckpointConfig {
    pub path: PathBuf,
    #[serde(default = "defaults::flush_interval_seconds")]
    pub flush_interval_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "defaults::channel_capacity")]
    pub channel_capacity: usize,
    /// Roll a chunk when the in-memory builder reaches this many *uncompressed*
    /// bytes. The on-disk Parquet file is typically 3-5x smaller after zstd.
    #[serde(default = "defaults::chunk_max_size_bytes")]
    pub chunk_max_size_bytes: usize,
    /// Roll a chunk when the in-memory builder reaches this many rows.
    #[serde(default = "defaults::chunk_max_rows")]
    pub chunk_max_rows: usize,
    /// Roll a chunk this many seconds after the first event was pushed into
    /// an otherwise empty in-memory buffer. The clock tracks the wall-clock
    /// age of the oldest unflushed row, not the time since the last arrival,
    /// so it fires regardless of arrival rate. Set on the first push into
    /// the buffer (the first event since the last successful flush); cleared
    /// on flush.
    #[serde(default = "defaults::chunk_max_buffer_seconds")]
    pub chunk_max_buffer_seconds: u64,
    /// Fraction of `chunk_max_buffer_seconds` over which to spread the
    /// buffer-age deadlines of different writers, subtracted from the budget
    /// per writer so the budget stays an upper bound on buffered-row age.
    ///
    /// A restart sets every writer's age clock within the same second or two,
    /// so without this they all roll together `chunk_max_buffer_seconds` later:
    /// on 2026-10-15 that was 443 flushes in one second on dev, which saturated
    /// the box and failed 178 of the 443 S3 PUTs - #57 (Restart-synchronized
    /// buffer-age flush burst). The offset is a
    /// hash of the writer's (class, kind, shard), so it is the same on every
    /// boot. `0.0` disables it; the accepted range is `0.0..=0.5`.
    /// Restart-required: read once when the writer tasks are built.
    #[serde(default = "defaults::chunk_buffer_age_jitter")]
    pub chunk_buffer_age_jitter: f64,
    /// Process-wide ceiling on chunk flushes (Parquet encode + object-store
    /// PUT) in flight at once, across every roll trigger.
    ///
    /// Bounds the two herds that do happen: the shutdown fan-out of
    /// #26 (Unbounded shutdown-flush fan-out fails many S3 PUTs at once) and
    /// any aligned steady-state trigger, which is #57 (Restart-synchronized
    /// buffer-age flush burst).
    /// Sizing: a shutdown must drain `N` non-empty writers inside the 120 s
    /// shutdown budget, so `N / this * per-flush-latency` has to fit; 32 leaves
    /// ~4x headroom at dev's writer count. Restart-required.
    #[serde(default = "defaults::chunk_flush_concurrency")]
    pub chunk_flush_concurrency: usize,
    /// `nostr-relay-pool` `MemoryDatabase` LRU capacity. The pool deduplicates
    /// `Event` notifications by id within this window; events that fall out of
    /// the LRU and re-arrive will reach the writer (and produce duplicate
    /// rows, deduped at read time via `QUALIFY row_number()`). Default 5M ≈
    /// 600 MB resident.
    #[serde(default = "defaults::pool_dedup_max_events")]
    pub pool_dedup_max_events: usize,
    /// 0 = fsync per WAL append (durable, slower). >0 = batch fsyncs across
    /// the given window in milliseconds (faster, with bounded loss window).
    #[serde(default)]
    pub wal_fsync_batch_ms: u64,
    /// ZSTD compression level for published Parquet chunks (valid range
    /// 1–22). Restart-required: read once when the writer tasks are built.
    ///
    /// Measured against real chunks, for #6 (Parquet writer emits tiny row groups
    /// (2–4 rows) for low-rate shards): on well-packed
    /// chunks (high-volume / backfilled kinds) raising the level from 1 to 9
    /// trims ~6–10% off disk, and 19 ~12–16%, with negligible gain beyond.
    /// It does **not** help low-arrival-rate, cross-version-redundant kinds
    /// like `k=38422` (~1.01× at any level) — that redundancy spans chunk
    /// files and is only recoverable by compaction. Default 9 balances the
    /// packed-kind win against encode CPU during heavy backfills; higher
    /// levels (≥13) enter ZSTD's expensive `btopt`/`btultra` strategies.
    #[serde(default = "defaults::parquet_zstd_level")]
    pub parquet_zstd_level: i32,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            channel_capacity: defaults::channel_capacity(),
            chunk_max_size_bytes: defaults::chunk_max_size_bytes(),
            chunk_max_rows: defaults::chunk_max_rows(),
            chunk_max_buffer_seconds: defaults::chunk_max_buffer_seconds(),
            chunk_buffer_age_jitter: defaults::chunk_buffer_age_jitter(),
            chunk_flush_concurrency: defaults::chunk_flush_concurrency(),
            pool_dedup_max_events: defaults::pool_dedup_max_events(),
            wal_fsync_batch_ms: 0,
            parquet_zstd_level: defaults::parquet_zstd_level(),
        }
    }
}

/// `[storage_metrics]` — the periodic per-kind resident-storage scan that
/// populates the `storage_resident_bytes` and `storage_resident_files` gauges.
/// Read once at startup;
/// changes require a restart (see `warn_on_restart_required`).
#[derive(Clone, Debug, Deserialize)]
pub struct StorageMetricsConfig {
    /// Master switch. When false, no scan loop is spawned and the gauge is
    /// never populated.
    #[serde(default = "defaults::storage_scan_enabled")]
    pub enabled: bool,
    /// How often to walk the object store and recompute resident bytes per
    /// kind. The scan is a full re-derivation, so this also bounds how stale
    /// the gauge can be. Default 300 s.
    #[serde(default = "defaults::storage_scan_interval_seconds")]
    pub scan_interval_seconds: u64,
    /// Maximum number of per-kind listings to run concurrently within one
    /// scan. Bounds I/O pressure on the storage backend. Default 4.
    #[serde(default = "defaults::storage_scan_concurrency")]
    pub scan_concurrency: usize,
}

impl Default for StorageMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::storage_scan_enabled(),
            scan_interval_seconds: defaults::storage_scan_interval_seconds(),
            scan_concurrency: defaults::storage_scan_concurrency(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "defaults::metrics_addr")]
    pub metrics_addr: SocketAddr,
    /// Optional admin/extraction HTTP listener serving `POST /admin/flush/{kind}`
    /// and `GET /export/{kind}`. Absent = disabled (no listener bound). Both
    /// routes are non-destructive and unauthenticated, so expose it only as far as
    /// you trust: bind loopback, or `0.0.0.0` inside a container whose compose
    /// publishes it on the host's loopback. Restart-required: a change does not rebind
    /// a running server on hot-reload; the binary logs a "requires restart" warning.
    #[serde(default)]
    pub admin_addr: Option<SocketAddr>,
    #[serde(default)]
    pub log_format: LogFormat,
    #[serde(default = "defaults::log_level")]
    pub log_level: String,
    /// If set, also write logs to this file path. The file is rotated daily;
    /// rotated files keep the same prefix with a `.YYYY-MM-DD` date suffix
    /// appended (e.g. `nozzl.log.2026-05-02`). Stderr logging is always on
    /// in addition; this layer is purely additive. Absent = stderr only.
    #[serde(default)]
    pub log_file: Option<PathBuf>,
    /// Maximum number of rotated log files to retain. Older files are
    /// automatically deleted by the appender. Only applies when `log_file`
    /// is set.
    ///
    /// - **Omit the key** for indefinite retention — every rotated file is kept
    ///   forever (the appender never prunes). This is the "keep logs
    ///   permanently" setting.
    /// - `Some(n)` with `n >= 1` keeps the `n` most-recent files.
    /// - `Some(0)` is **rejected** by [`Config::validate`]: the rolling
    ///   appender has no clean "keep zero" mode — `max_log_files(0)` underflows
    ///   its prune logic (panics in debug builds, deletes every rotated file in
    ///   release). Omit the key for indefinite retention instead.
    #[serde(default)]
    pub log_file_max_files: Option<usize>,
    /// Path to the `duckdb` CLI used by `GET /export/{kind}`. Absent = `"duckdb"`,
    /// resolved on `$PATH`. Set an absolute path when duckdb isn't on PATH in the
    /// container. Only consulted when `admin_addr` is set.
    #[serde(default)]
    pub duckdb_bin: Option<PathBuf>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_addr: defaults::metrics_addr(),
            admin_addr: None,
            log_format: LogFormat::default(),
            log_level: defaults::log_level(),
            log_file: None,
            // No file sink by default (`log_file: None`); when one is
            // configured without an explicit cap, retention is indefinite.
            log_file_max_files: None,
            duckdb_bin: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    #[default]
    Pretty,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SourcesConfig {
    pub relays_path: PathBuf,
    pub kinds_path: PathBuf,
}

// `deny_unknown_fields` is load-bearing, not hygiene. relays.toml / kinds.toml
// are hot-reloaded by the RUNNING binary, so a file written for a newer nozzl,
// with a NEW scoping field, can reach whatever older binary is still up.
// Without this, serde silently drops the unknown field and the old binary
// applies the config as if the scoping were absent -- e.g. acquiring a
// deliberately pinned kind from every relay, and checkpointing those backfills
// Complete so the new binary never revisits them. Failing instead is safe: the
// watcher logs the error and retains the previous config, and `nozzl --check`
// refuses the file, so a deployment can check a file with the binary that will
// read it before it installs the file. It also turns a misspelled field
// (`only_relay`) from a silent no-scoping into a loud error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RelayEntry {
    pub url: String,
    /// Optional per-relay kind restriction. When present, this relay is only
    /// asked for events matching these kinds (intersected with the global
    /// `kinds.toml` list). When absent, every configured kind is requested
    /// from this relay (the historical default).
    ///
    /// Use case: a relay like `relay.zapstore.dev` only meaningfully hosts a
    /// few app-distribution kinds (30063, 30267, 32267). Requesting other
    /// kinds drags the forward sub's `min(since)` back to whatever
    /// long-stale timestamp the relay happens to have for unrelated kinds,
    /// triggering a duplicate-event storm on every restart.
    ///
    /// Hot-reloadable. `apply_config` diffs each surviving relay's EFFECTIVE
    /// kind set (this field composed with each kind's `only_relays`) and
    /// re-issues the forward sub only on relays whose set changed, backfilling
    /// whatever each newly gained. Validation rejects any kind not in
    /// `kinds.toml` and rejects an empty list (use absence to mean "all
    /// configured kinds").
    #[serde(default)]
    pub only_kinds: Option<Vec<u16>>,
}

// See the note on `RelayEntry`: this is what stops an older binary from
// silently ignoring a newer scoping field during a deploy's build window.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Hash)]
#[serde(deny_unknown_fields)]
pub struct KindEntry {
    pub k: u16,
    /// Required, restart-stable metadata. The slug is **not** on disk — the
    /// layout is `c={class}/k={kind}/...` with no slug — it surfaces only in
    /// metrics labels and operator logs, sourced from this config `kind→slug`
    /// map. Editing it mid-run is honored by acquirer-side metric labels but the
    /// archive-side writer/scan metrics keep the slug captured at writer-spawn
    /// time until restart (a cosmetic, documented v1 limitation; see design.md
    /// §5.D9). Must match `^[a-z0-9][a-z0-9-]*$` and not exceed 63 bytes.
    pub slug: String,

    /// Optional per-kind relay restriction - the **inverse** of
    /// [`RelayEntry::only_kinds`]. When present, this kind is requested only
    /// from these relays. When absent, it is requested from every configured
    /// relay (the historical default), still subject to each relay's own
    /// `only_kinds`. The two compose as an intersection: a kind reaches a relay
    /// only if the relay's `only_kinds` admits the kind AND the kind's
    /// `only_relays` admits the relay.
    ///
    /// Use case: a kind worth acquiring from one dedicated source only.
    /// `only_kinds` cannot express that - it scopes a *relay* to a kind list,
    /// so pinning one kind to one relay with it would mean enumerating every
    /// *other* kind in every *other* relay's allowlist, and re-editing all of
    /// them on every future kind onboarding (a silent-drift tax: forget one and
    /// the new kind is acquired from nowhere). `only_relays` states the intent
    /// once, on the kind that needs it, and leaves the "all relays" default
    /// intact for every other kind.
    ///
    /// Entries must name a configured `[[relay]] url` (compared as parsed
    /// URLs, so a trailing slash does not matter). Validation
    /// rejects an unknown relay, an empty list (use absence to mean "all
    /// configured relays"), duplicates, and - composing both directions - any
    /// kind left with no relay at all, which would otherwise silently configure
    /// a kind that is never acquired.
    ///
    /// Hot-reloadable, like `only_kinds`: `apply_config` diffs each surviving
    /// relay's effective kind set and services the delta (scoped backfill for
    /// a relay that gains this kind, forward-sub re-issue for one that loses
    /// it). A kind added or removed wholesale takes the same path.
    #[serde(default)]
    pub only_relays: Option<Vec<String>>,
}

/// Helper file shapes used to deserialize the relay and kind list files.
#[derive(Debug, Deserialize)]
pub(crate) struct RelaysFile {
    #[serde(default)]
    pub relay: Vec<RelayEntry>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct KindsFile {
    #[serde(default)]
    pub kind: Vec<KindEntry>,
}

mod defaults {
    use std::net::SocketAddr;

    pub(super) fn backfill_safety_margin_seconds() -> u64 {
        60
    }
    pub(super) fn backfill_default_limit() -> u32 {
        500
    }
    pub(super) fn backfill_empty_pages_to_complete() -> u32 {
        8
    }
    pub(super) fn forward_silence_threshold_seconds() -> u64 {
        60
    }
    pub(super) fn probe_timeout_seconds() -> u64 {
        5
    }
    pub(super) fn probe_min_interval_seconds() -> u64 {
        60
    }
    pub(super) fn flush_interval_seconds() -> u64 {
        30
    }
    pub(super) fn channel_capacity() -> usize {
        1024
    }
    pub(super) fn chunk_max_size_bytes() -> usize {
        64 * 1024 * 1024
    }
    pub(super) fn chunk_max_rows() -> usize {
        50_000
    }
    pub(super) fn chunk_buffer_age_jitter() -> f64 {
        0.10
    }
    pub(super) fn chunk_flush_concurrency() -> usize {
        32
    }
    pub(super) fn chunk_max_buffer_seconds() -> u64 {
        3600
    }
    pub(super) fn pool_dedup_max_events() -> usize {
        5_000_000
    }
    pub(super) fn parquet_zstd_level() -> i32 {
        9
    }
    pub(super) fn metrics_addr() -> SocketAddr {
        // Matches the example in design.md §5.D9.
        "0.0.0.0:9090".parse().expect("hardcoded socket addr")
    }
    pub(super) fn log_level() -> String {
        "info".to_string()
    }
    pub(super) fn storage_scan_enabled() -> bool {
        true
    }
    pub(super) fn storage_scan_interval_seconds() -> u64 {
        300
    }
    pub(super) fn storage_scan_concurrency() -> usize {
        4
    }
}

/// Upper bound on `runtime.chunk_flush_concurrency`.
///
/// Not a resource limit, a typo guard: `tokio::sync::Semaphore::new` panics
/// above `usize::MAX >> 3`, and any value far above the modelled one makes the
/// bound decorative while still looking configured.
pub const MAX_CHUNK_FLUSH_CONCURRENCY: usize = 1_024;

impl Config {
    /// Validate cross-field invariants. Run after deserialization and after
    /// the relays/kinds vectors have been populated.
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        // Storage backend / required field consistency.
        match self.storage.backend {
            StorageBackend::Local => {
                if self.storage.root.is_none() {
                    return Err(ConfigError::validation(
                        "storage.backend = \"local\" requires storage.root",
                    ));
                }
            }
            StorageBackend::S3 => {
                if self.storage.bucket.is_none() {
                    return Err(ConfigError::validation(
                        "storage.backend = \"s3\" requires storage.bucket",
                    ));
                }
                if self.storage.prefix.is_none() {
                    return Err(ConfigError::validation(
                        "storage.backend = \"s3\" requires storage.prefix",
                    ));
                }
                if self.storage.region.is_none() {
                    return Err(ConfigError::validation(
                        "storage.backend = \"s3\" requires storage.region",
                    ));
                }
            }
        }

        // Runtime sanity: zero channel capacity would deadlock; reject at
        // parse time rather than at startup.
        if self.runtime.channel_capacity == 0 {
            return Err(ConfigError::validation(
                "runtime.channel_capacity must be >= 1",
            ));
        }

        // Runtime sanity: ZSTD only defines levels 1..=22. Reject out-of-range
        // at parse time rather than silently clamping at encode time, where a
        // bad value would otherwise fall back to the default per chunk.
        if !(1..=22).contains(&self.runtime.parquet_zstd_level) {
            return Err(ConfigError::validation(format!(
                "runtime.parquet_zstd_level must be in 1..=22, got {}",
                self.runtime.parquet_zstd_level
            )));
        }

        // Runtime sanity: a jitter fraction outside 0.0..=0.5 is a typo, not an
        // intent. Rejected rather than clamped so the operator finds out at
        // parse time instead of wondering why 5.0 behaved like 0.5. The upper
        // bound keeps "age <= chunk_max_buffer_seconds" a meaningful promise;
        // spreading over a whole period is a different mechanism - see
        // #57 (Restart-synchronized buffer-age flush burst).
        if !(0.0..=0.5).contains(&self.runtime.chunk_buffer_age_jitter)
            || !self.runtime.chunk_buffer_age_jitter.is_finite()
        {
            return Err(ConfigError::validation(format!(
                "runtime.chunk_buffer_age_jitter must be in 0.0..=0.5, got {}",
                self.runtime.chunk_buffer_age_jitter
            )));
        }

        // Runtime sanity: the permit count is bounded on BOTH sides.
        //
        // Zero would park every chunk flush forever (the archive clamps to 1
        // defensively, but a config that says 0 means something went wrong
        // upstream of here). The upper bound matters for two reasons: a value
        // above `usize::MAX >> 3` panics inside `tokio::sync::Semaphore::new`,
        // which would abort the process at boot rather than fail here with a
        // message; and, long before that, a large value silently stops
        // bounding anything, which is the exact failure this knob exists to
        // prevent. 1,024 is far above any modelled value (32 at ~1,000
        // writers on a small instance) and still below the point where the
        // bound is decorative.
        if !(1..=MAX_CHUNK_FLUSH_CONCURRENCY).contains(&self.runtime.chunk_flush_concurrency) {
            return Err(ConfigError::validation(format!(
                "runtime.chunk_flush_concurrency must be in 1..={MAX_CHUNK_FLUSH_CONCURRENCY}, got {}",
                self.runtime.chunk_flush_concurrency
            )));
        }

        // Observability sanity: `log_file_max_files = 0` cannot be honored by
        // the rolling appender — `max_log_files(0)` underflows its prune logic
        // (panics in debug, deletes every rotated file in release). Omit the
        // key entirely for indefinite retention; use >= 1 to cap.
        if self.observability.log_file_max_files == Some(0) {
            return Err(ConfigError::validation(
                "observability.log_file_max_files must be >= 1; omit it entirely for indefinite retention",
            ));
        }

        // Acquirer sanity: zero would mean backfill never declares complete.
        if self.acquirer.backfill_empty_pages_to_complete == 0 {
            return Err(ConfigError::validation(
                "acquirer.backfill_empty_pages_to_complete must be >= 1",
            ));
        }

        // Storage-metrics sanity: a zero interval would busy-loop the scan and
        // a zero concurrency would make no progress. Only meaningful when the
        // scan is enabled.
        if self.storage_metrics.enabled {
            if self.storage_metrics.scan_interval_seconds == 0 {
                return Err(ConfigError::validation(
                    "storage_metrics.scan_interval_seconds must be >= 1",
                ));
            }
            if self.storage_metrics.scan_concurrency == 0 {
                return Err(ConfigError::validation(
                    "storage_metrics.scan_concurrency must be >= 1",
                ));
            }
        }

        // Relays / kinds are mandatory in v1.
        if self.relays.is_empty() {
            return Err(ConfigError::validation("at least one relay is required"));
        }
        if self.kinds.is_empty() {
            return Err(ConfigError::validation("at least one kind is required"));
        }

        // Relay URLs must be syntactically valid ws/wss URLs. The acquirer
        // does its own canonicalization downstream; we only screen out
        // typos here.
        //
        // `only_kinds` (if present) must be non-empty (use absence for
        // "all kinds"), contain no duplicates, and reference only kinds
        // present in `kinds.toml`. The configured-kinds set is built once
        // outside the loop so the per-relay check is O(|only_kinds|).
        let configured_kinds: std::collections::HashSet<u16> =
            self.kinds.iter().map(|k| k.k).collect();
        // Raw config URL strings, for validating each kind's `only_relays`
        // below. Compared verbatim (not canonicalized) so the error message can
        // point at the exact literal the operator typed; the acquirer-side map
        // parses both sides through `RelayUrl` so normalization is consistent
        // there.
        // Normalized, not verbatim: `wss://host` and `wss://host/` are the same
        // relay, and the acquirer-side map keys on the parsed form, so
        // comparing raw strings here would hard-fail a load over a trailing
        // slash. Entries that fail to parse are caught by the relay-URL check
        // in the loop below (for relays) or reported as unknown (for kinds).
        let configured_relays: std::collections::HashSet<String> = self
            .relays
            .iter()
            .filter_map(|r| url::Url::parse(&r.url).ok().map(|u| u.to_string()))
            .collect();
        for relay in &self.relays {
            let parsed = url::Url::parse(&relay.url).map_err(|e| {
                ConfigError::validation(format!(
                    "relay url {:?} is not a valid URL: {e}",
                    relay.url
                ))
            })?;
            match parsed.scheme() {
                "ws" | "wss" => {}
                other => {
                    return Err(ConfigError::validation(format!(
                        "relay url {:?} has scheme {other:?}, expected ws or wss",
                        relay.url
                    )));
                }
            }
            if let Some(only) = &relay.only_kinds {
                if only.is_empty() {
                    return Err(ConfigError::validation(format!(
                        "relay {:?} has empty only_kinds; \
                         use absence (omit the field) to mean \"all configured kinds\"",
                        relay.url
                    )));
                }
                let mut seen: std::collections::HashSet<u16> = std::collections::HashSet::new();
                for k in only {
                    if !seen.insert(*k) {
                        return Err(ConfigError::validation(format!(
                            "relay {:?} has duplicate kind {} in only_kinds",
                            relay.url, k
                        )));
                    }
                    if !configured_kinds.contains(k) {
                        return Err(ConfigError::validation(format!(
                            "relay {:?} has only_kinds entry {} that is not in kinds.toml",
                            relay.url, k
                        )));
                    }
                }
            }
        }

        // Kinds must be archivable per NIP-01 — replaceable, addressable, or
        // regular. The only excluded class is ephemeral ([20000, 29999]),
        // which relays don't persist (so backfill is pointless and forward
        // capture is lossy-by-design). The acquirer rejects ephemeral
        // defensively at runtime; we refuse to even start with one configured.
        let mut seen_k: std::collections::HashSet<u16> = std::collections::HashSet::new();
        let mut seen_slug: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for kind in &self.kinds {
            if !is_archivable_kind(kind.k) {
                return Err(ConfigError::validation(format!(
                    "kind {} is ephemeral (20000-29999) and is not archived; \
                     only replaceable, addressable, and regular kinds are valid \
                     for archival",
                    kind.k
                )));
            }
            if !is_valid_slug(&kind.slug) {
                return Err(ConfigError::validation(format!(
                    "kind {} has invalid slug {:?}; slugs must match \
                     ^[a-z0-9][a-z0-9-]*$ and be ≤ 63 bytes",
                    kind.k, kind.slug
                )));
            }
            if !seen_k.insert(kind.k) {
                return Err(ConfigError::validation(format!(
                    "duplicate kind entry: k = {}",
                    kind.k
                )));
            }
            if !seen_slug.insert(kind.slug.as_str()) {
                return Err(ConfigError::validation(format!(
                    "duplicate slug {:?} (kind {})",
                    kind.slug, kind.k
                )));
            }
            if let Some(only) = &kind.only_relays {
                if only.is_empty() {
                    return Err(ConfigError::validation(format!(
                        "kind {} has empty only_relays; \
                         use absence (omit the field) to mean \"all configured relays\"",
                        kind.k
                    )));
                }
                let mut seen_url: std::collections::HashSet<&str> =
                    std::collections::HashSet::new();
                for url in only {
                    if !seen_url.insert(url.as_str()) {
                        return Err(ConfigError::validation(format!(
                            "kind {} has duplicate relay {:?} in only_relays",
                            kind.k, url
                        )));
                    }
                    let normalized = url::Url::parse(url).map(|u| u.to_string());
                    if !normalized
                        .as_ref()
                        .is_ok_and(|n| configured_relays.contains(n))
                    {
                        return Err(ConfigError::validation(format!(
                            "kind {} has only_relays entry {:?} that is not a \
                             configured relay in relays.toml",
                            kind.k, url
                        )));
                    }
                }
            }
        }

        // Compose both scoping directions and refuse to start with a kind that
        // no relay would ever be asked for. Without this, a typo in a relay's
        // `only_kinds` or a kind's `only_relays` yields a configured kind that
        // is silently never acquired -- no error, no warning, no data. This is
        // the guard that makes per-kind scoping safe to use routinely.
        for kind in &self.kinds {
            let reached = self.relays.iter().any(|relay| {
                let relay_admits = relay
                    .only_kinds
                    .as_ref()
                    .is_none_or(|only| only.contains(&kind.k));
                let kind_admits = kind.only_relays.as_ref().is_none_or(|only| {
                    let this = url::Url::parse(&relay.url).map(|u| u.to_string());
                    only.iter().any(|u| {
                        url::Url::parse(u).map(|p| p.to_string()).ok() == this.clone().ok()
                    })
                });
                relay_admits && kind_admits
            });
            if !reached {
                return Err(ConfigError::validation(format!(
                    "kind {} is configured but reaches no relay: every relay is \
                     excluded by the kind's only_relays or excludes the kind via \
                     its own only_kinds. It would never be acquired.",
                    kind.k
                )));
            }
        }

        Ok(())
    }
}

/// Slug validation: lowercase ASCII, digits, and dashes; must start with an
/// alphanumeric; ≤ 63 bytes (kept short to stay legible as a metric label and
/// log field — the slug is no longer a path component).
fn is_valid_slug(s: &str) -> bool {
    if s.is_empty() || s.len() > 63 {
        return false;
    }
    let mut chars = s.chars();
    let first = chars.next().expect("non-empty checked above");
    if !first.is_ascii_alphanumeric() || first.is_ascii_uppercase() {
        return false;
    }
    chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '-'))
}

/// Archivable per NIP-01: replaceable, addressable, or regular — i.e. every
/// class except ephemeral ([20000, 29999]). Mirrors the table in design.md §2
/// and the complement of the `Ephemeral` arm in `EventClass::classify`.
fn is_archivable_kind(k: u16) -> bool {
    !(20_000..30_000).contains(&k)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archivable_kinds_table() {
        assert!(is_archivable_kind(0)); // replaceable
        assert!(is_archivable_kind(3)); // replaceable
        assert!(is_archivable_kind(10_002)); // replaceable
        assert!(is_archivable_kind(19_999)); // replaceable
        assert!(is_archivable_kind(30_063)); // addressable
        assert!(is_archivable_kind(39_999)); // addressable
        // Regular kinds are now archivable (e.g. kind-9735 zap receipts).
        assert!(is_archivable_kind(1)); // regular
        assert!(is_archivable_kind(2)); // regular
        assert!(is_archivable_kind(4)); // regular
        assert!(is_archivable_kind(1_000)); // regular
        assert!(is_archivable_kind(9_735)); // regular (zap receipts)
        assert!(is_archivable_kind(9_999)); // regular
        assert!(is_archivable_kind(40_000)); // regular
        assert!(is_archivable_kind(65_535)); // regular (u16::MAX)
        // Ephemeral is the only excluded class.
        assert!(!is_archivable_kind(20_000)); // ephemeral
        assert!(!is_archivable_kind(22_500)); // ephemeral
        assert!(!is_archivable_kind(29_999)); // ephemeral
    }

    #[test]
    fn slug_validation_table() {
        assert!(is_valid_slug("profile-metadata"));
        assert!(is_valid_slug("zapstore-app-releases"));
        assert!(is_valid_slug("a"));
        assert!(is_valid_slug("kind-0"));
        assert!(is_valid_slug("0"));
        assert!(is_valid_slug("k0"));
        assert!(is_valid_slug(&"a".repeat(63)));

        assert!(!is_valid_slug(""));
        assert!(!is_valid_slug("Profile-Metadata")); // uppercase
        assert!(!is_valid_slug("-leading-dash"));
        assert!(!is_valid_slug("trailing space "));
        assert!(!is_valid_slug("with/slash"));
        assert!(!is_valid_slug("emoji-🚀"));
        assert!(!is_valid_slug(&"a".repeat(64))); // too long
    }

    #[test]
    fn defaults_match_design_doc() {
        let acq = AcquirerConfig::default();
        assert_eq!(acq.backfill_safety_margin_seconds, 60);
        assert_eq!(acq.backfill_default_limit, 500);
        assert_eq!(acq.backfill_empty_pages_to_complete, 8);
        assert_eq!(acq.forward_silence_threshold_seconds, 60);
        assert_eq!(acq.probe_timeout_seconds, 5);
        assert_eq!(acq.probe_min_interval_seconds, 60);

        let rt = RuntimeConfig::default();
        assert_eq!(rt.channel_capacity, 1024);
        assert_eq!(rt.chunk_max_rows, 50_000);
        assert_eq!(rt.chunk_max_buffer_seconds, 3600);
        assert!((rt.chunk_buffer_age_jitter - 0.10).abs() < f64::EPSILON);
        assert_eq!(rt.chunk_flush_concurrency, 32);
        assert_eq!(rt.pool_dedup_max_events, 5_000_000);
        assert_eq!(rt.parquet_zstd_level, 9);

        let obs = ObservabilityConfig::default();
        assert_eq!(obs.metrics_addr, "0.0.0.0:9090".parse().unwrap());
        assert!(matches!(obs.log_format, LogFormat::Pretty));
        assert_eq!(obs.log_level, "info");

        let sm = StorageMetricsConfig::default();
        assert!(sm.enabled);
        assert_eq!(sm.scan_interval_seconds, 300);
        assert_eq!(sm.scan_concurrency, 4);
    }
}
