//! Prometheus metric definitions and the typed [`Metrics`] handle.
//!
//! See `docs/crates/nozzl-metrics.md` for the full contract and
//! `docs/design.md` §5.D10 for the canonical metric list. The HTTP listener
//! that exposes these metrics (`GET /metrics`, `GET /health`) lives in the
//! `nozzl-http` crate; this crate is transport-agnostic.
//!
//! This crate depends only on the shared foundational types
//! (`nozzl-types`) — taking domain types like [`nozzl_types::RelayUrl`] in
//! label-setter signatures is what lets the metric surface *enforce*
//! normalized, validated labels rather than accept free-form strings. It is
//! otherwise independent of every workspace crate (no acquirer / archive /
//! config coupling). Callers obtain a typed [`Metrics`] handle and increment
//! counters / adjust gauges through it; the underlying [`prometheus::Registry`]
//! is also returned so callers can register their own metrics if needed.

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

/// Short git SHA baked in at compile time by `build.rs`.
/// Re-exported so dependent crates (e.g. the startup banner) can log it
/// without needing their own build script.
pub const GIT_SHA: &str = env!("NOZZL_GIT_SHA");

/// Every `action` value of `forward_sub_closed_total`, preallocated per relay so
/// a first increment is visible to `increase()`.
pub const FORWARD_SUB_CLOSED_ACTIONS: [&str; 4] = ["reissue", "split", "floor", "echo"];

/// Every `trigger` value of `chunk_flush_failures_total`, preallocated per
/// (class, kind, slug) so a first increment is visible to `increase()` and an
/// alert can read "zero failures" rather than "no data".
///
/// Mirrors `nozzl_archive`'s `RollReason::as_str`, which cannot be referenced
/// from here (the dependency runs the other way). `nozzl-archive` has a test
/// that fails if the two ever drift.
pub const CHUNK_FLUSH_TRIGGERS: [&str; 7] = [
    "size", "rows", "buffer_age", "force", "adhoc", "shutdown", "replay",
];

use std::time::{SystemTime, UNIX_EPOCH};

use nozzl_types::RelayUrl;
use prometheus::{
    Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry,
};

/// Typed value of the `relay_connection_state` gauge. Encapsulates the
/// `0/1/2` wire encoding so call sites pass an intent-revealing variant
/// instead of a magic integer, and the encoding lives in exactly one place
/// ([`RelayConnState::code`]). Mapping the upstream pool's status enum onto
/// this domain enum happens at the acquirer boundary, where the
/// `nostr-relay-pool` dependency lives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelayConnState {
    /// Not connected (pool `Disconnected` / `Terminated` / `Sleeping`).
    Disconnected,
    /// Connected and usable.
    Connected,
    /// Mid-connect (pool `Connecting` / `Initialized`).
    Connecting,
}

impl RelayConnState {
    /// The gauge wire value. This is the single definition of the `0/1/2`
    /// encoding referenced throughout dashboards and the design doc.
    #[must_use]
    pub fn code(self) -> i64 {
        match self {
            Self::Disconnected => 0,
            Self::Connected => 1,
            Self::Connecting => 2,
        }
    }
}

/// All errors surfaced by this crate.
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    /// A metric failed to register against the supplied registry — typically
    /// a duplicate registration on the same registry instance.
    #[error("failed to register metric `{name}`: {source}")]
    Registration {
        name: &'static str,
        #[source]
        source: prometheus::Error,
    },
}

/// Pre-built, registered handles for every metric documented in design.md
/// §5.D10. Field names are part of the public API — downstream crates depend
/// on them. Do not rename or retype without coordinating across the workspace.
///
/// Every kind-bearing metric also carries a `slug` label sourced from
/// `kinds.toml`. Slug is functionally determined by `kind` (1:1 in the config,
/// validated at load), so the label adds no series cardinality — it's a
/// legibility aid for `/metrics` scraping and dashboards. Empty slug
/// (`slug=""`) is the sentinel used when the kind is not in the configured
/// set (e.g. `events_rejected_total{reason="kind-not-configured"}`); slugs
/// must otherwise be non-empty per `nozzl_config::is_valid_slug`. If an
/// operator edits a slug in `kinds.toml` mid-run (slugs are documented
/// immutable — see `docs/design.md` §3.D9), the same `(relay, kind)` will
/// briefly carry two series until process restart.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Events received from the relay (pre-write). Labels: `relay`, `kind`, `slug`.
    pub events_received: IntCounterVec,
    /// Events accepted into the archive's per-shard writer mpsc (i.e. queued
    /// for WAL fsync + Parquet roll). Counts before the WAL fsync completes;
    /// in normal operation the lag is sub-second. Labels: `relay`, `kind`, `slug`.
    pub events_written: IntCounterVec,
    /// Writes that hit `AlreadyExists` from the object store (dedup).
    /// Labels: `relay`, `kind`, `slug`.
    pub events_deduped: IntCounterVec,
    /// Events rejected before write (signature, kind-not-configured, etc.).
    /// Labels: `relay`, `kind`, `slug`, `reason`. `slug` is `""` for
    /// `reason="kind-not-configured"` since the rejected kind has no slug.
    pub events_rejected: IntCounterVec,
    /// Events dropped after exhausting write retries.
    /// Labels: `relay`, `kind`, `slug`, `reason`.
    pub events_dropped: IntCounterVec,

    /// Per-relay connection state: 0 = disconnected, 1 = connected, 2 = connecting.
    /// Labels: `relay`. Updated on a 1 s tick from the relay task plus a 30 s
    /// heartbeat sweep — the per-relay tick is the fast path that catches
    /// short hourly-cycle reconnects.
    ///
    /// Private on purpose: mutate it only through
    /// [`Metrics::set_relay_connection_state`] and read it through
    /// [`Metrics::relay_connected`], both of which take a [`RelayUrl`] and a
    /// [`RelayConnState`]. This makes a mis-normalized label or a magic status
    /// code unrepresentable at every call site.
    relay_connection_state: IntGaugeVec,
    /// Count of observed transitions out of `Connected` per relay. Each Damus
    /// hourly Cloudflare-edge cutoff produces one increment; same for any
    /// other cause (ping fail, TCP RST, planned shutdown). Labels: `relay`.
    pub relay_disconnects: IntCounterVec,
    /// Seconds from a `Connected → !Connected` transition to the next
    /// `!Connected → Connected` transition for the same relay. Histogram so
    /// dashboards can chart P95 reconnect gap and alerts can fire on
    /// regression. Labels: `relay`. Initial-bring-up does **not** observe —
    /// the histogram only records gap durations that follow an observed
    /// disconnect.
    pub relay_reconnect_duration_seconds: HistogramVec,
    /// Number of REQ subscriptions active per relay (1 normal, higher when
    /// auto-split is in effect). Labels: `relay`.
    pub relay_req_split_count: IntGaugeVec,
    /// 0/1 indicator that an onboarding backfill is currently running.
    /// Labels: `relay`, `kind`, `slug`.
    pub backfill_in_progress: IntGaugeVec,
    /// `now() - checkpoint.since` in seconds, per `(relay, kind)` pair.
    /// Labels: `relay`, `kind`, `slug`. Only emitted for `(relay, kind)`
    /// pairs in the active subscription set (i.e. *not* emitted for kinds
    /// that the global `kinds.toml` configures but a per-relay `only_kinds`
    /// excludes from a given relay).
    pub checkpoint_lag_seconds: GaugeVec,
    /// 0/1 indicator that a given `(relay, kind)` pair is in the active
    /// subscription set. Labels: `relay`, `kind`, `slug`. Set to 1 at
    /// startup for each effective pair (after per-relay `only_kinds`
    /// intersection); restart-required to change.
    ///
    /// Operator-facing tools (a status watcher, for one) can use
    /// the presence of this series as the canonical "is this pair being
    /// archived right now?" signal — stale `(relay, kind)` entries from
    /// prior runs persist in `checkpoints.json` but do not appear here.
    pub kind_subscribed: IntGaugeVec,
    /// In-flight events between acquirer and writer pool.
    pub channel_depth: IntGauge,
    /// Parquet chunks successfully published to the object store. Incremented
    /// in `flush_and_truncate` after the PUT succeeds, before WAL truncate —
    /// so an increment guarantees the chunk is durable in storage, regardless
    /// of whether the subsequent WAL truncate succeeded.
    /// Labels: `class` (replaceable / addressable / regular), `kind`, `slug`,
    /// `shard` (single lowercase hex char, 0–f). `slug` mirrors `kind` per
    /// kinds.toml.
    pub chunks_published: IntCounterVec,
    /// Chunk flushes that failed to publish, by what triggered the roll.
    /// Incremented in `flush_and_truncate` on any error return - a failed
    /// Parquet encode or a failed PUT. A WAL-truncate failure *after* a
    /// successful PUT is not counted: the chunk is durable, and that path logs
    /// its own error.
    ///
    /// Not data loss: the rows stay in the WAL and are republished by the next
    /// roll trigger (steady state) or by replay on the next boot (shutdown).
    /// What it measures is deferred recovery, wasted encode CPU, and the
    /// herd-shaped failures of #26 (Unbounded shutdown-flush fan-out fails
    /// many S3 PUTs at once), #30 (Shutdown flush starves the tokio runtime)
    /// and #57 (Restart-synchronized buffer-age flush burst) - 178 failures in
    /// one
    /// second on 2026-10-15 were invisible to every alert because this counter
    /// did not exist.
    ///
    /// Labels: `class`, `kind`, `slug`, `trigger` (one of
    /// [`CHUNK_FLUSH_TRIGGERS`]). Deliberately *without* `shard`, unlike
    /// `chunks_published_total`: the shard is on the failure's log line, and 16
    /// shards x 7 triggers would cost ~7,600 mostly-zero series.
    ///
    /// The objection recorded in #26 (Unbounded shutdown-flush fan-out fails
    /// many S3 PUTs at once) (a counter that increments
    /// during teardown rarely survives to a scrape) stands for the `shutdown`
    /// trigger, which is why the reconciled `shutdown flush complete` log line
    /// remains the signal there. It does not apply to the other six, which fire
    /// with the process alive and the metrics endpoint up.
    pub chunk_flush_failures: IntCounterVec,
    /// Unix-seconds timestamp of the most recent successful chunk publish, per
    /// writer. Set to `now()` from `flush_and_truncate` right after the
    /// `chunks_published_total` increment, so the two metrics share an
    /// atomic-from-the-outside view. Companion to the counter: dashboards
    /// want "seconds since last publish per writer" — a query like
    /// `time() - timestamp(chunks_published_total)` does NOT produce that,
    /// because `timestamp()` returns the time of the latest scrape sample,
    /// which is ~now regardless of whether the counter incremented this
    /// scrape. Subtracting this gauge gives the actual idle interval.
    /// Labels match `chunks_published_total`.
    pub chunks_last_published_timestamp_seconds: GaugeVec,
    /// Bytes currently resident in the object store per archived kind, set
    /// from a periodic storage scan (see `nozzl::spawn_storage_scan_loop`).
    /// A fresh re-derivation each scan — not an accumulator — so it tracks
    /// deletions and eventual compaction automatically.
    /// Labels: `class` (replaceable / addressable / regular), `kind`, `slug`.
    ///
    /// Deliberately NOT preallocated: a kind appears only once its first scan
    /// finds resident `.parquet`, so the series is absent (never a phantom 0)
    /// until there is real data to report.
    pub storage_resident_bytes: GaugeVec,
    /// Number of resident `.parquet` chunk files in the object store per
    /// archived kind, set from the same periodic storage scan that drives
    /// `storage_resident_bytes` (see `nozzl::spawn_storage_scan_loop`). A fresh
    /// re-derivation each scan — not an accumulator — so it tracks compaction
    /// and deletions automatically. Labels match `storage_resident_bytes`.
    ///
    /// Deliberately NOT preallocated, for the same reason: a kind appears only
    /// once its first scan finds resident `.parquet`.
    pub storage_resident_files: GaugeVec,
    /// Bytes currently occupied on local disk by each writer's `_wal` file, set
    /// from the same periodic scan that drives `storage_resident_bytes` (see
    /// `nozzl::spawn_storage_scan_loop`). A fresh re-derivation each scan — not
    /// an accumulator — measured by `stat`-ing every `c=…-k=…-p=….wal` under the
    /// local WAL root. Unlike the resident-storage gauges this is local-disk
    /// (the WAL is always on disk even when the archive backend is S3) and
    /// per-writer rather than per-kind.
    ///
    /// Because `flush_and_truncate` truncates via `set_len(0)` (it never deletes
    /// the file), a writer's WAL file persists across flushes, so this series
    /// stays present and accurate for the writer's whole life: `0` right after a
    /// flush / while idle, `> 0` while events are buffered. A persistently large
    /// value is the decisive stalled-writer signal (a writer that stopped
    /// publishing but keeps accepting events). NOT preallocated: a `(class,
    /// kind, slug, shard)` series appears once the writer's WAL file first
    /// exists on disk.
    /// Labels: `class` (replaceable / addressable / regular), `kind`, `slug`,
    /// `shard` (single lowercase hex char, 0–f).
    pub wal_bytes: GaugeVec,

    /// Subscription-dispatch liveness probe outcomes per relay. `outcome` is
    /// `"alive"` (probe settled by EOSE / EVENT / CLOSED-for-probe before
    /// timeout) or `"timeout"` (no response within `probe_timeout`, which
    /// triggers a relay reconnect). Labels: `relay`, `outcome`.
    pub probe_total: IntCounterVec,
    /// Probe round-trip time, observed only on the `alive` outcome. Timeouts
    /// have a known value (`probe_timeout`) and observing them would just
    /// pile a single bucket — the histogram is intentionally scoped to
    /// successful probes so dashboards reflect real dispatch latency.
    /// Labels: `relay`.
    pub probe_alive_latency_seconds: HistogramVec,
    /// 1 if a dispatch-liveness probe is currently in flight against this
    /// relay; 0 otherwise. Labels: `relay`.
    pub probe_in_flight: IntGaugeVec,
    /// Outcomes of the teardown barrier that sequences the probe-timeout
    /// reconnect (`disconnect_relay` → await task exit → `connect_relay`;
    /// issue #8). `outcome` is `"cleared"` (barrier observed task exit and
    /// status was connectable), `"status_redrive"` (task exited but teardown
    /// left a non-connectable status — e.g. `Disconnected` from the lib's
    /// `_try_connect` Err-arm overwriting our eager `Terminated` — so we
    /// re-drove through `disconnect_relay` before connecting),
    /// `"timeout_connectable"` (barrier expired with the old task still
    /// running but status connectable — connect issued, the one residual-race
    /// sub-case), `"timeout_task_live"` (barrier expired, task still
    /// running and status not connectable — the task reconnected itself;
    /// connect skipped), or `"fallback_blind"` (relay unexpectedly missing
    /// from the pool — pre-barrier blind re-arm; should never fire).
    /// Labels: `relay`, `outcome`.
    ///
    /// Invariant: `sum by (relay) (teardown_barrier_total)` ==
    /// `probe_total{outcome="timeout"}` — every probe timeout takes exactly
    /// one of these arms. Healthy steady state: only `cleared` (and rarely
    /// `status_redrive`) ever increments. Any `timeout_*` or
    /// `fallback_blind` outcome is worth eyes on the relay's logs.
    pub teardown_barrier_total: IntCounterVec,
    /// Forward subscriptions a relay closed, per relay, by what nozzl did
    /// about it: `reissue` (not a filter rejection: re-opened after a back-off
    /// that doubles per CLOSED), `split` (a rejection: the kind group halved),
    /// `floor` (a rejection of a single-kind group: the kind suspended and
    /// retried later), or `echo` (an empty CLOSED right after nozzl's own CLOSE
    /// of that id, taken as the relay acknowledging it and kept pending an
    /// EOSE; an unconfirmed one also counts as `reissue`). Labels: `relay`,
    /// `action`. A steady `reissue` rate is a relay closing us in a loop; any
    /// `floor` is a kind nozzl is not reading from that relay until the retry.
    pub forward_sub_closed: IntCounterVec,

    /// `GET /export/{kind}` request outcomes. `outcome` is `"started"` (a 200
    /// stream began), `"rejected_429"` (single-flight contention), `"not_found"`
    /// (no parquet for the kind), `"error"` (duckdb failed to spawn → 500), or
    /// `"unsupported"` (no local archive root, e.g. an S3-only deploy → 503).
    /// Labels: `kind`, `outcome`.
    ///
    /// Deliberately NOT preallocated: export is config-independent (any kind on
    /// disk is exportable), so a `(kind, outcome)` series appears only on its
    /// first request — mirroring `probe_total`'s lazy-appearance convention
    /// rather than emitting phantom zeros for every conceivable kind.
    pub export_requests_total: IntCounterVec,

    /// Notifications dropped by the central listener's per-relay fan-out
    /// because the relay task's bounded channel ([`NOTIF_FANOUT_CAP`]) was
    /// full — i.e. the relay task is hung / not draining. Labels: `relay`.
    ///
    /// One of the two *only* ways an event the pool already accepted can
    /// vanish before it reaches the archive (the other is
    /// `notif_channel_lagged_total`). Both were `warn!`-only until this metric;
    /// they make the "loss ⟺ a dropped/lagged notification" invariant a live
    /// gauge instead of a log grep.
    /// Healthy steady state: `0`. Any non-zero value means in-pipeline loss on
    /// that relay.
    pub notif_fanout_dropped: IntCounterVec,
    /// Total notifications skipped by the pool's broadcast channel because the
    /// central listener fell more than the channel capacity (4096) behind —
    /// observed as `RecvError::Lagged(n)` and incremented **by `n`**. Labelless:
    /// the lag is a property of the single central consumer draining the pool's
    /// one shared broadcast channel; `Lagged` carries no relay, so attaching a
    /// `relay` label would be inventing data we don't have at the call site.
    ///
    /// The second of the two silent-loss escape valves (see
    /// `notif_fanout_dropped_total`). `n` counts skipped `Event` *and* `Message`
    /// slots, so it is an upper bound on lost unique events — the same accounting
    /// the `pool_loss_harness` regression test pins (`lost <= lagged_total`).
    /// Healthy steady state: `0`. `> 0` is the alert condition: it means the
    /// archive consumer stalled and the pool lapped it.
    pub notif_channel_lagged: IntCounter,
    /// Backfill pages that came back **partial** (`received < limit`) per
    /// `(relay, kind)`. A partial page is normal at a walk's end (the tail page,
    /// the re-read of its last second, the empty terminators) and on a relay that
    /// clamps or cuts its pages below `limit`; it does not mean the relay ran out
    /// of events at the cursor (#32 (Backfill silently drops boundary-tie events
    /// when a relay serves fewer events per REQ than `backfill_default_limit`)).
    /// A *rising* rate during a dense kind's walk is the dashboard-visible shadow
    /// of the per-kind backfill-completeness summary log line (the ~4 %
    /// coverage-gap signal one busy kind showed). Labels: `relay`, `kind`,
    /// `slug`. A page is attributed to every kind in its (possibly combined)
    /// group, since the page advances the cursor for all of them together.
    pub backfill_partial_pages: IntCounterVec,
    /// Backfill tie-stagnation steps per `(relay, kind)`: a page whose
    /// `oldest_created_at` equalled the previous page's, so the cursor could not
    /// advance inclusively and stepped `oldest - 1` (issue #18), accepting the
    /// remainder of that one-second band as unrecoverable, counted when the page
    /// was as large as the relay's pages get (full, or as large as the run's
    /// largest page on a relay that clamps or cuts below `limit`). Labels: `relay`,
    /// `kind`, `slug`. Like `backfill_partial_pages_total`, attributed to every
    /// kind in the page's group. Non-zero on a dense kind is the precise,
    /// dashboard-visible signal that the combined-REQ walk shed events at a
    /// boundary second.
    pub backfill_tie_stagnations: IntCounterVec,
    /// Unix timestamp (seconds) at which this process started. Exported as
    /// `process_start_time_seconds`. Labelless; the scraper adds `env` via
    /// target labels. Set once in [`Metrics::new`] and never updated.
    pub process_start_time: Gauge,
    /// Build metadata. Value is always `1.0`; inspect the `version` and
    /// `git_sha` labels. `version` comes from `CARGO_PKG_VERSION`; `git_sha`
    /// is baked in at compile time by the crate's `build.rs`.
    pub build_info: GaugeVec,
}

impl Metrics {
    /// Build the metric handles and register every one against `registry`.
    ///
    /// Calling this twice with the same registry returns
    /// [`MetricsError::Registration`] on the second call (Prometheus refuses
    /// duplicate registrations) — registration is therefore idempotent only
    /// in the sense that a single call registers each metric exactly once.
    ///
    /// # Errors
    /// Returns [`MetricsError::Registration`] if any underlying
    /// [`prometheus::Registry::register`] call fails.
    // Allow rationale: this method is intentionally one long flat block — each
    // metric is a small literal definition (name + help text + labels) and
    // factoring it into helper functions only obscures the canonical list
    // documented in design.md §5.D10. Stays readable as a top-to-bottom
    // catalogue of every metric the binary exposes.
    #[allow(clippy::too_many_lines)]
    pub fn new(registry: &Registry) -> Result<Self, MetricsError> {
        let events_received = register_int_counter_vec(
            registry,
            "events_received_total",
            "Events received from a relay (pre-write). `slug` mirrors `kind` (1:1 from kinds.toml).",
            &["relay", "kind", "slug"],
        )?;
        let events_written = register_int_counter_vec(
            registry,
            "events_written_total",
            "Events accepted into the archive's per-shard writer mpsc (queued for WAL fsync + Parquet roll, not yet durable on disk). `slug` mirrors `kind`.",
            &["relay", "kind", "slug"],
        )?;
        let events_deduped = register_int_counter_vec(
            registry,
            "events_deduped_total",
            "Events deduplicated by the archive layer. Under the chunked archive this is always 0 — pool-level LRU dedup is the only write-side mechanism, and read-side dedup happens at query time. `slug` mirrors `kind`.",
            &["relay", "kind", "slug"],
        )?;
        let events_rejected = register_int_counter_vec(
            registry,
            "events_rejected_total",
            "Events rejected before write (sig fail, kind-not-configured, etc.). `slug` is `\"\"` when the kind isn't configured (no slug exists for it); otherwise mirrors `kind`.",
            &["relay", "kind", "slug", "reason"],
        )?;
        let events_dropped = register_int_counter_vec(
            registry,
            "events_dropped_total",
            "Events dropped after exhausting write retries. `slug` mirrors `kind`.",
            &["relay", "kind", "slug", "reason"],
        )?;

        let relay_connection_state = register_int_gauge_vec(
            registry,
            "relay_connection_state",
            "Per-relay connection state: 0=disconnected, 1=connected, 2=connecting.",
            &["relay"],
        )?;
        let relay_disconnects = register_int_counter_vec(
            registry,
            "relay_disconnects_total",
            "Observed `Connected → !Connected` transitions per relay.",
            &["relay"],
        )?;
        let relay_reconnect_duration_seconds = register_histogram_vec(
            registry,
            "relay_reconnect_duration_seconds",
            "Seconds from observed disconnect to next observed connect, per relay. \
             Initial bring-up is not observed.",
            &["relay"],
            // Buckets sized for the regime we're targeting after the
            // `retry_interval=2s, adjust_retry_interval=false` change
            // (typically 3–5 s) up through the previous library-default
            // saturated regime (~60 s) and beyond for true outages.
            vec![1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0],
        )?;
        let relay_req_split_count = register_int_gauge_vec(
            registry,
            "relay_req_split_count",
            "Active REQ subscriptions per relay (1 normal, higher on auto-split).",
            &["relay"],
        )?;
        let backfill_in_progress = register_int_gauge_vec(
            registry,
            "backfill_in_progress",
            "0/1 indicator that an onboarding backfill is in flight. `slug` mirrors `kind`.",
            &["relay", "kind", "slug"],
        )?;
        let checkpoint_lag_seconds = register_gauge_vec(
            registry,
            "checkpoint_lag_seconds",
            "now() - checkpoint.since, in seconds, per (relay, kind). \
             Only emitted for (relay, kind) pairs in the active subscription \
             set (after per-relay only_kinds intersection). `slug` mirrors `kind`.",
            &["relay", "kind", "slug"],
        )?;
        let kind_subscribed = register_int_gauge_vec(
            registry,
            "kind_subscribed",
            "1 for every (relay, kind) pair the acquirer is currently \
             subscribed to (after per-relay only_kinds intersection). Use the \
             presence of this series as the canonical \"is this pair active?\" \
             signal; absent series mean either never-configured or excluded. \
             `slug` mirrors `kind`.",
            &["relay", "kind", "slug"],
        )?;
        let channel_depth = register_int_gauge(
            registry,
            "channel_depth",
            "In-flight events between acquirer and writer pool.",
        )?;
        let chunks_published = register_int_counter_vec(
            registry,
            "chunks_published_total",
            "Parquet chunks successfully published to the object store, after the PUT succeeds. `slug` mirrors `kind`; `shard` is a single lowercase hex char (0–f).",
            &["class", "kind", "slug", "shard"],
        )?;
        let chunk_flush_failures = register_int_counter_vec(
            registry,
            "chunk_flush_failures_total",
            "Chunk flushes that failed to publish (failed Parquet encode or failed object-store PUT), by the `trigger` that rolled the chunk: size, rows, buffer_age, force, adhoc, shutdown, replay. NOT data loss - the rows stay in the WAL and are republished by the next trigger or by replay on the next boot - but it measures deferred recovery and wasted encode CPU. `slug` mirrors `kind`; no `shard` label (it is on the log line). The `shutdown` trigger is best read from the `shutdown flush complete` log line, which always survives; the other six are scraped normally.",
            &["class", "kind", "slug", "trigger"],
        )?;
        let chunks_last_published_timestamp_seconds = register_gauge_vec(
            registry,
            "chunks_last_published_timestamp_seconds",
            "Unix-seconds timestamp of the most recent successful chunk publish per writer. Use as `time() - chunks_last_published_timestamp_seconds{...}` to get seconds-since-last-publish per (class, kind, slug, shard). This is the correct shape for `stalled writer` panels — `time() - timestamp(counter)` looks similar but reads scrape time, not event time.",
            &["class", "kind", "slug", "shard"],
        )?;
        let storage_resident_bytes = register_gauge_vec(
            registry,
            "storage_resident_bytes",
            "Bytes currently resident in the object store per archived kind, from a periodic storage scan. A fresh re-derivation each scan (not an accumulator), so it tracks deletions and eventual compaction. Not preallocated: a kind appears only once a scan finds resident `.parquet`.",
            &["class", "kind", "slug"],
        )?;
        let storage_resident_files = register_gauge_vec(
            registry,
            "storage_resident_files",
            "Number of resident `.parquet` chunk files in the object store per archived kind, from the same periodic storage scan that drives `storage_resident_bytes`. A fresh re-derivation each scan (not an accumulator), so it tracks compaction and deletions. Not preallocated: a kind appears only once a scan finds resident `.parquet`.",
            &["class", "kind", "slug"],
        )?;
        let wal_bytes = register_gauge_vec(
            registry,
            "wal_bytes",
            "Bytes currently occupied on local disk by each writer's `_wal` file, from the same periodic scan that drives `storage_resident_bytes` (re-derived each scan, not an accumulator). Local-disk and per-writer (the WAL is on disk even with an S3 archive backend). `0` right after a flush / while idle, `> 0` while events are buffered; a persistently large value is the decisive stalled-writer signal. Not preallocated: a series appears once the writer's WAL file first exists.",
            &["class", "kind", "slug", "shard"],
        )?;

        let probe_total = register_int_counter_vec(
            registry,
            "probe_total",
            "Subscription-dispatch liveness probe outcomes per relay. `outcome` is `\"alive\"` (settled by EOSE / EVENT / CLOSED-for-probe before timeout) or `\"timeout\"` (no response within `probe_timeout`, triggers a reconnect).",
            &["relay", "outcome"],
        )?;
        let probe_alive_latency_seconds = register_histogram_vec(
            registry,
            "probe_alive_latency_seconds",
            "Probe round-trip time, observed on the `alive` outcome only. Timeouts have a known value (probe_timeout) and are intentionally not observed here.",
            &["relay"],
            vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
        )?;
        let probe_in_flight = register_int_gauge_vec(
            registry,
            "probe_in_flight",
            "1 if a dispatch-liveness probe is currently in flight against this relay; 0 otherwise.",
            &["relay"],
        )?;
        let teardown_barrier_total = register_int_counter_vec(
            registry,
            "teardown_barrier_total",
            "Outcomes of the teardown barrier sequencing the probe-timeout reconnect (issue #8). `outcome` is `cleared` (task exit observed, status connectable), `status_redrive` (task exited, non-connectable status re-driven through disconnect), `timeout_connectable` (barrier expired, connect issued — residual race sub-case), `timeout_task_live` (barrier expired, lib task reconnected itself — connect skipped), or `fallback_blind` (relay missing from pool — blind pre-barrier re-arm; should never fire). Sums to probe_total{outcome=\"timeout\"} per relay. Healthy: only `cleared`/`status_redrive`.",
            &["relay", "outcome"],
        )?;
        let forward_sub_closed = register_int_counter_vec(
            registry,
            "forward_sub_closed_total",
            "Forward subscriptions a relay CLOSEd, by what nozzl did about it. `action` is `reissue` (not a filter rejection: re-opened after a back-off that doubles per CLOSED), `split` (a rejection: the kind group halved) or `floor` (a rejection of a single-kind group: the kind suspended on that relay and retried later) or `echo` (an empty CLOSED right after nozzl's own CLOSE of that id, taken as the relay acknowledging it; an unconfirmed one also counts as `reissue`).",
            &["relay", "action"],
        )?;

        let export_requests_total = register_int_counter_vec(
            registry,
            "export_requests_total",
            "GET /export/{kind} outcomes. `outcome` is `started` (200 stream began), `rejected_429` (single-flight contention), `not_found` (no parquet for the kind), `error` (duckdb spawn failure → 500), or `unsupported` (no local archive root → 503).",
            &["kind", "outcome"],
        )?;

        let notif_fanout_dropped = register_int_counter_vec(
            registry,
            "notif_fanout_dropped_total",
            "Notifications dropped by the central listener's per-relay fan-out because the relay task's bounded channel was full (relay task hung). One of the two ways an accepted event can be lost in-pipeline; healthy steady state is 0.",
            &["relay"],
        )?;
        let notif_channel_lagged = register_int_counter(
            registry,
            "notif_channel_lagged_total",
            "Notifications skipped by the pool's broadcast channel because the central listener lagged > capacity (RecvError::Lagged(n), incremented by n). Labelless (Lagged carries no relay). Upper bound on lost unique events; healthy steady state is 0.",
        )?;
        let backfill_partial_pages = register_int_counter_vec(
            registry,
            "backfill_partial_pages_total",
            "Backfill pages that came back partial (received < limit) per (relay, kind). Attributed to every kind in the page's group. A rising rate on a dense kind is the dashboard-visible shadow of the per-kind backfill-completeness summary log. `slug` mirrors `kind`.",
            &["relay", "kind", "slug"],
        )?;
        let backfill_tie_stagnations = register_int_counter_vec(
            registry,
            "backfill_tie_stagnations_total",
            "Backfill tie-stagnation steps per (relay, kind): a page as large as the relay's pages get (full, or as large as the run's largest page when the relay clamps or cuts below the limit) whose oldest_created_at equalled the previous page's, forcing an `oldest - 1` step that sheds the rest of that one-second band (issue #18). Attributed to every kind in the page's group. `slug` mirrors `kind`.",
            &["relay", "kind", "slug"],
        )?;

        let process_start_time = register_gauge(
            registry,
            "process_start_time_seconds",
            "Unix timestamp (seconds) when this process started. Use \
             `time() - process_start_time_seconds` in PromQL to get uptime.",
        )?;
        process_start_time.set(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        );

        let build_info = register_gauge_vec(
            registry,
            "build_info",
            "Build metadata. Value is always 1; inspect the `version` and `git_sha` labels.",
            &["version", "git_sha"],
        )?;
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION"), env!("NOZZL_GIT_SHA")])
            .set(1.0);

        Ok(Self {
            events_received,
            events_written,
            events_deduped,
            events_rejected,
            events_dropped,
            relay_connection_state,
            relay_disconnects,
            relay_reconnect_duration_seconds,
            relay_req_split_count,
            backfill_in_progress,
            checkpoint_lag_seconds,
            kind_subscribed,
            channel_depth,
            chunks_published,
            chunk_flush_failures,
            chunks_last_published_timestamp_seconds,
            storage_resident_bytes,
            storage_resident_files,
            wal_bytes,
            probe_total,
            probe_alive_latency_seconds,
            probe_in_flight,
            teardown_barrier_total,
            forward_sub_closed,
            export_requests_total,
            notif_fanout_dropped,
            notif_channel_lagged,
            backfill_partial_pages,
            backfill_tie_stagnations,
            process_start_time,
            build_info,
        })
    }
}

/// Inputs to [`Metrics::preallocate_known_series`]. Each field documents
/// which metric families it seeds. Lifetimes are deliberately tight —
/// callers build the borrowed slices from their own owned storage and the
/// preallocation runs once at startup, so there's no need to take owned
/// values.
#[derive(Debug, Clone, Copy)]
pub struct PreallocSeed<'a> {
    /// Normalized relay URLs (typically `NozzlRelayUrl::as_str()`).
    ///
    /// Seeds every metric labeled `relay`: `relay_connection_state`,
    /// `relay_disconnects_total`, `relay_reconnect_duration_seconds`,
    /// `relay_req_split_count`, `probe_alive_latency_seconds`,
    /// `probe_in_flight`, plus the `(relay, outcome)` combinations of
    /// `probe_total`, `teardown_barrier_total` and `forward_sub_closed_total`.
    pub relays: &'a [&'a str],
    /// `(kind, slug)` tuples from the configured kind list. Combined with
    /// `relays` to seed `events_received_total`, `events_written_total`,
    /// `events_deduped_total`, and `backfill_in_progress`.
    pub kind_slugs: &'a [(u16, &'a str)],
    /// `(class_label, kind, slug)` tuples for writer-side chunk metrics.
    /// `class_label` is `"replaceable"`, `"addressable"`, or `"regular"` per
    /// the `chunk_writer` label convention. Only kinds whose class actually
    /// produces chunks should appear here — the writer never increments
    /// chunk metrics for Ephemeral kinds (the one non-archived class).
    pub class_kind_slugs: &'a [(&'a str, u16, &'a str)],
}

impl Metrics {
    /// Set the `relay_connection_state` gauge for `relay`. The only writer
    /// surface for this gauge: taking [`RelayUrl`] (normalized by construction)
    /// and [`RelayConnState`] (the encoded value) means every writer — the
    /// per-relay task and the 30 s heartbeat sweep alike — necessarily agrees
    /// on both the label and the wire encoding, so the two can never diverge.
    pub fn set_relay_connection_state(&self, relay: &RelayUrl, state: RelayConnState) {
        self.relay_connection_state
            .with_label_values(&[relay.as_str()])
            .set(state.code());
    }

    /// Read whether `relay` is currently `Connected`, keyed by the same
    /// [`RelayUrl`] the writers use. Returns `false` if the series is absent
    /// (never observed) — the health snapshot treats that as "not connected".
    #[must_use]
    pub fn relay_connected(&self, relay: &RelayUrl) -> bool {
        self.relay_connection_state
            .get_metric_with_label_values(&[relay.as_str()])
            .is_ok_and(|g| g.get() == RelayConnState::Connected.code())
    }

    /// Seed the relay-only labeled series for one relay. Called for every
    /// relay at startup by [`Self::preallocate_known_series`], and by the
    /// acquirer for a relay added by hot-reload, which would otherwise get
    /// each series only at its first increment, invisible to `increase()`.
    /// `set(0)` / `inc_by(0)` both force the series into the registry; the
    /// value is whatever the production code subsequently sets/inc's it to.
    pub fn preallocate_relay(&self, relay: &str) {
        // Gauges are only registered, never set: startup seeds them after
        // `Acquirer::spawn`, when a relay task may already have set its split
        // count or connection state. A new child starts at 0.
        let _ = self.relay_connection_state.with_label_values(&[relay]);
        self.relay_disconnects
            .with_label_values(&[relay])
            .inc_by(0);
        let _ = self.relay_req_split_count.with_label_values(&[relay]);
        let _ = self.probe_in_flight.with_label_values(&[relay]);
        self.notif_fanout_dropped
            .with_label_values(&[relay])
            .inc_by(0);
        // For histograms, `with_label_values` returns a `Histogram` (a
        // child) — accessing it is enough to register the series in
        // the registry; we discard the returned handle.
        let _ = self
            .relay_reconnect_duration_seconds
            .with_label_values(&[relay]);
        let _ = self.probe_alive_latency_seconds.with_label_values(&[relay]);
        for outcome in ["alive", "timeout"] {
            self.probe_total
                .with_label_values(&[relay, outcome])
                .inc_by(0);
        }
        for outcome in [
            "cleared",
            "status_redrive",
            "timeout_connectable",
            "timeout_task_live",
            "fallback_blind",
        ] {
            self.teardown_barrier_total
                .with_label_values(&[relay, outcome])
                .inc_by(0);
        }
        for action in FORWARD_SUB_CLOSED_ACTIONS {
            self.forward_sub_closed
                .with_label_values(&[relay, action])
                .inc_by(0);
        }
    }

    /// Force-register every time series whose label combination is known at
    /// startup, so `/metrics` exposes a real value (`0` / baseline) for them
    /// even before any production code observes anything.
    ///
    /// Without this, Prometheus `*Vec` metrics are lazy-created on first
    /// observation per label combination — by design, to avoid cartesian
    /// blow-up of label spaces that are never used. A slow-to-fire counter
    /// (e.g. `chunks_published_total`, gated on a 1-hour buffer-age
    /// trigger) or a relay that has never disconnected
    /// (`relay_disconnects_total`) shows up as **absent** in `/metrics` —
    /// not as `0`. Dashboards that query `sum(...)` then fall back to the
    /// previous-restart value via Grafana's `lastNotNull` reducer, making
    /// the panel look like it survived the restart. After this call the
    /// queries see a real `0` immediately and the dashboard reflects
    /// reality across restarts.
    ///
    /// Series pre-allocated here can still be incremented / set by
    /// production code without coordination — these aren't reservations,
    /// they're just a first observation that happens to be a no-op.
    ///
    /// Idempotent against repeat preallocation of the same series (counter
    /// `inc_by(0)` and gauge `set(0)` both leave the underlying child
    /// unchanged on subsequent calls).
    ///
    /// Some labelled metrics are intentionally **not** pre-allocated:
    /// - `events_rejected_total` / `events_dropped_total` carry a `reason`
    ///   label whose value space is open-ended (e.g. arbitrary kinds end up
    ///   in `reason="kind-not-configured"`). Pre-allocating only some
    ///   reasons would create an asymmetric surface that misleads — they
    ///   appear lazily on first observation as today.
    /// - `checkpoint_lag_seconds` is populated by the gauge loop; pre-
    ///   allocating a zero would be misleading until the first real lag is
    ///   computed.
    /// - `kind_subscribed` is owned by the periodic gauge loop in the
    ///   top-level crate (`spawn_gauge_loop`), which asserts `1` for each
    ///   active `(relay, kind)` pair every tick and removes stale ones; we
    ///   leave it to that loop rather than seeding here.
    /// - `channel_depth` is labelless and always exposed.
    pub fn preallocate_known_series(&self, seed: PreallocSeed<'_>) {
        // Labelless loss counter — force the series so `/metrics` shows a real
        // `0` immediately (the whole point: "did the pipeline lose anything?"
        // must read 0, not absent, on a healthy deployment).
        self.notif_channel_lagged.inc_by(0);
        for &relay in seed.relays {
            self.preallocate_relay(relay);

            // (relay, kind, slug) labeled metrics.
            for &(kind, slug) in seed.kind_slugs {
                let kind_str = kind.to_string();
                self.events_received
                    .with_label_values(&[relay, kind_str.as_str(), slug])
                    .inc_by(0);
                self.events_written
                    .with_label_values(&[relay, kind_str.as_str(), slug])
                    .inc_by(0);
                self.events_deduped
                    .with_label_values(&[relay, kind_str.as_str(), slug])
                    .inc_by(0);
                self.backfill_in_progress
                    .with_label_values(&[relay, kind_str.as_str(), slug])
                    .set(0);
                self.backfill_partial_pages
                    .with_label_values(&[relay, kind_str.as_str(), slug])
                    .inc_by(0);
                self.backfill_tie_stagnations
                    .with_label_values(&[relay, kind_str.as_str(), slug])
                    .inc_by(0);
            }
        }

        // (class, kind, slug, shard) — writer-side chunk metrics. Shards
        // are the 16 lowercase hex chars 0..f; iterate eagerly because
        // any pubkey published in the future may land in any shard.
        for &(class, kind, slug) in seed.class_kind_slugs {
            let kind_str = kind.to_string();
            // Shard-free, so it sits outside the shard loop: one series per
            // (class, kind, slug, trigger), reading a real 0 so an alert on
            // `increase(chunk_flush_failures_total[10m]) > 0` has something to
            // compare against from the first scrape.
            for trigger in CHUNK_FLUSH_TRIGGERS {
                self.chunk_flush_failures
                    .with_label_values(&[class, kind_str.as_str(), slug, trigger])
                    .inc_by(0);
            }
            for shard_n in 0u8..16 {
                // Single hex char, lowercase, matches `PubkeyShard::as_char()`.
                let shard = format!("{shard_n:x}");
                self.chunks_published
                    .with_label_values(&[class, kind_str.as_str(), slug, shard.as_str()])
                    .inc_by(0);
                // The "last published" gauge sits at 0.0 until a real
                // chunk publish writes a real epoch-seconds value.
                // Dashboards already compute `time() - <gauge>` so a 0.0
                // sentinel is correctly interpreted as "very stale" — the
                // operator-visible signal is the same as "no value
                // present" but without the lastNotNull confusion.
                self.chunks_last_published_timestamp_seconds
                    .with_label_values(&[class, kind_str.as_str(), slug, shard.as_str()])
                    .set(0.0);
            }
        }
    }
}

/// Update `checkpoint_lag_seconds` for every `(relay, kind, slug, since)`
/// tuple in `snapshot`. Lag is `now_unix_seconds - since`; negative values
/// (clock skew or future-dated events) are written as-is so the operator sees
/// the raw signal rather than a clamped lie.
///
/// `slug` is the human-readable slug from `kinds.toml` for the corresponding
/// `kind`. Caller is responsible for the lookup — the checkpoint store doesn't
/// know about slugs. Pass `""` if no slug is known (the metric still exists,
/// the label is just empty).
///
/// The caller chooses `now_unix_seconds` so tests can pin time. In production
/// the acquirer/orchestrator passes `SystemTime::now()` converted to seconds.
///
/// This function does not clear time series for coordinates omitted from
/// `snapshot`. In practice the snapshot covers every `(relay, kind)` ever
/// observed, so series only shrink when the binary restarts with a smaller
/// kind set — which is also when the Prometheus scrape will simply stop seeing
/// the old labels and they'll age out of the registry on restart.
#[allow(clippy::cast_precision_loss)]
pub fn update_checkpoint_lag<I, S>(metrics: &Metrics, snapshot: I, now_unix_seconds: i64)
where
    I: IntoIterator<Item = (S, u16, S, i64)>,
    S: AsRef<str>,
{
    for (relay, kind, slug, since) in snapshot {
        let lag_seconds = (now_unix_seconds - since) as f64;
        let kind_str = kind.to_string();
        metrics
            .checkpoint_lag_seconds
            .with_label_values(&[relay.as_ref(), kind_str.as_str(), slug.as_ref()])
            .set(lag_seconds);
    }
}

// ---- registration helpers ------------------------------------------------

fn register_int_counter_vec(
    registry: &Registry,
    name: &'static str,
    help: &str,
    labels: &[&str],
) -> Result<IntCounterVec, MetricsError> {
    let vec = IntCounterVec::new(Opts::new(name, help), labels)
        .map_err(|source| MetricsError::Registration { name, source })?;
    registry
        .register(Box::new(vec.clone()))
        .map_err(|source| MetricsError::Registration { name, source })?;
    Ok(vec)
}

fn register_int_counter(
    registry: &Registry,
    name: &'static str,
    help: &str,
) -> Result<IntCounter, MetricsError> {
    let counter = IntCounter::new(name, help)
        .map_err(|source| MetricsError::Registration { name, source })?;
    registry
        .register(Box::new(counter.clone()))
        .map_err(|source| MetricsError::Registration { name, source })?;
    Ok(counter)
}

fn register_int_gauge_vec(
    registry: &Registry,
    name: &'static str,
    help: &str,
    labels: &[&str],
) -> Result<IntGaugeVec, MetricsError> {
    let vec = IntGaugeVec::new(Opts::new(name, help), labels)
        .map_err(|source| MetricsError::Registration { name, source })?;
    registry
        .register(Box::new(vec.clone()))
        .map_err(|source| MetricsError::Registration { name, source })?;
    Ok(vec)
}

fn register_gauge_vec(
    registry: &Registry,
    name: &'static str,
    help: &str,
    labels: &[&str],
) -> Result<GaugeVec, MetricsError> {
    let vec = GaugeVec::new(Opts::new(name, help), labels)
        .map_err(|source| MetricsError::Registration { name, source })?;
    registry
        .register(Box::new(vec.clone()))
        .map_err(|source| MetricsError::Registration { name, source })?;
    Ok(vec)
}

fn register_int_gauge(
    registry: &Registry,
    name: &'static str,
    help: &str,
) -> Result<IntGauge, MetricsError> {
    let gauge = IntGauge::new(name, help)
        .map_err(|source| MetricsError::Registration { name, source })?;
    registry
        .register(Box::new(gauge.clone()))
        .map_err(|source| MetricsError::Registration { name, source })?;
    Ok(gauge)
}

fn register_gauge(
    registry: &Registry,
    name: &'static str,
    help: &str,
) -> Result<Gauge, MetricsError> {
    let gauge = Gauge::new(name, help)
        .map_err(|source| MetricsError::Registration { name, source })?;
    registry
        .register(Box::new(gauge.clone()))
        .map_err(|source| MetricsError::Registration { name, source })?;
    Ok(gauge)
}

fn register_histogram_vec(
    registry: &Registry,
    name: &'static str,
    help: &str,
    labels: &[&str],
    buckets: Vec<f64>,
) -> Result<HistogramVec, MetricsError> {
    let opts = HistogramOpts::new(name, help).buckets(buckets);
    let vec = HistogramVec::new(opts, labels)
        .map_err(|source| MetricsError::Registration { name, source })?;
    registry
        .register(Box::new(vec.clone()))
        .map_err(|source| MetricsError::Registration { name, source })?;
    Ok(vec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn relay_conn_state_codes_are_stable() {
        // The 0/1/2 wire encoding is a contract with dashboards and the
        // design doc; pin it here so a reordering of the enum can't silently
        // shift the values.
        assert_eq!(RelayConnState::Disconnected.code(), 0);
        assert_eq!(RelayConnState::Connected.code(), 1);
        assert_eq!(RelayConnState::Connecting.code(), 2);
    }

    #[test]
    fn relay_connection_state_accessor_round_trips_typed_value() {
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).expect("register");
        let relay = RelayUrl::parse("wss://relay.example.com").expect("valid url");

        // Absent series reads as "not connected".
        assert!(!metrics.relay_connected(&relay));

        // Only `Connected` counts as connected; the other states do not.
        metrics.set_relay_connection_state(&relay, RelayConnState::Connecting);
        assert!(!metrics.relay_connected(&relay));
        metrics.set_relay_connection_state(&relay, RelayConnState::Connected);
        assert!(metrics.relay_connected(&relay));
        metrics.set_relay_connection_state(&relay, RelayConnState::Disconnected);
        assert!(!metrics.relay_connected(&relay));
    }

    #[test]
    fn relay_connection_state_label_is_the_normalized_url() {
        // The accessor takes a `RelayUrl`, so write and read share one
        // normalization path. Two `RelayUrl`s that normalize to the same
        // canonical string address the same series — regardless of how the
        // caller spelled the input — and a different relay is independent.
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).expect("register");

        let a = RelayUrl::parse("wss://relay.example.com").expect("valid");
        // `url::Url` appends the root-path slash; a re-parse of the canonical
        // form must hit the same series the first write created.
        let a_canonical = RelayUrl::parse(a.as_str()).expect("valid");
        let b = RelayUrl::parse("wss://other.example.com").expect("valid");

        metrics.set_relay_connection_state(&a, RelayConnState::Connected);
        assert!(metrics.relay_connected(&a_canonical));
        assert!(!metrics.relay_connected(&b));

        // The series is keyed by exactly the normalized label, so a raw
        // lookup with that string sees the value the accessor set.
        let v = metrics
            .relay_connection_state
            .get_metric_with_label_values(&[a.as_str()])
            .expect("series exists")
            .get();
        assert_eq!(v, RelayConnState::Connected.code());
    }

    // A flat list of one seed per vec metric plus one name per family. The
    // lint targets complexity, and a sequence of independent `.inc()` calls has
    // none; splitting it would only hide which metrics are covered.
    #[allow(clippy::too_many_lines)]
    #[test]
    fn metrics_new_registers_all_metrics() {
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).expect("first registration succeeds");

        // Seed one observation per vec metric. `Registry::gather()` prunes
        // empty MetricFamilies, so without seeding, vec-typed families
        // would not appear in the output.
        metrics.events_received.with_label_values(&["r", "0", "s"]).inc();
        metrics.events_written.with_label_values(&["r", "0", "s"]).inc();
        metrics.events_deduped.with_label_values(&["r", "0", "s"]).inc();
        metrics
            .events_rejected
            .with_label_values(&["r", "0", "s", "x"])
            .inc();
        metrics
            .events_dropped
            .with_label_values(&["r", "0", "s", "x"])
            .inc();
        metrics.relay_connection_state.with_label_values(&["r"]).set(1);
        metrics.relay_disconnects.with_label_values(&["r"]).inc();
        metrics
            .relay_reconnect_duration_seconds
            .with_label_values(&["r"])
            .observe(3.0);
        metrics.relay_req_split_count.with_label_values(&["r"]).set(1);
        metrics.backfill_in_progress.with_label_values(&["r", "0", "s"]).set(0);
        metrics.checkpoint_lag_seconds.with_label_values(&["r", "0", "s"]).set(0.0);
        metrics.kind_subscribed.with_label_values(&["r", "0", "s"]).set(1);
        metrics.channel_depth.set(0);
        metrics
            .chunks_published
            .with_label_values(&["replaceable", "0", "s", "0"])
            .inc();
        metrics
            .chunks_last_published_timestamp_seconds
            .with_label_values(&["replaceable", "0", "s", "0"])
            .set(1_700_000_000.0);
        metrics
            .storage_resident_bytes
            .with_label_values(&["replaceable", "0", "s"])
            .set(0.0);
        metrics
            .storage_resident_files
            .with_label_values(&["replaceable", "0", "s"])
            .set(0.0);
        metrics.probe_total.with_label_values(&["r", "alive"]).inc();
        metrics
            .probe_alive_latency_seconds
            .with_label_values(&["r"])
            .observe(0.1);
        metrics.probe_in_flight.with_label_values(&["r"]).set(0);
        metrics
            .teardown_barrier_total
            .with_label_values(&["r", "cleared"])
            .inc();
        metrics
            .forward_sub_closed
            .with_label_values(&["r", "reissue"])
            .inc();
        metrics.notif_fanout_dropped.with_label_values(&["r"]).inc();
        metrics.notif_channel_lagged.inc();
        metrics.backfill_partial_pages.with_label_values(&["r", "0", "s"]).inc();
        metrics.backfill_tie_stagnations.with_label_values(&["r", "0", "s"]).inc();
        metrics
            .chunk_flush_failures
            .with_label_values(&["replaceable", "0", "s", "buffer_age"])
            .inc();

        let families = registry.gather();
        let names: Vec<&str> = families
            .iter()
            .map(prometheus::proto::MetricFamily::name)
            .collect();

        for expected in [
            "events_received_total",
            "events_written_total",
            "events_deduped_total",
            "events_rejected_total",
            "events_dropped_total",
            "relay_connection_state",
            "relay_disconnects_total",
            "relay_reconnect_duration_seconds",
            "relay_req_split_count",
            "backfill_in_progress",
            "checkpoint_lag_seconds",
            "channel_depth",
            "kind_subscribed",
            "chunks_published_total",
            "chunks_last_published_timestamp_seconds",
            "storage_resident_bytes",
            "storage_resident_files",
            "probe_total",
            "probe_alive_latency_seconds",
            "probe_in_flight",
            "teardown_barrier_total",
            "forward_sub_closed_total",
            "notif_fanout_dropped_total",
            "notif_channel_lagged_total",
            "backfill_partial_pages_total",
            "backfill_tie_stagnations_total",
            "chunk_flush_failures_total",
            "process_start_time_seconds",
            "build_info",
        ] {
            assert!(
                names.contains(&expected),
                "expected metric `{expected}` to be registered; got {names:?}",
            );
        }
        assert_eq!(families.len(), 29, "exactly 29 metric families expected");
    }

    #[test]
    fn preallocate_known_series_creates_expected_zero_valued_families() {
        // Pre-allocation must make `/metrics` expose `0` for every label
        // combination derivable from config — so dashboards see a real
        // zero immediately after restart instead of falling back to the
        // pre-restart `lastNotNull` value.
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).expect("register");

        let relays: &[&str] = &["wss://r.example/", "wss://r2.example/"];
        let kind_slugs: &[(u16, &str)] = &[(0, "profile-metadata"), (10002, "relay-list-metadata")];
        let class_kind_slugs: &[(&str, u16, &str)] = &[
            ("replaceable", 0, "profile-metadata"),
            ("replaceable", 10002, "relay-list-metadata"),
        ];
        metrics.preallocate_known_series(PreallocSeed {
            relays,
            kind_slugs,
            class_kind_slugs,
        });

        let families = registry.gather();
        let by_name: HashMap<&str, &prometheus::proto::MetricFamily> = families
            .iter()
            .map(|f| (f.name(), f))
            .collect();

        // Every label-bearing metric we promised to preallocate must have
        // a non-empty MetricFamily in /metrics gather output.
        for expected in [
            "events_received_total",
            "events_written_total",
            "events_deduped_total",
            "relay_connection_state",
            "relay_disconnects_total",
            "relay_reconnect_duration_seconds",
            "relay_req_split_count",
            "backfill_in_progress",
            "backfill_partial_pages_total",
            "backfill_tie_stagnations_total",
            "chunks_published_total",
            "chunks_last_published_timestamp_seconds",
            "probe_total",
            "probe_alive_latency_seconds",
            "probe_in_flight",
            "teardown_barrier_total",
            "forward_sub_closed_total",
            "notif_fanout_dropped_total",
            "notif_channel_lagged_total",
            "chunk_flush_failures_total",
        ] {
            let fam = by_name.get(expected).unwrap_or_else(|| {
                panic!("expected metric family `{expected}` to be present after preallocate; got {:?}", by_name.keys().collect::<Vec<_>>())
            });
            assert!(
                !fam.get_metric().is_empty(),
                "metric `{expected}` has no series after preallocate",
            );
        }

        // Cardinality spot-checks for the most error-prone combinations.
        // (relays × {alive, timeout}) = 2 × 2 = 4 series for probe_total.
        let probe_total = by_name.get("probe_total").unwrap();
        assert_eq!(probe_total.get_metric().len(), 4, "probe_total cardinality");
        // (relays × 5 outcomes) = 2 × 5 = 10 series for teardown_barrier_total.
        let barrier = by_name.get("teardown_barrier_total").unwrap();
        assert_eq!(
            barrier.get_metric().len(),
            10,
            "teardown_barrier_total cardinality"
        );
        // (class_kind_slugs x 7 triggers) = 2 x 7 = 14 series for
        // chunk_flush_failures_total, every one of them a real zero.
        //
        // This assertion is load-bearing for consumers, not just for us: an
        // alert on this counter can treat no data as OK only because the
        // counter is preallocated to 0 for every (class, kind, slug, trigger).
        // If the preallocation loop is ever deleted, the series appear only on
        // the first failure, no data stops meaning "renamed or gone", and such
        // an alert degrades to a no-op on a freshly restarted process.
        let flush_failures = by_name.get("chunk_flush_failures_total").unwrap();
        assert_eq!(
            flush_failures.get_metric().len(),
            class_kind_slugs.len() * CHUNK_FLUSH_TRIGGERS.len(),
            "chunk_flush_failures_total cardinality: one series per (class, kind, slug) x trigger"
        );
        for m in flush_failures.get_metric() {
            // A counter is non-negative, so this is "is zero" without the
            // strict float comparison clippy denies.
            assert!(
                m.get_counter().value() < f64::EPSILON,
                "a preallocated flush-failure series must read a real zero, got {}",
                m.get_counter().value()
            );
        }

        // (relays x 4 actions) = 2 x 4 = 8 series for forward_sub_closed_total.
        let fwd_closed = by_name.get("forward_sub_closed_total").unwrap();
        assert_eq!(
            fwd_closed.get_metric().len(),
            8,
            "forward_sub_closed_total cardinality"
        );
        // (class × kind × slug × 16 shards) = 1 × 2 × 1 × 16 = 32 series
        // for chunks_published_total (slug is 1:1 with kind so it's just
        // "kinds × shards" effective).
        let chunks = by_name.get("chunks_published_total").unwrap();
        assert_eq!(chunks.get_metric().len(), 32, "chunks_published cardinality");

        // Values are zero (the point of preallocation is "real zero, not
        // absent"). Cross-check via the typed counter accessor —
        // `with_label_values(...).get()` returns the underlying u64 and
        // avoids the unstable protobuf-field-accessor API.
        for &(class, kind, slug) in class_kind_slugs {
            let kind_str = kind.to_string();
            for shard_n in 0u8..16 {
                let shard = format!("{shard_n:x}");
                let v = metrics
                    .chunks_published
                    .with_label_values(&[class, kind_str.as_str(), slug, shard.as_str()])
                    .get();
                assert_eq!(v, 0, "chunks_published {class}/{kind}/{slug}/{shard} not zero");
            }
        }
        for &relay in relays {
            let v = metrics
                .relay_connection_state
                .with_label_values(&[relay])
                .get();
            assert_eq!(v, 0, "relay_connection_state {relay} not zero");
        }
    }

    #[test]
    fn preallocate_known_series_skips_open_ended_reasons() {
        // events_rejected_total / events_dropped_total carry a `reason`
        // label whose value space is open-ended; they MUST NOT be
        // pre-allocated (the lazy-on-first-observation semantics is what
        // makes the metric honest about what reasons have actually been
        // observed in this process lifetime).
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).expect("register");
        metrics.preallocate_known_series(PreallocSeed {
            relays: &["wss://r/"],
            kind_slugs: &[(0, "profile-metadata")],
            class_kind_slugs: &[("replaceable", 0, "profile-metadata")],
        });

        let families = registry.gather();
        let names: Vec<&str> = families
            .iter()
            .map(prometheus::proto::MetricFamily::name)
            .collect();
        // The two open-ended families have no series until the first
        // production observation; they should not appear at all in
        // /metrics gather output.
        assert!(
            !names.contains(&"events_rejected_total"),
            "events_rejected_total must not be pre-allocated"
        );
        assert!(
            !names.contains(&"events_dropped_total"),
            "events_dropped_total must not be pre-allocated"
        );
    }

    #[test]
    fn preallocate_known_series_is_idempotent() {
        // Calling twice with the same seed must not double-count, error,
        // or reset values that production code may have observed in
        // between.
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).expect("register");
        let seed = PreallocSeed {
            relays: &["wss://r/"],
            kind_slugs: &[(0, "profile-metadata")],
            class_kind_slugs: &[("replaceable", 0, "profile-metadata")],
        };
        metrics.preallocate_known_series(seed);

        // Production code observes a real value between preallocations.
        metrics
            .events_received
            .with_label_values(&["wss://r/", "0", "profile-metadata"])
            .inc_by(42);

        metrics.preallocate_known_series(seed);

        // Re-pre-allocation must leave the real observation untouched.
        let observed = metrics
            .events_received
            .with_label_values(&["wss://r/", "0", "profile-metadata"])
            .get();
        assert_eq!(observed, 42, "preallocate must not reset observed counter");
    }

    #[test]
    fn metrics_new_is_not_idempotent_on_same_registry() {
        // Prometheus refuses duplicate registration; documenting the behavior.
        let registry = Registry::new();
        let _first = Metrics::new(&registry).expect("first call succeeds");
        let err = Metrics::new(&registry).expect_err("second call must fail");
        assert!(matches!(err, MetricsError::Registration { .. }));
    }

    #[test]
    fn metrics_new_succeeds_on_independent_registries() {
        // The same Metrics struct can be built against two independent
        // registries. Validates that registration is per-registry.
        let r1 = Registry::new();
        let r2 = Registry::new();
        let _m1 = Metrics::new(&r1).expect("r1");
        let _m2 = Metrics::new(&r2).expect("r2");
    }

    #[test]
    fn label_cardinality_matches_design() {
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).unwrap();

        // Wrong arity panics inside prometheus; right arity does not.
        metrics.events_received.with_label_values(&["wss://r", "0", "profile-metadata"]).inc();
        metrics.events_rejected.with_label_values(&["wss://r", "0", "profile-metadata", "sig_fail"]).inc();
        metrics.events_dropped.with_label_values(&["wss://r", "0", "profile-metadata", "write_failed"]).inc();
        metrics.relay_connection_state.with_label_values(&["wss://r"]).set(1);
        metrics.relay_disconnects.with_label_values(&["wss://r"]).inc();
        metrics
            .relay_reconnect_duration_seconds
            .with_label_values(&["wss://r"])
            .observe(4.2);
        metrics.relay_req_split_count.with_label_values(&["wss://r"]).set(1);
        metrics.backfill_in_progress.with_label_values(&["wss://r", "0", "profile-metadata"]).set(0);
        metrics.checkpoint_lag_seconds.with_label_values(&["wss://r", "0", "profile-metadata"]).set(0.0);
        metrics.channel_depth.set(0);
    }

    #[test]
    fn update_checkpoint_lag_writes_now_minus_since_per_pair() {
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).unwrap();

        let now: i64 = 2_000_000_000;
        let snapshot = vec![
            (
                "wss://relay.damus.io/".to_string(),
                0_u16,
                "profile-metadata".to_string(),
                1_999_999_900_i64,
            ),
            (
                "wss://nos.lol/".to_string(),
                30023_u16,
                "long-form-content".to_string(),
                1_999_700_000_i64,
            ),
        ];
        update_checkpoint_lag(&metrics, snapshot, now);

        let damus_lag = metrics
            .checkpoint_lag_seconds
            .with_label_values(&["wss://relay.damus.io/", "0", "profile-metadata"])
            .get();
        let nos_lag = metrics
            .checkpoint_lag_seconds
            .with_label_values(&["wss://nos.lol/", "30023", "long-form-content"])
            .get();
        assert!((damus_lag - 100.0).abs() < f64::EPSILON);
        assert!((nos_lag - 300_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn update_checkpoint_lag_emits_negative_lag_for_future_since() {
        // Clock-skew or future-dated events: report the raw signal so the
        // operator sees the skew rather than a clamped lie.
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).unwrap();

        update_checkpoint_lag(
            &metrics,
            [(
                "wss://r/".to_string(),
                1_u16,
                "follow-lists".to_string(),
                1_000_000_500_i64,
            )],
            1_000_000_000,
        );

        let lag = metrics
            .checkpoint_lag_seconds
            .with_label_values(&["wss://r/", "1", "follow-lists"])
            .get();
        assert!((lag - (-500.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn update_checkpoint_lag_overwrites_previous_value_on_repeat_call() {
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).unwrap();

        update_checkpoint_lag(
            &metrics,
            [(
                "wss://r/".to_string(),
                1_u16,
                "follow-lists".to_string(),
                100_i64,
            )],
            200,
        );
        // First call: lag = 100. Second call at later `now` with same `since`:
        // lag = 500 — the gauge must reflect the newest write.
        update_checkpoint_lag(
            &metrics,
            [(
                "wss://r/".to_string(),
                1_u16,
                "follow-lists".to_string(),
                100_i64,
            )],
            600,
        );

        let lag = metrics
            .checkpoint_lag_seconds
            .with_label_values(&["wss://r/", "1", "follow-lists"])
            .get();
        assert!((lag - 500.0).abs() < f64::EPSILON);
    }

    #[test]
    fn update_checkpoint_lag_empty_snapshot_is_a_noop() {
        let registry = Registry::new();
        let metrics = Metrics::new(&registry).unwrap();

        let snapshot: Vec<(String, u16, String, i64)> = vec![];
        update_checkpoint_lag(&metrics, snapshot, 1_000_000_000);

        // No time series should have been created.
        let families = registry.gather();
        let lag_family = families
            .iter()
            .find(|f| f.name() == "checkpoint_lag_seconds");
        assert!(
            lag_family.is_none(),
            "empty snapshot must not register any series"
        );
    }

}
