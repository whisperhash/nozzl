# nozzl-metrics - crate brief

> **Update (2026-06):** this brief is historical. The HTTP listener (`/metrics`,
> `/health`), the `HealthSource` trait, and the `install` fn described below have
> since moved to the **`nozzl-http`** crate, which now owns nozzl's entire HTTP
> surface (including `POST /admin/flush/{kind}`). `nozzl-metrics` is now
> transport-agnostic: just the registry and the typed `Metrics` handle. See
> `docs/design.md` §5.D10 and §9 for the current shape.

## Purpose

Owns the Prometheus metrics registry and the typed `Metrics` struct that other
crates use to record observations without touching the registry directly.
(Originally also served the `/metrics` and `/health` HTTP endpoints — see the
update note above; that surface now lives in `nozzl-http`.)

The full metrics list is in [../design.md §5.D10](../design.md#d10--observability).

## Dependencies

crates.io:
- `prometheus` (the standard Rust prom client) — alternatively `metrics` + `metrics-exporter-prometheus`. Prefer `prometheus` directly for simplicity unless there's a reason otherwise.
- `axum` (or `hyper` directly) — for the HTTP endpoint
- `tokio`
- `tracing`
- `thiserror`

sister crates: `nozzl-types` only — for the shared domain types that appear
in label-setter signatures (e.g. `RelayUrl`), so the metric surface can
*enforce* normalized, validated labels instead of accepting free-form
strings. Otherwise stay independent (no acquirer / archive / config coupling)
so the metrics layer remains reusable.

## Public API contract

```rust
pub struct Metrics {
    // Counters. `slug` is the human-readable name from kinds.toml for the
    // corresponding `kind`; functionally determined by kind (1:1, validated
    // at config load) so it adds no series cardinality. Empty slug ("") is
    // the sentinel used when the kind isn't configured (currently only
    // events_rejected_total{reason="kind-not-configured"}).
    pub events_received: IntCounterVec,    // labels: relay, kind, slug
    pub events_written: IntCounterVec,
    pub events_deduped: IntCounterVec,
    pub events_rejected: IntCounterVec,    // labels: relay, kind, slug, reason
    pub events_dropped:  IntCounterVec,    // labels: relay, kind, slug, reason

    // Gauges
    // relay_connection_state is PRIVATE: mutate via
    //   set_relay_connection_state(&RelayUrl, RelayConnState)
    // and read via relay_connected(&RelayUrl). Taking the typed RelayUrl +
    // RelayConnState (encodes the 0|1|2 wire value) makes a mis-normalized
    // label or a magic status code unrepresentable at call sites.
    relay_connection_state: IntGaugeVec,     // labels: relay; values: 0|1|2
    pub relay_req_split_count:  IntGaugeVec, // labels: relay
    pub backfill_in_progress:   IntGaugeVec, // labels: relay, kind, slug
    pub checkpoint_lag_seconds: GaugeVec,    // labels: relay, kind, slug
    pub kind_subscribed:        IntGaugeVec, // labels: relay, kind, slug
    pub channel_depth:          IntGauge,    // single value

    // Reconnect observability. Observed on a 1 s tick inside the
    // per-relay task in nozzl-acquirer.
    pub relay_disconnects: IntCounterVec,                // labels: relay
    pub relay_reconnect_duration_seconds: HistogramVec,  // labels: relay

    // Writer-task durability signal. Incremented inside
    // chunk_writer::flush_and_truncate after a successful object-store PUT
    // and before the WAL truncate, so an increment is a durability ack
    // (chunk is in storage, regardless of whether the subsequent WAL
    // truncate succeeded). class ∈ {"replaceable","addressable"};
    // shard is a single lowercase hex char 0–f.
    pub chunks_published: IntCounterVec,                 // labels: class, kind, slug, shard

    // Companion to chunks_published — unix-seconds timestamp of the most
    // recent successful publish per writer. Set in the same block as the
    // counter increment. Needed because `time() - timestamp(counter)` reads
    // scrape time, not event time, so the natural-sounding stalled-writer
    // query against the counter alone does not work. Use:
    //   (time() - chunks_last_published_timestamp_seconds{...}) > 21600
    // to flag writers that haven't rolled in 6 h.
    pub chunks_last_published_timestamp_seconds: GaugeVec, // labels: class, kind, slug, shard
}

impl Metrics {
    /// Create a Metrics struct registered against the supplied registry.
    pub fn new(registry: &prometheus::Registry) -> Result<Self, MetricsError>;

    /// Force every label combination derivable from config to appear in
    /// /metrics at zero / baseline, instead of lazy-creating on first
    /// observation. Idempotent; safe to call once at startup after config
    /// is loaded. See the doc comment on the impl for the full rationale.
    pub fn preallocate_known_series(&self, seed: PreallocSeed<'_>);
}

/// Inputs to `Metrics::preallocate_known_series`. Borrowed slices —
/// callers build owned storage from their config and pass references.
pub struct PreallocSeed<'a> {
    pub relays: &'a [&'a str],
    pub kind_slugs: &'a [(u16, &'a str)],
    /// `(class_label, kind, slug)` — `class_label` is `"replaceable"` or
    /// `"addressable"`; Regular/Ephemeral kinds are filtered out by the
    /// caller because the writer never produces chunks for them.
    pub class_kind_slugs: &'a [(&'a str, u16, &'a str)],
}

/// Health probe — implemented by the binary crate. Metrics calls it for /health.
#[async_trait::async_trait]
pub trait HealthSource: Send + Sync {
    async fn is_healthy(&self) -> bool;
}

/// Spin up the HTTP endpoint on `addr`, serving /metrics and /health.
/// Returns the registered Metrics (callers record observations through it),
/// the registry (for any caller that wants to register additional metrics),
/// and a JoinHandle for the server task.
pub fn install(
    addr: SocketAddr,
    health: Arc<dyn HealthSource>,
) -> Result<(Arc<Metrics>, Arc<prometheus::Registry>, JoinHandle<()>), MetricsError>;

/// Update `checkpoint_lag_seconds` from a snapshot of
/// `(relay, kind, slug, since)` tuples and a caller-supplied `now`. Pure
/// helper — the orchestrator drives it on a 1 Hz tick so the gauge grows
/// when observations stop arriving. The checkpoint store doesn't know about
/// slugs, so the caller (the binary's gauge loop) joins them in from its
/// shared kind→slug mirror; pass `""` if no slug is known.
pub fn update_checkpoint_lag<I, S>(metrics: &Metrics, snapshot: I, now_unix_seconds: i64)
where
    I: IntoIterator<Item = (S, u16, S, i64)>,
    S: AsRef<str>;

#[derive(Debug, thiserror::Error)]
pub enum MetricsError { /* registration, bind, IO */ }
```

## Acceptance criteria

1. Every metric in [../design.md §5.D10](../design.md#d10--observability) is created with the documented name and label set.
2. `/metrics` returns Prometheus exposition format (`Content-Type: text/plain; version=0.0.4`).
3. `/health` returns 200 if `HealthSource::is_healthy().await == true`, else 503. Body is empty or a small JSON `{"status": "ok"|"unhealthy"}`.
4. The HTTP server uses `axum` (or `hyper`) on the provided socket address. Binds on startup; returns `MetricsError::Bind` if the port is taken.
5. Label values are sanitized — relay URLs go in unmodified (Prometheus tolerates them); reason strings are restricted to `[a-z_]+` (callers responsible).
6. `IntCounterVec::with_label_values(&[...])` calls in hot paths must not allocate beyond the first call per label tuple. (Prometheus crate handles this internally; we just need to keep the typed `Metrics` struct stable so callers reuse handles.)
7. `update_checkpoint_lag` writes `now - since` per `(relay, kind, slug)` in the snapshot. Negative values (clock skew, future-dated events) are written as-is rather than clamped, so operators see the raw signal.
8. `preallocate_known_series` exposes every (relays × kinds × slugs × classes × shards × probe-outcomes) label combination derivable from its input at zero on the registry, idempotent across repeat calls, and does not pre-allocate metrics with open-ended `reason` labels (`events_rejected_total`, `events_dropped_total`).

## Testing

Unit:
- `Metrics::new` registers exactly the documented metrics; double-construction against the same registry fails (registry detects duplicate registration).
- `update_checkpoint_lag` writes the right gauge values (positive lag, negative lag on future since, no-op on empty snapshot, repeat call overwrites).

Integration (`tests/`):
- `install` on a free port; `GET /metrics` returns content with the expected metric names; `GET /health` flips correctly with a stub `HealthSource`.
- Concurrent observation: spawn many tasks that increment a counter, verify final count is correct.

## Out of scope

- Pushing metrics anywhere (Pushgateway, OTLP). v1 is scrape-only.
- Tracing-to-OTel forwarding. Logs use `tracing` separately.
- Authentication on `/metrics`. Bind address controls exposure.
