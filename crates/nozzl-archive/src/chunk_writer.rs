//! Per-(class, kind, shard) writer task.
//!
//! Each task owns:
//! - one [`Wal`](crate::wal::Wal) on local disk;
//! - one [`ChunkBuilder`](crate::parquet_writer::ChunkBuilder) accumulating
//!   events in memory;
//! - an `mpsc::Receiver` for events demuxed by [`Demux`](crate::demux::Demux);
//! - thresholds for rolling the in-memory batch into a Parquet chunk.
//!
//! Lifecycle:
//!
//! ```text
//! spawn ─► replay WAL into builder ─► signal "ready"
//!                                          │
//!                                          ▼
//!                              recv loop (event mpsc + buffer-age timer)
//!                                          │
//!                                  threshold met OR shutdown
//!                                          ▼
//!                                  flush_chunk() ─► PUT to object_store
//!                                          │
//!                                          ▼
//!                              wal.truncate() (durability boundary cleared)
//! ```
//!
//! On graceful shutdown (sender of the event mpsc dropped, or explicit
//! `Shutdown` control message), the writer flushes its current buffer
//! regardless of size and truncates its WAL before exiting.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use nostr::{Event, EventId};
use nozzl_types::EventClass;
use object_store::{ObjectStore, PutMode, PutOptions};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::{Duration, Instant, MissedTickBehavior};
use ulid::Ulid;

use crate::config::ChunkWriterConfig;
use crate::parquet_writer::{ChunkBuilder, ChunkSerializeError};
use crate::path_enc::{chunk_path, date_from_unix_secs, wal_path, PubkeyShard};
use crate::wal::{self, ReplayItem, Wal, WalError};

/// Per-writer buffer-age budget: `chunk_max_buffer_seconds` minus a
/// deterministic per-writer offset, so writers whose age clocks started
/// together do not all come due in the same second.
///
/// **Why this exists.** #57 (Restart-synchronized buffer-age flush burst):
/// `buffer_started_at` is set on a writer's first push since its last
/// successful flush and is never refreshed, so a restart synchronizes the
/// whole fleet: every writer takes its first push within a second or two of
/// boot, and `chunk_max_buffer_seconds` later they all flush at once. On 2026-10-15 that put 443 flushes (340,095 rows) into
/// one second on a small instance, saturating its CPU and failing 178 of the 443
/// S3 PUTs on `connect_timeout`. The herd is the bug; the per-writer offset
/// removes it at the source. [`ChunkedArchive`](crate::demux::ChunkedArchive)'s
/// flush permits bound what is left (the size/rows/replay/ad-hoc triggers can
/// still coincide), so the two defenses are complements, not alternatives.
///
/// **Why subtract rather than add.** `chunk_max_buffer_seconds` is a *ceiling*
/// on the age of unflushed rows (it bounds visibility lag and WAL replay cost),
/// and the config documents it as such. Jittering downward keeps that promise
/// exactly; jittering upward would quietly break it. This is the same shape as
/// Cortex's `-ingester.chunk-age-jitter`, documented as "range of time to
/// subtract from `-ingester.max-chunk-age` to spread out flushes", against the
/// same 12 h default chunk age we run on dev.
///
/// **Why deterministic rather than random.** A hash of the writer key gives the
/// same offset for the same writer on every boot: reproducible in tests, and no
/// chance of a random draw re-clustering the fleet on a later restart. Cortex
/// reached the same conclusion, replacing its random jitter with
/// `-ingester.spread-flushes` ("spread series flushes across the whole period
/// of `MaxChunkAge`"), which derives each series' slot from its fingerprint.
/// Full-period spreading is the natural next step from here, but it needs a
/// minimum-age floor to avoid rolling nearly-empty chunks when a writer's slot
/// lands just after a size roll - that interacts with #6 (Parquet writer emits
/// tiny row groups), so it is left to a follow-up.
///
/// **Why 0.10 by default.** The burst above cost ~53 CPU-seconds of encode
/// (baseline removed) for 621 flushes, so ~0.09 CPU-s each. Spreading that
/// work over a window `W` on `C` CPUs adds `53 / (C * W)` utilization: with two
/// or more CPUs, `W >= 120 s` already keeps the burst under a quarter of the
/// host, and
/// `W = jitter * chunk_max_buffer_seconds` is 4,320 s (72 min) at 0.10 on dev's
/// 12 h budget - a ~35x margin, and ~0.1 flushes/s at the observed 443-writer
/// herd. Cortex shipped 20 min out of 12 h (2.8%), which would also do; the cost
/// of the larger window is only that the mean buffer age drops by half the
/// jitter (5%), i.e. ~5% more chunks, each ~5% smaller. For the low-rate writers
/// that dominate #6 (Parquet writer emits tiny row groups) that is a couple of
/// rows, so the margin is worth more than the packing.
///
/// **The window has to be much larger than the age tick.** The deadline is
/// polled once a second, so offsets spread over less than a second land every
/// writer on the same tick and spread nothing. Production budgets are hours, so
/// the window is thousands of ticks wide; the 1-second budgets used in tests get
/// a 100 ms window, which the tick swallows - harmless, and the reason those
/// tests still see a flush within one tick of the budget.
///
/// Clamps: `jitter` outside `0.0..=0.5` is clamped (0.5 keeps "age ~= budget"
/// meaningful; beyond that, use full-period spreading instead), and a
/// non-finite value is treated as 0. A window that rounds to zero milliseconds
/// yields the unjittered budget.
#[must_use]
pub fn jittered_buffer_budget(key: &WriterKey, seconds: u64, jitter: f64) -> Duration {
    let base_ms = seconds.max(1).saturating_mul(1_000);
    let frac = if jitter.is_finite() {
        jitter.clamp(0.0, 0.5)
    } else {
        0.0
    };
    // `base_ms as f64` is exact for any budget under ~285,000 years, and the
    // product is <= base_ms / 2, so the cast back cannot overflow.
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let window_ms = (base_ms as f64 * frac) as u64;
    if window_ms == 0 {
        return Duration::from_millis(base_ms);
    }
    Duration::from_millis(base_ms - writer_key_hash(key) % window_ms)
}

/// FNV-1a over the writer key's three components. Deliberately hand-rolled
/// rather than `DefaultHasher`: `std`'s hasher is explicitly not stable across
/// releases, and this value decides when a writer flushes, so it must be
/// reproducible across rebuilds and asserted in tests.
fn writer_key_hash(key: &WriterKey) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    let mut eat = |b: u8| {
        h ^= u64::from(b);
        h = h.wrapping_mul(PRIME);
    };
    eat(key.class as u8);
    eat((key.kind >> 8) as u8);
    eat((key.kind & 0xff) as u8);
    eat(key.shard.as_char() as u8);
    h
}

/// Identity of a single writer task. Stable for the writer's lifetime.
///
/// The per-kind slug is **not** part of the key — it's no longer on disk and is
/// functionally determined by `kind`. Keying on `{class, kind, shard}` only
/// guarantees exactly one writer per on-disk chunk dir / WAL file even if the
/// slug were edited mid-run (which would otherwise spawn a second writer
/// contending for the same slugless path). The slug rides along on
/// [`WriterState`] purely for the metric label, captured when the writer is
/// first spawned.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WriterKey {
    pub class: EventClass,
    pub kind: u16,
    pub shard: PubkeyShard,
}

/// Inbound message to a writer task.
#[derive(Debug)]
pub enum WriterMessage {
    /// A fresh event observed by the acquirer. Will be appended to the WAL,
    /// pushed to the in-memory builder, and (eventually) included in the next
    /// chunk roll.
    ///
    /// `ingested_at` is the demux's wall-clock at observation. Stored
    /// alongside the event so replays from WAL preserve original ingest time.
    Event {
        event: Box<Event>,
        ingested_at: u64,
    },
    /// Force a flush + WAL truncate. Tests use this to verify chunk contents
    /// without waiting for a threshold. Fire-and-forget; logged as
    /// `trigger=force`. Production ad-hoc flushes use [`WriterMessage::AdhocFlush`]
    /// instead so the two are distinguishable in logs.
    ForceFlush,
    /// Operator-triggered ad-hoc flush (HTTP admin endpoint). Publishes the
    /// current buffer immediately and reports the result back on `ack` so the
    /// caller can block until the chunk is durable. Logged as `trigger=adhoc`.
    AdhocFlush {
        ack: oneshot::Sender<ShardFlushOutcome>,
    },
}

/// Result of an [`WriterMessage::AdhocFlush`], sent back to the requester on the
/// message's `ack` channel. `published` is `false` with `rows == 0` when the
/// buffer was already empty (a no-op flush); `error` is `Some` when the chunk
/// PUT failed (the rows remain durable in the WAL for a later retry).
#[derive(Debug, Clone)]
pub struct ShardFlushOutcome {
    /// Rows written to the published chunk (0 when nothing was published).
    pub rows: u64,
    /// Whether a chunk was actually published this flush.
    pub published: bool,
    /// Set when the flush was attempted but failed; the rows stay in the WAL.
    pub error: Option<String>,
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("chunk serialize error: {0}")]
    Serialize(#[from] ChunkSerializeError),
    #[error("object store error publishing chunk {path}: {source}")]
    Store {
        path: String,
        #[source]
        source: object_store::Error,
    },
    #[error("filesystem error preparing wal directory at {path}: {source}")]
    FsPrep {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("no chunk path could be derived for class {class:?}")]
    InvalidClass { class: EventClass },
}

/// Terminal outcome of a writer task, reported through its `JoinHandle` so
/// [`crate::demux::ChunkedArchive::shutdown`] can emit a single reconciled
/// summary line (`shutdown flush complete: writers/flushed/failed/…`) instead
/// of operators having to grep and tally N per-writer lines. A shutdown-only
/// counter is deliberately *not* a metric: the event happens in the process's
/// final moments, so it would rarely survive to a Prometheus scrape and would
/// reset on the next boot — a log line ships to Loki and persists.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WriterExit {
    /// Exited during startup (non-archived class, or a WAL dir/open failure)
    /// before entering the event loop — never held buffered rows.
    Aborted,
    /// Graceful shutdown with an empty buffer — nothing to flush.
    Empty,
    /// Graceful shutdown; the final flush published successfully.
    Flushed,
    /// Graceful shutdown; the final flush failed. The rows stay in the WAL and
    /// replay on the next boot (`flush_and_truncate` truncates only after a
    /// successful PUT), so this is deferred recovery, not data loss.
    Failed,
}

/// Run a writer task. Consumes the receiver; returns when the channel closes
/// or after a `Shutdown` control signal.
///
/// On error, the task logs the failure and continues — chunk publish errors
/// are retried on the next roll opportunity (the WAL still holds the data).
/// A persistent `object_store` failure will eventually trigger backpressure
/// from the channel filling up. The returned [`WriterExit`] reports the task's
/// terminal state for the shutdown summary.
#[allow(clippy::too_many_lines)] // single linear state machine: replay → tokio::select! event loop
// A writer task's whole world arrives here: identity, config, the two external
// resources (WAL root, store), its inbox, and the two process-wide handles
// (metrics, flush permits). Bundling them would only move the list.
#[allow(clippy::too_many_arguments)]
pub async fn run_writer(
    key: WriterKey,
    slug: String,
    cfg: ChunkWriterConfig,
    wal_root: std::path::PathBuf,
    store: Arc<dyn ObjectStore>,
    mut rx: mpsc::Receiver<WriterMessage>,
    metrics: Option<Arc<nozzl_metrics::Metrics>>,
    flush_permits: Arc<Semaphore>,
) -> WriterExit {
    let Some(wal_file_path) = wal_path(&wal_root, key.class, key.kind, key.shard) else {
        tracing::error!(?key, "writer task spawned for non-archived class; exiting");
        return WriterExit::Aborted;
    };

    if let Some(parent) = wal_file_path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            tracing::error!(error = %e, ?parent, "could not create wal parent dir; exiting writer");
            return WriterExit::Aborted;
        }
    }

    // Phase 1: replay any existing WAL into the in-memory builder, WITHOUT
    // appending those events back into the WAL — the WAL already contains
    // them. After replay, the writer enters normal operation.
    let mut state = WriterState::new(slug);

    let replay_count = match wal::replay(&wal_file_path).await {
        Ok(items) => {
            let mut events = 0usize;
            for item in items {
                match item {
                    ReplayItem::Event { event, ingested_at } => {
                        if state.batch_ids.insert(event.id) {
                            state.builder.push(&event, ingested_at);
                            state.last_ingested_at = ingested_at.max(state.last_ingested_at);
                            events += 1;
                        }
                    }
                    ReplayItem::Corrupt { offset, len, reason } => {
                        tracing::warn!(?key, offset, len, %reason, "wal record corrupt; skipping");
                    }
                }
            }
            events
        }
        Err(e) => {
            tracing::error!(?key, error = %e, "wal replay failed; continuing with empty builder");
            0
        }
    };
    if replay_count > 0 {
        tracing::info!(?key, replay_count, "wal replay produced events");
    }

    // Open the WAL for fresh appends. Existing content (the records we just
    // replayed) is preserved; we'll truncate it after the next successful
    // chunk publish.
    let mut wal_handle = match Wal::open(&wal_file_path).await {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(?key, error = %e, "wal open failed; writer exiting");
            return WriterExit::Aborted;
        }
    };

    // Phase 1.5: if WAL replay produced rows, publish them now as a chunk
    // tagged `trigger=replay`. Without this the recovered rows would sit
    // invisible in the in-memory builder until something else triggers a
    // roll — under the default `chunk_max_buffer_seconds = 3600` that's a
    // 1 h visibility lag after every restart, even with the WAL safely
    // recovered. Flushing immediately makes the post-crash state observable
    // as soon as the writer is alive.
    //
    // On flush failure the rows remain durable in the WAL: `truncate` only
    // runs after a successful PUT, so the next restart will replay them
    // and try again. `state.buffer_started_at` is still `None` here (set
    // only on the main-loop push path), so it stays consistent whether
    // the replay flush succeeds or fails.
    if !state.builder.is_empty() {
        if let Err(e) = flush_and_truncate(
            &key,
            &store,
            &mut wal_handle,
            &mut state,
            RollReason::Replay,
            metrics.as_deref(),
            cfg.parquet_zstd_level,
            &flush_permits,
        )
        .await
        {
            tracing::error!(
                ?key,
                error = %e,
                "post-replay flush failed; rows remain in WAL for next restart"
            );
        }
    }

    // Phase 2: main recv loop. We multiplex incoming events with the
    // buffer-age timer via `tokio::select!`.
    //
    // The buffer-age trigger gates on the wall-clock age of the *oldest*
    // unflushed event in the in-memory builder — `buffer_started_at` is set
    // to `Some(now)` on the first push since the last successful flush, and
    // cleared back to `None` by `flush_and_truncate`. Because the clock is
    // *not* refreshed on subsequent pushes, the trigger fires unconditionally
    // once the configured budget elapses, regardless of arrival rate. This
    // bounds buffered-data age and WAL replay cost under any traffic shape.
    //
    // Replaces the original idle-since-last-event trigger, which was
    // structurally invisible to shards under sustained traffic.
    //
    // Tick cadence is a fixed 1 s and independent of
    // `chunk_max_buffer_seconds`: short (test-scale, ≥1 s) budgets fire
    // within a single tick, and long (production-scale) budgets pay
    // essentially nothing for a 1 Hz poll. `max(1)` on the seconds value
    // pins the minimum buffer budget at 1 s; sub-second budgets are not
    // supported. Use `interval_at(now + period, period)` so the first
    // tick fires after a full period — not immediately — guaranteeing
    // no premature flush at startup.
    // Per-writer, not `chunk_max_buffer_seconds` flat: see
    // `jittered_buffer_budget` for why the deadline is spread and why it is
    // spread *downward*. Computed once, so a writer's deadline is stable for
    // its whole lifetime.
    let max_buffer_dur = jittered_buffer_budget(
        &key,
        cfg.chunk_max_buffer_seconds,
        cfg.chunk_buffer_age_jitter,
    );
    let tick_period = Duration::from_secs(1);
    let mut age_tick =
        tokio::time::interval_at(Instant::now() + tick_period, tick_period);
    age_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Defensive: under the happy path `state.buffer_started_at` is already
    // `None` here — either no replay happened or the replay flush above
    // cleared it. If a replay flush failed and somehow left rows in the
    // builder anyway, start the clock so they aren't forgotten.
    if state.buffer_started_at.is_none() && !state.builder.is_empty() {
        state.buffer_started_at = Some(Instant::now());
    }

    loop {
        tokio::select! {
            biased;
            maybe_msg = rx.recv() => {
                match maybe_msg {
                    Some(WriterMessage::Event { event, ingested_at }) => {
                        if !state.batch_ids.insert(event.id) {
                            // duplicate within current batch (e.g. WAL
                            // replay-then-resend); silently skip.
                            continue;
                        }
                        if let Err(e) = wal_handle.append(ingested_at, &event).await {
                            tracing::error!(?key, error = %e, "wal append failed; dropping event");
                            // Remove from dedup set so a retry would land.
                            state.batch_ids.remove(&event.id);
                            continue;
                        }
                        state.last_ingested_at = ingested_at.max(state.last_ingested_at);
                        state.builder.push(&event, ingested_at);
                        // Start the buffer-age clock on the first push since
                        // the last flush. Subsequent pushes leave it intact.
                        if state.buffer_started_at.is_none() {
                            state.buffer_started_at = Some(Instant::now());
                        }

                        if let Some(reason) = should_roll(&state.builder, &cfg) {
                            if let Err(e) = flush_and_truncate(
                                &key, &store, &mut wal_handle, &mut state, reason,
                                metrics.as_deref(), cfg.parquet_zstd_level, &flush_permits,
                            ).await {
                                tracing::error!(?key, error = %e, "chunk publish failed; will retry on next trigger");
                            }
                        }
                    }
                    Some(WriterMessage::ForceFlush) => {
                        if !state.builder.is_empty() {
                            if let Err(e) = flush_and_truncate(
                                &key, &store, &mut wal_handle, &mut state, RollReason::Force,
                                metrics.as_deref(), cfg.parquet_zstd_level, &flush_permits,
                            ).await {
                                tracing::error!(?key, error = %e, "force-flush failed");
                            }
                        }
                    }
                    Some(WriterMessage::AdhocFlush { ack }) => {
                        // Same single-actor flush path as every other trigger,
                        // so an ad-hoc publish updates `chunks_published_total`
                        // and `chunks_last_published_timestamp_seconds` exactly
                        // like a timer/size/rows roll. Capture the row count
                        // before flushing — `flush_and_truncate` clears the
                        // builder on success.
                        let outcome = if state.builder.is_empty() {
                            ShardFlushOutcome { rows: 0, published: false, error: None }
                        } else {
                            let rows =
                                u64::try_from(state.builder.row_count()).unwrap_or(u64::MAX);
                            match flush_and_truncate(
                                &key, &store, &mut wal_handle, &mut state, RollReason::Adhoc,
                                metrics.as_deref(), cfg.parquet_zstd_level, &flush_permits,
                            ).await {
                                Ok(()) => ShardFlushOutcome { rows, published: true, error: None },
                                Err(e) => {
                                    tracing::error!(?key, error = %e, "ad-hoc flush failed; rows remain in WAL for a later trigger");
                                    ShardFlushOutcome {
                                        rows: 0,
                                        published: false,
                                        error: Some(e.to_string()),
                                    }
                                }
                            }
                        };
                        if ack.send(outcome).is_err() {
                            tracing::warn!(?key, "ad-hoc flush ack receiver dropped before result delivery (caller went away)");
                        }
                    }
                    None => {
                        // Channel closed: graceful shutdown. Flush the
                        // remaining buffer (if any) and exit, reporting the
                        // outcome for the shutdown summary.
                        let exit = if state.builder.is_empty() {
                            WriterExit::Empty
                        } else {
                            match flush_and_truncate(
                                &key, &store, &mut wal_handle, &mut state, RollReason::Shutdown,
                                metrics.as_deref(), cfg.parquet_zstd_level, &flush_permits,
                            ).await {
                                Ok(()) => WriterExit::Flushed,
                                Err(e) => {
                                    tracing::error!(?key, error = %e, "shutdown flush failed");
                                    WriterExit::Failed
                                }
                            }
                        };
                        tracing::debug!(?key, ?exit, "writer task exiting");
                        return exit;
                    }
                }
            }
            _ = age_tick.tick() => {
                if let Some(started) = state.buffer_started_at {
                    if started.elapsed() >= max_buffer_dur && !state.builder.is_empty() {
                        if let Err(e) = flush_and_truncate(
                            &key, &store, &mut wal_handle, &mut state, RollReason::BufferAge,
                            metrics.as_deref(), cfg.parquet_zstd_level, &flush_permits,
                        ).await {
                            tracing::error!(?key, error = %e, "buffer-age flush failed");
                        }
                    }
                }
            }
        }
    }
}

/// Why a chunk roll was triggered. Threaded through `flush_and_truncate` and
/// emitted as a structured field on the `"chunk published"` log so operators
/// can attribute rolls to the right cause without grepping the source.
#[derive(Copy, Clone, Debug)]
enum RollReason {
    /// `estimated_bytes >= chunk_max_size_bytes`
    Size,
    /// `row_count >= chunk_max_rows`
    Rows,
    /// `buffer_started_at.elapsed() >= chunk_max_buffer_seconds`
    BufferAge,
    /// Explicit `WriterMessage::ForceFlush` (test helper)
    Force,
    /// Operator-triggered `WriterMessage::AdhocFlush` (HTTP admin endpoint)
    Adhoc,
    /// Channel-closed graceful shutdown
    Shutdown,
    /// WAL replay on writer startup produced rows; flushed immediately so
    /// the recovered data becomes visible without waiting for the
    /// buffer-age timer.
    Replay,
}

impl RollReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Size => "size",
            Self::Rows => "rows",
            Self::BufferAge => "buffer_age",
            Self::Force => "force",
            Self::Adhoc => "adhoc",
            Self::Shutdown => "shutdown",
            Self::Replay => "replay",
        }
    }
}

/// Mutable per-writer in-memory state, threaded through the main loop
/// and `flush_and_truncate` as a single bundle.
///
/// Bundling these four fields enforces the failure-recovery invariant by
/// signature: a flush either clears `builder`, `batch_ids`, and
/// `buffer_started_at` together (on success) or leaves all three intact
/// (on pre-PUT failure). The previous 8-arg parameter list invited
/// drift between callers if anyone forgot to thread one of them.
///
/// External resources — the `Wal` handle and the `Arc<dyn ObjectStore>` —
/// are deliberately *not* in this struct. They have independent lifecycles
/// (open at writer startup, shared with the process respectively) and
/// keeping them out of `WriterState` keeps the bundle's borrow scope
/// narrow.
struct WriterState {
    builder: ChunkBuilder,
    batch_ids: HashSet<EventId>,
    /// Monotonic clock at the first push since the last successful flush.
    /// `None` while the builder is empty; drives the buffer-age trigger.
    buffer_started_at: Option<Instant>,
    /// Wall-clock unix seconds of the newest event ever observed by this
    /// writer. Becomes the partition-date hint on the next flush.
    last_ingested_at: u64,
    /// The kind's slug, captured when this writer was spawned. Not on disk and
    /// not part of [`WriterKey`]; held only to label the `chunks_published` /
    /// `chunks_last_published_timestamp_seconds` metrics.
    slug: String,
}

impl WriterState {
    fn new(slug: String) -> Self {
        Self {
            builder: ChunkBuilder::new(),
            batch_ids: HashSet::new(),
            buffer_started_at: None,
            last_ingested_at: unix_now(),
            slug,
        }
    }
}

fn should_roll(builder: &ChunkBuilder, cfg: &ChunkWriterConfig) -> Option<RollReason> {
    if builder.row_count() >= cfg.chunk_max_rows {
        Some(RollReason::Rows)
    } else if builder.estimated_bytes() >= cfg.chunk_max_size_bytes {
        Some(RollReason::Size)
    } else {
        None
    }
}

/// Encode the buffered rows, PUT them, and truncate the WAL. Wraps
/// [`flush_and_truncate_inner`] with the two cross-cutting concerns every roll
/// trigger needs and none of them should have to remember:
///
/// - **The flush permit.** Held across encode *and* PUT (both are the contended
///   resource: ZSTD-9 runs inline on a worker thread, and a PUT needs a TLS
///   connection), acquired here so no call site can bypass the bound. The
///   acquisition is deliberately in this wrapper and not inside
///   [`flush_and_truncate_inner`] around the `put_opts` alone: the encode is
///   the CPU hog that starves in-flight PUT futures, so a bound that covered
///   only the PUT would bound the wrong thing.
///
///   Writers that cannot get a permit wait, which back-pressures their own
///   inbox rather than S3. The signal chain, in order: a parked writer fills
///   its own `WRITER_CHANNEL_CAPACITY`-deep channel first (which has **no
///   gauge** - that is the gap), only then does the single central consumer
///   block and `channel_depth` move, and only if the pool then laps that
///   consumer does anything become loss, counted by
///   `notif_channel_lagged_total` / `notif_fanout_dropped_total`. So those two
///   counters, not `channel_depth`, are what to alert on if the bound is ever
///   set too low; `channel_depth` is a mid-chain hint that moves late.
/// - **`chunk_flush_failures_total`.** Incremented on every error return, which
///   is exactly "the chunk was not published": a WAL-truncate failure after a
///   successful PUT logs and returns `Ok`, so it does not count here.
#[allow(clippy::too_many_arguments)] // same list as `run_writer`, minus the inbox
async fn flush_and_truncate(
    key: &WriterKey,
    store: &Arc<dyn ObjectStore>,
    wal_handle: &mut Wal,
    state: &mut WriterState,
    trigger: RollReason,
    metrics: Option<&nozzl_metrics::Metrics>,
    zstd_level: i32,
    flush_permits: &Semaphore,
) -> Result<(), WriterError> {
    let _permit = flush_permits
        .acquire()
        .await
        .expect("flush semaphore never closed");
    let outcome = flush_and_truncate_inner(
        key, store, wal_handle, state, trigger, metrics, zstd_level,
    )
    .await;
    if outcome.is_err() {
        if let Some(m) = metrics {
            // Mirrors the `chunks_published` label convention, minus `shard`:
            // the per-shard detail is on the failure's own log line, and
            // multiplying 16 shards by 7 triggers would cost ~7,600 series for
            // a counter that reads 0 on a healthy deployment.
            let class_label = key.class.chunk_metric_label().unwrap_or("other");
            let kind_str = key.kind.to_string();
            m.chunk_flush_failures
                .with_label_values(&[
                    class_label,
                    kind_str.as_str(),
                    state.slug.as_str(),
                    trigger.as_str(),
                ])
                .inc();
        }
    }
    outcome
}

async fn flush_and_truncate_inner(
    key: &WriterKey,
    store: &Arc<dyn ObjectStore>,
    wal_handle: &mut Wal,
    state: &mut WriterState,
    trigger: RollReason,
    metrics: Option<&nozzl_metrics::Metrics>,
    zstd_level: i32,
) -> Result<(), WriterError> {
    let row_count = state.builder.row_count();
    let date = date_from_unix_secs(state.last_ingested_at);
    let ulid = Ulid::new();
    let path = chunk_path(key.class, key.kind, key.shard, date, ulid)
        .ok_or(WriterError::InvalidClass { class: key.class })?;

    // Serialize from a borrow so a serialize or PUT failure leaves the
    // builder, dedup set, and buffer-age clock intact — the next size /
    // rows / age trigger then retries the same rows naturally. Previous
    // ordering (`mem::replace` before serialize) dropped the in-memory
    // rows on any pre-PUT error, leaving them recoverable only via WAL
    // replay on next process restart.
    let bytes = state.builder.serialize(zstd_level)?;
    let payload_len = bytes.len();
    let payload = object_store::PutPayload::from(bytes);

    store
        .put_opts(&path, payload, PutOptions::from(PutMode::Create))
        .await
        .map_err(|e| WriterError::Store {
            path: path.to_string(),
            source: e,
        })?;

    // Increment the published-chunks counter and update the
    // last-published-timestamp gauge before the WAL truncate so a truncate
    // failure doesn't suppress either signal. The chunk is durable in object
    // storage at this point regardless of what truncate does next.
    //
    // The gauge is the companion that makes "time since last publish per
    // writer" actually queryable: a query like `time() - timestamp(counter)`
    // returns scrape time, not event time — every scrape produces a sample
    // for the counter regardless of whether it incremented, so timestamp()
    // always reads ~now. Storing the event time as a gauge gives dashboards
    // a real signal.
    if let Some(m) = metrics {
        // Writer tasks for non-archived classes are guarded against at
        // `run_writer` entry (returns early when `wal_path()` yields
        // `None`), so `chunk_metric_label()` returning `None` here would
        // mean an invariant was already violated. Fall through to a
        // benign label rather than panic — single source of truth for
        // the archived-class predicate is `EventClass::chunk_metric_label`.
        let class_label = key.class.chunk_metric_label().unwrap_or("other");
        let kind_str = key.kind.to_string();
        let shard_str = key.shard.as_char().to_string();
        let labels = [
            class_label,
            kind_str.as_str(),
            state.slug.as_str(),
            shard_str.as_str(),
        ];
        m.chunks_published.with_label_values(&labels).inc();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        m.chunks_last_published_timestamp_seconds
            .with_label_values(&labels)
            .set(now);
    }

    // PUT succeeded; the chunk is the source of truth from here. Drop the
    // in-memory rows and dedup state, then truncate the WAL on a
    // best-effort basis: if truncate fails, the rows are durable in both
    // the new chunk and the (still-populated) WAL, and the next restart's
    // replay flush will emit a duplicate chunk that read-side dedup
    // collapses. Don't roll back the in-memory clear; that would re-emit
    // the same rows next flush and double-archive without dedup help.
    state.builder = ChunkBuilder::new();
    state.batch_ids.clear();
    state.buffer_started_at = None;
    if let Err(e) = wal_handle.truncate().await {
        tracing::error!(
            ?key,
            error = %e,
            "wal truncate after successful chunk publish failed; \
             a redundant replay chunk will land on next restart"
        );
    }
    tracing::info!(
        ?key,
        chunk_path = %path,
        row_count,
        bytes = payload_len,
        trigger = trigger.as_str(),
        "chunk published"
    );
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests;
