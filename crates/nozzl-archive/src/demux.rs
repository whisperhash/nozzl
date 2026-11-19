//! Per-(class, kind, shard) writer task demux.
//!
//! `ChunkedArchive` implements the [`Archive`] trait and routes each
//! `put_event` call to the correct writer task. Writer tasks are spawned
//! lazily on first observation of a `(class, kind, shard)` triple — no
//! upfront enumeration of kinds is required, which keeps hot-reload of
//! `kinds.toml` simple (the acquirer just starts emitting events for new
//! kinds; the demux spawns writers as it sees them).
//!
//! Trade-off: the cost of "lazy spawn" is one extra synchronization moment
//! per *new* `(kind, shard)` combination — the demux acquires a write lock
//! to insert into the registry. Steady-state dispatch is read-only and
//! lock-free except for the brief `HashMap` lookup.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures_util::future::join_all;
use nostr::Event;
use nozzl_types::EventClass;
use object_store::ObjectStore;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;

use crate::archive::Archive;
use crate::chunk_writer::{WriterExit, WriterKey, WriterMessage, run_writer};
use crate::config::ChunkWriterConfig;
use crate::error::{ArchiveError, PutOutcome, SkipReason};
use crate::path_enc::PubkeyShard;

/// Capacity of each per-writer mpsc channel. Larger = more headroom under
/// burst, more memory; smaller = tighter backpressure to the acquirer.
const WRITER_CHANNEL_CAPACITY: usize = 1024;

/// Implementation of `Archive` that writes Parquet chunks via a registry of
/// per-`(class, kind, shard)` writer tasks. Each writer task buffers events
/// in memory + a local-disk WAL, rolling chunks on size/row/idle thresholds.
///
/// Construction does NOT spawn any writer tasks; the first `put_event` for
/// a given `(class, kind, shard)` lazily spawns the task and registers it.
/// Use [`ChunkedArchive::shutdown`] to drop all senders and await every
/// writer's final flush.
pub struct ChunkedArchive {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<dyn ObjectStore>,
    wal_root: PathBuf,
    cfg: ChunkWriterConfig,
    metrics: Option<Arc<nozzl_metrics::Metrics>>,
    writers: RwLock<HashMap<WriterKey, WriterEntry>>,
    /// Process-wide flush bound, shared by every writer and every roll
    /// trigger. See [`ChunkedArchive::flush_permits`].
    flush_permits: Arc<Semaphore>,
}

struct WriterEntry {
    sender: mpsc::Sender<WriterMessage>,
    handle: JoinHandle<WriterExit>,
}

/// Bucketed writer terminal outcomes for the shutdown summary log. By
/// construction `flushed + failed + empty + aborted` equals the number of
/// writers tallied.
#[derive(Debug, Default, PartialEq, Eq)]
struct ShutdownTally {
    flushed: usize,
    failed: usize,
    empty: usize,
    aborted: usize,
}

/// Categorize each writer's [`WriterExit`] into a [`ShutdownTally`]. Split out
/// as a pure function so the categorization is unit-testable without a tracing
/// capture (the summary is otherwise only observable as a log side effect).
fn tally_exits(exits: impl IntoIterator<Item = WriterExit>) -> ShutdownTally {
    let mut t = ShutdownTally::default();
    for exit in exits {
        match exit {
            WriterExit::Flushed => t.flushed += 1,
            WriterExit::Failed => t.failed += 1,
            WriterExit::Empty => t.empty += 1,
            WriterExit::Aborted => t.aborted += 1,
        }
    }
    t
}

/// A point-in-time measurement of resident storage for one archived kind,
/// produced by [`ChunkedArchive::scan_resident_usage`].
///
/// `file_count` is tallied in the same listing pass as `total_bytes`; both are
/// surfaced as per-kind gauges (`storage_resident_bytes` /
/// `storage_resident_files`) from a single scan — no extra directory walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindResidentUsage {
    /// Bare class metric label: `"replaceable"`, `"addressable"`, or `"regular"`
    /// (from `EventClass::chunk_metric_label`) — deliberately NOT the on-disk
    /// Hive segment `c=replaceable`. This is what lands on `storage_resident_*`.
    pub class: &'static str,
    /// Nostr event kind.
    pub kind: u16,
    /// Human-readable kind slug from `kinds.toml` (e.g. `profile-metadata`),
    /// resolved from the config `kind→slug` map — empty if the kind isn't
    /// configured. No longer parsed from the (slugless) on-disk path.
    pub slug: String,
    /// Sum of resident `.parquet` object sizes for this kind, in bytes.
    pub total_bytes: u64,
    /// Number of resident `.parquet` objects for this kind.
    pub file_count: u64,
}

/// A point-in-time measurement of one writer's on-disk WAL file size, produced
/// by [`ChunkedArchive::scan_wal_usage`].
///
/// Unlike [`KindResidentUsage`] this is **local-disk and per-writer** (per
/// `(class, kind, shard)`): the WAL always lives on local disk, even when the
/// archive backend is S3. Surfaced as the per-shard `wal_bytes` gauge. A
/// just-flushed / idle writer reports `0` (the file is truncated via
/// `set_len(0)`, never deleted), so every live writer's series stays present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalShardUsage {
    /// Bare class metric label (`"replaceable"` / `"addressable"` / `"regular"`)
    /// from `EventClass::chunk_metric_label` — matches `chunks_published_total`.
    pub class: &'static str,
    /// Nostr event kind.
    pub kind: u16,
    /// Human-readable kind slug from the config `kind→slug` map (empty if the
    /// kind isn't configured — the file is still measured).
    pub slug: String,
    /// Shard label: single lowercase hex char `0`–`f` (`PubkeyShard::as_char`).
    pub shard: char,
    /// Bytes currently occupied by this writer's `.wal` file on local disk.
    pub bytes: u64,
}

/// Decode a `c=<class>-k=<kind>-p=<shard>.wal` filename into its
/// `(class, kind, shard)` triple, or `None` if it doesn't parse. Shared by the
/// `wal_bytes` scan; `recover_from_wal` keeps its own copy because at startup an
/// unparseable name is an alarm worth a per-case WARN, whereas the scan runs
/// every tick and skips silently.
fn parse_wal_filename(name: &str) -> Option<(EventClass, u16, PubkeyShard)> {
    let stem = name.strip_suffix(".wal")?;
    let (class_seg, rest) = stem.split_once("-k=")?;
    let class = crate::path_enc::parse_class_segment(class_seg)?;
    let (kind_str, shard_seg) = rest.split_once("-p=")?;
    let kind = kind_str.parse::<u16>().ok()?;
    let shard = PubkeyShard::from_pubkey_hex(shard_seg)?;
    Some((class, kind, shard))
}

impl ChunkedArchive {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        wal_root: PathBuf,
        cfg: ChunkWriterConfig,
        metrics: Option<Arc<nozzl_metrics::Metrics>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                wal_root,
                cfg,
                metrics,
                writers: RwLock::new(HashMap::new()),
                // `max(1)` because a zero-permit semaphore would park every
                // flush forever. `nozzl-config` already rejects 0; this is the
                // second line of defense for callers that build the config
                // programmatically (tests, tools).
                flush_permits: Arc::new(Semaphore::new(cfg.chunk_flush_concurrency.max(1))),
            }),
        }
    }

    /// Force-flush every currently-registered writer. Test helper. Fire-and-forget
    /// (`WriterMessage::ForceFlush`); for the operator-facing, confirmed,
    /// per-kind flush see [`FlushControl::flush_kind`].
    pub async fn force_flush_all(&self) {
        let senders: Vec<mpsc::Sender<WriterMessage>> = {
            let g = self.inner.writers.read().expect("writers lock poisoned");
            g.values().map(|w| w.sender.clone()).collect()
        };
        for s in senders {
            let _ = s.send(WriterMessage::ForceFlush).await;
        }
    }

    /// The process-wide flush bound: how many writers may be inside the
    /// encode-then-PUT step of a chunk flush at once, across **every** roll
    /// trigger (size, rows, buffer-age, replay, force, ad-hoc, shutdown).
    /// Sized by `chunk_flush_concurrency` (default 32) and acquired inside
    /// `flush_and_truncate`, so no trigger can bypass it.
    ///
    /// The archive spawns one writer per (class, kind, pubkey-shard), which on
    /// dev is over a thousand. Two things make them flush together:
    ///
    /// - **Shutdown.** Dropping every writer's `Sender` is what starts its final
    ///   flush, so an unbounded shutdown PUTs every non-empty buffer at the same
    ///   instant. On 2026-07-01 a ~68-way fan-out produced 68 simultaneous
    ///   client-side send failures ("error sending request") within 0.3 s, and
    ///   the same deploy's unbounded run produced 342 - #26 (Unbounded
    ///   shutdown-flush fan-out fails many S3 PUTs at once). Bounding it fixed
    ///   that.
    /// - **Any aligned steady-state trigger.** A restart synchronizes every
    ///   writer's buffer-age clock, so `chunk_max_buffer_seconds` later they all
    ///   come due in the same second: 443 flushes and 178 failed PUTs on
    ///   2026-10-15 - #57 (Restart-synchronized buffer-age flush burst). Boot
    ///   replay is the same shape: 342 writers replayed at once after the
    ///   2026-07-01 incident. The bound added for #26 (Unbounded
    ///   shutdown-flush fan-out fails many S3 PUTs at once) never covered
    ///   either, because it lived in `shutdown()`.
    ///
    /// A failed flush is *not* data loss - the rows stay in the WAL and are
    /// republished by the next trigger or by replay on the next boot
    /// (`flush_and_truncate` truncates only after a successful PUT) - but it
    /// defers recovery, wastes the encode, and floods the log.
    ///
    /// **Choosing the bound.** A shutdown must finish inside the process budget
    /// (`SHUTDOWN_BUDGET` = 120 s in `nozzl::run`), so with `N` non-empty
    /// writers and per-flush latency `L` the drain needs `N/K * L < 120 s`. At
    /// the dev writer count (`N` ~= 1,000) with a realistic `L ~= 1 s`,
    /// `K = 32` gives ~= 31 s, which matches the measured per-generation cost
    /// of the five shutdowns since the bound shipped: 32.8 s over ~26
    /// generations at 832 writers is ~1.26 s each. (The 1.35 s shutdown of
    /// 2026-07-01 is excluded - 205 of 573 writers were non-empty and it ran
    /// well under one generation.) Local-FS flushes are sub-millisecond, so
    /// the bound costs nothing there.
    ///
    /// Raising [`crate::archive`]'s connect timeout to 15 s is per-flush
    /// insurance, but it does lengthen the pathological aggregate: against a
    /// blackholed endpoint, where a `PutMode::Create` connect timeout does not
    /// retry, the drain costs `ceil(N/K) * 15 s` rather than `* 5 s`. Rows are
    /// safe either way (they stay in the WAL) and `SHUTDOWN_BUDGET` enforces
    /// liveness, so this trades a longer teardown for far fewer failures.
    ///
    /// **One shared set rather than a shutdown-only one** (decided 2026-10-16):
    /// one knob and one invariant, and the steady-state paths are the ones
    /// failing now. The only new interaction is that steady-state flushes may
    /// hold permits when `SIGTERM` lands, so the shutdown drain can wait up to
    /// one flush latency for its first permits - a few seconds against a 120 s
    /// budget that has never been exceeded. A per-PUT timeout was considered
    /// again and declined again: at `N = 1,017`, `K = 32` and a 10 s cap the
    /// bound is still 318 s, so it buys no guarantee while adding a way to turn
    /// a healthy-but-slow PUT into a WAL replay.
    ///
    /// Splitting encode parallelism from PUT parallelism (they are different
    /// resources) is the natural refinement, but it only pays once the encode
    /// moves off the worker threads - #30 (Shutdown flush starves the tokio
    /// runtime).
    fn flush_permits(&self) -> &Arc<Semaphore> {
        &self.inner.flush_permits
    }

    /// Drop every writer's `Sender` (signaling graceful shutdown to each
    /// writer task) and await all writers to complete their final flush.
    /// Safe to call multiple times (subsequent calls are no-ops).
    ///
    /// Senders are dropped immediately (dropping one is what triggers that
    /// writer's final flush); the fan-out is bounded by the shared
    /// [`Self::flush_permits`], which each writer acquires inside its flush, so
    /// at most `K` final flushes are ever encoding or `PUT`ting and the next one
    /// starts the instant a permit frees - no batch barrier. Before 2026-10-16
    /// this method held its own semaphore and gated the *drop*; the bound now
    /// lives one level down so the steady-state triggers share it, which is
    /// what #57 (Restart-synchronized buffer-age flush burst) needed.
    /// Rows in a writer that fails to flush survive in the WAL for replay
    /// regardless of ordering.
    ///
    /// On completion, emits a single reconciled summary line
    /// (`shutdown flush complete` with `writers`/`flushed`/`failed`/`empty`/
    /// `aborted`/`elapsed_ms`) so "did the shutdown flush cleanly, and how long
    /// did it take?" is one Loki query rather than a tally of N per-writer lines.
    /// `elapsed_ms` covers only this archive-flush step — the dominant term, but a
    /// *lower bound* on total shutdown time (the outer `timeout(SHUTDOWN_BUDGET, …)`
    /// in `nozzl::run` wraps additional teardown). It's the standing signal to
    /// watch for creep toward `SHUTDOWN_BUDGET` (120s) — the trigger to revisit the
    /// deferred per-PUT flush timeout / `retry_timeout` cap.
    pub async fn shutdown(&self) {
        let entries = {
            let mut g = self.inner.writers.write().expect("writers lock poisoned");
            std::mem::take(&mut *g)
        };
        let total = entries.len();
        let started = std::time::Instant::now();
        let mut set: tokio::task::JoinSet<WriterExit> = tokio::task::JoinSet::new();
        for entry in entries.into_values() {
            set.spawn(async move {
                // Dropping the sender starts that writer's final flush; the
                // writer then queues on the shared flush permits, so the drop
                // needs no gate of its own. A writer parked on a permit is
                // holding only its buffer, which it was holding anyway.
                drop(entry.sender);
                // A panicked/aborted writer task counts as a failed flush — its
                // rows are still safe in the WAL and replay on next boot.
                entry.handle.await.unwrap_or(WriterExit::Failed)
            });
        }
        let mut exits: Vec<WriterExit> = Vec::with_capacity(total);
        while let Some(res) = set.join_next().await {
            // A panicked/aborted OUTER task counts as a failed flush. Defensive:
            // the spawned closure is panic-free, so this is near-unreachable.
            exits.push(res.unwrap_or(WriterExit::Failed));
        }
        let ShutdownTally {
            flushed,
            failed,
            empty,
            aborted,
        } = tally_exits(exits);
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if failed > 0 {
            tracing::warn!(
                writers = total, flushed, failed, empty, aborted, elapsed_ms,
                "shutdown flush complete with failures; failed writers' rows remain in the WAL and replay on next boot"
            );
        } else {
            tracing::info!(
                writers = total, flushed, failed, empty, aborted, elapsed_ms,
                "shutdown flush complete"
            );
        }
    }

    /// Measure the bytes currently resident in the object store per archived
    /// kind. This is a fresh re-derivation from a storage listing on every
    /// call — never an accumulator — so it reflects deletions and eventual
    /// compaction automatically.
    ///
    /// Discovery uses `list_with_delimiter` on the three Hive class roots
    /// (`c=replaceable/`, `c=addressable/`, `c=regular/`) to enumerate the
    /// `k={kind}/` prefixes that actually contain objects; a kind with no resident
    /// data is simply absent from the result (no phantom zero series). Each
    /// discovered prefix is then summed via a recursive `list`, counting only
    /// `.parquet` objects so an in-flight `object_store` local-FS staging file can
    /// never inflate a measurement. Per-kind listings run concurrently, bounded to
    /// `concurrency` (clamped to at least 1). The slug for each kind is resolved
    /// from `kind_slugs` (config), not the path; the `class` label is the bare
    /// metric label, not the on-disk `c=` segment.
    ///
    /// Returns one [`KindResidentUsage`] per non-empty `(class, kind)`. Errors
    /// if any underlying store listing fails.
    pub async fn scan_resident_usage(
        &self,
        concurrency: usize,
        kind_slugs: &HashMap<u16, String>,
    ) -> Result<Vec<KindResidentUsage>, ArchiveError> {
        use futures_util::StreamExt;
        use object_store::path::Path as StorePath;

        // 1) Discover (class, prefix, kind, slug) for every kind dir that holds
        //    at least one object. `common_prefixes` only contains prefixes with
        //    objects beneath them, so never-written kinds never appear.
        //
        // The on-disk class dir is Hive-style (`c=replaceable`), but the metric
        // *label* must stay bare (`replaceable`) — so list under
        // `class_dir_segment` (→ `c=…`) yet tag each result with
        // `chunk_metric_label` (→ bare). The slug is no longer on disk; it's
        // resolved from the config `kind→slug` map (empty if unconfigured).
        let mut prefixes: Vec<(&'static str, StorePath, u16, String)> = Vec::new();
        for class in [
            EventClass::Replaceable,
            EventClass::Addressable,
            EventClass::Regular,
        ] {
            let (Some(dir), Some(label)) = (
                crate::path_enc::class_dir_segment(class),
                class.chunk_metric_label(),
            ) else {
                continue; // ephemeral — not archived
            };
            let root = StorePath::from(dir);
            let listing = self.inner.store.list_with_delimiter(Some(&root)).await?;
            for cp in listing.common_prefixes {
                // The final path segment is `k={kind}` (exact, no slug). Resolve
                // kind, then look up its slug for the metric label.
                let Some(kind) = cp
                    .parts()
                    .next_back()
                    .and_then(|seg| crate::path_enc::parse_kind_segment(seg.as_ref()))
                else {
                    continue;
                };
                let slug = kind_slugs.get(&kind).cloned().unwrap_or_default();
                prefixes.push((label, cp, kind, slug));
            }
        }

        // 2) Sum each kind prefix's `.parquet` object sizes, with bounded
        //    concurrency across kinds. Each per-kind listing runs as its own
        //    task; a shared semaphore caps how many list concurrently.
        let permits = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
        let mut set: tokio::task::JoinSet<Result<KindResidentUsage, ArchiveError>> =
            tokio::task::JoinSet::new();
        for (class, prefix, kind, slug) in prefixes {
            let store = Arc::clone(&self.inner.store);
            let permits = Arc::clone(&permits);
            set.spawn(async move {
                // Held for the duration of this kind's listing; released on drop.
                let _permit = permits
                    .acquire_owned()
                    .await
                    .expect("scan semaphore never closed");
                let mut total_bytes: u64 = 0;
                let mut file_count: u64 = 0;
                let mut stream = store.list(Some(&prefix));
                while let Some(meta) = stream.next().await {
                    let meta = meta?;
                    let is_parquet = meta
                        .location
                        .parts()
                        .next_back()
                        .is_some_and(|p| p.as_ref().ends_with(".parquet"));
                    if is_parquet {
                        total_bytes += meta.size;
                        file_count += 1;
                    }
                }
                Ok(KindResidentUsage {
                    class,
                    kind,
                    slug,
                    total_bytes,
                    file_count,
                })
            });
        }

        let mut usage: Vec<KindResidentUsage> = Vec::new();
        while let Some(joined) = set.join_next().await {
            // Inner `?` propagates the first store listing error; the outer
            // `expect` surfaces a task panic (which would be a bug, not a
            // recoverable condition) rather than silently dropping a kind.
            let measured = joined.expect("storage scan task panicked")?;
            // Drop kinds whose prefix exists but holds no `.parquet` (e.g. only
            // a transient staging file, or — once compaction lands — every
            // chunk deleted). This preserves "absent until actually measured":
            // no zero-byte series.
            if measured.file_count > 0 {
                usage.push(measured);
            }
        }
        Ok(usage)
    }

    /// Measure the bytes currently occupied on local disk by each writer's WAL
    /// file. A fresh re-derivation on every call (never an accumulator): it
    /// `read_dir`s `{wal_root}/_wal`, parses each `c=…-k=…-p=….wal` filename, and
    /// `stat`s it for its current length. Companion to
    /// [`scan_resident_usage`](Self::scan_resident_usage) but for the local WAL
    /// rather than the (possibly remote) object store.
    ///
    /// Returns one [`WalShardUsage`] per `.wal` file, **including 0-byte files**
    /// — a just-flushed/idle writer leaves a truncated (not deleted) WAL, and
    /// reporting its `0` keeps the per-writer series present (a real `0 = idle`,
    /// not absent). Files whose name doesn't parse, or that can't be `stat`ed,
    /// are skipped (the latter logged at `warn`). A missing `_wal` dir yields an
    /// empty vec. The `slug` is resolved from `kind_slugs` (config), empty if
    /// the kind isn't configured.
    ///
    /// Best-effort and infallible by design: WAL sizing is observability, never
    /// on the durability path, so a transient FS hiccup degrades the gauge for
    /// one tick rather than erroring the scan loop.
    pub async fn scan_wal_usage(&self, kind_slugs: &HashMap<u16, String>) -> Vec<WalShardUsage> {
        let wal_dir = self.inner.wal_root.join("_wal");
        let entries = match tokio::fs::read_dir(&wal_dir).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
            Err(e) => {
                tracing::warn!(error = %e, ?wal_dir, "could not list wal dir for wal_bytes scan");
                return Vec::new();
            }
        };

        let mut entries = tokio_stream_wrapper(entries);
        let mut usage: Vec<WalShardUsage> = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(error = %e, "wal dir entry error during wal_bytes scan");
                    continue;
                }
            };
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some((class, kind, shard)) = parse_wal_filename(&name) else {
                // Unparseable names are WARNed loudly once at startup by
                // `recover_from_wal`; skip silently here to avoid per-tick noise.
                continue;
            };
            let Some(class_label) = class.chunk_metric_label() else {
                continue; // ephemeral — never archived, never has a WAL
            };
            let bytes = match entry.metadata().await {
                Ok(m) => m.len(),
                Err(e) => {
                    tracing::warn!(wal = %name, error = %e, "could not stat wal file for wal_bytes scan");
                    continue;
                }
            };
            let slug = kind_slugs.get(&kind).cloned().unwrap_or_default();
            usage.push(WalShardUsage {
                class: class_label,
                kind,
                slug,
                shard: shard.as_char(),
                bytes,
            });
        }
        usage
    }

    /// Replay any existing WAL files under `wal_root` by spawning writer tasks
    /// for each one found. Call this once at startup before any `put_event`.
    /// Without this step, WAL files for kinds that haven't yet seen a fresh
    /// event would not be replayed until a new event for that exact
    /// `(class, kind, shard)` arrives.
    ///
    /// Returns the number of writer tasks spawned for replay.
    pub async fn recover_from_wal(&self, configured: &[(EventClass, u16, String)]) -> usize {
        let wal_dir = self.inner.wal_root.join("_wal");
        let entries = match tokio::fs::read_dir(&wal_dir).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 0,
            Err(e) => {
                tracing::warn!(error = %e, ?wal_dir, "could not list wal dir");
                return 0;
            }
        };

        let by_kind: HashMap<(EventClass, u16), &str> = configured
            .iter()
            .map(|(c, k, slug)| ((*c, *k), slug.as_str()))
            .collect();

        let mut entries = tokio_stream_wrapper(entries);
        let mut spawned = 0usize;
        while let Some(entry) = entries.next().await {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(error = %e, "wal dir entry error");
                    continue;
                }
            };
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !std::path::Path::new(&name)
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("wal"))
            {
                continue;
            }

            // Decode `c=<class>-k=<kind>-p=<shard>.wal` via the shared
            // `parse_wal_filename` decoder, so this durability-critical recovery
            // path and the `wal_bytes` scan can never drift on the grammar. Any
            // `.wal` that fails to parse is WARNED, not silently skipped:
            // post-migration a legacy (bare-class, slugged) name here is an
            // alarm — the new writer truncates a *different* file, so a missed
            // non-empty legacy WAL would lose its events invisibly. The one-off
            // migration that renamed the tree ran on 2026-06-21.
            let Some((class, kind, shard)) = parse_wal_filename(&name) else {
                // Re-walk the segments solely to attribute the alarm to a
                // specific cause (the shared decoder returns a bare `None`).
                let stem = name.trim_end_matches(".wal");
                if let Some((class_seg, rest)) = stem.split_once("-k=") {
                    if crate::path_enc::parse_class_segment(class_seg).is_none() {
                        tracing::warn!(wal = %name, class_seg, "WAL filename has an unrecognized class segment; skipping (possible un-migrated pre-`c=` legacy file)");
                    } else if let Some((kind_str, shard_seg)) = rest.split_once("-p=") {
                        if kind_str.parse::<u16>().is_err() {
                            tracing::warn!(wal = %name, kind_str, "WAL filename has a non-numeric kind; skipping");
                        } else {
                            tracing::warn!(wal = %name, shard_seg, "WAL filename has an invalid shard; skipping");
                        }
                    } else {
                        tracing::warn!(wal = %name, "WAL filename has no `-p=` shard; skipping");
                    }
                } else {
                    tracing::warn!(wal = %name, "WAL filename has no `-k=`; skipping (possible un-migrated legacy file)");
                }
                continue;
            };

            // Resolve the slug from config (metric-label legibility only). A WAL
            // for a kind no longer configured is still drained (its data must not
            // be lost); it simply carries an empty slug label until reconfigured.
            let slug = by_kind.get(&(class, kind)).copied().unwrap_or("");

            self.ensure_writer(WriterKey { class, kind, shard }, slug);
            spawned += 1;
        }

        spawned
    }

    fn ensure_writer(&self, key: WriterKey, slug: &str) -> mpsc::Sender<WriterMessage> {
        // Fast path: read-locked lookup.
        {
            let g = self.inner.writers.read().expect("writers lock poisoned");
            if let Some(entry) = g.get(&key) {
                return entry.sender.clone();
            }
        }
        // Slow path: take the write lock, double-check (another caller may
        // have raced us), then spawn.
        let mut g = self.inner.writers.write().expect("writers lock poisoned");
        if let Some(entry) = g.get(&key) {
            return entry.sender.clone();
        }
        let (tx, rx) = mpsc::channel(WRITER_CHANNEL_CAPACITY);
        let handle = tokio::spawn(run_writer(
            key.clone(),
            slug.to_string(),
            self.inner.cfg,
            self.inner.wal_root.clone(),
            self.inner.store.clone(),
            rx,
            self.inner.metrics.clone(),
            Arc::clone(self.flush_permits()),
        ));
        g.insert(
            key,
            WriterEntry {
                sender: tx.clone(),
                handle,
            },
        );
        tx
    }
}

#[async_trait]
impl Archive for ChunkedArchive {
    async fn put_event(&self, event: &Event, slug: &str) -> Result<PutOutcome, ArchiveError> {
        let kind = event.kind.as_u16();
        let class = EventClass::classify(kind);
        // Replaceable, addressable, and regular are archived; only ephemeral
        // (which relays don't persist) is skipped here.
        if !matches!(
            class,
            EventClass::Replaceable | EventClass::Addressable | EventClass::Regular
        ) {
            return Ok(PutOutcome::Skipped {
                reason: SkipReason::UnsupportedKind { kind },
            });
        }
        let pubkey_hex = event.pubkey.to_hex();
        let shard = PubkeyShard::from_pubkey_hex(&pubkey_hex).ok_or_else(|| {
            ArchiveError::Other(format!("pubkey hex unexpectedly empty for kind {kind}"))
        })?;

        let key = WriterKey { class, kind, shard };
        let sender = self.ensure_writer(key, slug);

        let ingested_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());

        sender
            .send(WriterMessage::Event {
                event: Box::new(event.clone()),
                ingested_at,
            })
            .await
            .map_err(|_| ArchiveError::Other("writer task died".to_string()))?;

        // Under chunked storage, we acknowledge after the event reaches the
        // writer task's channel. The writer fsyncs to WAL before processing
        // the next message; durability is guaranteed by the WAL even if the
        // process crashes before the chunk is published.
        Ok(PutOutcome::Written)
    }
}

/// Operator-facing flush control surface, consumed by the HTTP admin endpoint.
///
/// Kept as a trait (rather than handing the HTTP layer a concrete
/// `Arc<ChunkedArchive>`) so `nozzl-http` depends only on this contract and can
/// unit-test its handler against a mock — mirroring how `nozzl-http`'s `/health`
/// endpoint depends on a `HealthSource` trait, not on the binary directly.
#[async_trait]
pub trait FlushControl: Send + Sync {
    /// Force every currently-registered writer for `kind` (all shards of the
    /// one archived class for that kind) to publish its buffered events as a
    /// chunk *now*, waiting up to `timeout` for each to confirm.
    ///
    /// Safe to call concurrently and repeatedly: each call enqueues one
    /// `AdhocFlush` per matching writer onto that writer's existing mpsc, so it
    /// is serialized with normal event ingest and automatic rolls inside the
    /// single-actor writer loop — no event is lost or double-published. A
    /// writer whose buffer is already empty (e.g. a second concurrent call, or
    /// an automatic roll won the race) treats it as a no-op.
    ///
    /// A `timeout` elapsing is **not** data loss: the `AdhocFlush` is already
    /// enqueued, so the slow shard still completes its publish + WAL truncate
    /// shortly after; the request merely stops waiting and reports it under
    /// [`FlushSummary::timed_out`].
    async fn flush_kind(&self, kind: u16, timeout: Duration) -> FlushSummary;
}

/// Aggregate result of [`FlushControl::flush_kind`].
#[derive(Debug, Clone)]
pub struct FlushSummary {
    /// The kind that was flushed.
    pub kind: u16,
    /// Number of writer tasks (shards) the flush was dispatched to. Zero for a
    /// configured-but-idle kind that has no writer spawned yet.
    pub writers_signaled: usize,
    /// How many of those actually published a chunk (had a non-empty buffer).
    pub shards_published: usize,
    /// Total rows published across all shards this flush.
    pub rows: u64,
    /// Shards that did not confirm within `timeout`.
    pub timed_out: usize,
    /// Shards whose publish failed; rows remain durable in the WAL for retry.
    pub errors: Vec<ShardError>,
}

/// One shard's failure within a [`FlushSummary`]. The kind is already on the
/// enclosing [`FlushSummary::kind`] (and slug is 1:1 with kind), so a shard error
/// only needs to name the shard. Slug is no longer carried here — it's not on the
/// writer key anymore.
#[derive(Debug, Clone)]
pub struct ShardError {
    /// Pubkey shard character (`0`-`f`) of the failing writer.
    pub shard: char,
    /// Human-readable failure detail.
    pub message: String,
}

#[async_trait]
impl FlushControl for ChunkedArchive {
    async fn flush_kind(&self, kind: u16, timeout: Duration) -> FlushSummary {
        // Snapshot matching (key, sender) pairs under the read lock, then drop
        // the guard before awaiting — the std `RwLock` must never be held
        // across an `.await` (same discipline as `force_flush_all`).
        let targets: Vec<(WriterKey, mpsc::Sender<WriterMessage>)> = {
            let g = self.inner.writers.read().expect("writers lock poisoned");
            g.iter()
                .filter(|(k, _)| k.kind == kind)
                .map(|(k, w)| (k.clone(), w.sender.clone()))
                .collect()
        };
        let writers_signaled = targets.len();
        tracing::info!(kind, writers_signaled, "ad-hoc flush requested");

        // Dispatch one AdhocFlush per matching writer and await its ack. Each
        // shard's send+recv is bounded by `timeout` so a full channel
        // (backpressure) or a wedged writer can't hang the request.
        let dispatched = targets.into_iter().map(|(key, sender)| async move {
            let (tx, rx) = oneshot::channel();
            let result = tokio::time::timeout(timeout, async move {
                sender
                    .send(WriterMessage::AdhocFlush { ack: tx })
                    .await
                    .map_err(|_| "writer task channel closed".to_string())?;
                rx.await
                    .map_err(|_| "writer dropped ack without responding".to_string())
            })
            .await;
            (key, result)
        });
        let results = join_all(dispatched).await;

        let mut summary = FlushSummary {
            kind,
            writers_signaled,
            shards_published: 0,
            rows: 0,
            timed_out: 0,
            errors: Vec::new(),
        };
        for (key, result) in results {
            match result {
                Err(_elapsed) => {
                    summary.timed_out += 1;
                    tracing::warn!(
                        ?key,
                        timeout_s = timeout.as_secs(),
                        "ad-hoc flush shard did not confirm within timeout (still durable in WAL)"
                    );
                }
                Ok(Err(msg)) => {
                    tracing::error!(?key, error = %msg, "ad-hoc flush shard failed to dispatch/confirm");
                    summary.errors.push(ShardError {
                        shard: key.shard.as_char(),
                        message: msg,
                    });
                }
                Ok(Ok(outcome)) => {
                    if let Some(msg) = outcome.error {
                        summary.errors.push(ShardError {
                            shard: key.shard.as_char(),
                            message: msg,
                        });
                    } else if outcome.published {
                        summary.shards_published += 1;
                        summary.rows += outcome.rows;
                    }
                }
            }
        }
        tracing::info!(
            kind,
            writers_signaled = summary.writers_signaled,
            shards_published = summary.shards_published,
            rows = summary.rows,
            timed_out = summary.timed_out,
            errors = summary.errors.len(),
            "ad-hoc flush complete"
        );
        summary
    }
}

// Tiny adapter so we can iterate a `tokio::fs::ReadDir` with an async iterator
// shape without pulling in a full `tokio-stream` dependency. Unused outside
// of `recover_from_wal`.
struct ReadDirIter(tokio::fs::ReadDir);

impl ReadDirIter {
    async fn next(&mut self) -> Option<std::io::Result<tokio::fs::DirEntry>> {
        self.0.next_entry().await.transpose()
    }
}

fn tokio_stream_wrapper(rd: tokio::fs::ReadDir) -> ReadDirIter {
    ReadDirIter(rd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader;
    use bytes::Bytes;
    use futures_util::stream::BoxStream;
    use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};
    use object_store::local::LocalFileSystem;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path as ObjPath,
    };
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    fn make_event(kind: u16, content: &str) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(Kind::from_u16(kind), content)
            .custom_created_at(Timestamp::from_secs(1_700_000_000))
            .sign_with_keys(&keys)
            .unwrap()
    }

    fn make_addressable(d: &str) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(Kind::from_u16(30063), "release")
            .tags(vec![Tag::identifier(d)])
            .custom_created_at(Timestamp::from_secs(1_700_000_000))
            .sign_with_keys(&keys)
            .unwrap()
    }

    fn rig() -> (ChunkedArchive, std::path::PathBuf, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let archive = ChunkedArchive::new(
            store,
            dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
            None,
        );
        let chunks_root = dir.path().to_path_buf();
        (archive, chunks_root, dir)
    }

    #[tokio::test]
    async fn put_event_routes_to_correct_writer_and_shutdown_flushes() {
        let (archive, chunks_root, _dir) = rig();

        let e1 = make_event(0, "alice");
        let e2 = make_addressable("myapp@1.0");
        let e3 = make_event(0, "bob");
        let e4 = make_event(9735, "zap receipt"); // regular class

        archive.put_event(&e1, "profile-metadata").await.unwrap();
        archive
            .put_event(&e2, "zapstore-app-releases")
            .await
            .unwrap();
        archive.put_event(&e3, "profile-metadata").await.unwrap();
        archive.put_event(&e4, "zap-receipts").await.unwrap();

        // Shutdown drops all senders and awaits every writer's final flush.
        archive.shutdown().await;

        let all = reader::read_all_events(&chunks_root).unwrap();
        assert_eq!(all.len(), 4);

        let kinds: Vec<u16> = all.iter().map(|r| r.event.kind.as_u16()).collect();
        assert!(kinds.contains(&0));
        assert!(kinds.contains(&30063));
        assert!(kinds.contains(&9735));

        // On-disk class dirs are Hive-style (`c=…`), distinct from the bare
        // metric labels.
        let class_dirs: Vec<_> = std::fs::read_dir(&chunks_root)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().into_string().unwrap_or_default())
            .filter(|n| n == "c=replaceable" || n == "c=addressable" || n == "c=regular")
            .collect();
        assert!(class_dirs.contains(&"c=replaceable".to_string()));
        assert!(class_dirs.contains(&"c=addressable".to_string()));
        assert!(class_dirs.contains(&"c=regular".to_string()));
    }

    #[tokio::test]
    async fn put_event_archives_regular_and_skips_ephemeral() {
        let (archive, _chunks_root, _dir) = rig();
        // Regular kinds (e.g. kind-9735 zap receipts) are now archived.
        let regular = make_event(9735, "zap receipt");
        let outcome = archive.put_event(&regular, "zap-receipts").await.unwrap();
        assert!(matches!(outcome, PutOutcome::Written));
        // Ephemeral kinds are still skipped (relays don't persist them).
        let ephemeral = make_event(22500, "ephemeral");
        let outcome = archive
            .put_event(&ephemeral, "ephemeral-thing")
            .await
            .unwrap();
        assert!(matches!(outcome, PutOutcome::Skipped { .. }));
        archive.shutdown().await;
    }

    #[tokio::test]
    async fn force_flush_all_publishes_buffered_events() {
        let (archive, chunks_root, _dir) = rig();
        let e = make_event(0, "alice");
        archive.put_event(&e, "profile-metadata").await.unwrap();

        // Without force-flush + shutdown, the event is in WAL/builder but not
        // yet in a chunk.
        archive.force_flush_all().await;
        // Need shutdown to drain any in-flight ForceFlush before checking disk.
        archive.shutdown().await;

        let all = reader::read_all_events(&chunks_root).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].event.id, e.id);
    }

    /// An `ObjectStore` that records the peak number of concurrent `put_opts`
    /// calls in flight, delegating actual storage to `inner`. Each PUT rendezvous
    /// on a `Barrier` sized to the expected concurrency `cohort` *before* touching
    /// the inner store, so a cohort of `cohort` genuinely-concurrent final flushes
    /// is held open together and the peak is observed **deterministically** — no
    /// wall-clock overlap window to race (a local-FS PUT would otherwise finish
    /// before the next one starts, and a fixed sleep only makes the peak *likely*,
    /// not certain, which flaked under CI scheduling jitter). Every other method
    /// delegates unchanged.
    ///
    /// The barrier is what makes the peak deterministic: it releases only once
    /// `cohort` PUTs are simultaneously parked in it, so a correctly-bounded,
    /// genuinely-concurrent fan-out reaches exactly `cohort`. A fan-out that
    /// serialized (or whose bound fell below `cohort`) never assembles the barrier
    /// — the caller must guard that with a timeout so it fails loud rather than
    /// hanging. `cohort` must therefore divide the number of non-empty writers.
    struct ConcurrencyProbeStore {
        inner: Arc<dyn ObjectStore>,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        gate: tokio::sync::Barrier,
    }

    impl ConcurrencyProbeStore {
        fn new(inner: Arc<dyn ObjectStore>, cohort: usize) -> Self {
            Self {
                inner,
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                gate: tokio::sync::Barrier::new(cohort),
            }
        }
    }

    impl std::fmt::Debug for ConcurrencyProbeStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ConcurrencyProbeStore").finish()
        }
    }
    impl std::fmt::Display for ConcurrencyProbeStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ConcurrencyProbeStore")
        }
    }

    #[async_trait]
    impl ObjectStore for ConcurrencyProbeStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            // Rendezvous the cohort so genuinely-concurrent final flushes are all
            // in flight at once when the peak is recorded — deterministic, unlike a
            // fixed sleep. The barrier releases only when `cohort` PUTs have parked
            // here together, so `peak` provably reaches the cohort size.
            self.gate.wait().await;
            let res = self.inner.put_opts(location, payload, opts).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            res
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn get_ranges(
            &self,
            location: &ObjPath,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        fn list_with_offset(
            &self,
            prefix: Option<&ObjPath>,
            offset: &ObjPath,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The shutdown summary's categorization is only observable as a log side
    /// effect, so the pure `tally_exits` helper carries the bucketing logic and is
    /// tested directly here: every outcome lands in its own bucket, `Aborted` is
    /// kept distinct from `Failed` (so startup-aborted writers can't spuriously
    /// flip the summary to warn), and the count is conserved
    /// (`flushed+failed+empty+aborted == writers`).
    #[test]
    fn tally_exits_buckets_each_outcome_and_conserves_count() {
        let exits = vec![
            WriterExit::Flushed,
            WriterExit::Flushed,
            WriterExit::Flushed,
            WriterExit::Failed,
            WriterExit::Empty,
            WriterExit::Empty,
            WriterExit::Aborted,
        ];
        let n = exits.len();
        let tally = tally_exits(exits);
        assert_eq!(
            tally,
            ShutdownTally {
                flushed: 3,
                failed: 1,
                empty: 2,
                aborted: 1,
            }
        );
        assert_eq!(
            tally.flushed + tally.failed + tally.empty + tally.aborted,
            n,
            "every writer must be bucketed exactly once"
        );
        // Aborted writers must NOT count as failures (else a fleet of
        // non-archived/early-exit writers would flip the summary to warn).
        assert_eq!(tally_exits([WriterExit::Aborted; 5]).failed, 0);
        // Empty input tallies to all-zero (no writers, no summary noise).
        assert_eq!(tally_exits([]), ShutdownTally::default());
    }

    /// A shutdown with more non-empty writers than the fan-out bound must drive
    /// concurrent final PUTs to *exactly* the bound (not above — the safety
    /// property; not below — which would mean the flushes serialized), and must
    /// still flush every writer (no rows dropped). Regression guard for the
    /// 2026-07-01 incident where an unbounded 68-writer S3 fan-out failed all of
    /// its final PUTs at once.
    ///
    /// Determinism comes from a `Barrier` (not a wall-clock sleep) inside the
    /// probe: it releases a cohort of PUTs only once `bound` of them are parked in
    /// it together, so a correctly-bounded, genuinely-concurrent fan-out reaches a
    /// peak of exactly `bound` every time. `writer_count` is `2 × bound` — an exact
    /// multiple so the sliding window drains and refills into a clean second cohort
    /// (proving permits are *reused*, the sliding-window property), with no
    /// leftover writers that could strand the barrier. The two failure modes the
    /// test guards manifest distinctly: an *over-permissive* bound lets `in_flight`
    /// climb past `bound` → `peak > bound` (asserted); a *serialized* fan-out (or a
    /// bound below `bound`) never assembles a full barrier cohort → `shutdown`
    /// hangs → the `timeout` below fails the test loud rather than deadlocking it.
    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_flush_fan_out_is_bounded() {
        let dir = tempdir().unwrap();
        let chunks_root = dir.path().to_path_buf();
        let inner: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&chunks_root).unwrap());
        let bound = ChunkWriterConfig::default().chunk_flush_concurrency;
        // Probe barrier sized to the bound: each cohort of `bound` concurrent PUTs
        // rendezvous before touching the store, making the observed peak exact.
        let probe = Arc::new(ConcurrencyProbeStore::new(inner, bound));
        let archive = ChunkedArchive::new(
            Arc::clone(&probe) as Arc<dyn ObjectStore>,
            chunks_root.clone(),
            ChunkWriterConfig::default(),
            None,
        );

        // One buffered event per distinct kind → one writer each. Exactly two
        // cohorts (2 × bound) so the window fills to the bound, drains, and refills
        // — with no remainder that would leave the barrier waiting forever.
        let writer_count = bound * 2;
        // Load-bearing: the probe barrier is sized `bound`, so `writer_count` must
        // be a whole number of cohorts. A non-multiple strands the trailing writers
        // on the barrier → the timeout below would fire and blame a nonexistent
        // production serialization bug. Fail loud here with the real reason instead.
        debug_assert_eq!(
            writer_count % bound,
            0,
            "writer_count must be a whole number of barrier cohorts"
        );
        for k in 0..writer_count {
            let ev = make_event(u16::try_from(1000 + k).unwrap(), "x");
            archive.put_event(&ev, "test").await.unwrap();
        }

        // A serialized or under-bounded fan-out can never assemble a full barrier
        // cohort, so it would hang here — the timeout turns that into a loud failure
        // instead of a deadlock. Its only job is that liveness backstop: the
        // local-FS flushes here are sub-millisecond, so any value comfortably above
        // a worst-case CI stall works; 30s is deliberate slack, not a latency bound.
        tokio::time::timeout(Duration::from_secs(30), archive.shutdown())
            .await
            .expect("shutdown flush deadlocked: fan-out never reached the bound (serialized, or bound below the barrier cohort)");

        // Every writer's final flush landed — no rows lost.
        let all = reader::read_all_events(&chunks_root).unwrap();
        assert_eq!(all.len(), writer_count);

        // Safety cap — the property whose absence caused the 2026-07-01 incident:
        // the semaphore never lets more than `bound` final PUTs run at once.
        let peak = probe.peak.load(Ordering::SeqCst);
        assert!(
            peak <= bound,
            "shutdown fan-out peak {peak} exceeded the {bound} safety cap"
        );
        // Liveness — the window actually *fills* to the bound (flushes ran
        // concurrently, not serialized). Deterministic via the barrier: a full
        // cohort of `bound` PUTs was held open together, so the peak is exact.
        assert_eq!(
            peak, bound,
            "shutdown fan-out peak {peak} did not fill to bound {bound}"
        );
    }

    /// The property whose absence caused #57 (Restart-synchronized buffer-age
    /// flush burst): the bound applies to the *steady-state* triggers too, not
    /// only to shutdown.
    ///
    /// Before 2026-10-16 the 32-permit window lived inside `shutdown()`, so a
    /// buffer-age (or size, rows, replay, ad-hoc) herd fanned out unbounded.
    /// On 2026-10-15 that put 443 flushes into one second on dev and failed 178
    /// of their S3 PUTs. This test drives the fan-out through `ForceFlush` -
    /// which takes the same `flush_and_truncate` path as the buffer-age timer,
    /// and is drivable without waiting out a wall-clock budget - and asserts the
    /// peak is the bound.
    ///
    /// Same two failure modes as the shutdown test, distinguishable the same
    /// way: an over-permissive bound shows up as `peak > bound`, while a
    /// serialized fan-out never assembles a barrier cohort and trips the
    /// timeout rather than hanging.
    #[tokio::test(flavor = "current_thread")]
    async fn steady_state_flush_fan_out_is_bounded() {
        let dir = tempdir().unwrap();
        let chunks_root = dir.path().to_path_buf();
        let inner: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&chunks_root).unwrap());
        let bound = ChunkWriterConfig::default().chunk_flush_concurrency;
        let probe = Arc::new(ConcurrencyProbeStore::new(inner, bound));
        let archive = ChunkedArchive::new(
            Arc::clone(&probe) as Arc<dyn ObjectStore>,
            chunks_root.clone(),
            ChunkWriterConfig::default(),
            None,
        );

        // Exactly two barrier cohorts of buffered writers, as in the shutdown
        // test: the window fills to the bound, drains, and refills with no
        // remainder to strand the barrier.
        let writer_count = bound * 2;
        debug_assert_eq!(
            writer_count % bound,
            0,
            "writer_count must be a whole number of barrier cohorts"
        );
        for k in 0..writer_count {
            let ev = make_event(u16::try_from(1000 + k).unwrap(), "x");
            archive.put_event(&ev, "test").await.unwrap();
        }

        // Fire-and-forget, like the buffer-age timer: every writer flushes on
        // its own, with nothing coordinating them. `shutdown()` then waits for
        // the writers, by which point each has drained its ForceFlush; their
        // final flushes find empty builders, so they issue no further PUTs and
        // cannot contribute to the peak.
        archive.force_flush_all().await;
        tokio::time::timeout(Duration::from_secs(30), archive.shutdown())
            .await
            .expect(
                "steady-state fan-out never reached the bound (serialized, or bound below the barrier cohort)",
            );

        let all = reader::read_all_events(&chunks_root).unwrap();
        assert_eq!(all.len(), writer_count, "every force flush must have landed");

        let peak = probe.peak.load(Ordering::SeqCst);
        assert!(
            peak <= bound,
            "steady-state fan-out peak {peak} exceeded the {bound} safety cap"
        );
        assert_eq!(
            peak, bound,
            "steady-state fan-out peak {peak} did not fill to bound {bound}"
        );
    }

    /// The bound is the *configured* one, not a hard-coded 32: a narrower
    /// `chunk_flush_concurrency` must actually narrow the fan-out. Guards
    /// against the knob being accepted by config and then ignored.
    #[tokio::test(flavor = "current_thread")]
    async fn configured_flush_concurrency_sets_the_bound() {
        let dir = tempdir().unwrap();
        let chunks_root = dir.path().to_path_buf();
        let inner: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&chunks_root).unwrap());
        let bound = 4;
        let probe = Arc::new(ConcurrencyProbeStore::new(inner, bound));
        let archive = ChunkedArchive::new(
            Arc::clone(&probe) as Arc<dyn ObjectStore>,
            chunks_root.clone(),
            ChunkWriterConfig {
                chunk_flush_concurrency: bound,
                ..ChunkWriterConfig::default()
            },
            None,
        );
        let writer_count = bound * 3;
        for k in 0..writer_count {
            let ev = make_event(u16::try_from(2000 + k).unwrap(), "x");
            archive.put_event(&ev, "test").await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(30), archive.shutdown())
            .await
            .expect("shutdown with a narrowed bound never assembled a cohort");
        assert_eq!(
            probe.peak.load(Ordering::SeqCst),
            bound,
            "peak must equal the configured chunk_flush_concurrency"
        );
        assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), writer_count);
    }

    #[tokio::test]
    async fn scan_resident_usage_sums_published_chunks_per_kind() {
        let (archive, _root, _dir) = rig();

        // Two replaceable (kind 0), one addressable (kind 30063), one regular
        // (kind 9735) event — exercising all three class roots in the scan.
        archive
            .put_event(&make_event(0, "alice"), "profile-metadata")
            .await
            .unwrap();
        archive
            .put_event(&make_event(0, "bob"), "profile-metadata")
            .await
            .unwrap();
        archive
            .put_event(&make_addressable("myapp@1.0"), "zapstore-app-releases")
            .await
            .unwrap();
        archive
            .put_event(&make_event(9735, "zap receipt"), "zap-receipts")
            .await
            .unwrap();
        archive.shutdown().await; // forces final flush of every writer

        // Slug is sourced from config now (not the path), so supply the map.
        let slugs = std::collections::HashMap::from([
            (0u16, "profile-metadata".to_string()),
            (30063u16, "zapstore-app-releases".to_string()),
            (9735u16, "zap-receipts".to_string()),
        ]);
        let mut usage = archive.scan_resident_usage(4, &slugs).await.unwrap();
        usage.sort_by_key(|u| u.kind);

        assert_eq!(usage.len(), 3, "one entry per non-empty kind: {usage:?}");

        let prof = usage.iter().find(|u| u.kind == 0).expect("kind 0 present");
        // Class label stays BARE (`replaceable`), not the on-disk `c=replaceable`.
        assert_eq!(prof.class, "replaceable");
        assert_eq!(prof.slug, "profile-metadata");
        assert!(prof.total_bytes > 0, "expected non-zero bytes: {prof:?}");
        assert!(prof.file_count >= 1);

        let zap = usage
            .iter()
            .find(|u| u.kind == 30063)
            .expect("kind 30063 present");
        assert_eq!(zap.class, "addressable");
        assert_eq!(zap.slug, "zapstore-app-releases");
        assert!(zap.total_bytes > 0);
        assert!(zap.file_count >= 1);

        let zap_receipt = usage
            .iter()
            .find(|u| u.kind == 9735)
            .expect("kind 9735 present");
        assert_eq!(zap_receipt.class, "regular");
        assert_eq!(zap_receipt.slug, "zap-receipts");
        assert!(zap_receipt.total_bytes > 0);
        assert!(zap_receipt.file_count >= 1);
    }

    #[tokio::test]
    async fn scan_resident_usage_absent_for_unwritten_kinds() {
        let (archive, _root, _dir) = rig();
        archive
            .put_event(&make_event(0, "alice"), "profile-metadata")
            .await
            .unwrap();
        archive.shutdown().await;

        let slugs = std::collections::HashMap::from([(0u16, "profile-metadata".to_string())]);
        let usage = archive.scan_resident_usage(4, &slugs).await.unwrap();
        // Only the one written kind appears; nothing for un-archived kinds.
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].kind, 0);
    }

    #[test]
    fn parse_wal_filename_decodes_and_rejects() {
        // Valid: `c=<class>-k=<kind>-p=<shard>.wal`.
        let (class, kind, shard) =
            parse_wal_filename("c=replaceable-k=0-p=a.wal").expect("valid name parses");
        assert_eq!(class, EventClass::Replaceable);
        assert_eq!(kind, 0);
        assert_eq!(shard.as_char(), 'a');

        let (class, kind, shard) =
            parse_wal_filename("c=regular-k=9735-p=3.wal").expect("valid name parses");
        assert_eq!(class, EventClass::Regular);
        assert_eq!(kind, 9735);
        assert_eq!(shard.as_char(), '3');

        // Rejections: no .wal suffix, missing segments, bad class, non-numeric
        // kind, invalid shard char.
        assert!(parse_wal_filename("c=replaceable-k=0-p=a").is_none());
        assert!(parse_wal_filename("garbage.wal").is_none());
        assert!(parse_wal_filename("c=replaceable-k=0.wal").is_none());
        assert!(parse_wal_filename("c=bogus-k=0-p=a.wal").is_none());
        assert!(parse_wal_filename("c=replaceable-k=xx-p=a.wal").is_none());
        assert!(parse_wal_filename("c=replaceable-k=0-p=z.wal").is_none());
    }

    #[tokio::test]
    async fn scan_wal_usage_reports_each_writer_including_zero_byte() {
        let (archive, root, _dir) = rig();
        let wal_dir = root.join("_wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // A buffered writer (non-empty WAL), an idle one (0-byte, truncated but
        // not deleted — the key case the gauge must still report), plus two
        // entries the scan must skip: an unparseable name and a non-`.wal` file.
        std::fs::write(wal_dir.join("c=replaceable-k=0-p=a.wal"), b"hello").unwrap();
        std::fs::write(wal_dir.join("c=regular-k=1-p=3.wal"), b"").unwrap();
        std::fs::write(wal_dir.join("not-a-wal-name.wal"), b"junk").unwrap();
        std::fs::write(wal_dir.join("c=replaceable-k=0-p=a.tmp"), b"staging").unwrap();

        // Slug resolves from config; kind 1 is intentionally absent → empty slug.
        let slugs = std::collections::HashMap::from([(0u16, "profile-metadata".to_string())]);
        let mut usage = archive.scan_wal_usage(&slugs).await;
        usage.sort_by_key(|w| w.kind);

        assert_eq!(
            usage.len(),
            2,
            "two valid .wal files, others skipped: {usage:?}"
        );

        let buffered = usage.iter().find(|w| w.kind == 0).expect("kind 0 present");
        assert_eq!(buffered.class, "replaceable"); // bare label, not `c=replaceable`
        assert_eq!(buffered.slug, "profile-metadata");
        assert_eq!(buffered.shard, 'a');
        assert_eq!(buffered.bytes, 5);

        let idle = usage.iter().find(|w| w.kind == 1).expect("kind 1 present");
        assert_eq!(idle.class, "regular");
        assert_eq!(idle.slug, "", "kind not in config map → empty slug");
        assert_eq!(idle.shard, '3');
        assert_eq!(idle.bytes, 0, "idle writer reports a real 0, not absent");
    }

    #[tokio::test]
    async fn scan_wal_usage_empty_when_no_wal_dir() {
        let (archive, _root, _dir) = rig();
        // No `_wal` directory yet (no puts) → empty, never an error.
        let usage = archive
            .scan_wal_usage(&std::collections::HashMap::new())
            .await;
        assert!(usage.is_empty());
    }

    #[tokio::test]
    async fn scan_wal_usage_finds_real_writer_wal_zero_after_flush() {
        // End-to-end against a real writer (not a hand-placed file): proves the
        // actual on-disk WAL filename parses, and that after a flush the file is
        // truncated (not deleted) so the series stays present reading 0. (The
        // buffered `> 0` case is covered deterministically by the hand-placed
        // test above; a real put's WAL append is async, so its pre-flush size
        // can't be observed without a race.)
        let (archive, root, _dir) = rig();
        archive
            .put_event(&make_event(0, "alice"), "profile-metadata")
            .await
            .unwrap();
        archive.force_flush_all().await;
        archive.shutdown().await; // drains the in-flight flush + truncate

        // The `.wal` file must still exist (set_len(0), never removed), so the
        // scan still finds it — now reading 0.
        assert!(root.join("_wal").exists());
        let slugs = std::collections::HashMap::from([(0u16, "profile-metadata".to_string())]);
        let after = archive.scan_wal_usage(&slugs).await;
        assert_eq!(
            after.len(),
            1,
            "real writer's WAL file persists post-flush: {after:?}"
        );
        assert_eq!(after[0].kind, 0);
        assert_eq!(after[0].class, "replaceable");
        assert_eq!(after[0].slug, "profile-metadata");
        assert_eq!(after[0].bytes, 0, "truncated WAL reports 0, stays present");
    }

    /// B1/B2 guard: the metric `class` label stays bare (`replaceable`, never the
    /// on-disk `c=replaceable`), and the slug is sourced from the supplied config
    /// map — empty when the kind isn't in it, never parsed from the path.
    #[tokio::test]
    async fn scan_resident_usage_class_bare_and_slug_from_map() {
        let (archive, _root, _dir) = rig();
        archive
            .put_event(&make_event(0, "alice"), "profile-metadata")
            .await
            .unwrap();
        archive.shutdown().await;

        // Empty map → slug resolves to "" (NOT scraped from the slugless path),
        // and the class label is still the bare metric label.
        let empty = std::collections::HashMap::new();
        let usage = archive.scan_resident_usage(4, &empty).await.unwrap();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].class, "replaceable", "class label must stay bare");
        assert_eq!(usage[0].slug, "", "unconfigured kind → empty slug label");

        // With the kind configured, the slug label is populated from the map.
        let slugs = std::collections::HashMap::from([(0u16, "profile-metadata".to_string())]);
        let usage = archive.scan_resident_usage(4, &slugs).await.unwrap();
        assert_eq!(usage[0].slug, "profile-metadata");
    }

    #[tokio::test]
    async fn scan_resident_usage_on_empty_store_is_empty() {
        let (archive, _root, _dir) = rig();
        let slugs = std::collections::HashMap::new();
        let usage = archive.scan_resident_usage(4, &slugs).await.unwrap();
        assert!(usage.is_empty(), "no data → no series: {usage:?}");
    }

    #[tokio::test]
    async fn scan_resident_usage_ignores_non_parquet_objects() {
        use object_store::path::Path as StorePath;
        use object_store::{ObjectStoreExt, PutPayload};

        let (archive, root, _dir) = rig();
        archive
            .put_event(&make_event(0, "alice"), "profile-metadata")
            .await
            .unwrap();
        archive.shutdown().await;

        let slugs = std::collections::HashMap::from([(0u16, "profile-metadata".to_string())]);
        let parquet_only = archive.scan_resident_usage(4, &slugs).await.unwrap();
        let baseline = parquet_only.iter().find(|u| u.kind == 0).unwrap().clone();

        // Drop a stray non-parquet staging file under the SAME kind prefix and
        // a never-parquet kind prefix; neither should affect the measurement.
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(&root).unwrap()) as Arc<dyn ObjectStore>;
        store
            .put(
                &StorePath::from("c=replaceable/k=0/p=0/dt=2026-05-02/events-staging.tmp"),
                PutPayload::from_static(b"junk-not-parquet"),
            )
            .await
            .unwrap();
        // A kind dir that contains ONLY a non-parquet file must not appear.
        store
            .put(
                &StorePath::from("c=replaceable/k=3/p=0/dt=2026-05-02/staging.tmp"),
                PutPayload::from_static(b"more-junk"),
            )
            .await
            .unwrap();

        let after = archive.scan_resident_usage(4, &slugs).await.unwrap();
        let measured = after.iter().find(|u| u.kind == 0).unwrap();
        assert_eq!(
            (measured.total_bytes, measured.file_count),
            (baseline.total_bytes, baseline.file_count),
            "non-parquet staging file must be ignored"
        );
        assert!(
            after.iter().all(|u| u.kind != 3),
            "kind dir with only a non-parquet file must be absent: {after:?}"
        );
    }

    #[tokio::test]
    async fn recover_from_wal_picks_up_orphan_wal_files() {
        let dir = tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        // Stage 1: write events through the archive, then *abandon* the
        // ChunkedArchive without shutting it down. WAL files remain on disk
        // with un-published events.
        let archive1 = ChunkedArchive::new(
            store.clone(),
            dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
            None,
        );
        let mut originals: Vec<Event> = (0..3).map(|i| make_event(0, &format!("e{i}"))).collect();
        for e in &originals {
            archive1.put_event(e, "profile-metadata").await.unwrap();
        }
        // Also stage a regular-class event so recovery exercises the
        // "regular" WAL-filename parse branch.
        let regular = make_event(9735, "zap receipt");
        archive1.put_event(&regular, "zap-receipts").await.unwrap();
        originals.push(regular);
        // Wait for the writer task to drain the channel + fsync the WAL.
        // We can't observe this directly without changing public API, so
        // poll the WAL file size until it's stable and non-zero.
        wait_for_wal(&dir.path().join("_wal")).await;
        // Drop the archive without calling shutdown — emulates a crash where
        // graceful shutdown didn't run. We can't drop archive1 cleanly with
        // outstanding JoinHandles, so abort them via a forced drop.
        std::mem::forget(archive1);

        // Stage 2: a new ChunkedArchive recovers from WAL.
        let archive2 = ChunkedArchive::new(
            store,
            dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
            None,
        );
        let configured = vec![
            (
                EventClass::Replaceable,
                0_u16,
                "profile-metadata".to_string(),
            ),
            (EventClass::Regular, 9735_u16, "zap-receipts".to_string()),
        ];
        let spawned = archive2.recover_from_wal(&configured).await;
        assert!(spawned >= 1, "at least one wal file should be replayed");

        archive2.force_flush_all().await;
        archive2.shutdown().await;

        let all = reader::read_all_events(dir.path()).unwrap();
        let recovered_ids: std::collections::HashSet<_> = all.iter().map(|r| r.event.id).collect();
        for e in &originals {
            assert!(
                recovered_ids.contains(&e.id),
                "event {} not recovered",
                e.id
            );
        }
    }

    /// Wait for any `_wal/*.wal` files in `wal_dir` to appear and become
    /// non-empty, then for their size to stabilize (a proxy for "writer
    /// finished its appends"). Times out at ~5s.
    async fn wait_for_wal(wal_dir: &std::path::Path) {
        use tokio::time::{Duration, sleep};
        for _ in 0..50 {
            sleep(Duration::from_millis(100)).await;
            let mut total = 0u64;
            if let Ok(rd) = std::fs::read_dir(wal_dir) {
                for e in rd.flatten() {
                    if let Ok(meta) = e.metadata() {
                        total += meta.len();
                    }
                }
            }
            if total > 0 {
                // One more pause to let any in-flight fsync complete.
                sleep(Duration::from_millis(100)).await;
                return;
            }
        }
    }

    // ------------------------------------------------------------------
    // FlushControl::flush_kind — per-kind ad-hoc flush.
    // ------------------------------------------------------------------

    use std::time::Duration as StdDuration;

    const TO: StdDuration = StdDuration::from_secs(10);

    /// `flush_kind` targets only the requested kind's writers; other kinds stay
    /// buffered until shutdown. Also checks the summary fields.
    #[tokio::test]
    async fn flush_kind_targets_only_matching_kind() {
        let (archive, chunks_root, _dir) = rig();

        // Three kind-0 (profile-metadata) events, which are replaceable, plus
        // one kind-30063 (zapstore-app-releases), which is addressable.
        for i in 0..3 {
            archive
                .put_event(&make_event(0, &format!("p{i}")), "profile-metadata")
                .await
                .unwrap();
        }
        archive
            .put_event(&make_addressable("app@1"), "zapstore-app-releases")
            .await
            .unwrap();

        let summary = archive.flush_kind(0, TO).await;
        assert_eq!(summary.kind, 0);
        assert!(summary.writers_signaled >= 1);
        assert!(summary.shards_published >= 1);
        assert_eq!(summary.rows, 3, "all three kind-0 events published");
        assert!(summary.errors.is_empty());
        assert_eq!(summary.timed_out, 0);

        // Only kind 0 is on disk; kind 30063 is still buffered.
        let on_disk = reader::read_all_events(&chunks_root).unwrap();
        assert_eq!(on_disk.len(), 3, "only kind-0 events flushed: {on_disk:?}");
        assert!(on_disk.iter().all(|r| r.event.kind.as_u16() == 0));

        // Shutdown flushes the rest; now both kinds are present.
        archive.shutdown().await;
        let all = reader::read_all_events(&chunks_root).unwrap();
        assert_eq!(all.len(), 4);
    }

    /// A configured-but-idle kind (no writer spawned yet) flushes to a no-op
    /// summary rather than erroring.
    #[tokio::test]
    async fn flush_kind_idle_kind_signals_no_writers() {
        let (archive, _chunks_root, _dir) = rig();
        let summary = archive.flush_kind(31337, TO).await;
        assert_eq!(summary.writers_signaled, 0);
        assert_eq!(summary.shards_published, 0);
        assert_eq!(summary.rows, 0);
        assert!(summary.errors.is_empty());
        assert_eq!(summary.timed_out, 0);
        archive.shutdown().await;
    }

    /// No event loss when ad-hoc flushes race live ingest: a producer keeps
    /// `put_event`-ing while `flush_kind` fires repeatedly. After shutdown every
    /// unique event id appears exactly once — no loss, no duplication.
    #[tokio::test]
    async fn flush_kind_concurrent_with_ingest_no_loss() {
        const N: usize = 200;
        let (archive, chunks_root, _dir) = rig();
        let archive = Arc::new(archive);
        let mut sent_ids = std::collections::HashSet::new();

        // Fire ad-hoc flushes concurrently with ingest.
        let flusher = {
            let archive = Arc::clone(&archive);
            tokio::spawn(async move {
                for _ in 0..20 {
                    let _ = archive.flush_kind(0, TO).await;
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
        };

        for i in 0..N {
            let e = make_event(0, &format!("e{i}"));
            sent_ids.insert(e.id);
            archive.put_event(&e, "profile-metadata").await.unwrap();
        }
        flusher.await.unwrap();

        archive.shutdown().await;
        let all = reader::read_all_events(&chunks_root).unwrap();
        let got: std::collections::HashSet<_> = all.iter().map(|r| r.event.id).collect();
        assert_eq!(
            all.len(),
            N,
            "exactly N events on disk (no dup): got {}",
            all.len()
        );
        assert_eq!(
            got, sent_ids,
            "every sent id present exactly once (no loss)"
        );
    }

    /// Many concurrent `flush_kind` calls for the same kind don't double-publish
    /// or lose events: the first wins, the rest see an empty buffer.
    #[tokio::test]
    async fn flush_kind_concurrent_duplicate_calls_are_safe() {
        const N: usize = 50;
        let (archive, chunks_root, _dir) = rig();
        let archive = Arc::new(archive);
        let mut ids = std::collections::HashSet::new();
        for i in 0..N {
            let e = make_event(0, &format!("e{i}"));
            ids.insert(e.id);
            archive.put_event(&e, "profile-metadata").await.unwrap();
        }

        // 8 concurrent flushes of the same kind.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let archive = Arc::clone(&archive);
            handles.push(tokio::spawn(async move { archive.flush_kind(0, TO).await }));
        }
        let mut total_rows = 0u64;
        for h in handles {
            total_rows += h.await.unwrap().rows;
        }
        // Across all concurrent calls, exactly the N rows are published once.
        assert_eq!(
            total_rows, N as u64,
            "rows published exactly once across racing calls"
        );

        archive.shutdown().await;
        let all = reader::read_all_events(&chunks_root).unwrap();
        let got: std::collections::HashSet<_> = all.iter().map(|r| r.event.id).collect();
        assert_eq!(all.len(), N);
        assert_eq!(got, ids);
    }

    /// Metric continuity: an ad-hoc flush increments `chunks_published_total`
    /// and sets `chunks_last_published_timestamp_seconds` exactly like a
    /// timer/size/rows roll - so a dashboard's chunk publish rate and time
    /// since the last chunk reflect ad-hoc publishes.
    #[tokio::test]
    async fn flush_kind_updates_published_metrics() {
        use nostr::{EventBuilder, Keys, Kind, Timestamp};

        let dir = tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(nozzl_metrics::Metrics::new(&registry).expect("register"));
        let archive = ChunkedArchive::new(
            store,
            dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
            Some(Arc::clone(&metrics)),
        );

        // Fixed keypair so every event lands on one shard, giving a known label.
        let keys = Keys::generate();
        let shard = PubkeyShard::from_pubkey_hex(&keys.public_key().to_hex()).unwrap();
        let shard_str = shard.as_char().to_string();
        for i in 0..4 {
            let e = EventBuilder::new(Kind::from_u16(0), format!("m{i}"))
                .custom_created_at(Timestamp::from_secs(1_700_000_000))
                .sign_with_keys(&keys)
                .unwrap();
            archive.put_event(&e, "profile-metadata").await.unwrap();
        }

        let summary = archive.flush_kind(0, TO).await;
        assert_eq!(summary.shards_published, 1);
        assert_eq!(summary.rows, 4);

        let labels = ["replaceable", "0", "profile-metadata", shard_str.as_str()];
        assert_eq!(
            metrics.chunks_published.with_label_values(&labels).get(),
            1,
            "ad-hoc publish must increment chunks_published_total"
        );
        assert!(
            metrics
                .chunks_last_published_timestamp_seconds
                .with_label_values(&labels)
                .get()
                > 0.0,
            "ad-hoc publish must set chunks_last_published_timestamp_seconds"
        );

        archive.shutdown().await;
    }
}
