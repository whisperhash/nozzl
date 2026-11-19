//! `nozzl` — the binary's orchestration layer, exposed as a library so
//! end-to-end tests can drive the whole stack in-process.
//!
//! Public surface:
//!  - [`run`] — wire every workspace crate together and block until shutdown.
//!  - [`mappings`] — pure transforms used to derive each subsystem's config.
//!  - [`writer`] — single consumer task that drains the acquirer's mpsc into
//!    the chunked archive (which demuxes to per-(kind, shard) writer tasks).
//!  - [`health`] — the `HealthSource` implementation.

#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

pub mod health;
pub mod mappings;
pub mod writer;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use nozzl_acquirer::{Acquirer, AcquirerConfig, EventEnvelope};
use nozzl_checkpoint::{CheckpointStore, FileCheckpointStore};
use nozzl_config::{Config, LogFormat};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::health::SharedHealth;
use crate::mappings::{acquirer_config_from, storage_config_from};

/// Maximum time we'll wait for graceful shutdown before exiting anyway.
/// Sized for the chunked archive: at high ingest rates each of the ~96 writer
/// tasks can hold a non-trivial buffer; a final flush has to encode Parquet
/// (incl. zstd compression and bloom filter construction) for each before
/// publishing. 120 s is comfortable headroom even on slower disks. Anything
/// not flushed by the deadline survives in the WAL and is replayed on the
/// next startup.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(120);

/// How long an ad-hoc `POST /admin/flush/{kind}` waits for each shard writer to
/// confirm its publish before giving up on *that shard* (reported as
/// `timed_out` in the response). A timeout is not data loss: the flush is
/// already enqueued and completes shortly after — we merely stop waiting.
/// Generous because a flush encodes Parquet (zstd + bloom filters) and PUTs
/// per shard. Not operator-configurable; promote to config only if a real need
/// for tuning appears.
const ADHOC_FLUSH_TIMEOUT: Duration = Duration::from_secs(60);

/// Run the service.
///
/// `cfg_path` is the path to `nozzl.toml`; the relay/kind list paths are
/// resolved through it. When `check_only` is true, the function returns
/// `Ok(())` once the config has loaded and the storage and acquirer settings
/// startup derives from it have been built, having printed `"config OK"` to
/// stdout, without starting any background work.
///
/// Returns once a SIGTERM or SIGINT has been received and the graceful
/// shutdown sequence has completed.
#[allow(clippy::too_many_lines)] // linear startup → spawn-and-supervise wiring; splitting would obscure the order
pub async fn run(cfg_path: PathBuf, check_only: bool) -> Result<()> {
    // 1) Load config. Fail fast.
    let initial_cfg = nozzl_config::load(&cfg_path)
        .with_context(|| format!("loading config from {}", cfg_path.display()))?;

    // 2) --check short-circuit. Print to stdout for shell-friendly use. It
    // builds what startup builds from the config first: the acquirer parses each
    // relay URL again, as rust-nostr's RelayUrl, which refuses some URLs the
    // validator accepts, and a file that only loads would pass here and then
    // fail at startup, or be ignored by a running binary's reload.
    if check_only {
        storage_config_from(&initial_cfg).context("building the storage settings")?;
        acquirer_config_from(&initial_cfg).context("building the acquirer settings")?;
        println!("config OK");
        return Ok(());
    }

    // 3) Subscriber. After this point, `tracing` macros emit to stderr (and,
    // if `[observability] log_file` is set, also to the rolling log file).
    // The guard MUST be held until shutdown — dropping it earlier would let
    // the non-blocking writer's worker thread exit and silently drop logs.
    let _log_guard = init_tracing(&initial_cfg)?;

    let initial_cfg = Arc::new(initial_cfg);

    log_startup_banner(&initial_cfg);

    // 4) Health + metrics. Installed before the archive so the archive can
    // increment `chunks_published_total` and set
    // `chunks_last_published_timestamp_seconds` from its writer tasks. The
    // /metrics HTTP listener is up early, so scrapes during a slow startup
    // return the current (mostly zero) series rather than connection refused.
    let health = SharedHealth::new();
    let (metrics, _registry, metrics_handle) = nozzl_http::install_metrics(
        initial_cfg.observability.metrics_addr,
        Arc::clone(&health) as Arc<dyn nozzl_http::HealthSource>,
    )
    .with_context(|| {
        format!(
            "installing metrics endpoint on {}",
            initial_cfg.observability.metrics_addr
        )
    })?;

    // 5) Storage backend + archive.
    let storage_cfg = storage_config_from(&initial_cfg)?;
    // The WAL lives next to the chunks for local-FS deployments. For S3, the
    // WAL still lives on local disk under the same path the local backend
    // would use; the binary is responsible for exposing that to operators.
    let wal_root = resolve_wal_root(&storage_cfg, initial_cfg.storage.root.as_deref());
    let chunk_cfg = nozzl_archive::ChunkWriterConfig {
        chunk_max_size_bytes: initial_cfg.runtime.chunk_max_size_bytes,
        chunk_max_rows: initial_cfg.runtime.chunk_max_rows,
        chunk_max_buffer_seconds: initial_cfg.runtime.chunk_max_buffer_seconds,
        chunk_buffer_age_jitter: initial_cfg.runtime.chunk_buffer_age_jitter,
        chunk_flush_concurrency: initial_cfg.runtime.chunk_flush_concurrency,
        wal_fsync_batch_ms: initial_cfg.runtime.wal_fsync_batch_ms,
        parquet_zstd_level: initial_cfg.runtime.parquet_zstd_level,
    };
    let archive = nozzl_archive::build_archive(
        &storage_cfg,
        wal_root,
        chunk_cfg,
        Some(Arc::clone(&metrics)),
    )
    .context("building archive")?;
    // Replay any orphan WAL files left from a previous crash. Configured kinds
    // come from the kind list — each gets its writers spawned and any
    // pre-existing WAL contents replayed before the consumer task opens.
    {
        let configured: Vec<(nozzl_types::EventClass, u16, String)> = initial_cfg
            .kinds
            .iter()
            .map(|k| {
                let class = nozzl_types::EventClass::classify(k.k);
                (class, k.k, k.slug.clone())
            })
            .collect();
        let recovered = archive.recover_from_wal(&configured).await;
        if recovered > 0 {
            info!(wal_files = recovered, "wal recovery: spawned writer tasks");
        }
    }

    // 6) Checkpoint store.
    let ckpt = FileCheckpointStore::open(
        initial_cfg.checkpoint.path.clone(),
        Duration::from_secs(initial_cfg.checkpoint.flush_interval_seconds),
    )
    .await
    .context("opening checkpoint store")?;

    // 7) The bounded mpsc between acquirer and the event-consumer task.
    let (sink, recv) = mpsc::channel::<EventEnvelope>(initial_cfg.runtime.channel_capacity);

    // 8) Single consumer task draining the channel into the chunked archive.
    let archive_for_consumer: Arc<dyn nozzl_archive::Archive> =
        Arc::clone(&archive) as Arc<dyn nozzl_archive::Archive>;
    let writer_handle = writer::spawn_consumer(recv, archive_for_consumer, Arc::clone(&metrics));

    // 9) Acquirer.
    let acquirer_cfg = acquirer_config_from(&initial_cfg)?;
    let acquirer = Acquirer::spawn(
        acquirer_cfg.clone(),
        sink.clone(),
        Arc::clone(&ckpt) as Arc<dyn nozzl_checkpoint::CheckpointStore>,
        Arc::clone(&metrics),
    );

    // 9.5) Pre-allocate every `/metrics` time series whose label
    // combination is known at startup. Without this, Prometheus *Vec
    // metrics are lazy-created on first observation — so a slow-to-fire
    // counter like `chunks_published_total` (gated on the 1 h buffer-age
    // trigger by default) is absent from `/metrics` for up to an hour
    // after restart, and dashboards `lastNotNull`-fall-back to the
    // pre-restart value. See `Metrics::preallocate_known_series` for the
    // full rationale and the list of metrics it covers.
    {
        let relay_labels: Vec<&str> = acquirer_cfg
            .relays
            .iter()
            .map(nozzl_types::RelayUrl::as_str)
            .collect();
        let kind_slug_pairs: Vec<(u16, &str)> = initial_cfg
            .kinds
            .iter()
            .map(|k| (k.k, k.slug.as_str()))
            .collect();
        // Filter the preallocation set down to classes the writer pool
        // will actually emit chunks for. `EventClass::chunk_metric_label`
        // is the single source of truth — it returns `None` for classes
        // we don't archive (today: Regular / Ephemeral). When extending
        // the archived set (e.g. adding regular-kind support), only that
        // method changes and the preallocation surface follows
        // automatically.
        let class_kind_slugs: Vec<(&str, u16, &str)> = initial_cfg
            .kinds
            .iter()
            .filter_map(|k| {
                let label = nozzl_types::EventClass::classify(k.k).chunk_metric_label()?;
                Some((label, k.k, k.slug.as_str()))
            })
            .collect();
        metrics.preallocate_known_series(nozzl_metrics::PreallocSeed {
            relays: &relay_labels,
            kind_slugs: &kind_slug_pairs,
            class_kind_slugs: &class_kind_slugs,
        });
    }

    // 10) Periodic gauge updater for channel depth, health snapshot, and
    //     checkpoint lag.
    //
    // Hand the gauge loop owned `RelayUrl`s. It reads connection state through
    // the typed `Metrics::relay_connected(&RelayUrl)` accessor — the same one
    // the acquirer writes through — so the health read and the writers share
    // one normalization path and can't disagree on the label.
    let gauge_relays: Vec<nozzl_types::RelayUrl> = acquirer_cfg.relays.clone();
    // Shared kind→slug map. The acquirer attaches slugs to event envelopes
    // from its own private copy; this mirror exists so the gauge loop can
    // attach the same slug label to `checkpoint_lag_seconds`. Updated below
    // on each hot-reload, before `apply_config` is called, so the next gauge
    // tick observes the new mapping.
    let gauge_kind_slugs: Arc<RwLock<HashMap<u16, String>>> =
        Arc::new(RwLock::new(acquirer_cfg.kind_slugs.clone()));
    // Shared active-pairs set: every `(relay_normalized, kind)` the acquirer
    // is currently subscribed to (after per-relay `only_kinds` intersection).
    // Drives both the `kind_subscribed` metric emission and the filter applied
    // before `checkpoint_lag_seconds` is updated, so a status watcher and
    // any other consumer see exactly the pairs being
    // actively archived — stale `(relay, kind)` entries from prior runs
    // remain in `checkpoints.json` but are absent from the metric surface.
    let gauge_active_pairs: Arc<RwLock<std::collections::HashSet<(String, u16)>>> =
        Arc::new(RwLock::new(compute_active_pairs(&acquirer_cfg)));
    let gauge_handle = spawn_gauge_loop(
        sink.clone(),
        Arc::clone(&metrics),
        Arc::clone(&health),
        gauge_relays,
        Arc::clone(&ckpt) as Arc<dyn CheckpointStore>,
        Arc::clone(&gauge_kind_slugs),
        Arc::clone(&gauge_active_pairs),
    );

    // 10.5) Periodic per-kind resident-storage scan → `storage_resident_bytes`
    //       + `storage_resident_files`.
    //       Optional (off when `storage_metrics.enabled = false`); much slower
    //       cadence than the 1 Hz gauge loop, so it's a separate task.
    let storage_scan_handle = if initial_cfg.storage_metrics.enabled {
        info!(
            interval_s = initial_cfg.storage_metrics.scan_interval_seconds,
            concurrency = initial_cfg.storage_metrics.scan_concurrency,
            "storage resident scan enabled"
        );
        Some(spawn_storage_scan_loop(
            Arc::clone(&archive),
            Arc::clone(&metrics),
            &initial_cfg.storage_metrics,
            // Slug is no longer on disk; the scan resolves each kind's slug label
            // from this hot-reload-tracked mirror (read-locked per pass).
            Arc::clone(&gauge_kind_slugs),
        ))
    } else {
        info!("storage resident scan disabled (storage_metrics.enabled = false)");
        None
    };

    // 10.7) Optional admin/control + extraction HTTP listener
    //       (`POST /admin/flush/{kind}` and `GET /export/{kind}`).
    //       Opt-in via `observability.admin_addr`. Wired here — after
    //       `gauge_kind_slugs` exists — so the flush handler validates the
    //       requested kind against the *hot-reload-tracked* configured set: a
    //       kind added via `kinds.toml` reload validates, and a removed one
    //       starts returning 404. Shares the gauge loop's mirror rather than
    //       snapshotting the kind list at startup.
    //
    //       Export reads a LOCAL Parquet archive root; under the S3 backend there
    //       is none (chunks are in the bucket), so resolve_export_root returns
    //       None and export returns 503 while flush still works. Keyed on the
    //       backend, NOT storage.root — under S3 storage.root is the WAL root.
    //       `duckdb_bin` defaults to "duckdb" on PATH.
    let admin_handle = if let Some(admin_addr) = initial_cfg.observability.admin_addr {
        let export_root =
            resolve_export_root(initial_cfg.storage.backend, initial_cfg.storage.root.as_deref());
        let duckdb_bin = initial_cfg
            .observability
            .duckdb_bin
            .clone()
            .unwrap_or_else(|| std::path::PathBuf::from("duckdb"));
        let handle = nozzl_http::install_admin(
            admin_addr,
            Arc::clone(&archive) as Arc<dyn nozzl_archive::FlushControl>,
            Arc::clone(&gauge_kind_slugs),
            ADHOC_FLUSH_TIMEOUT,
            export_root,
            duckdb_bin,
            metrics.export_requests_total.clone(),
        )
        .with_context(|| format!("installing admin endpoint on {admin_addr}"))?;
        info!(%admin_addr, "admin endpoint enabled (flush + export)");
        Some(handle)
    } else {
        info!("admin endpoint disabled (observability.admin_addr unset)");
        None
    };

    // 11) Config-watch loop. Forwards new acquirer configs over a channel
    // so the acquirer stays owned by the main task (its `shutdown` consumes
    // Self).
    let (reload_tx, mut reload_rx) = mpsc::channel::<AcquirerConfig>(8);
    let watcher_handle = spawn_watcher_loop(
        cfg_path.clone(),
        Arc::clone(&initial_cfg),
        reload_tx,
    );

    info!("nozzl: startup complete; awaiting shutdown signal");

    // 12) Wait for SIGTERM / SIGINT, while concurrently servicing reloads.
    let shutdown_signal = wait_for_shutdown_signal();
    tokio::pin!(shutdown_signal);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown_signal => break,
            Some(new_acq_cfg) = reload_rx.recv() => {
                // Refresh the gauge loop's mirrors before handing the
                // config to the acquirer, so a tick that fires immediately
                // after `apply_config` already sees the new state.
                if let Ok(mut guard) = gauge_kind_slugs.write() {
                    guard.clone_from(&new_acq_cfg.kind_slugs);
                }
                if let Ok(mut guard) = gauge_active_pairs.write() {
                    *guard = compute_active_pairs(&new_acq_cfg);
                }
                acquirer.apply_config(new_acq_cfg).await;
                info!("config hot-reload applied");
            }
        }
    }

    info!("nozzl: shutdown signal received; beginning graceful shutdown");

    // Run the shutdown sequence on a deadline; if anything hangs we still
    // exit, leaving the orchestrator to restart us.
    let shutdown = async {
        // 1) Stop the watcher first so it can't push more reloads.
        watcher_handle.abort();
        // 2) Stop the gauge loop and *wait* for it to actually drop its sink
        // clone. `abort()` alone only sends a cancellation; the task must be
        // polled to drop its captures. Awaiting the handle blocks until it's
        // truly gone. Doing this before the writer-pool drain is critical —
        // otherwise writers see the gauge loop's sender as alive and never
        // observe end-of-stream.
        gauge_handle.abort();
        let _ = gauge_handle.await;
        // 2b) Stop the storage-scan loop. It holds no channel sinks, so its
        // ordering relative to the writer drain is unimportant — but abort it
        // here so a long in-flight scan doesn't outlive shutdown.
        if let Some(h) = storage_scan_handle {
            h.abort();
            let _ = h.await;
        }
        // 3) Stop the acquirer so no new events flow into the channel and the
        // acquirer's sink clones are dropped.
        acquirer.shutdown().await;
        // 4) Drop the original sink so the consumer sees end-of-stream once
        // it drains the channel.
        drop(sink);
        // 5) Wait for the consumer task to finish draining.
        let _ = writer_handle.await;
        // 6) Tell the archive to flush every writer task's pending buffer
        // and truncate the WALs.
        archive.shutdown().await;
        // 7) Final checkpoint flush via dedicated shutdown.
        if let Err(e) = ckpt.shutdown().await {
            warn!(error = %e, "checkpoint shutdown reported error");
        }
        // 8) Stop the metrics server.
        metrics_handle.abort();
        // 8b) Stop the admin server, if it was enabled.
        if let Some(h) = admin_handle {
            h.abort();
        }
    };

    if tokio::time::timeout(SHUTDOWN_BUDGET, shutdown).await.is_err() {
        warn!(
            budget_secs = SHUTDOWN_BUDGET.as_secs(),
            "shutdown budget exceeded; exiting anyway"
        );
    } else {
        info!("nozzl: shutdown complete");
    }

    Ok(())
}

/// Resolve the on-disk root for the WAL (and other local state).
///
/// - **Local-FS backend:** the WAL lives alongside the chunks, at the configured
///   `[storage].root` (carried here as `storage_cfg.local.root`).
/// - **S3 backend:** chunks go to the bucket, but the WAL still lives on local
///   disk. Honor `[storage].root` as that local-state root when set — so it can
///   point at a durable mounted volume — falling back to `./data/events` when
///   omitted.
///
/// The S3 branch previously hardcoded `./data/events` (relative to the process
/// cwd, e.g. `/home/nozzl` in the container), silently ignoring `[storage].root`
/// and stranding the WAL in the container's ephemeral layer. This restores the
/// intent documented at the call site.
fn resolve_wal_root(
    storage_cfg: &nozzl_archive::StorageConfig,
    config_root: Option<&std::path::Path>,
) -> PathBuf {
    match &storage_cfg.local {
        Some(local) => local.root.clone(),
        None => match config_root {
            Some(p) => p.to_path_buf(),
            None => PathBuf::from("./data/events"),
        },
    }
}

/// Resolve the root the `/export` endpoint reads local Parquet from.
///
/// Export shells out to duckdb against an on-disk `c=*/k=*/…` tree, so it only
/// works when the archive is local. Under the S3 backend the chunks live in the
/// bucket and there is nothing local to read — return `None` so the admin server
/// answers `/export` with an honest 503 ("unsupported on this deploy") rather
/// than globbing an empty dir and reporting "no data" for every kind. Keyed on
/// the BACKEND, not on `[storage].root`: under S3 `root` is the WAL root (a real
/// path), so reusing it here would silently break the 503 path.
fn resolve_export_root(
    backend: nozzl_config::StorageBackend,
    config_root: Option<&std::path::Path>,
) -> Option<PathBuf> {
    match backend {
        nozzl_config::StorageBackend::Local => config_root.map(std::path::Path::to_path_buf),
        nozzl_config::StorageBackend::S3 => None,
    }
}

/// Initialize the global `tracing` subscriber from config.
///
/// Returns the file-appender's `WorkerGuard` when `[observability] log_file`
/// is set; the caller MUST hold it until shutdown so the non-blocking
/// writer's worker thread keeps draining queued log lines.
///
/// `try_init` is a no-op (returns Err) if a subscriber is already set; e2e
/// tests in the same process call `run` multiple times, so we treat "already
/// initialized" as a successful-enough outcome and only error on genuine
/// config-level problems (a malformed `log_level` string, an unwritable log
/// directory).
///
/// Why the file appender is safe to add: `tracing_appender::non_blocking`
/// hands log lines to a bounded MPSC and returns immediately — file I/O,
/// rotation, and retention pruning all happen on a dedicated worker thread.
/// Disk full, permission errors, or a failed roll log to stderr and drop
/// further file writes; the relay/archive/checkpoint hot paths are never
/// blocked or panicked by file-side failures. Channel saturation drops file
/// log lines (never blocks the caller); stderr remains unaffected.
fn init_tracing(cfg: &Config) -> Result<Option<WorkerGuard>> {
    let filter = EnvFilter::try_new(&cfg.observability.log_level).with_context(|| {
        format!(
            "invalid log_level {:?}",
            cfg.observability.log_level.as_str()
        )
    })?;

    let stderr_layer = make_fmt_layer(cfg.observability.log_format, std::io::stderr, true);

    let (file_layer, guard) = if let Some(path) = cfg.observability.log_file.as_ref() {
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .with_context(|| format!("log_file {} has no parent directory", path.display()))?;
        let prefix = path
            .file_name()
            .with_context(|| format!("log_file {} has no file name", path.display()))?
            .to_str()
            .context("log_file name is not valid UTF-8")?;
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating log directory {}", dir.display()))?;
        let appender = build_log_appender(dir, prefix, cfg.observability.log_file_max_files)
            .with_context(|| format!("creating rolling log appender at {}", path.display()))?;
        let (writer, guard) = tracing_appender::non_blocking(appender);
        let layer = make_fmt_layer(cfg.observability.log_format, writer, false);
        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    let registry = tracing_subscriber::registry().with(filter).with(stderr_layer);
    let result = match file_layer {
        Some(layer) => registry.with(layer).try_init(),
        None => registry.try_init(),
    };

    if result.is_err() {
        // A subscriber was already installed (typical in tests that call
        // `run` repeatedly). Drop the guard so we don't keep an orphaned
        // worker thread around with no subscriber pointing at it.
        return Ok(None);
    }
    Ok(guard)
}

/// Build the daily-rotating file appender for the log file.
///
/// `max_files == None` (the config key was omitted) means **indefinite
/// retention**: we leave the builder's limit unset, so the appender never
/// prunes. `Some(n)` is passed straight through to `.max_log_files(n)`, which
/// keeps the `n` most-recent files. `n` is guaranteed `>= 1` by
/// [`nozzl_config::Config::validate`], which rejects `Some(0)` — passing `0`
/// here would underflow tracing-appender's prune logic (panic in debug,
/// delete-all in release), so it must never reach this function.
fn build_log_appender(
    dir: &std::path::Path,
    prefix: &str,
    max_files: Option<usize>,
) -> std::result::Result<RollingFileAppender, tracing_appender::rolling::InitError> {
    let mut builder = RollingFileAppender::builder()
        .filename_prefix(prefix)
        .rotation(Rotation::DAILY);
    if let Some(n) = max_files {
        builder = builder.max_log_files(n);
    }
    builder.build(dir)
}

/// Build an `fmt` layer with the configured format and ANSI policy. Used for
/// both the stderr and file sinks; the file sink disables ANSI so log files
/// don't accumulate escape sequences.
fn make_fmt_layer<S, W>(
    format: LogFormat,
    writer: W,
    ansi: bool,
) -> Box<dyn Layer<S> + Send + Sync + 'static>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    match format {
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .with_writer(writer)
                .with_ansi(ansi)
                .json()
                .with_current_span(true)
                .with_span_list(false),
        ),
        LogFormat::Pretty => Box::new(
            tracing_subscriber::fmt::layer()
                .with_writer(writer)
                .with_ansi(ansi),
        ),
    }
}

fn log_startup_banner(cfg: &Config) {
    let kinds: Vec<u16> = cfg.kinds.iter().map(|k| k.k).collect();
    let relays: Vec<&str> = cfg.relays.iter().map(|r| r.url.as_str()).collect();
    info!(
        version = env!("CARGO_PKG_VERSION"),
        git_sha = nozzl_metrics::GIT_SHA,
        env = environment_tag(),
        kinds = ?kinds,
        relays = ?relays,
        "nozzl starting"
    );
    // Complete effective-config snapshot at startup, dumped in full rather than
    // as a hand-picked subset, so the running configuration can always be
    // reconstructed from the logs alone — every scalar, plus the per-kind slugs
    // and per-relay `only_kinds` carried by the nested `Debug`. This is the
    // startup snapshot: it equals the effective config until a hot-reload of the
    // relay/kind lists (the only hot-applied fields); restart-required and
    // read-once fields stay at exactly these values for the process lifetime, so
    // this line is the authoritative record of them.
    //
    // Logged at INFO (not DEBUG) on purpose: it must be captured on every
    // deployment regardless of `log_level`, and one line per process start is
    // negligible. Dumping the whole `Config` via `Debug` is complete-by-
    // construction — a newly-added schema field cannot be silently omitted here
    // (the old hand-enumerated form drifted as the schema grew). `Config`
    // carries no secrets, so a full dump is safe.
    info!(config = ?cfg, "effective config (startup snapshot)");
}

/// Best-effort environment tag. Pulled from `NOZZL_ENV` if set; otherwise
/// `"unknown"`. Avoids piling another required field into the TOML schema.
fn environment_tag() -> String {
    std::env::var("NOZZL_ENV").unwrap_or_else(|_| "unknown".to_string())
}

/// Periodically refresh `channel_depth`, `checkpoint_lag_seconds`, and the
/// health snapshot. The lag update is tick-based (not per-`observe`) on
/// purpose: the gauge's whole job is to *grow* when nothing arrives, so
/// updating only on observation would silently freeze it during stalls.
///
/// `relays` is the set the health snapshot iterates. The loop reads each
/// relay's state through [`nozzl_metrics::Metrics::relay_connected`] — the
/// same typed, [`RelayUrl`]-keyed accessor the acquirer writes through — so
/// the read and write labels are normalized identically by construction and
/// can't drift (the previous hand-maintained `Vec<String>` of normalized
/// labels could).
///
/// `kind_slugs` is the shared mirror of the acquirer's slug map, refreshed
/// on hot-reload by the main task before `apply_config` runs. The gauge loop
/// reads it under a short-lived lock to attach the slug label to each
/// `checkpoint_lag_seconds` series; an unconfigured kind (one present in the
/// checkpoint but no longer in `kinds.toml`) gets the empty-string sentinel,
/// matching the convention used elsewhere in the metric surface.
fn spawn_gauge_loop(
    sink: mpsc::Sender<EventEnvelope>,
    metrics: Arc<nozzl_metrics::Metrics>,
    health: Arc<SharedHealth>,
    relays: Vec<nozzl_types::RelayUrl>,
    ckpt: Arc<dyn CheckpointStore>,
    kind_slugs: Arc<RwLock<HashMap<u16, String>>>,
    active_pairs: Arc<RwLock<std::collections::HashSet<(String, u16)>>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // What we emitted as `kind_subscribed = 1` on the previous tick.
        // Pairs that disappear from `active_pairs` (because kinds.toml was
        // edited or a relay was removed) are explicitly cleared via
        // `remove_label_values` so the metric reflects the current state
        // rather than holding 1 forever on labels nobody is subscribing to.
        let mut last_subscribed: std::collections::HashMap<(String, u16), String> =
            std::collections::HashMap::new();
        loop {
            tick.tick().await;
            // Channel depth: max_capacity is the configured bound; capacity()
            // is the remaining permits. Their difference is in-flight events.
            let total = sink.max_capacity();
            let free = sink.capacity();
            let in_flight = total.saturating_sub(free);
            metrics
                .channel_depth
                .set(i64::try_from(in_flight).unwrap_or(i64::MAX));

            // Health snapshot: a relay is "connected" if its typed
            // relay_connection_state reads `Connected`. Snapshot every
            // configured relay; if any one is connected, we're healthy.
            let any_connected = relays.iter().any(|relay| metrics.relay_connected(relay));
            health.set_connected(any_connected);

            // Snapshot the shared mirrors under short-lived locks; locks are
            // never held across an await point.
            let (slugs_snapshot, active_snapshot) = {
                let slugs = match kind_slugs.read() {
                    Ok(g) => g.clone(),
                    Err(poisoned) => poisoned.into_inner().clone(),
                };
                let active = match active_pairs.read() {
                    Ok(g) => g.clone(),
                    Err(poisoned) => poisoned.into_inner().clone(),
                };
                (slugs, active)
            };

            // Refresh `kind_subscribed = 1` for every currently-active pair,
            // and remove the label for any pair that left the active set
            // since the last tick (config reload that shrunk it).
            let mut current_subscribed: std::collections::HashMap<(String, u16), String> =
                std::collections::HashMap::with_capacity(active_snapshot.len());
            for (relay, kind) in &active_snapshot {
                let slug = slugs_snapshot
                    .get(kind)
                    .cloned()
                    .unwrap_or_default();
                let kind_str = kind.to_string();
                metrics
                    .kind_subscribed
                    .with_label_values(&[relay.as_str(), kind_str.as_str(), slug.as_str()])
                    .set(1);
                current_subscribed.insert((relay.clone(), *kind), slug);
            }
            for (pair, slug) in &last_subscribed {
                if !current_subscribed.contains_key(pair) {
                    let kind_str = pair.1.to_string();
                    let _ = metrics.kind_subscribed.remove_label_values(&[
                        pair.0.as_str(),
                        kind_str.as_str(),
                        slug.as_str(),
                    ]);
                }
            }
            last_subscribed = current_subscribed;

            // Checkpoint lag: now() - since per (relay, kind, slug), filtered
            // to the active subscription set. Pairs the binary is no longer
            // subscribed to (because a relay has an only_kinds restriction
            // excluding them, or kinds.toml shrunk) keep their checkpoint
            // entry on disk but do not emit a fresh gauge value, so the
            // operator's watch tool can intersect with `kind_subscribed`.
            let raw = ckpt.since_snapshot().await;
            let snapshot: Vec<(String, u16, String, i64)> = raw
                .into_iter()
                .filter_map(|(r, k, ts)| {
                    let relay_str = r.to_string();
                    if !active_snapshot.contains(&(relay_str.clone(), k)) {
                        return None;
                    }
                    let slug = slugs_snapshot.get(&k).cloned().unwrap_or_default();
                    Some((relay_str, k, slug, ts))
                })
                .collect();
            nozzl_metrics::update_checkpoint_lag(&metrics, snapshot, now_unix_seconds());
        }
    })
}

/// Periodically scan the object store and publish, per archived kind, the
/// bytes currently resident to the `storage_resident_bytes` gauge and the
/// resident `.parquet` chunk-file count to the `storage_resident_files` gauge
/// — both from a single scan pass.
///
/// Each tick re-derives the totals from scratch via
/// [`nozzl_archive::ChunkedArchive::scan_resident_usage`] (never an
/// accumulator), so the gauge tracks deletions and eventual compaction. The
/// scan duration is logged — not turned into a metric — as the early-warning
/// signal for when the scan cost grows large relative to the interval. The
/// gauge is deliberately not preallocated: a kind appears only once a scan
/// finds resident `.parquet`, so the series is absent until there is real
/// data (never a phantom zero).
///
/// The same tick also re-derives the `wal_bytes` gauge from a cheap local-disk
/// walk of `_wal/*.wal` (`ChunkedArchive::scan_wal_usage`) — per-writer WAL
/// occupancy, `0` when idle and `> 0` while buffered. That walk is infallible
/// (observability, never durability) so it never fails the loop.
fn spawn_storage_scan_loop(
    archive: Arc<nozzl_archive::ChunkedArchive>,
    metrics: Arc<nozzl_metrics::Metrics>,
    cfg: &nozzl_config::StorageMetricsConfig,
    kind_slugs: Arc<RwLock<HashMap<u16, String>>>,
) -> JoinHandle<()> {
    let interval = Duration::from_secs(cfg.scan_interval_seconds);
    let concurrency = cfg.scan_concurrency;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        // If a scan ever runs long, skip missed ticks rather than firing a
        // burst of back-to-back scans afterward.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let started = Instant::now();
            // Clone the current slug map (small) and drop the guard before the
            // await — never hold a std `RwLock` guard across `.await`. Picks up
            // hot-reloaded slugs on the next pass.
            let slugs = kind_slugs
                .read()
                .expect("gauge_kind_slugs lock poisoned")
                .clone();
            match archive.scan_resident_usage(concurrency, &slugs).await {
                Ok(usage) => {
                    let mut total_bytes: u64 = 0;
                    let mut total_files: u64 = 0;
                    for u in &usage {
                        total_bytes = total_bytes.saturating_add(u.total_bytes);
                        total_files = total_files.saturating_add(u.file_count);
                        // f64 carries integer byte counts exactly up to 2^53,
                        // far beyond any realistic per-kind archive size.
                        #[allow(clippy::cast_precision_loss)]
                        {
                            let kind = u.kind.to_string();
                            let labels = [u.class, kind.as_str(), u.slug.as_str()];
                            metrics
                                .storage_resident_bytes
                                .with_label_values(&labels)
                                .set(u.total_bytes as f64);
                            metrics
                                .storage_resident_files
                                .with_label_values(&labels)
                                .set(u.file_count as f64);
                        }
                    }
                    info!(
                        elapsed_ms = started.elapsed().as_millis(),
                        kinds = usage.len(),
                        total_bytes,
                        total_files,
                        "storage resident scan complete"
                    );
                }
                Err(e) => {
                    warn!(
                        elapsed_ms = started.elapsed().as_millis(),
                        error = %e,
                        "storage resident scan failed; will retry next interval"
                    );
                }
            }

            // WAL sizing rides the same tick: a cheap local-disk walk of
            // `_wal/*.wal`, re-derived each pass like the resident gauges. It is
            // infallible by construction (observability, never durability), so
            // it can't fail the loop — it just sets `wal_bytes` per writer.
            let wal_started = Instant::now();
            let wal_usage = archive.scan_wal_usage(&slugs).await;
            let mut wal_total_bytes: u64 = 0;
            for w in &wal_usage {
                wal_total_bytes = wal_total_bytes.saturating_add(w.bytes);
                let kind = w.kind.to_string();
                let shard = w.shard.to_string();
                let labels = [w.class, kind.as_str(), w.slug.as_str(), shard.as_str()];
                // f64 carries integer byte counts exactly up to 2^53 — a WAL
                // file never approaches that.
                #[allow(clippy::cast_precision_loss)]
                metrics
                    .wal_bytes
                    .with_label_values(&labels)
                    .set(w.bytes as f64);
            }
            info!(
                elapsed_ms = wal_started.elapsed().as_millis(),
                wal_files = wal_usage.len(),
                wal_total_bytes,
                "wal size scan complete"
            );
        }
    })
}

/// Build the active `(relay_normalized, kind)` set from an `AcquirerConfig`.
/// "Active" = the kinds each relay is effectively subscribed to after
/// per-relay `only_kinds` intersection with the global `kinds` list.
///
/// The returned set uses `RelayUrl::as_str()` (the canonical, slash-
/// normalized form) for the relay component — matching how the acquirer
/// registers every per-relay metric. Reading the metric with the raw config
/// string would target a different time series.
fn compute_active_pairs(
    cfg: &AcquirerConfig,
) -> std::collections::HashSet<(String, u16)> {
    let mut set = std::collections::HashSet::new();
    for url in &cfg.relays {
        let label = url.as_str().to_string();
        for k in cfg.effective_kinds_for(url) {
            set.insert((label.clone(), k));
        }
    }
    set
}

/// Current UNIX time in seconds. Saturates to 0 if the system clock is set
/// before the epoch — same fallback as the checkpoint store's ISO timestamp
/// emitter, kept consistent so a clock-skewed host produces 0-lag rather than
/// a wraparound.
#[allow(clippy::cast_possible_wrap)]
fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Watch the config files; on every successful reload, derive the new
/// `AcquirerConfig` and forward it over `reload_tx`. The receiver is owned
/// by the main task, which holds the acquirer.
fn spawn_watcher_loop(
    cfg_path: PathBuf,
    initial: Arc<Config>,
    reload_tx: mpsc::Sender<AcquirerConfig>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (_, mut watcher) = match nozzl_config::watch(&cfg_path) {
            Ok(pair) => pair,
            Err(e) => {
                warn!(error = %e, "could not start config watcher; hot-reload disabled");
                return;
            }
        };

        let mut last_applied = initial;
        while let Some(new_cfg) = watcher.next().await {
            let new_acq_cfg = match acquirer_config_from(&new_cfg) {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, "reload: failed to map new config; ignoring");
                    continue;
                }
            };

            warn_on_restart_required(&last_applied, &new_cfg);

            if reload_tx.send(new_acq_cfg).await.is_err() {
                debug!("reload receiver dropped; watcher loop exiting");
                return;
            }
            last_applied = new_cfg;
        }
    })
}

#[allow(clippy::too_many_lines)] // one warn! per restart-required field; intentionally exhaustive
fn warn_on_restart_required(old: &Config, new: &Config) {
    if old.runtime.channel_capacity != new.runtime.channel_capacity {
        warn!(
            old = old.runtime.channel_capacity,
            new = new.runtime.channel_capacity,
            "runtime.channel_capacity change requires restart; staying at old value"
        );
    }
    if old.runtime.chunk_max_size_bytes != new.runtime.chunk_max_size_bytes {
        warn!(
            old = old.runtime.chunk_max_size_bytes,
            new = new.runtime.chunk_max_size_bytes,
            "runtime.chunk_max_size_bytes change requires restart; staying at old value"
        );
    }
    if old.runtime.chunk_max_rows != new.runtime.chunk_max_rows {
        warn!(
            old = old.runtime.chunk_max_rows,
            new = new.runtime.chunk_max_rows,
            "runtime.chunk_max_rows change requires restart; staying at old value"
        );
    }
    if old.runtime.chunk_max_buffer_seconds != new.runtime.chunk_max_buffer_seconds {
        warn!(
            old = old.runtime.chunk_max_buffer_seconds,
            new = new.runtime.chunk_max_buffer_seconds,
            "runtime.chunk_max_buffer_seconds change requires restart; staying at old value"
        );
    }
    #[allow(clippy::float_cmp)] // exact inequality is what "the operator edited it" means
    if old.runtime.chunk_buffer_age_jitter != new.runtime.chunk_buffer_age_jitter {
        warn!(
            old = old.runtime.chunk_buffer_age_jitter,
            new = new.runtime.chunk_buffer_age_jitter,
            "runtime.chunk_buffer_age_jitter change requires restart; staying at old value"
        );
    }
    if old.runtime.chunk_flush_concurrency != new.runtime.chunk_flush_concurrency {
        warn!(
            old = old.runtime.chunk_flush_concurrency,
            new = new.runtime.chunk_flush_concurrency,
            "runtime.chunk_flush_concurrency change requires restart; staying at old value"
        );
    }
    if old.runtime.wal_fsync_batch_ms != new.runtime.wal_fsync_batch_ms {
        warn!(
            old = old.runtime.wal_fsync_batch_ms,
            new = new.runtime.wal_fsync_batch_ms,
            "runtime.wal_fsync_batch_ms change requires restart; staying at old value"
        );
    }
    if old.runtime.parquet_zstd_level != new.runtime.parquet_zstd_level {
        warn!(
            old = old.runtime.parquet_zstd_level,
            new = new.runtime.parquet_zstd_level,
            "runtime.parquet_zstd_level change requires restart; staying at old value"
        );
    }
    if old.runtime.pool_dedup_max_events != new.runtime.pool_dedup_max_events {
        warn!(
            old = old.runtime.pool_dedup_max_events,
            new = new.runtime.pool_dedup_max_events,
            "runtime.pool_dedup_max_events change requires restart; staying at old value"
        );
    }
    // NOTE: `only_kinds` (per relay) and `only_relays` (per kind) used to be
    // reported here as restart-required. They no longer are. `apply_config`
    // now diffs each surviving relay's EFFECTIVE kind set -- the composition of
    // both allowlists -- and sends `UpdateKinds` to exactly the relays whose set
    // changed, backfilling whatever each one newly gained. That services an
    // allowlist edit correctly: a relay that gains a kind gets a scoped backfill
    // (so it has a checkpoint, so its forward sub keeps a bounded `since`), and
    // one that loses a kind simply stops requesting it. Warning "requires
    // restart" while the change is in fact applied would be worse than saying
    // nothing, so the two warn blocks were removed rather than reworded.
    if old.storage.backend != new.storage.backend {
        warn!(
            old = ?old.storage.backend,
            new = ?new.storage.backend,
            "storage.backend change requires restart; staying at old value"
        );
    }
    if old.observability.metrics_addr != new.observability.metrics_addr {
        warn!(
            old = %old.observability.metrics_addr,
            new = %new.observability.metrics_addr,
            "observability.metrics_addr change requires restart; staying at old value"
        );
    }
    if old.observability.admin_addr != new.observability.admin_addr {
        warn!(
            old = ?old.observability.admin_addr,
            new = ?new.observability.admin_addr,
            "observability.admin_addr change requires restart; staying at old value"
        );
    }
    if old.observability.log_file != new.observability.log_file {
        warn!(
            old = ?old.observability.log_file,
            new = ?new.observability.log_file,
            "observability.log_file change requires restart; staying at old value"
        );
    }
    if old.observability.log_file_max_files != new.observability.log_file_max_files {
        warn!(
            old = ?old.observability.log_file_max_files,
            new = ?new.observability.log_file_max_files,
            "observability.log_file_max_files change requires restart; staying at old value"
        );
    }
    if old.observability.log_format != new.observability.log_format {
        warn!(
            old = ?old.observability.log_format,
            new = ?new.observability.log_format,
            "observability.log_format change requires restart; staying at old value"
        );
    }
    if old.observability.log_level != new.observability.log_level {
        warn!(
            old = %old.observability.log_level,
            new = %new.observability.log_level,
            "observability.log_level change requires restart; staying at old value"
        );
    }
    if old.storage_metrics.enabled != new.storage_metrics.enabled {
        warn!(
            old = old.storage_metrics.enabled,
            new = new.storage_metrics.enabled,
            "storage_metrics.enabled change requires restart; staying at old value"
        );
    }
    if old.storage_metrics.scan_interval_seconds != new.storage_metrics.scan_interval_seconds {
        warn!(
            old = old.storage_metrics.scan_interval_seconds,
            new = new.storage_metrics.scan_interval_seconds,
            "storage_metrics.scan_interval_seconds change requires restart; staying at old value"
        );
    }
    if old.storage_metrics.scan_concurrency != new.storage_metrics.scan_concurrency {
        warn!(
            old = old.storage_metrics.scan_concurrency,
            new = new.storage_metrics.scan_concurrency,
            "storage_metrics.scan_concurrency change requires restart; staying at old value"
        );
    }
}

/// Wait for SIGTERM or SIGINT (Unix) / Ctrl+C (Windows).
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "could not install SIGTERM handler; falling back to Ctrl+C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "could not install SIGINT handler; falling back to Ctrl+C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received"),
            _ = sigint.recv() => info!("SIGINT received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::{build_log_appender, resolve_export_root, resolve_wal_root};
    use nozzl_archive::{LocalConfig, S3Config, StorageBackend, StorageConfig};
    use std::path::{Path, PathBuf};

    fn storage(local_root: Option<&str>) -> StorageConfig {
        StorageConfig {
            backend: if local_root.is_some() {
                StorageBackend::Local
            } else {
                StorageBackend::S3
            },
            local: local_root.map(|r| LocalConfig { root: r.into() }),
            s3: Some(S3Config {
                bucket: "example_archive".into(),
                prefix: String::new(),
                region: "us-east-1".into(),
            }),
        }
    }

    /// S3 backend with `[storage].root` set must root the WAL there (so it can
    /// point at a durable mounted volume) — the regression this guards is the
    /// old hardcoded `./data/events` that ignored the config.
    #[test]
    fn wal_root_s3_honors_storage_root() {
        let cfg = storage(None);
        assert_eq!(
            resolve_wal_root(&cfg, Some(Path::new("/data/events"))),
            PathBuf::from("/data/events"),
        );
    }

    /// S3 backend with `[storage].root` omitted falls back to the historical
    /// cwd-relative default.
    #[test]
    fn wal_root_s3_defaults_when_root_absent() {
        let cfg = storage(None);
        assert_eq!(resolve_wal_root(&cfg, None), PathBuf::from("./data/events"));
    }

    /// Local backend is unchanged: it uses the archive's `local.root` and
    /// ignores the passed `config_root` entirely.
    #[test]
    fn wal_root_local_uses_local_root() {
        let cfg = storage(Some("/data/events"));
        assert_eq!(
            resolve_wal_root(&cfg, Some(Path::new("/should-be-ignored"))),
            PathBuf::from("/data/events"),
        );
    }

    /// Local backend: export reads from the configured local archive root.
    #[test]
    fn export_root_local_uses_storage_root() {
        assert_eq!(
            resolve_export_root(
                nozzl_config::StorageBackend::Local,
                Some(Path::new("/data/events"))
            ),
            Some(PathBuf::from("/data/events")),
        );
    }

    /// S3 backend: export root is None EVEN WHEN storage.root is set (under S3
    /// that's the WAL root, not a local Parquet archive). Guards the regression
    /// where reusing storage.root made /export glob an empty dir instead of 503.
    #[test]
    fn export_root_s3_is_none_even_with_root_set() {
        assert_eq!(
            resolve_export_root(
                nozzl_config::StorageBackend::S3,
                Some(Path::new("/data/events"))
            ),
            None,
        );
    }

    const SEEDED: [&str; 3] = [
        "nozzl.log.2020-01-01",
        "nozzl.log.2020-01-02",
        "nozzl.log.2020-01-03",
    ];

    /// Pre-seed `dir` with rotated-log files named `prefix.<date>`.
    fn seed(dir: &std::path::Path, names: &[&str]) {
        for n in names {
            std::fs::write(dir.join(n), b"old log line\n").unwrap();
        }
    }

    fn nozzl_log_count(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with("nozzl.log"))
            })
            .count()
    }

    /// Indefinite retention: `None` (config key omitted) must keep every
    /// pre-existing rotated file — the appender never prunes. Pruning, if any,
    /// happens during `build`. (`Some(0)` is rejected by config validation and
    /// must never reach `build_log_appender`, so it isn't exercised here — it
    /// would underflow tracing-appender's prune logic.)
    #[test]
    fn indefinite_retention_keeps_all_rotated_files() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &SEEDED);

        let _appender = build_log_appender(dir.path(), "nozzl.log", None).unwrap();

        for n in SEEDED {
            assert!(
                dir.path().join(n).exists(),
                "{n} must survive with max_files=None (indefinite retention)"
            );
        }
    }

    /// A positive `Some(n)` still prunes, proving indefinite retention is
    /// specifically the `None` case, not a blanket disable.
    #[test]
    fn positive_max_files_prunes_old_files() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &SEEDED);

        let _appender = build_log_appender(dir.path(), "nozzl.log", Some(2)).unwrap();

        assert!(
            nozzl_log_count(dir.path()) < SEEDED.len(),
            "max_files=Some(2) must prune at least one of {} seeded files",
            SEEDED.len()
        );
    }
}
