//! Shared test helpers for the binary's integration suite.
//!
//! # Driving `nozzl::run` from tests
//!
//! Always use [`NozzlHandle`] (NOT `tokio::spawn(nozzl::run(...))` directly).
//! The wrapper isolates nozzl on its own tokio runtime so that
//! [`NozzlHandle::kill`] can atomically terminate every task nozzl spawned —
//! writer, acquirer, forward sub, checkpoint flusher, metrics server, and the
//! optional admin server. A bare
//! `JoinHandle::abort()` only cancels the top-level task; detached
//! `tokio::spawn`ed children keep running and leak across test phases. See
//! [`NozzlHandle`]'s docs for the full rationale.

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tempfile::TempDir;
use tokio::time::timeout;

/// Compose and write a temp `nozzl.toml`, `relays.toml`, and `kinds.toml`
/// inside `cfg_dir`. The metrics endpoint is bound to `127.0.0.1:0` so
/// parallel tests don't fight over a fixed port.
pub fn write_config(
    cfg_dir: &TempDir,
    archive_root: &Path,
    relay_urls: &[&str],
    kinds: &[u16],
) -> PathBuf {
    write_config_with_log_file(cfg_dir, archive_root, relay_urls, kinds, None)
}

/// Like [`write_config`] but writes an `[observability] log_file = ...`
/// line when `log_file` is `Some`. Other config tests pass `None`.
pub fn write_config_with_log_file(
    cfg_dir: &TempDir,
    archive_root: &Path,
    relay_urls: &[&str],
    kinds: &[u16],
    log_file: Option<&Path>,
) -> PathBuf {
    write_config_full(cfg_dir, archive_root, relay_urls, kinds, log_file, "127.0.0.1:0")
}

/// Like [`write_config_with_log_file`] but lets the caller pin a specific
/// metrics-server address. Required for tests that need to scrape `/metrics`
/// (since the default `127.0.0.1:0` returns an OS-picked port we can't
/// rediscover from outside `nozzl::run`).
pub fn write_config_full(
    cfg_dir: &TempDir,
    archive_root: &Path,
    relay_urls: &[&str],
    kinds: &[u16],
    log_file: Option<&Path>,
    metrics_addr: &str,
) -> PathBuf {
    let nozzl_path = cfg_dir.path().join("nozzl.toml");
    let archive_root_disp = archive_root.display();
    let log_file_line = log_file
        .map(|p| format!("log_file = \"{}\"\n", p.display()))
        .unwrap_or_default();
    let nozzl_toml = format!(
        r#"
[storage]
backend = "local"
root = "{archive_root_disp}"

[acquirer]
backfill_safety_margin_seconds = 60
backfill_default_limit = 500
backfill_empty_pages_to_complete = 8

[checkpoint]
path = "{archive_root_disp}/_state/checkpoints.json"
flush_interval_seconds = 1

[runtime]
channel_capacity = 256
# Tight chunk-roll thresholds so tests don't wait an hour for a chunk to
# land. Production runs use much larger values; see the storage plan.
chunk_max_size_bytes = 1048576
chunk_max_rows = 100
chunk_max_buffer_seconds = 2
pool_dedup_max_events = 100000

[observability]
metrics_addr = "{metrics_addr}"
log_format = "pretty"
log_level = "info"
{log_file_line}
# Fast resident-storage scan so tests don't wait the 300 s production default
# to see `storage_resident_bytes` / `storage_resident_files`. Cheap on a small
# temp archive.
[storage_metrics]
scan_interval_seconds = 1
scan_concurrency = 4

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#
    );
    fs::write(&nozzl_path, nozzl_toml).unwrap();
    write_relays(cfg_dir, relay_urls);
    write_kinds(cfg_dir, kinds);
    // The archive root must already exist for object_store::LocalFileSystem.
    fs::create_dir_all(archive_root).unwrap();
    nozzl_path
}

/// Bind to an ephemeral port, ask the OS what it actually got, and reuse
/// that. The listener is dropped before returning, leaving a tiny race window
/// — acceptable for tests.
#[must_use]
pub fn pick_free_metrics_addr() -> std::net::SocketAddr {
    let listener =
        std::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

pub fn write_relays(cfg_dir: &TempDir, urls: &[&str]) {
    let mut s = String::new();
    for url in urls {
        s.push_str(&format!("[[relay]]\nurl = \"{url}\"\n\n"));
    }
    fs::write(cfg_dir.path().join("relays.toml"), s).unwrap();
}

pub fn write_kinds(cfg_dir: &TempDir, kinds: &[u16]) {
    let mut s = String::new();
    for k in kinds {
        // Slug is required and must match `^[a-z0-9][a-z0-9-]*$`. The
        // value is opaque to the test — what matters is that it's stable
        // across invocations so resume tests find the same paths.
        s.push_str(&format!("[[kind]]\nk = {k}\nslug = \"k{k}-test\"\n\n"));
    }
    fs::write(cfg_dir.path().join("kinds.toml"), s).unwrap();
}

/// Count individual event records inside Parquet chunks under `root`. The
/// chunk layout is `{root}/c={class}/k={kind}/p={shard}/dt={date}/events-{ulid}.parquet`.
/// Files under `_wal/` and `_state/` are excluded.
///
/// Replaces the v0 `count_event_files` helper which counted one file per
/// event; under the chunked archive a single chunk holds many events so we
/// have to read them back to get an accurate count.
#[must_use]
pub fn count_event_files(root: &Path) -> usize {
    match nozzl_archive::reader::read_all_events(root) {
        Ok(events) => events.len(),
        Err(_) => 0,
    }
}

/// Count Parquet chunk *files* under `root`. Useful for asserting that a
/// chunk has actually been written to disk (vs. still buffered in memory).
#[must_use]
pub fn count_chunk_files(root: &Path) -> usize {
    fn visit(p: &Path, count: &mut usize) {
        let Ok(entries) = fs::read_dir(p) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with('_') {
                    continue;
                }
            }
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                visit(&path, count);
            } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                *count += 1;
            }
        }
    }
    let mut count = 0;
    visit(root, &mut count);
    count
}

/// Poll the predicate every `interval` until it returns true or the caller's
/// outer timeout fires.
pub async fn wait_until<F>(interval: Duration, mut pred: F)
where
    F: FnMut() -> bool,
{
    loop {
        if pred() {
            return;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Test-only lifecycle wrapper for a running `nozzl::run(...)` instance.
///
/// # Why this exists
///
/// The naive pattern —
/// `let task = tokio::spawn(nozzl::run(cfg, false)); task.abort();` —
/// looks like a clean crash-restart story but **isn't**. `tokio::spawn`ed
/// children (the per-(kind, shard) chunk writer, the per-relay forward sub,
/// the checkpoint flusher, the metrics server, the optional admin server) are detached: `abort`ing the
/// parent does not abort them. Detached writers keep WAL-appending, detached
/// forward subs keep latching events from the relay — observably leaking
/// into the next test phase. The bug surfaced in `checkpoint_resume_after_restart`
/// as phase-2 events being WAL-appended to phase-1's already-"crashed"
/// writer and double-archived on restart.
///
/// [`NozzlHandle::start`] boots nozzl on its **own dedicated tokio runtime**.
/// [`NozzlHandle::kill`] then drops that runtime, which atomically cancels
/// every task on it and synchronously drops their owned resources (TCP
/// sockets to relays, file handles, channel senders, in-memory builders).
/// That is the closest in-process analogue of a SIGKILL — and unlike
/// `JoinHandle::abort`, it reaches every detached `tokio::spawn`ed child.
///
/// # Guidance for future tests
///
/// - **Always** drive nozzl through this wrapper. Do not write
///   `tokio::spawn(nozzl::run(...))` inline; the resulting `JoinHandle` does
///   not support a faithful kill.
/// - Use [`Self::kill`] (NOT `.abort()`) to terminate nozzl. The `Drop` impl
///   is a safety net for panicking tests; prefer explicit `kill` so any
///   leaked-task failures surface in the assertion path.
/// - For restart-resume tests, construct a fresh `NozzlHandle` with the
///   same `cfg_path` and `archive_root` as the killed predecessor. That is
///   how `checkpoint_resume_after_restart` exercises checkpoint replay.
pub struct NozzlHandle {
    rt: Option<tokio::runtime::Runtime>,
    cfg_path: PathBuf,
    archive_root: PathBuf,
}

impl NozzlHandle {
    /// Boot nozzl on its own multi-threaded tokio runtime. Returns once the
    /// runtime is built and `nozzl::run` has been spawned onto it — NOT once
    /// nozzl is ready to serve. Tests should poll [`Self::wait_for_events`]
    /// (or the archive directory directly) for readiness.
    #[must_use]
    pub fn start(cfg_path: PathBuf, archive_root: PathBuf) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("nozzl-test-instance")
            .build()
            .expect("nozzl test: failed to build instance runtime");

        let cfg_for_task = cfg_path.clone();
        rt.spawn(async move {
            if let Err(e) = nozzl::run(cfg_for_task, false).await {
                tracing::error!(error = ?e, "nozzl::run exited with error in test fixture");
            }
        });

        Self {
            rt: Some(rt),
            cfg_path,
            archive_root,
        }
    }

    /// Crash nozzl. Faithful to a SIGKILL: every task spawned on nozzl's
    /// runtime is force-cancelled, every resource owned by those tasks (TCP
    /// sockets, file handles, channel senders, in-memory builders) drops,
    /// and the runtime's worker threads are joined. After this returns, no
    /// nozzl code can be executing.
    ///
    /// `kill` consumes `self`. The archive directory and config file are
    /// untouched — they are owned by the test, not the handle, and can be
    /// fed to a fresh [`Self::start`] to simulate restart.
    pub fn kill(mut self) {
        if let Some(rt) = self.rt.take() {
            // shutdown_timeout(ZERO) sends the cancellation signal to every
            // task, joins worker threads, and returns. With a zero budget
            // for the blocking pool, we don't wait for `spawn_blocking`
            // operations — they may continue briefly on detached threads,
            // but no async nozzl future will be polled again. Blocks the
            // calling thread for the duration of worker-thread join, which
            // is measured in single-digit milliseconds in practice and is
            // safe on a `#[tokio::test(flavor = "multi_thread")]` runtime
            // because we're shutting down a *different* runtime than the
            // one we're running on.
            rt.shutdown_timeout(Duration::ZERO);
        }
    }

    /// Archive root passed to [`Self::start`]. Convenience for tests that
    /// poll on-disk state directly via [`count_event_files`].
    #[must_use]
    pub fn archive_root(&self) -> &Path {
        &self.archive_root
    }

    /// Config file path passed to [`Self::start`]. Useful for tests that
    /// rewrite TOML between phases (hot-reload) or feed the same path to a
    /// successor handle (crash-restart).
    #[must_use]
    pub fn cfg_path(&self) -> &Path {
        &self.cfg_path
    }

    /// Poll the archive at 100 ms intervals until at least `target` event
    /// records are persisted, or `budget` elapses. Returns `(success, count)`
    /// where `success` is `true` iff the target was reached within the
    /// budget. The final count is always returned so callers can include it
    /// in assertion messages whether they succeeded or timed out.
    pub async fn wait_for_events(&self, target: usize, budget: Duration) -> (bool, usize) {
        let archive = self.archive_root.clone();
        let inner = async {
            wait_until(Duration::from_millis(100), || {
                count_event_files(&archive) >= target
            })
            .await;
        };
        let success = timeout(budget, inner).await.is_ok();
        (success, count_event_files(&self.archive_root))
    }
}

impl Drop for NozzlHandle {
    fn drop(&mut self) {
        // Safety net for tests that panic before reaching `kill`. We can't
        // call `shutdown_timeout` here because Drop is sometimes invoked
        // from contexts where blocking is unsafe; `shutdown_background`
        // signals every task without blocking and lets worker threads
        // finish in the background. Prefer explicit `kill` in tests so
        // leaked-task failures surface in the assertion path.
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}
