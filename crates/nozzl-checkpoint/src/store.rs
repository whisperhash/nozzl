//! [`CheckpointStore`] trait and the file-backed [`FileCheckpointStore`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use nozzl_types::{EventId, RelayUrl, Timestamp};
use tempfile::NamedTempFile;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::task::JoinHandle;

use crate::error::CheckpointError;
use crate::state::{
    BackfillState, FileFormat, KindState, SCHEMA_VERSION, build_relay_map, flatten_relay_map,
};

/// Per-(relay, kind) checkpoint persistence. The acquirer consumes this trait
/// to resume forward subscriptions and onboarding backfills across restarts.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Newest observed `created_at` for `(relay, kind)`, or `None` if no
    /// observation has ever been recorded.
    async fn since(&self, relay: &RelayUrl, kind: u16) -> Option<Timestamp>;

    /// Snapshot of every `(relay, kind, since)` triple where `since` is set.
    /// Coordinates that have never observed an event are omitted — Prometheus
    /// treats absent series as "no observation" rather than "lag=now-epoch",
    /// which is the honest reading for an unobserved kind.
    async fn since_snapshot(&self) -> Vec<(RelayUrl, u16, Timestamp)>;

    /// Record an event observation. Updates `since` only if `created_at` is
    /// strictly greater than the current value; updates `last_event_id`
    /// alongside that newer value. Idempotent and safe to call concurrently
    /// from multiple writer tasks.
    async fn observe(&self, relay: &RelayUrl, kind: u16, created_at: Timestamp, event_id: &EventId);

    /// Combined read of `(since, backfill_state)` for `(relay, kind)` in a
    /// single lock acquisition. Callers that need both values should prefer
    /// this over two separate calls to avoid a redundant lock round-trip.
    async fn since_and_backfill_state(&self, relay: &RelayUrl, kind: u16) -> (Option<Timestamp>, BackfillState);

    /// Current backfill state for `(relay, kind)`. Defaults to
    /// [`BackfillState::Pending`] for unrecorded coordinates.
    async fn backfill_state(&self, relay: &RelayUrl, kind: u16) -> BackfillState;

    /// Set the backfill state for `(relay, kind)`. Replaces the previous value.
    async fn record_backfill(&self, relay: &RelayUrl, kind: u16, state: BackfillState);

    /// Force an immediate flush. The background flush task continues to run
    /// independently; this method only short-circuits the next interval tick
    /// for cases where the caller needs durability now (e.g. on shutdown).
    async fn flush(&self) -> Result<(), CheckpointError>;
}

// In-memory key. `RelayUrl` is already normalized and `Hash + Eq`, so we use it
// directly as the map key.
type Key = (RelayUrl, u16);

/// World-readable mode for the persisted checkpoint file (Unix).
///
/// The archive's other observability artifacts (WAL segments, logs) are written
/// `0644`, so a read-only account
/// can inspect live state. `NamedTempFile` creates its temp file `0600` and
/// `persist()` carries that mode across the rename, which would leave
/// `checkpoints.json` unreadable by that account - the lone artifact it can't
/// read. We widen to `0644` *before* persist so the file appears at its final
/// mode atomically.
///
/// Group-scoped `0640` would be the tighter choice, but it's insufficient here:
/// that account can be a different user that shares no group with the producer,
/// so only the world bit grants it read. The
/// checkpoint holds only cursor state (relay URLs, timestamps, event ids) — no
/// secrets — so world-read is acceptable. (Per the repo's "never log/expose
/// secrets to the observer" rule, this file must carry none — and it doesn't.)
///
/// Applied via `fchmod` (an explicit `set_permissions` on the temp fd), NOT via
/// the `open(2)` create-mode — the latter is filtered by the process umask
/// (`mode & ~umask`), so under a restrictive umask it would silently yield
/// `0600` again. `fchmod` is umask-independent, which is the property we need.
#[cfg(unix)]
const CHECKPOINT_FILE_MODE: u32 = 0o644;

/// File-backed [`CheckpointStore`]. Holds state in a `tokio::sync::RwLock` for
/// concurrent observation and runs a single dedicated flush task that fires on
/// the configured interval.
pub struct FileCheckpointStore {
    path: PathBuf,
    state: RwLock<HashMap<Key, KindState>>,
    /// Set whenever a mutation lands. Cleared by the flusher on a successful
    /// rename. Lets idle periods skip the I/O.
    dirty: AtomicBool,
    /// Serializes flushes so a force-flush from a caller and the background
    /// tick can never race on the same target file.
    flush_lock: Mutex<()>,
    /// Signals the background flusher to stop on shutdown.
    shutdown: Arc<Notify>,
    /// Indicates `shutdown` has already run, making it idempotent.
    shutdown_done: AtomicBool,
    /// Handle for the background task. Joined during shutdown.
    flush_task: Mutex<Option<JoinHandle<()>>>,
}

impl FileCheckpointStore {
    /// Load existing state from `path` and spawn a background flush task that
    /// fires every `flush_interval`.
    ///
    /// If the file does not exist, the store starts empty and will create it
    /// on the first flush after a state mutation.
    ///
    /// # Errors
    /// - [`CheckpointError::Io`] for I/O failures.
    /// - [`CheckpointError::Corrupt`] if the file is unparseable.
    /// - [`CheckpointError::UnsupportedSchema`] if `schema_version` differs
    ///   from the supported value.
    pub async fn open(
        path: PathBuf,
        flush_interval: Duration,
    ) -> Result<Arc<Self>, CheckpointError> {
        let initial = load_from_disk(&path).await?;

        let store = Arc::new(Self {
            path,
            state: RwLock::new(initial),
            dirty: AtomicBool::new(false),
            flush_lock: Mutex::new(()),
            shutdown: Arc::new(Notify::new()),
            shutdown_done: AtomicBool::new(false),
            flush_task: Mutex::new(None),
        });

        let task = {
            let store = Arc::clone(&store);
            let shutdown = Arc::clone(&store.shutdown);
            tokio::spawn(async move {
                run_flush_loop(store, shutdown, flush_interval).await;
            })
        };
        *store.flush_task.lock().await = Some(task);

        Ok(store)
    }

    /// Stop the background flush task and write any pending state to disk one
    /// last time. Idempotent — repeated calls after the first are no-ops.
    ///
    /// # Errors
    /// Returns [`CheckpointError`] if the final flush fails. The background
    /// task is still stopped and the in-memory state retained.
    pub async fn shutdown(self: Arc<Self>) -> Result<(), CheckpointError> {
        if self.shutdown_done.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        // `notify_one` stores a permit if no waiter is currently registered, so
        // this signal is durable even if the flusher hasn't reached its select
        // yet. `notify_waiters` would not be: it drops the wakeup on the floor
        // when no one is awaiting `notified()`, and the flusher would then
        // hang on its (very long) interval tick.
        self.shutdown.notify_one();

        let handle = self.flush_task.lock().await.take();
        if let Some(handle) = handle {
            // If the flusher panicked we still want to perform the final flush
            // below, so we ignore the join result.
            let _ = handle.await;
        }

        self.flush_now().await
    }

    /// Mutate the in-memory entry for `(relay, kind)`, returning whether the
    /// mutation actually changed state.
    async fn mutate<F>(&self, relay: &RelayUrl, kind: u16, f: F)
    where
        F: FnOnce(&mut KindState) -> bool,
    {
        let key = (relay.clone(), kind);
        let mut guard = self.state.write().await;
        let entry = guard.entry(key).or_default();
        if f(entry) {
            self.dirty.store(true, Ordering::Release);
        }
    }

    /// Perform a single flush right now, regardless of dirty state. Used by
    /// [`CheckpointStore::flush`] and [`Self::shutdown`]. The background loop
    /// uses [`Self::flush_if_dirty`] instead so idle periods do no I/O.
    async fn flush_now(&self) -> Result<(), CheckpointError> {
        let _flush_guard = self.flush_lock.lock().await;
        let snapshot = self.snapshot().await;
        // Clear dirty *before* the write — if the write fails we re-set dirty
        // so the next interval retries. Setting it after would lose mutations
        // that landed between snapshot capture and clear.
        self.dirty.store(false, Ordering::Release);
        if let Err(e) = write_atomically(&self.path, &snapshot).await {
            self.dirty.store(true, Ordering::Release);
            return Err(e);
        }
        Ok(())
    }

    async fn flush_if_dirty(&self) -> Result<(), CheckpointError> {
        if !self.dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        self.flush_now().await
    }

    /// Capture a serializable snapshot of current state. Held only as long as
    /// it takes to clone the entries; the lock is released before any I/O.
    async fn snapshot(&self) -> FileFormat {
        let guard = self.state.read().await;
        let entries: Vec<(RelayUrl, u16, KindState)> = guard
            .iter()
            .map(|((r, k), s)| (r.clone(), *k, s.clone()))
            .collect();
        drop(guard);
        FileFormat {
            schema_version: SCHEMA_VERSION,
            updated_at: now_iso8601(),
            relays: build_relay_map(entries),
        }
    }
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn since(&self, relay: &RelayUrl, kind: u16) -> Option<Timestamp> {
        let guard = self.state.read().await;
        guard.get(&(relay.clone(), kind)).and_then(|s| s.since)
    }

    async fn since_snapshot(&self) -> Vec<(RelayUrl, u16, Timestamp)> {
        let guard = self.state.read().await;
        guard
            .iter()
            .filter_map(|((relay, kind), state)| {
                state.since.map(|ts| (relay.clone(), *kind, ts))
            })
            .collect()
    }

    async fn observe(
        &self,
        relay: &RelayUrl,
        kind: u16,
        created_at: Timestamp,
        event_id: &EventId,
    ) {
        let event_id = *event_id;
        self.mutate(relay, kind, |entry| match entry.since {
            Some(prev) if created_at <= prev => false,
            _ => {
                entry.since = Some(created_at);
                entry.last_event_id = Some(event_id);
                true
            }
        })
        .await;
    }

    async fn since_and_backfill_state(&self, relay: &RelayUrl, kind: u16) -> (Option<Timestamp>, BackfillState) {
        let guard = self.state.read().await;
        match guard.get(&(relay.clone(), kind)) {
            Some(s) => (s.since, s.backfill),
            None => (None, BackfillState::Pending),
        }
    }

    async fn backfill_state(&self, relay: &RelayUrl, kind: u16) -> BackfillState {
        let guard = self.state.read().await;
        guard
            .get(&(relay.clone(), kind))
            .map_or(BackfillState::Pending, |s| s.backfill)
    }

    async fn record_backfill(&self, relay: &RelayUrl, kind: u16, state: BackfillState) {
        self.mutate(relay, kind, |entry| {
            if entry.backfill == state {
                false
            } else {
                entry.backfill = state;
                true
            }
        })
        .await;
    }

    async fn flush(&self) -> Result<(), CheckpointError> {
        self.flush_now().await
    }
}

async fn run_flush_loop(
    store: Arc<FileCheckpointStore>,
    shutdown: Arc<Notify>,
    flush_interval: Duration,
) {
    let mut ticker = tokio::time::interval(flush_interval);
    // First tick fires immediately by default; skip it so the very first flush
    // is delayed by `flush_interval` (matches operator intuition).
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            () = shutdown.notified() => {
                tracing::debug!("checkpoint flush loop received shutdown");
                return;
            }
            _ = ticker.tick() => {
                if let Err(e) = store.flush_if_dirty().await {
                    tracing::warn!(error = %e, "checkpoint flush failed; will retry on next interval");
                }
            }
        }
    }
}

async fn load_from_disk(path: &Path) -> Result<HashMap<Key, KindState>, CheckpointError> {
    let bytes = match tokio::fs::read(path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(source) => {
            return Err(CheckpointError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };

    let parsed: FileFormat =
        serde_json::from_slice(&bytes).map_err(|source| CheckpointError::Corrupt {
            path: path.to_path_buf(),
            source,
        })?;

    if parsed.schema_version != SCHEMA_VERSION {
        return Err(CheckpointError::UnsupportedSchema {
            path: path.to_path_buf(),
            found: parsed.schema_version,
            expected: SCHEMA_VERSION,
        });
    }

    let mut map = HashMap::new();
    for (key, state) in flatten_relay_map(parsed.relays) {
        map.insert(key, state);
    }
    Ok(map)
}

/// Atomic flush implementation:
///  1. Ensure the parent directory exists (`create_dir_all`).
///  2. `tempfile::NamedTempFile::new_in(parent)` so the temp file is on the
///     same filesystem, making rename atomic.
///  3. Write the JSON, fsync the temp file.
///  4. `persist()` atomically renames the temp file to the target path.
///  5. Best-effort fsync the parent directory so the rename is durable across
///     a power loss (Unix only; Windows does not expose directory fsync).
async fn write_atomically(path: &Path, data: &FileFormat) -> Result<(), CheckpointError> {
    let bytes = serde_json::to_vec_pretty(data).map_err(CheckpointError::Serialize)?;

    let parent = path
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    if !parent.as_os_str().is_empty() {
        if let Err(source) = tokio::fs::create_dir_all(&parent).await {
            return Err(CheckpointError::Io {
                path: parent.clone(),
                source,
            });
        }
    }

    let path = path.to_path_buf();
    // The blocking work — tempfile create, write, fsync, rename, dir fsync —
    // runs on `spawn_blocking` so we don't stall the runtime. The whole
    // operation is small (a few KB) so this overhead is negligible.
    tokio::task::spawn_blocking(move || -> Result<(), CheckpointError> {
        write_atomically_blocking(&path, &parent, &bytes)
    })
    .await
    .map_err(|join_err| CheckpointError::Io {
        path: PathBuf::new(),
        source: std::io::Error::other(join_err),
    })?
}

fn write_atomically_blocking(
    path: &Path,
    parent: &Path,
    bytes: &[u8],
) -> Result<(), CheckpointError> {
    use std::io::Write;

    let mut tmp = NamedTempFile::new_in(parent).map_err(|source| CheckpointError::Io {
        path: parent.to_path_buf(),
        source,
    })?;
    tmp.write_all(bytes).map_err(|source| CheckpointError::Io {
        path: tmp.path().to_path_buf(),
        source,
    })?;
    tmp.as_file_mut()
        .sync_all()
        .map_err(|source| CheckpointError::Io {
            path: tmp.path().to_path_buf(),
            source,
        })?;

    // Widen perms on the temp file *before* persist so the renamed-into-place
    // file lands at its final mode atomically (see CHECKPOINT_FILE_MODE). Set
    // explicitly via the mode, not via umask, so it's `0644` regardless of the
    // process umask.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tmp.as_file()
            .set_permissions(std::fs::Permissions::from_mode(CHECKPOINT_FILE_MODE))
            .map_err(|source| CheckpointError::Io {
                path: tmp.path().to_path_buf(),
                source,
            })?;
    }

    tmp.persist(path).map_err(|e| CheckpointError::Io {
        path: path.to_path_buf(),
        source: e.error,
    })?;

    // Best-effort parent directory fsync. Required on Linux for true durability
    // of the rename across power loss. macOS honors it. Windows can't open a
    // directory as a file, so we silently skip it there.
    #[cfg(unix)]
    {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

fn now_iso8601() -> String {
    // Minimal RFC3339 / ISO-8601 emitter: avoids pulling chrono just for one
    // string. Format: `YYYY-MM-DDTHH:MM:SSZ`. Falls back to `1970-01-01T00:00:00Z`
    // if the system clock is set before the epoch (shouldn't happen). The
    // `u64 → i64` cast is safe in practice — durations since epoch fit in i64
    // for ~292 billion years and our build will be off long before then.
    #[allow(clippy::cast_possible_wrap)]
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    format_unix_seconds_iso8601(secs)
}

fn format_unix_seconds_iso8601(secs: i64) -> String {
    // Civil-from-days algorithm by Howard Hinnant — single-char locals match
    // the reference implementation's notation.
    // https://howardhinnant.github.io/date_algorithms.html
    #[allow(clippy::many_single_char_names)]
    {
        let (mut days, mut secs_of_day) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
        days += 719_468;
        let era = days.div_euclid(146_097);
        let doe = days.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };

        let h = secs_of_day / 3600;
        secs_of_day %= 3600;
        let mi = secs_of_day / 60;
        let s = secs_of_day % 60;

        format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_epoch() {
        assert_eq!(format_unix_seconds_iso8601(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn iso8601_unix_billion() {
        // 2001-09-09T01:46:40Z — well-known Unix billion-second mark.
        assert_eq!(
            format_unix_seconds_iso8601(1_000_000_000),
            "2001-09-09T01:46:40Z"
        );
    }

    #[test]
    fn iso8601_leap_day() {
        // 2024-02-29T12:34:56Z
        assert_eq!(
            format_unix_seconds_iso8601(1_709_210_096),
            "2024-02-29T12:34:56Z"
        );
    }

    #[test]
    fn iso8601_format_is_strict() {
        // Spot-check: any round-the-clock value yields 20-char string ending in `Z`.
        let s = format_unix_seconds_iso8601(1_714_579_200);
        assert_eq!(s.len(), 20);
        assert!(s.ends_with('Z'));
        assert!(s.starts_with("2024-"));
    }
}
