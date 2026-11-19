//! Hot-reloading file watcher.
//!
//! Watches the parent directory of each of the three config files (rather
//! than the files themselves) so we survive atomic-rename and
//! delete-recreate save patterns. Events are coalesced through a 250 ms
//! quiet-period debounce before triggering a reload. Bad reloads (parse
//! error, validation failure) are logged and the previous config is
//! retained.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::error::ConfigError;
use crate::loader::load;
use crate::schema::Config;

/// Quiet period for coalescing rapid-fire `notify` events. Editor save
/// patterns (write-truncate-write, atomic-rename) typically emit several
/// events within a few milliseconds; 250 ms is comfortably longer than
/// that and shorter than human reaction time.
const DEBOUNCE: Duration = Duration::from_millis(250);

/// Channel capacity for emitted snapshots. The consumer is expected to
/// drain promptly; oversized capacity hides bugs.
const SNAPSHOT_CHANNEL: usize = 8;

/// Channel capacity for raw notify events. Large enough that bursts
/// (e.g. an editor saving three files within milliseconds) never drop.
const EVENT_CHANNEL: usize = 256;

/// Streams new `Arc<Config>` snapshots whenever the watched files change.
///
/// The watcher does **not** propagate errors over the channel: a bad
/// reload is logged and dropped, and the previous valid `Config` remains
/// authoritative. The consumer therefore always sees a stream of
/// well-formed configs.
///
/// Drop the watcher to stop watching.
pub struct ConfigWatcher {
    rx: mpsc::Receiver<Arc<Config>>,
    _handles: Vec<JoinHandle<()>>,
    /// Held for its lifetime; dropping the watcher releases the OS-level
    /// subscriptions.
    _watcher: RecommendedWatcher,
}

impl ConfigWatcher {
    /// Await the next config snapshot. Returns `None` once the watcher
    /// has been shut down (typically via drop of all senders, which
    /// happens when the background task exits).
    pub async fn next(&mut self) -> Option<Arc<Config>> {
        self.rx.recv().await
    }
}

/// Load the config and start watching the three files for changes.
///
/// Returns the initial `Config` and a [`ConfigWatcher`] that streams
/// subsequent valid snapshots.
///
/// # Errors
///
/// Returns the same errors as [`load`] for the initial parse, plus
/// [`ConfigError::Watch`] if the underlying filesystem watcher cannot be
/// installed (e.g. the directories of the three files do not exist).
pub fn watch(nozzl_path: &Path) -> Result<(Arc<Config>, ConfigWatcher), ConfigError> {
    let nozzl_path = nozzl_path.to_path_buf();
    let initial = Arc::new(load(&nozzl_path)?);

    let (event_tx, event_rx) = mpsc::channel::<()>(EVENT_CHANNEL);
    let (snapshot_tx, snapshot_rx) = mpsc::channel::<Arc<Config>>(SNAPSHOT_CHANNEL);

    // notify runs the callback on a background thread; we hop into the
    // tokio world via `try_send` of a unit ping.
    let event_tx_for_cb = event_tx.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        match res {
            Ok(event) => {
                if !is_relevant_event_kind(event.kind) {
                    return;
                }
                debug!(
                    target: "nozzl_config",
                    kind = ?event.kind,
                    paths = ?event.paths,
                    "filesystem event"
                );
                // Best-effort send; if the channel is full the
                // debouncer will pick up the change on the next event.
                let _ = event_tx_for_cb.try_send(());
            }
            Err(e) => {
                warn!(target: "nozzl_config", error = %e, "notify error");
            }
        }
    })?;

    // Watch each *directory* containing a config file, not the files
    // themselves. Editors that atomic-rename (write to temp, mv over) or
    // delete-then-recreate would otherwise unwatch our target inode
    // permanently.
    let watched_dirs = collect_watched_dirs(&nozzl_path, &initial);
    for dir in &watched_dirs {
        watcher.watch(dir, RecursiveMode::NonRecursive)?;
        info!(target: "nozzl_config", dir = %dir.display(), "watching directory");
    }

    let task_handle = tokio::spawn(reload_loop(
        nozzl_path,
        initial.clone(),
        event_rx,
        snapshot_tx,
    ));

    // Drop our extra clone; the closure inside the watcher holds the
    // remaining sender. When the watcher is dropped, the channel closes
    // and the reload loop exits.
    drop(event_tx);

    Ok((
        initial,
        ConfigWatcher {
            rx: snapshot_rx,
            _handles: vec![task_handle],
            _watcher: watcher,
        },
    ))
}

/// Returns true for filesystem events that may indicate a config edit.
/// We deliberately accept Modify, Create, and Remove (an atomic rename
/// shows up as Remove of the old inode followed by Create of the new
/// one).
fn is_relevant_event_kind(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_) | EventKind::Any
    )
}

/// Collect the unique directories we need to watch given the resolved
/// config. The set is small (≤ 3) so a Vec is fine.
///
/// `cfg.sources.relays_path` / `cfg.sources.kinds_path` are already the
/// fully-resolved paths the loader used — we must NOT re-resolve them
/// against `nozzl_path.parent()`, that would double-join.
fn collect_watched_dirs(nozzl_path: &Path, cfg: &Config) -> Vec<PathBuf> {
    let base = nozzl_path
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    let relays_dir = cfg
        .sources
        .relays_path
        .parent()
        .map_or_else(|| base.clone(), Path::to_path_buf);
    let kinds_dir = cfg
        .sources
        .kinds_path
        .parent()
        .map_or_else(|| base.clone(), Path::to_path_buf);

    let mut seen = HashSet::new();
    let mut dirs = Vec::new();
    for d in [base, relays_dir, kinds_dir] {
        if seen.insert(d.clone()) {
            dirs.push(d);
        }
    }
    dirs
}

/// Receives notify pings, debounces, and emits new config snapshots.
async fn reload_loop(
    nozzl_path: PathBuf,
    initial: Arc<Config>,
    mut event_rx: mpsc::Receiver<()>,
    snapshot_tx: mpsc::Sender<Arc<Config>>,
) {
    let mut current = initial;

    loop {
        // Block until we see at least one event.
        let Some(()) = event_rx.recv().await else {
            debug!(target: "nozzl_config", "event channel closed; reload loop exiting");
            return;
        };

        // Debounce: extend the deadline every time another event arrives
        // until we have DEBOUNCE of quiet.
        let mut deadline = Instant::now() + DEBOUNCE;
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let remaining = deadline - now;
            match tokio::time::timeout(remaining, event_rx.recv()).await {
                Ok(Some(())) => {
                    deadline = Instant::now() + DEBOUNCE;
                }
                Ok(None) => {
                    debug!(target: "nozzl_config", "event channel closed during debounce");
                    return;
                }
                Err(_) => break,
            }
        }

        // Drain any events that snuck in during the gap; we'll reload
        // exactly once for all of them.
        while event_rx.try_recv().is_ok() {}

        match load(&nozzl_path) {
            Ok(new_cfg) => {
                let new_arc = Arc::new(new_cfg);
                info!(
                    target: "nozzl_config",
                    relays = new_arc.relays.len(),
                    kinds = new_arc.kinds.len(),
                    "config reloaded"
                );
                current = new_arc.clone();
                if snapshot_tx.send(new_arc).await.is_err() {
                    debug!(target: "nozzl_config", "snapshot consumer dropped; reload loop exiting");
                    return;
                }
            }
            Err(e) => {
                error!(
                    target: "nozzl_config",
                    error = %e,
                    "config reload failed; retaining previous config"
                );
                // Keep `current` as-is; we never propagate the error.
                let _ = &current;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{CreateKind, ModifyKind};

    #[test]
    fn relevant_event_filters() {
        assert!(is_relevant_event_kind(EventKind::Modify(ModifyKind::Any)));
        assert!(is_relevant_event_kind(EventKind::Create(CreateKind::Any)));
        assert!(is_relevant_event_kind(EventKind::Any));
        assert!(!is_relevant_event_kind(EventKind::Access(
            notify::event::AccessKind::Any
        )));
    }
}
