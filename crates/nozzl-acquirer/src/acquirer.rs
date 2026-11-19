//! [`Acquirer`] — public entry point.
//!
//! Owns the [`RelayPool`], a per-relay command channel, the central
//! notifications-fan-out task, and the heartbeat task.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use nostr_database::{MemoryDatabase, MemoryDatabaseOptions};
use nostr_relay_pool::pool::RelayPoolBuilder;
use nostr_relay_pool::{RelayLimits, RelayOptions, RelayPool, RelayPoolNotification};
use nozzl_checkpoint::CheckpointStore;
use nozzl_metrics::{Metrics, RelayConnState};
use nozzl_types::{EventEnvelope, RelayUrl as NozzlRelayUrl};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::config::{AcquirerConfig, ActionPlan};
use crate::relay_task::{self, RelayCommand};
use crate::transport::UserAgentTransport;

/// Heartbeat cadence (design.md §5.D10).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Per-relay notification fan-out channel capacity. Big enough to absorb a
/// burst of EOSE-driven backfill events without forcing the central listener
/// to drop. If we ever exceed it, fix the root cause (slow sink) rather than
/// raising the cap silently.
const NOTIF_FANOUT_CAP: usize = 1024;

/// Public acquirer handle. See `docs/crates/nozzl-acquirer.md` for the
/// behavioral contract.
pub struct Acquirer {
    inner: Arc<Inner>,
}

struct Inner {
    pool: RelayPool,
    sink: mpsc::Sender<EventEnvelope>,
    ckpt: Arc<dyn CheckpointStore>,
    metrics: Arc<Metrics>,
    /// Active relay-task handles, keyed by URL.
    relays: Mutex<HashMap<NozzlRelayUrl, RelayTaskHandle>>,
    /// Last applied config — read by `apply_config` to compute the diff.
    config: Mutex<AcquirerConfig>,
    /// Listener task that fans `RelayPool` notifications out to per-relay
    /// channels and sends heartbeats. Held in an `Option` so `shutdown` can
    /// take ownership.
    central_task: Mutex<Option<JoinHandle<()>>>,
    /// Signals the central task to stop. Used by `shutdown`.
    shutdown_central: Mutex<Option<oneshot::Sender<()>>>,
}

struct RelayTaskHandle {
    cmd_tx: mpsc::Sender<RelayCommand>,
    notif_tx: mpsc::Sender<RelayPoolNotification>,
    join: JoinHandle<()>,
}

impl Acquirer {
    /// Construct and spawn the acquirer.
    ///
    /// One task per relay is spawned immediately. Each task connects (with
    /// the pool's automatic reconnect loop), fetches NIP-11, and opens a
    /// long-lived forward subscription plus an onboarding backfill if the
    /// relay's checkpoint state for any kind is `Pending` or `InProgress`.
    pub fn spawn(
        cfg: AcquirerConfig,
        sink: mpsc::Sender<EventEnvelope>,
        ckpt: Arc<dyn CheckpointStore>,
        metrics: Arc<Metrics>,
    ) -> Self {
        // Configure the pool's in-memory dedup tracker to a generous capacity
        // so a single backfill run rarely evicts ids before duplicates from
        // other relays arrive. See storage plan, "Dedup" section.
        let db = MemoryDatabase::with_opts(MemoryDatabaseOptions {
            events: false,
            max_events: Some(cfg.pool_dedup_max_events.max(1)),
        });
        // The transport adds a `User-Agent` to every handshake, which some relays require
        // (`transport.rs`).
        let pool = RelayPoolBuilder {
            __database: Arc::new(db),
            websocket_transport: Arc::new(UserAgentTransport),
            ..RelayPoolBuilder::default()
        }
        .build();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let inner = Arc::new(Inner {
            pool: pool.clone(),
            sink: sink.clone(),
            ckpt: Arc::clone(&ckpt),
            metrics: Arc::clone(&metrics),
            relays: Mutex::new(HashMap::new()),
            config: Mutex::new(cfg.clone()),
            central_task: Mutex::new(None),
            shutdown_central: Mutex::new(Some(shutdown_tx)),
        });

        // Spawn the central listener first so initial notifications aren't
        // dropped on the floor.
        let central = {
            let inner = Arc::clone(&inner);
            tokio::spawn(run_central(inner, shutdown_rx))
        };
        // Lock-free here: nobody else can see this Inner yet.
        if let Ok(mut slot) = inner.central_task.try_lock() {
            *slot = Some(central);
        }

        let acquirer = Self {
            inner: Arc::clone(&inner),
        };

        // Add and start each relay synchronously through the runtime. Per-relay
        // kind scoping (via `relay_only_kinds`) is applied here: each relay
        // task is spawned with the intersection of the global kind list and
        // that relay's allowlist.
        let cfg_for_spawn = cfg.clone();
        let kind_slugs = cfg.kind_slugs.clone();
        let safety = cfg.backfill_safety_margin.as_secs();
        let limit = cfg.backfill_default_limit;
        let empty_pages_to_complete = cfg.backfill_empty_pages_to_complete;
        let forward_silence_threshold = cfg.forward_silence_threshold;
        let probe_timeout = cfg.probe_timeout;
        let probe_min_interval = cfg.probe_min_interval;
        tokio::spawn(async move {
            for url in &cfg_for_spawn.relays {
                let effective = cfg_for_spawn.effective_kinds_for(url);
                if effective.is_empty() {
                    tracing::warn!(
                        relay = %url,
                        "relay has no effective kinds after only_kinds intersection; \
                         skipping (no forward sub will be opened)"
                    );
                    continue;
                }
                if let Err(e) = inner
                    .add_relay_internal(
                        url,
                        &effective,
                        &kind_slugs,
                        safety,
                        limit,
                        empty_pages_to_complete,
                        forward_silence_threshold,
                        probe_timeout,
                        probe_min_interval,
                        // Empty `backfill_kinds` at startup: the relay-task
                        // will consult `CheckpointStore::backfill_state` and
                        // only backfill kinds whose state is Pending or
                        // InProgress. Passing the full kind set here would
                        // force a re-walk of already-Complete histories on
                        // every restart and clobber the Complete checkpoint
                        // back to InProgress.
                        Vec::new(),
                    )
                    .await
                {
                    tracing::error!(relay = %url, error = %e, "failed to add relay at startup");
                }
            }
        });

        acquirer
    }

    /// Apply a new config. Diffs the old vs. new state and:
    ///  - spawns tasks for added relays (with full backfill across configured kinds),
    ///  - tears down removed relays,
    ///  - re-issues forward subscriptions on every existing relay if the kind
    ///    list changed,
    ///  - kicks off scoped backfill REQs for newly added kinds against every
    ///    existing relay.
    pub async fn apply_config(&self, cfg: AcquirerConfig) {
        let plan = {
            let mut guard = self.inner.config.lock().await;
            let plan = AcquirerConfig::diff(&guard, &cfg);
            *guard = cfg.clone();
            plan
        };
        tracing::info!(
            relays_added = ?plan.relays_added.iter().map(|r| r.as_str().to_string()).collect::<Vec<_>>(),
            relays_removed = ?plan.relays_removed.iter().map(|r| r.as_str().to_string()).collect::<Vec<_>>(),
            kinds_added = ?plan.kinds_added,
            kinds_removed = ?plan.kinds_removed,
            relays_rescoped = ?plan.relay_kind_updates.iter()
                .map(|u| format!("{}{:?}", u.url.as_str(), u.gained))
                .collect::<Vec<_>>(),
            "config reload: applying diff",
        );
        self.inner.apply_plan(plan, cfg).await;
    }

    /// Stop all relay-tasks and the central listener task.
    ///
    /// Issues `CLOSE` (via `unsubscribe_all`), drops the channel sender into
    /// the writer pool (so the binary's writers can drain), and waits for
    /// every relay-task to exit. Idempotent in spirit (calling twice would
    /// require keeping `Self` available, which `shutdown` consumes).
    pub async fn shutdown(self) {
        // 1) Tell every relay task to drain.
        let urls: Vec<NozzlRelayUrl> = {
            let guard = self.inner.relays.lock().await;
            guard.keys().cloned().collect()
        };
        for url in &urls {
            self.inner.send_command(url, RelayCommand::Shutdown).await;
        }
        // 2) Wait for them to terminate.
        let handles: Vec<JoinHandle<()>> = {
            let mut guard = self.inner.relays.lock().await;
            let mut out = Vec::with_capacity(guard.len());
            for (_, h) in guard.drain() {
                out.push(h.join);
            }
            out
        };
        for h in handles {
            let _ = h.await;
        }
        // 3) Stop the central task.
        if let Some(tx) = self.inner.shutdown_central.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.inner.central_task.lock().await.take() {
            let _ = handle.await;
        }
        // 4) Disconnect the pool. Drop the sink by going out of scope.
        self.inner.pool.shutdown().await;
    }
}

impl Inner {
    async fn send_command(&self, url: &NozzlRelayUrl, cmd: RelayCommand) {
        let guard = self.relays.lock().await;
        if let Some(h) = guard.get(url) {
            let _ = h.cmd_tx.send(cmd).await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn add_relay_internal(
        self: &Arc<Self>,
        url: &NozzlRelayUrl,
        kinds: &[u16],
        kind_slugs: &std::collections::HashMap<u16, String>,
        safety: u64,
        limit: u32,
        empty_pages_to_complete: u32,
        forward_silence_threshold: Duration,
        probe_timeout: Duration,
        probe_min_interval: Duration,
        backfill_kinds: Vec<u16>,
    ) -> Result<(), String> {
        // 1) Add to the pool (READ flag default; reconnect default true).
        //
        // Disable per-event size and tag-count limits: some kinds routinely
        // carry giant tag arrays, and there the tags are the data. A
        // power-user follow list, one ["p", ...] per follow, can hold
        // 5,000+ pubkeys, but the
        // library's defaults (`max_num_tags = 2_000`, plus a per-Kind
        // override of `Some(10_000)` for ContactList) reject anything
        // bigger at the Message-handler level — the event never reaches
        // our acquirer code, so it's silently lost. Observed in
        // production on 2026-05-09 as `Received event with too many
        // tags: tags=4902, max_tags=2000` ERROR lines, with high-profile
        // accounts' events missing.
        //
        // Defense in depth: the message-level size cap
        // (`limits.messages.max_size`) is preserved at its default —
        // it's a safety net against absurd JSON payloads at the wire
        // level, independent of event semantics.
        let mut limits = RelayLimits::default();
        limits.events.max_size = None;
        limits.events.max_size_per_kind.clear();
        limits.events.max_num_tags = None;
        limits.events.max_num_tags_per_kind.clear();
        // Tune the auto-reconnect loop.
        //
        // Defaults give `retry_interval = 10 s` with
        // `adjust_retry_interval = true`. The "adjust" path multiplies the
        // sleep by `1 + (attempts − success) / 2` using monotonic atomic
        // counters that never reset — so any transient connection failure
        // over the process lifetime permanently lengthens every subsequent
        // reconnect, capped at 60 s. Observed live: Damus's hourly Cloudflare
        // cutoff (TCP RST without close handshake at `HH:15:47–:48 UTC`,
        // ~1 h max-WS-age on the CF edge — not strfry; see D6 in docs/design.md) yields a
        // 12 s gap on a fresh process and a 30 s gap once `diff` has crept
        // past 4. An earlier tool running the same library shows the gap
        // pinned at 60 s after several days of DNS hiccups.
        //
        // We set `retry_interval = 2 s` and disable the adjust path:
        //  - Cloudflare and strfry have no per-IP reconnect penalty for a
        //    single sticky client (verified from strfry source + multi-month
        //    observations); aggressive reconnect is safe.
        //  - On a hard-down relay we're effectively rate-limited by
        //    `DEFAULT_CONNECTION_TIMEOUT = 60 s` per attempt anyway.
        //  - Replaceable events (kind-0 (profile-metadata),
        //    kind-3 (follow-lists), kind-10002 (relay-list-metadata), ...)
        //    lose intermediate versions during the gap; cutting it from ~30 s to ~3-5 s is a
        //    direct data-quality win.
        let options = RelayOptions::default()
            .limits(limits)
            .retry_interval(Duration::from_secs(2))
            .adjust_retry_interval(false);
        if let Err(e) = self.pool.add_relay(url.as_str(), options).await {
            return Err(e.to_string());
        }
        // 2) Connect (the pool's connection task handles reconnect with the
        //    `retry_interval` set above; no per-call adjustment).
        if let Err(e) = self.pool.connect_relay(url.as_str()).await {
            return Err(e.to_string());
        }
        // 3) Spawn the per-relay task, its relay-only series seeded first: for a
        // relay added by hot-reload the startup preallocation never ran, and a
        // first increment (a floored kind, a fan-out drop) would be invisible
        // to `increase()`.
        self.metrics.preallocate_relay(url.as_str());
        let (notif_tx, notif_rx) = mpsc::channel::<RelayPoolNotification>(NOTIF_FANOUT_CAP);
        let (cmd_tx, join) = relay_task::spawn(
            url.clone(),
            kinds.to_vec(),
            kind_slugs.clone(),
            self.pool.clone(),
            self.sink.clone(),
            Arc::clone(&self.ckpt),
            Arc::clone(&self.metrics),
            safety,
            limit,
            empty_pages_to_complete,
            forward_silence_threshold,
            probe_timeout,
            probe_min_interval,
            notif_rx,
            backfill_kinds,
        );
        let handle = RelayTaskHandle {
            cmd_tx,
            notif_tx,
            join,
        };
        self.relays.lock().await.insert(url.clone(), handle);
        Ok(())
    }

    async fn remove_relay_internal(&self, url: &NozzlRelayUrl) {
        let handle = {
            let mut guard = self.relays.lock().await;
            guard.remove(url)
        };
        if let Some(h) = handle {
            let _ = h.cmd_tx.send(RelayCommand::Shutdown).await;
            let _ = h.join.await;
        }
        let _ = self.pool.force_remove_relay(url.as_str()).await;
    }

    async fn apply_plan(self: &Arc<Self>, plan: ActionPlan, cfg: AcquirerConfig) {
        let safety = cfg.backfill_safety_margin.as_secs();
        let limit = cfg.backfill_default_limit;
        let empty_pages_to_complete = cfg.backfill_empty_pages_to_complete;
        let forward_silence_threshold = cfg.forward_silence_threshold;
        let probe_timeout = cfg.probe_timeout;
        let probe_min_interval = cfg.probe_min_interval;

        // Tear down removed relays first so we don't churn over their state
        // while propagating kind changes to the rest.
        for url in &plan.relays_removed {
            self.remove_relay_internal(url).await;
        }

        // For surviving relays: if the global kind set changed, re-issue
        // forward subs and kick off a scoped backfill for `kinds_added`.
        // Per-relay `only_kinds` (if present) gates which kinds reach each
        // surviving relay: a kind newly added to `kinds.toml` is *not*
        // backfilled on a relay whose `only_kinds` doesn't include it, and
        // the relay's effective forward-sub kind set is the intersection.
        //
        // Driven by the per-relay effective-set diff, NOT by the global
        // "kind set changed" flag. A relay whose effective set is unchanged is
        // left strictly alone: re-issuing there would `close_all_forward_subs`
        // + `compute_initial_groups` (discarding any NOTICE-learned REQ split)
        // + `open_all_forward_subs`, replaying that relay's history from its
        // `min(since)` for no change in what is requested. Onboarding a kind
        // pinned to one relay must be a true no-op on every other relay.
        {
            let live: std::collections::HashSet<NozzlRelayUrl> = {
                let guard = self.relays.lock().await;
                guard.keys().cloned().collect()
            };
            for update in &plan.relay_kind_updates {
                // Skip relays that aren't running yet (added this same reload
                // -- the relay-add path below spawns them with the right set).
                if !live.contains(&update.url) || plan.relays_added.contains(&update.url) {
                    continue;
                }
                self.send_command(
                    &update.url,
                    RelayCommand::UpdateKinds {
                        kinds: update.effective.clone(),
                        backfill_kinds: update.gained.clone(),
                        kind_slugs: cfg.kind_slugs.clone(),
                    },
                )
                .await;
            }
        }

        // Add new relays last. Backfill is driven via the checkpoint-walking
        // path (empty `backfill_kinds`): a freshly-added relay has no
        // checkpoint entries, so every kind is Pending and gets backfilled —
        // same end state as forcing the kind list, but uniform with the
        // startup path and safe if we ever re-add a relay whose state is
        // already Complete.
        //
        // A newly added relay starts with its effective kind set: its
        // `only_kinds` composed with each kind's `only_relays`. A surviving
        // relay whose effective set changed, through either field or the
        // kind list, was already updated in place by the `UpdateKinds` loop
        // above; neither field needs a restart.
        for url in &plan.relays_added {
            let effective = cfg.effective_kinds_for(url);
            if effective.is_empty() {
                tracing::warn!(
                    relay = %url,
                    "relay has no effective kinds after only_kinds intersection; \
                     skipping (no forward sub will be opened)"
                );
                continue;
            }
            if let Err(e) = self
                .add_relay_internal(
                    url,
                    &effective,
                    &cfg.kind_slugs,
                    safety,
                    limit,
                    empty_pages_to_complete,
                    forward_silence_threshold,
                    probe_timeout,
                    probe_min_interval,
                    Vec::new(),
                )
                .await
            {
                tracing::error!(relay = %url, error = %e, "failed to add relay during reload");
            }
        }
    }
}

async fn run_central(inner: Arc<Inner>, mut shutdown: oneshot::Receiver<()>) {
    let mut notifications = inner.pool.notifications();
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // skip immediate fire

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::debug!("acquirer central listener: shutdown");
                return;
            }
            res = notifications.recv() => {
                match res {
                    Ok(notif) => {
                        if let Some(url) = notification_relay(&notif) {
                            // Convert the pool's RelayUrl into our newtype by string.
                            // The pool's RelayUrl normalizes the trailing slash, as
                            // does ours, so this round-trip is identity.
                            if let Ok(nozzl_url) = NozzlRelayUrl::parse(url.as_str()) {
                                let guard = inner.relays.lock().await;
                                if let Some(h) = guard.get(&nozzl_url) {
                                    // Best-effort: a full per-relay channel
                                    // means the relay task is hung; we record
                                    // the drop (below) and keep dispatching
                                    // rather than blocking the fan-out.
                                    if h.notif_tx.try_send(notif).is_err() {
                                        // One of the two silent-loss escape
                                        // valves (the other is the broadcast
                                        // `Lagged` below). Count it so the
                                        // "loss ⟺ a dropped notification"
                                        // invariant is a live `/metrics` gauge,
                                        // not just a log grep.
                                        inner
                                            .metrics
                                            .notif_fanout_dropped
                                            .with_label_values(&[nozzl_url.as_str()])
                                            .inc();
                                        tracing::warn!(relay = %nozzl_url, "relay notif channel full; dropping notif");
                                    }
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        // The second silent-loss escape valve. `n` is the count
                        // of broadcast slots the central listener fell behind by
                        // — skipped `Event` AND `Message` notifications — so it
                        // is an upper bound on lost unique events. Increment BY
                        // `n` (not by 1) so the counter is a true accounting of
                        // the skip, matching the `pool_loss_harness` invariant
                        // (`lost <= lagged_total`).
                        inner.metrics.notif_channel_lagged.inc_by(n);
                        tracing::warn!(n, "acquirer notifications channel lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return;
                    }
                }
            }
            _ = heartbeat.tick() => {
                emit_heartbeat(&inner).await;
            }
        }
    }
}

async fn emit_heartbeat(inner: &Inner) {
    let urls: Vec<NozzlRelayUrl> = inner.relays.lock().await.keys().cloned().collect();
    let mut connected: BTreeSet<&str> = BTreeSet::new();
    let pool_relays = inner.pool.all_relays().await;
    for u in &urls {
        if let Some(r) = pool_relays.get(&into_pool_url(u)) {
            let state = relay_task::conn_state(r.status());
            inner.metrics.set_relay_connection_state(u, state);
            if state == RelayConnState::Connected {
                connected.insert(u.as_str());
            }
        }
    }
    // NOTE: `channel_depth` is owned by the periodic gauge loop in the
    // top-level crate, which sets it to the true in-flight count
    // (max_capacity - capacity). Do not set it here: `sink.capacity()` is the
    // *free* permits (the inverse of depth), so writing it would report 1024
    // on an empty channel and fight the gauge loop on a different cadence.
    tracing::info!(
        relays = urls.len(),
        connected = connected.len(),
        "heartbeat",
    );
}

/// Convert our newtype URL into the pool's URL type. Both parse with
/// `url::Url::parse` and take only ws/wss, and `RelayUrl::parse` also refuses
/// the one thing the pool's parser adds, a second `://`, so every `RelayUrl`
/// converts and the `expect` cannot fire. `pool_url_tests` holds that.
fn into_pool_url(u: &NozzlRelayUrl) -> nostr::RelayUrl {
    nostr::RelayUrl::parse(u.as_str()).expect("RelayUrl invariant violated")
}

fn notification_relay(notif: &RelayPoolNotification) -> Option<&nostr::RelayUrl> {
    match notif {
        RelayPoolNotification::Event { relay_url, .. }
        | RelayPoolNotification::Message { relay_url, .. } => Some(relay_url),
        RelayPoolNotification::Shutdown => None,
    }
}

#[cfg(test)]
mod pool_url_tests {
    use super::*;

    /// Every URL nozzl's `RelayUrl` takes, the pool's takes too, so
    /// `into_pool_url` never panics on a configured relay.
    #[test]
    fn every_relay_url_converts_to_the_pools() {
        for s in [
            "wss://relay.example",
            "ws://127.0.0.1:7777",
            "wss://relay.example:443/path?q=1",
            "wss://relay.example/#fragment",
            "wss://relay.example/?via=wss://other.example",
            "wss://relay.example/a://b",
            "wss://user:pass@relay.example",
            "https://relay.example",
        ] {
            if let Ok(u) = NozzlRelayUrl::parse(s) {
                assert!(
                    nostr::RelayUrl::parse(u.as_str()).is_ok(),
                    "{s} is a RelayUrl the pool refuses"
                );
            }
        }
    }
}
