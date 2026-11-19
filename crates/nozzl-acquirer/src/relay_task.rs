//! Per-relay controller task.
//!
//! Owns one logical relay's lifecycle:
//!  - waits for connection,
//!  - fetches NIP-11,
//!  - opens the forward subscription (split into N halved subs on filter-shape
//!    rejection only — concurrency-pressure NOTICEs go through the back-off
//!    path instead),
//!  - drives the onboarding backfill (paginated by `until`), with a stuck-REQ
//!    timeout that re-issues a sub if EOSE fails to arrive,
//!  - reacts to `NOTICE` / `CLOSED` (auto-split for filter-shape, back-off for
//!    concurrency pressure, kind-floor logic),
//!  - tracks status transitions for metrics.
//!
//! The task is fed [`RelayPoolNotification`]s by the central listener via a
//! per-relay `mpsc::Receiver`. Control commands (kind-list change, reload,
//! shutdown, backfill-only-for-new-kinds) arrive on a separate channel.
//!
//! Canonical specs for the rejection / pressure / split logic:
//! - `docs/design.md` §5.D6 "Kind-list auto-split" + glossary entry of the
//!   same name.
//! - [`crate::rejection`] for the substring fixture table that classifies
//!   filter-shape rejections vs concurrency-pressure responses.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nostr::{Event, RelayMessage, SubscriptionId};
use nostr_relay_pool::{Relay, RelayPool, RelayPoolNotification, RelayStatus, SubscribeOptions};
use nozzl_checkpoint::{BackfillState, CheckpointStore};
use nozzl_metrics::{Metrics, RelayConnState};
use nozzl_types::{EventClass, EventEnvelope, RelayUrl as NozzlRelayUrl};
use tokio::sync::mpsc;

use crate::filter::{backfill_filter, forward_filter, probe_filter, since_with_safety_margin};
use crate::nip11::{self, Nip11Info};
use crate::rejection;

/// Minimum acceptable per-page size for backfill, regardless of NIP-11.
const BACKFILL_MIN_LIMIT: u32 = 50;

/// Maximum time a backfill REQ may stay open without an EOSE before we
/// declare it stuck and re-issue from the same `until_cursor`. Production
/// relays sometimes silently drop subscriptions (e.g. after a "too many
/// concurrent REQs" NOTICE) without sending CLOSED, leaving the backfill
/// loop hung indefinitely. The timeout is an upper bound — well-behaved
/// relays EOSE within a few seconds even on the slowest pages we've seen.
const BACKFILL_REQ_TIMEOUT: Duration = Duration::from_secs(60);

/// How often to scan for stuck backfill REQs. Cheap (`HashMap` iteration), so
/// running it on a fixed cadence is fine.
const STUCK_BACKFILL_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How often the dispatch-liveness probe trigger scans (relay-wide silence
/// against `last_activity_at`). Decoupled from
/// [`STUCK_BACKFILL_CHECK_INTERVAL`] so detection latency is bounded by ~1 s
/// regardless of the configured threshold — important for tests that drive
/// short thresholds, harmless in production where the per-relay scan is a
/// single `Instant` comparison.
const SILENCE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Back-off applied between successive concurrent-pressure NOTICEs (e.g.
/// "too many concurrent REQs"). Halving the kind list on these makes the
/// situation strictly worse, so we sleep instead and let in-flight REQs
/// drain. Capped via [`MAX_PRESSURE_BACKOFF`].
const PRESSURE_BACKOFF_INITIAL: Duration = Duration::from_secs(2);
const MAX_PRESSURE_BACKOFF: Duration = Duration::from_secs(30);

/// Back-off before re-opening a forward sub that the relay closed for a reason
/// that is not a filter rejection: #48 (Forward-sub CLOSED handling) and #40
/// (Forward-sub CLOSED path re-issues with no backoff). It used to be re-opened
/// at once, so a relay that keeps closing (an exhausted read budget, a rate
/// limit, an upstream's CLOSED passed through) got a REQ per round trip. The
/// wait doubles per CLOSED up to the max, and starts again from the initial
/// value once the relay has gone [`FORWARD_REISSUE_RESET_AFTER`] without
/// closing a forward sub. The re-issue replays from the checkpoint, so the
/// dark window costs latency, not events, while the relay's replay holds it.
const FORWARD_REISSUE_BACKOFF_INITIAL: Duration = Duration::from_secs(2);
const FORWARD_REISSUE_BACKOFF_MAX: Duration = Duration::from_secs(30);
const FORWARD_REISSUE_RESET_AFTER: Duration = Duration::from_secs(120);

/// Longest wait before retrying kinds that a relay rejected at the
/// single-kind floor. The first retry waits `probe_min_interval` (both are "how
/// soon may we ask a relay again after it said no"); each further floor within
/// this window doubles the wait, up to it.
const FLOOR_RETRY_MAX: Duration = Duration::from_secs(6 * 3600);

/// How long after nozzl's own CLOSE of a forward id an empty-reason CLOSED for
/// that id counts as the relay acknowledging the CLOSE, rather than closing the
/// sub nozzl re-opened under the same id right after it. relay.conduit.market
/// answers every CLOSE that way, within a round trip; the sub it closed is the
/// old one, since a socket delivers the CLOSE before the REQ that follows it.
const FORWARD_CLOSE_ECHO_WINDOW: Duration = Duration::from_secs(5);

/// How long a sub kept through a suspected echo waits for an EOSE or EVENT on
/// its id before the CLOSED is taken as the relay's close after all. A relay
/// that really re-opened it answers with its stored events at once.
const FORWARD_ECHO_CONFIRM: Duration = Duration::from_secs(10);

/// The floor retry's back-off: `probe_min_interval` (at least 1 s), doubling to
/// [`FLOOR_RETRY_MAX`]. Its `reset_after` is twice the cap: equal to it, a floor
/// that had waited the full cap would always count as fresh and start the
/// doubling over, so a kind a relay never accepts would cycle back to minutes.
fn floor_retry_backoff(probe_min_interval: Duration) -> DecayingBackoff {
    DecayingBackoff::new(
        probe_min_interval.max(Duration::from_secs(1)),
        FLOOR_RETRY_MAX,
        FLOOR_RETRY_MAX * 2,
    )
}

/// An exponential back-off that forgets. Each [`Self::next`] doubles the wait
/// (from `initial`, capped at `max`), unless the previous call was `reset_after`
/// or more ago, in which case it starts again at `initial`.
#[derive(Debug)]
struct DecayingBackoff {
    initial: Duration,
    max: Duration,
    reset_after: Duration,
    current: Duration,
    last_at: Option<Instant>,
}

impl DecayingBackoff {
    fn new(initial: Duration, max: Duration, reset_after: Duration) -> Self {
        Self {
            initial,
            max,
            reset_after,
            current: Duration::ZERO,
            last_at: None,
        }
    }

    /// The wait to apply for an event (a CLOSED, a floor) observed at `now`.
    fn next(&mut self, now: Instant) -> Duration {
        let fresh = self
            .last_at
            .is_none_or(|t| now.saturating_duration_since(t) >= self.reset_after);
        self.current = if fresh || self.current.is_zero() {
            self.initial
        } else {
            (self.current * 2).min(self.max)
        };
        self.last_at = Some(now);
        self.current
    }
}

/// Upper bound on how long [`teardown_barrier_and_rearm`] waits for the
/// lib's old connection task to exit after `disconnect_relay`. Must exceed
/// the lib's own dead-peer close bound — `close_ws` is capped by
/// `WEBSOCKET_TX_TIMEOUT = 10 s` (`nostr-relay-pool` `constants.rs`) — so a
/// barrier timeout means "the task never exited" (it reconnected itself),
/// not "the close was slow". Healthy-peer teardown is ms-scale; this bound
/// is the pathological tail only.
pub const TEARDOWN_BARRIER_TIMEOUT: Duration = Duration::from_secs(12);
/// Poll cadence for the teardown barrier. Teardown is typically ms-scale,
/// so 10 ms keeps the added reconnect latency negligible.
const TEARDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Stable forward-sub-id base. When auto-splitting we append `-N`.
const FORWARD_SUB_BASE: &str = "forward";
/// Stable backfill-sub-id base. We append `-N` for the page index.
const BACKFILL_SUB_BASE: &str = "backfill";
/// Stable subscription-dispatch-probe sub-id base. Probe ids are formed as
/// `format!("{PROBE_SUB_BASE}-{}", new_nonce())`. The distinct `"probe-"`
/// prefix keeps `group_index_of` ([`group_index_of`]) from misclassifying
/// probe ids as forward-sub groups.
pub(super) const PROBE_SUB_BASE: &str = "probe";

/// Outcome of [`teardown_barrier_and_rearm`]. `as_label()` is the `outcome`
/// label on the `teardown_barrier_total` metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownBarrierOutcome {
    /// Old task's exit observed and status already connectable —
    /// `connect()` issued deterministically (the normal path).
    Cleared,
    /// Old task's exit observed but teardown left a non-connectable status
    /// (e.g. the lib's `_try_connect` Err-arm overwrote our eager
    /// `Terminated` with `Disconnected`) — re-drove through `disconnect()`
    /// to `Terminated`, then connected. Still deterministic: no task is
    /// alive, so nothing races the re-drive.
    StatusRedrive,
    /// Barrier expired with the old task still running, but status was
    /// connectable — `connect()` issued anyway. The one sub-case that
    /// genuinely re-enters the original race window (issue #8).
    TimeoutConnectable,
    /// Barrier expired with the old task still running and status not
    /// connectable - the task survived our disconnect (the lib can lose a
    /// disconnect's termination: it wakes the task before it sets
    /// `Terminated`, and `_try_connect` can overwrite an early `Terminated`;
    /// `docs/nostr-relay-pool-limitations.md` entry #11) and is managing the
    /// connection itself; a `connect()` would guard-no-op, so it is
    /// skipped. Recovery is the lib's (or, failing that, an alert on
    /// `relay_connection_state`).
    TimeoutTaskLive,
}

impl TeardownBarrierOutcome {
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Cleared => "cleared",
            Self::StatusRedrive => "status_redrive",
            Self::TimeoutConnectable => "timeout_connectable",
            Self::TimeoutTaskLive => "timeout_task_live",
        }
    }
}

/// The lib's `RelayStatus::can_connect()` is `pub(crate)`, so mirror it:
/// `Relay::connect()` silently returns unless the status is one of these
/// (verified against `nostr-relay-pool` 0.44 `status.rs` / `mod.rs:288`).
fn status_is_connectable(status: RelayStatus) -> bool {
    matches!(
        status,
        RelayStatus::Initialized | RelayStatus::Terminated | RelayStatus::Sleeping
    )
}

/// Sequence the explicit reconnect after a `disconnect_relay`, without racing
/// the old connection task's teardown (issue #8, friction-log entry #8).
///
/// The lib clears its internal `running` flag only at the connection task's
/// exit — *after* the `close_ws` handshake — while `disconnect_relay` returns
/// immediately. Calling `connect_relay` inside that window can be silently
/// no-op'd by the spawn guard while the old task then exits on a stale local
/// status snapshot, leaving the relay permanently `!Connected` with no task
/// (observed live 4×: nos.lol 2026-06-06, damus 2026-06-25/27, fountain
/// 2026-07-03). Two-stage fix, both stages required:
///
/// 1. **Teardown barrier** — poll `Relay::is_connection_task_running()`
///    (an accessor added in our `whisperhash/nostr` fork; see
///    `docs/nostr-relay-pool-limitations.md` → "The whisperhash fork") until
///    the old task has provably exited, bounded by
///    [`TEARDOWN_BARRIER_TIMEOUT`].
/// 2. **Status re-drive** — `connect()` also silently no-ops unless the
///    status is connectable ([`status_is_connectable`]), and teardown can
///    leave `Disconnected` (the lib's `_try_connect` Err-arm overwrites our
///    eager `Terminated`). If so, `disconnect()` once more — no task is
///    alive, so this deterministically re-drives to `Terminated` — then
///    connect.
///
/// Blocking scope: callers run on their own per-relay tokio task, so the
/// bounded wait stalls only that relay's `select!` loop — including its
/// `Shutdown`/`UpdateKinds` command handling and notification draining —
/// for up to the 12 s bound in the pathological tail (normal clears are
/// ms-scale). That is safe: the central fan-out is `try_send` based (no
/// cross-relay backpressure; worst case some `notif_fanout_dropped` on
/// THIS relay during a pathological wait, recovered by the checkpointed
/// forward-sub reopen), and `remove_relay_internal` joins this task before
/// force-removing the relay, so a mid-barrier handle can never re-arm a
/// removed relay. Callers must have already called `disconnect_relay` and
/// must be the only concurrent `connect` caller for this relay (true in
/// production: startup/hot-reload connect only relays that have no task
/// yet).
pub async fn teardown_barrier_and_rearm(relay: &Relay) -> TeardownBarrierOutcome {
    let started = Instant::now();

    // Stage 1: wait for the old connection task to actually exit.
    let task_exited = loop {
        if !relay.is_connection_task_running() {
            break true;
        }
        if started.elapsed() >= TEARDOWN_BARRIER_TIMEOUT {
            break false;
        }
        tokio::time::sleep(TEARDOWN_POLL_INTERVAL).await;
    };
    let waited_ms = started.elapsed().as_millis();
    let status = relay.status();

    if task_exited {
        // Stage 2: `connect()` is still a silent no-op unless the status is
        // connectable — re-drive through `Terminated` if teardown left
        // something else behind.
        if status_is_connectable(status) {
            tracing::info!(
                relay = %relay.url(),
                waited_ms,
                %status,
                "teardown barrier cleared; re-arming connection",
            );
            relay.connect();
            TeardownBarrierOutcome::Cleared
        } else {
            tracing::warn!(
                relay = %relay.url(),
                waited_ms,
                %status,
                "teardown left a non-connectable status; re-driving through \
                 Terminated before re-arming",
            );
            // This disconnect() targets a relay with NO task alive, so its
            // terminate signal (`Notify::notify_one`) is stored as a stale
            // permit that the fresh task's first `_try_connect` select will
            // consume — aborting the first connect attempt with
            // `TerminationRequest`. Benign: the task survives, walks to
            // `Disconnected`, and the lib retries after `retry_interval`
            // (~2 s in production). Recovery on this arm is therefore
            // "one aborted attempt slower" than `Cleared`, not immediate.
            relay.disconnect();
            relay.connect();
            TeardownBarrierOutcome::StatusRedrive
        }
    } else if status_is_connectable(status) {
        // Task never exited within the bound, but the status would accept a
        // connect. Issue it — this is the one residual sub-case that
        // re-enters the original race window; an alert on
        // `relay_connection_state` remains the net if it loses.
        tracing::warn!(
            relay = %relay.url(),
            waited_ms,
            %status,
            "teardown barrier TIMED OUT with connectable status; re-arming \
             anyway (residual issue-#8 race window)",
        );
        relay.connect();
        TeardownBarrierOutcome::TimeoutConnectable
    } else {
        // Task never exited and the status is live (Connecting/Connected/
        // Disconnected-retrying): the old task survived our disconnect and
        // is managing the connection itself. A connect() here would silently
        // guard-no-op — skip it and say what actually happened.
        tracing::info!(
            relay = %relay.url(),
            waited_ms,
            %status,
            "teardown barrier timed out but the lib connection task is still \
             live (it survived the disconnect and reconnected itself); \
             leaving recovery to it",
        );
        TeardownBarrierOutcome::TimeoutTaskLive
    }
}

/// Concurrency-pressure throttle state for backfill REQs against one relay.
///
/// Surfaced via `["NOTICE", "ERROR: too many concurrent REQs"]` and the
/// equivalent `CLOSED` reasons. Each observation doubles the backoff (capped
/// at `max`), pushing the next-page open further into the future. A page
/// that EOSEs without observing any pressure resets the backoff to zero so
/// steady-state throughput is unimpaired.
///
/// Why `seen_during_page` rather than reset-on-every-EOSE: the original code
/// reset on every EOSE, which wiped the backoff before the next-page open
/// path could honour it (Bug 1, observed live against nos.lol on 2026-05-02
/// — 95 REQs/s sustained while the relay screamed "too many concurrent
/// REQs"). The flag survives across the EOSE that ends the page in which
/// pressure was observed, so the throttle actually fires.
#[derive(Debug)]
struct PressureState {
    /// Current backoff window. Doubles on `note()`, capped at `max`.
    backoff: Duration,
    /// Wall-clock instant before which no new backfill REQ may be opened.
    resume_at: Instant,
    /// Whether any pressure observation arrived during the currently-open
    /// backfill page. Cleared by `on_eose`. Gates the EOSE-driven decay so
    /// pressure observed mid-page survives the EOSE that ends that page.
    seen_during_page: bool,
    /// Initial backoff used on the first observation after a clean state.
    initial: Duration,
    /// Maximum backoff. Caps the doubling so a relay that just keeps yelling
    /// doesn't push us past a useful timescale.
    max: Duration,
}

impl PressureState {
    fn new(initial: Duration, max: Duration) -> Self {
        Self {
            backoff: Duration::ZERO,
            resume_at: Instant::now(),
            seen_during_page: false,
            initial,
            max,
        }
    }

    /// Record a pressure observation. Doubles the backoff (or sets to
    /// `initial` if currently zero), capped at `max`. Updates `resume_at`.
    /// Marks the current page as having seen pressure so the backoff
    /// survives the next EOSE.
    fn note(&mut self) {
        self.seen_during_page = true;
        self.backoff = if self.backoff.is_zero() {
            self.initial
        } else {
            (self.backoff * 2).min(self.max)
        };
        self.resume_at = Instant::now() + self.backoff;
    }

    /// Wait duration required before opening the next backfill REQ. `None`
    /// when the window has already elapsed.
    fn wait_required(&self, now: Instant) -> Option<Duration> {
        if self.resume_at > now {
            Some(self.resume_at - now)
        } else {
            None
        }
    }

    /// Called when a backfill page EOSEs. Returns the wait that must be
    /// applied before opening the next page (`None` if no wait is needed)
    /// and resets the per-page pressure flag.
    ///
    /// If pressure was observed during the page, the backoff persists. If
    /// not, the page completed cleanly and the backoff decays to zero so
    /// the next page fires promptly.
    fn on_eose(&mut self, now: Instant) -> Option<Duration> {
        let was_pressured = self.seen_during_page;
        self.seen_during_page = false;
        if !was_pressured {
            self.backoff = Duration::ZERO;
            self.resume_at = now;
            return None;
        }
        self.wait_required(now)
    }

    /// Current backoff duration. Diagnostic only.
    fn backoff(&self) -> Duration {
        self.backoff
    }
}

/// Outcome of a subscription-dispatch liveness probe.
///
/// `Alive` collapses three lib-level settlement signals on the probe
/// `sub_id` — `EOSE`, `EVENT`, or `CLOSED` — into one bucket. NDK's
/// `keepalive.ts` makes the same conflation; the only divergence is that
/// `RelayMessage::Notice` is **not** routed (NOTICE has no `sub_id`, so
/// using it as a settlement signal would be ambient noise rather than
/// proof-of-dispatch).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ProbeOutcome {
    /// Relay confirmed subscription-dispatch is working (EOSE / EVENT /
    /// CLOSED for our probe `sub_id`).
    Alive,
    /// No response within `probe_timeout`; treat the relay's app-layer
    /// dispatch as dead and force a reconnect.
    Timeout,
}

/// State of a subscription-dispatch liveness probe currently in flight. At
/// most one of these exists per relay (see [`RelayTask::pending_probe`]).
#[derive(Debug)]
struct PendingProbe {
    /// The probe's `sub_id`, e.g. `"probe-abc123def"`. Routed by the four
    /// settlement entry points (`handle_event`, `RelayMessage::Event`,
    /// `RelayMessage::Closed`, `handle_eose`).
    sub_id: String,
    /// When the probe was issued. Used both for the timeout decision and
    /// for the `probe_alive_latency_seconds` observation on settlement.
    sent_at: Instant,
}

/// Commands sent to a relay-task.
#[derive(Debug)]
pub(crate) enum RelayCommand {
    /// Replace the kind set, re-issuing the forward sub. If `backfill_kinds`
    /// is non-empty, kick off a scoped backfill REQ for those kinds against
    /// this relay. `kind_slugs` is the new full slug map; the relay task
    /// adopts it atomically so subsequent envelopes carry up-to-date slugs.
    UpdateKinds {
        kinds: Vec<u16>,
        backfill_kinds: Vec<u16>,
        kind_slugs: HashMap<u16, String>,
    },
    /// Shutdown gracefully.
    Shutdown,
}

/// Spawn a relay task. The returned tuple contains the command sender and
/// the join handle so the [`Acquirer`] can issue commands and await termination.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn(
    relay_url: NozzlRelayUrl,
    initial_kinds: Vec<u16>,
    initial_kind_slugs: HashMap<u16, String>,
    pool: RelayPool,
    sink: mpsc::Sender<EventEnvelope>,
    ckpt: Arc<dyn CheckpointStore>,
    metrics: Arc<Metrics>,
    safety_margin_seconds: u64,
    default_limit: u32,
    empty_pages_to_complete: u32,
    forward_silence_threshold: Duration,
    probe_timeout: Duration,
    probe_min_interval: Duration,
    notifications: mpsc::Receiver<RelayPoolNotification>,
    initial_backfill_kinds: Vec<u16>,
) -> (mpsc::Sender<RelayCommand>, tokio::task::JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::channel::<RelayCommand>(8);
    let task = RelayTask {
        url: relay_url,
        kinds: initial_kinds,
        kind_slugs: initial_kind_slugs,
        kind_groups: Vec::new(),
        floored_kinds: BTreeSet::new(),
        forward_subs: HashMap::new(),
        active_backfills: HashMap::new(),
        backfill_run_stats: HashMap::new(),
        pool,
        sink,
        ckpt,
        metrics,
        safety_margin_seconds,
        default_limit,
        empty_pages_to_complete,
        forward_silence_threshold,
        probe_timeout,
        probe_min_interval,
        nip11: None,
        pressure: PressureState::new(PRESSURE_BACKOFF_INITIAL, MAX_PRESSURE_BACKOFF),
        forward_reissue_backoff: DecayingBackoff::new(
            FORWARD_REISSUE_BACKOFF_INITIAL,
            FORWARD_REISSUE_BACKOFF_MAX,
            FORWARD_REISSUE_RESET_AFTER,
        ),
        forward_reissue_at: None,
        floor_retry_backoff: floor_retry_backoff(probe_min_interval),
        floor_retry_at: None,
        forward_closed_by_us: HashMap::new(),
        forward_echo_pending: HashMap::new(),
        last_observed_state: RelayConnState::Disconnected,
        disconnected_at: None,
        pending_probe: None,
        last_probe_attempt_at: None,
        last_activity_at: Instant::now(),
    };
    let join = tokio::spawn(task.run(cmd_rx, notifications, initial_backfill_kinds));
    (cmd_tx, join)
}

/// One backfill REQ in flight (one page of paginated history).
///
/// Cursor strategy: `handle_eose` uses an *inclusive* `until = oldest` on
/// every non-empty page whose oldest second moved, full or short, so events
/// that share the boundary timestamp but were not served are recovered on the
/// next request. A short page is no proof that the relay ran out at that
/// second (#32 (Backfill silently drops boundary-tie events when a relay
/// serves fewer events per REQ than `backfill_default_limit`)). The pool's
/// event-id tracker (one pool-wide `MemoryDatabase` of ids, shared by every
/// relay) discards the handful of re-delivered boundary events transparently. Full algorithm and cross-project evidence
/// are documented in
/// [`docs/req-semantics.md` §Backfill cursor strategy](../../docs/req-semantics.md).
#[allow(
    dead_code,
    reason = "sub_id retained for diagnostic logging in panic dumps"
)]
struct BackfillSub {
    sub_id: String,
    kinds: Vec<u16>,
    /// Number of wire-level EVENTs received in this page so far. Tracked from
    /// `RelayMessage::Event` (pre-pool-dedup) so that re-delivered boundary
    /// events are counted and `oldest_created_at` is updated correctly even
    /// when the pool LRU has already seen those event ids.
    received: u32,
    /// Smallest `created_at` observed on the wire in this page; `None` until
    /// the first event arrives.
    oldest_created_at: Option<i64>,
    /// Smallest `created_at` from the *previous* completed page. `None` for
    /// the first page and for re-issued pages (where context is lost).
    ///
    /// Used by `handle_eose` to detect cursor stagnation: when this page's
    /// `oldest_created_at` equals `prev_oldest_created_at`, the inclusive
    /// retry served nothing older than the boundary second it asked for
    /// again. On a page as large as this relay's pages get (`limit`, or the
    /// smaller size it clamps or cuts to) that means at least a page of events
    /// share that second and we cannot advance without skipping; on a smaller
    /// page the second was re-read whole and the walk moves on. Either way the
    /// stagnation guard falls back to `oldest - 1` to break the loop.
    prev_oldest_created_at: Option<i64>,
    /// Number of consecutive past pages that returned zero events. Reaching
    /// `RelayTask::empty_pages_to_complete` means history before the cursor
    /// is exhausted on this relay; backfill is declared complete.
    ///
    /// Named `short_streak` for historical reasons (an earlier heuristic
    /// used "fewer than limit" as the termination signal). The field now
    /// counts only genuinely *empty* pages. Near-full pages from
    /// timestamp-tie quirks (e.g. damus returning 498/500 indefinitely)
    /// must not contribute to this counter; see `handle_eose`.
    short_streak: u32,
    /// The page's effective `limit` (≥ `BACKFILL_MIN_LIMIT`).
    limit: u32,
    /// The inclusive upper-time bound sent to the relay for this page.
    /// `None` on the very first page of a fresh backfill (relay defaults to
    /// "now").
    until: Option<i64>,
    /// When this REQ was opened. Used to detect REQs that have stalled
    /// without an EOSE — see [`BACKFILL_REQ_TIMEOUT`].
    opened_at: Instant,
}

/// Per-backfill-run completeness accounting for one kind-group, accumulated
/// across every page of the walk and emitted as a single `info!` summary line
/// when the group reaches `Complete` (see [`RelayTask::finish_backfill_run`]).
///
/// This is the line that answers "did this kind's backfill walk cleanly cover
/// its history?" without per-page log spam: one busy kind's ~4 %
/// combined-REQ coverage-gap question would have been
/// answerable at a glance from `partial_pages` / `tie_stagnations`. A run is
/// keyed by the group's sorted kind list (stable across the page chain even as
/// each page gets a fresh `sub_id`), so a solo kind-7 backfill produces exactly
/// one summary line.
#[derive(Debug, Clone, Default)]
struct BackfillRunStats {
    /// Total pages walked (one per EOSE'd backfill REQ in this run).
    pages: u32,
    /// Pages where `received == limit` (the relay had a full page or more at
    /// the cursor).
    full_pages: u32,
    /// Pages where `received < limit` (includes empty pages).
    partial_pages: u32,
    /// Pages that stagnated (`oldest_created_at` equalled the previous page's,
    /// forcing an `oldest - 1` step, issue #18) while as large as this relay's
    /// pages get: full, or as large as the run's largest page, the size of a
    /// clamp or cut below `limit`. Counts shed boundary bands. The re-read of a
    /// walk's last second, smaller than that, is not counted.
    tie_stagnations: u32,
    /// The largest page this run has received: what this relay serves per
    /// page when it clamps or cuts below `limit`.
    max_received: u32,
    /// Total wire-level EVENTs received across the run (pre-pool-dedup count,
    /// summed from each page's `received`).
    events: u64,
    /// Newest `created_at` observed across the run (`None` until first event).
    newest_created_at: Option<i64>,
    /// Oldest `created_at` observed across the run (`None` until first event).
    oldest_created_at: Option<i64>,
}

impl BackfillRunStats {
    /// Fold one EOSE'd page into the run accounting. `tie_stagnation` is `true`
    /// when this page's cursor could not advance (oldest == prev oldest) and the
    /// page was as large as this relay's pages get (see `tie_stagnations`).
    fn record_page(
        &mut self,
        received: u32,
        limit: u32,
        page_oldest: Option<i64>,
        tie_stagnation: bool,
    ) {
        self.pages = self.pages.saturating_add(1);
        if received >= limit {
            self.full_pages = self.full_pages.saturating_add(1);
        } else {
            self.partial_pages = self.partial_pages.saturating_add(1);
        }
        if tie_stagnation {
            self.tie_stagnations = self.tie_stagnations.saturating_add(1);
        }
        self.max_received = self.max_received.max(received);
        self.events = self.events.saturating_add(u64::from(received));
        if let Some(o) = page_oldest {
            self.oldest_created_at = Some(self.oldest_created_at.map_or(o, |cur| cur.min(o)));
            self.newest_created_at = Some(self.newest_created_at.map_or(o, |cur| cur.max(o)));
        }
    }
}

/// Stable identity for a backfill run: the group's kinds, sorted. The page
/// chain re-issues a fresh `sub_id` per page, so we key the run on the kind set
/// instead — which is invariant across the whole walk.
fn backfill_run_key(kinds: &[u16]) -> Vec<u16> {
    let mut k = kinds.to_vec();
    k.sort_unstable();
    k
}

/// One forward sub in flight (post-split, possibly more than one).
#[allow(dead_code, reason = "sub_id and kinds retained for diagnostic logging")]
struct ForwardSub {
    sub_id: String,
    kinds: Vec<u16>,
    /// When the most recent matching event arrived for this forward sub.
    /// Initialised to "sub-open time"; bumped on every event delivered with
    /// this `sub_id`. Diagnostic only now: the dispatch-liveness probe keys on
    /// the relay-wide [`RelayTask::last_activity_at`] instead, so backfill
    /// traffic counts as liveness and the probe can't false-fire mid-backfill.
    last_event_at: Instant,
}

struct RelayTask {
    url: NozzlRelayUrl,
    kinds: Vec<u16>,
    /// Current `kind → slug` map. Populated at spawn and replaced atomically
    /// on every `UpdateKinds` command. Used to attach a slug to each
    /// outbound `EventEnvelope`.
    kind_slugs: HashMap<u16, String>,
    /// Current grouping for forward subs (one element per active sub). Starts
    /// as a single group; splitting halves a group on rejection.
    kind_groups: Vec<Vec<u16>>,
    /// Kinds we've stopped subscribing to on this relay because they were
    /// rejected even at the floor of one-per-sub. Retried after
    /// [`Self::floor_retry_backoff`], at once after a reconnect, and cleared
    /// by `UpdateKinds`. Before #48 (Forward-sub CLOSED handling) only
    /// `UpdateKinds` cleared it, so a relay whose only kind was floored stayed
    /// connected and silent until a config change.
    floored_kinds: BTreeSet<u16>,
    forward_subs: HashMap<String, ForwardSub>,
    active_backfills: HashMap<String, BackfillSub>,
    /// Per-run completeness accounting, keyed by [`backfill_run_key`] (the
    /// group's sorted kinds). Seeded when a run starts, folded on each EOSE'd
    /// page, and consumed into the once-per-run summary line at `Complete`.
    backfill_run_stats: HashMap<Vec<u16>, BackfillRunStats>,
    pool: RelayPool,
    sink: mpsc::Sender<EventEnvelope>,
    ckpt: Arc<dyn CheckpointStore>,
    metrics: Arc<Metrics>,
    safety_margin_seconds: u64,
    default_limit: u32,
    /// Consecutive zero-event backfill pages required before declaring a
    /// kind-group's backfill complete. Configurable via
    /// `acquirer.backfill_empty_pages_to_complete`. Larger values tolerate
    /// transient empty bands at the cost of one extra round trip per step.
    empty_pages_to_complete: u32,
    nip11: Option<Nip11Info>,
    /// Concurrency-pressure throttle state for backfill REQs. See
    /// [`PressureState`] for the rationale and EOSE-survival semantics.
    pressure: PressureState,
    /// Back-off for re-opening forward subs the relay closed for a reason
    /// other than a filter rejection. See [`FORWARD_REISSUE_BACKOFF_INITIAL`].
    forward_reissue_backoff: DecayingBackoff,
    /// When the forward subs the relay closed are due to be re-opened, if any.
    /// Checked on the 1 s tick ([`Self::tick_forward_retries`]).
    forward_reissue_at: Option<Instant>,
    /// Back-off for retrying [`Self::floored_kinds`]: `probe_min_interval`,
    /// doubling to [`FLOOR_RETRY_MAX`].
    floor_retry_backoff: DecayingBackoff,
    /// When the floored kinds are due to be retried, if any.
    floor_retry_at: Option<Instant>,
    /// When nozzl itself last closed each forward id on this relay, kept for
    /// [`FORWARD_CLOSE_ECHO_WINDOW`]. See [`Self::close_at_relay`].
    forward_closed_by_us: HashMap<String, Instant>,
    /// Forward ids kept through a suspected echo, with the deadline for the
    /// EOSE or EVENT that confirms it. See [`FORWARD_ECHO_CONFIRM`].
    forward_echo_pending: HashMap<String, Instant>,
    /// How long a forward sub may go without an event before the acquirer
    /// fires a subscription-dispatch liveness probe at the relay (see
    /// [`Self::tick_forward_sub_probe`]). Zero disables the probe entirely
    /// — the kill switch preserves the pre-feature behavior.
    forward_silence_threshold: Duration,
    /// How long to wait for an issued probe to settle (EOSE / EVENT /
    /// CLOSED for the probe `sub_id`) before declaring the relay's
    /// subscription dispatch dead and forcing a reconnect. See
    /// [`Self::tick_forward_sub_probe`].
    probe_timeout: Duration,
    /// Minimum spacing between successive probe **attempts** against this
    /// relay (anchored on issue time, not settlement). Prevents a tight
    /// probe-timeout → reconnect → re-probe loop; caps probe rate at
    /// ~ `3600 / probe_min_interval.as_secs()` per hour per relay.
    probe_min_interval: Duration,
    /// Last observed pool-side connection state, as the typed
    /// [`RelayConnState`]. Sampled on the 1 s silence-check tick alongside the
    /// watchdog. Used to detect `Connected ↔ !Connected` transitions and feed
    /// `relay_disconnects_total` + `relay_reconnect_duration_seconds`.
    /// Initialised to `Disconnected`; the initial bring-up
    /// `Disconnected → Connected` transition is *not* counted as a reconnect —
    /// see [`Self::disconnected_at`].
    last_observed_state: RelayConnState,
    /// Instant of the most recent observed `Connected → !Connected` transition,
    /// or `None` if no disconnect has been observed since startup (or since
    /// the last reconnect). Gates histogram observation so the initial
    /// bring-up gap is not recorded as a reconnect duration.
    disconnected_at: Option<Instant>,
    /// Currently outstanding subscription-dispatch probe, if any. At most
    /// one probe is in flight per relay; an `is_some()` value suppresses
    /// the trigger predicate in [`Self::tick_forward_sub_probe`] and is
    /// cleared on settlement (EOSE / EVENT / CLOSED for the probe `sub_id`,
    /// `probe_timeout` elapse, `Shutdown`, `UpdateKinds`, or
    /// `!Connected → Connected` transition).
    pending_probe: Option<PendingProbe>,
    /// Instant at which the most recent probe was **issued** (independent
    /// of whether it settled or timed out). Anchors the
    /// `probe_min_interval` rate limit so two consecutive timeouts can't
    /// fire back-to-back probes against a still-recovering relay.
    last_probe_attempt_at: Option<Instant>,
    /// Instant of the most recent **subscription-dispatch signal** from this
    /// relay — an event on any of our subs (forward OR backfill), an EOSE, or
    /// a CLOSED for one of our REQs. Bumped via [`Self::mark_dispatch_activity`]
    /// at exactly the four points that also settle a probe as Alive
    /// (`handle_event`, the `RelayMessage::Event`/`RelayMessage::Closed` arms of
    /// `handle_message`, and `handle_eose`). Ambient NOTICE/OK/AUTH traffic
    /// deliberately does **not** bump it — that is the T15 "NOTICE is not
    /// liveness" contract. This is the relay-wide dispatch-liveness signal the
    /// probe trigger reads (see [`Self::tick_forward_sub_probe`]): a long
    /// backfill keeps it fresh even when the forward sub is deprioritized by the
    /// relay, so the probe does not false-fire a reconnect mid-backfill (which
    /// would trigger a `resubscribe()` reflood that overflows the fan-out
    /// channel). Distinct from `ForwardSub::last_event_at`, which is per-sub and
    /// now drives nothing but diagnostics.
    last_activity_at: Instant,
}

impl RelayTask {
    async fn run(
        mut self,
        mut cmd_rx: mpsc::Receiver<RelayCommand>,
        mut notifications: mpsc::Receiver<RelayPoolNotification>,
        initial_backfill_kinds: Vec<u16>,
    ) {
        // Initial bring-up.
        self.set_status_metric(RelayConnState::Disconnected);
        self.set_status_metric(RelayConnState::Connecting);

        // NIP-11 + initial subs. Best-effort; failures fall back to defaults.
        self.nip11 = nip11::fetch(&self.url).await;
        self.compute_initial_groups();
        if let Err(e) = self.open_all_forward_subs().await {
            tracing::warn!(relay = %self.url, error = %e, "failed to open initial forward sub");
        }
        // Either resume an in-progress backfill or kick off a new one.
        self.kick_off_initial_backfill(initial_backfill_kinds).await;

        let mut stuck_check = tokio::time::interval(STUCK_BACKFILL_CHECK_INTERVAL);
        stuck_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate fire so the very first tick lands one interval
        // after startup rather than racing the initial backfill REQ open.
        stuck_check.tick().await;

        // Silence watchdog ticks frequently so detection latency is bounded
        // by ~1 s regardless of the configured threshold. Cost is negligible
        // (one HashMap scan per relay per second).
        let mut silence_check = tokio::time::interval(SILENCE_CHECK_INTERVAL);
        silence_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        silence_check.tick().await;

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => match cmd {
                    RelayCommand::Shutdown => {
                        tracing::info!(relay = %self.url, "relay task: shutdown received");
                        // Close every active sub, on this relay only (see
                        // `close_at_relay`; send errors are logged there).
                        for sub_id in self.forward_subs.keys().cloned().collect::<Vec<_>>() {
                            self.close_at_relay(&sub_id).await;
                        }
                        for sub_id in self.active_backfills.keys().cloned().collect::<Vec<_>>() {
                            self.close_at_relay(&sub_id).await;
                        }
                        // Cancel any in-flight dispatch-liveness probe so its
                        // sub-id is removed from the lib's resub table before
                        // the disconnect (mirrors the timeout branch's load-
                        // bearing ordering — see [`Self::tick_forward_sub_probe`]).
                        if let Some(probe) = self.pending_probe.take() {
                            self.close_at_relay(&probe.sub_id).await;
                            self.metrics
                                .probe_in_flight
                                .with_label_values(&[self.url.as_str()])
                                .set(0);
                        }
                        // The CLOSEs are queued, not written; give the send loop time
                        // to flush them before the hard disconnect.
                        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                        if let Err(e) = self.pool.disconnect_relay(self.url.as_str()).await {
                            tracing::debug!(relay = %self.url, error = %e, "disconnect_relay during shutdown");
                        }
                        self.set_status_metric(RelayConnState::Disconnected);
                        return;
                    }
                    RelayCommand::UpdateKinds { kinds, backfill_kinds, kind_slugs } => {
                        tracing::info!(
                            relay = %self.url,
                            new_kinds = ?kinds,
                            backfill_kinds = ?backfill_kinds,
                            "relay task: kinds updated, re-issuing forward sub",
                        );
                        // Cancel any in-flight dispatch-liveness probe. Without
                        // this, the probe's eventual settlement would tear down
                        // the freshly-reloaded forward subs via
                        // `on_probe_settled(Alive)` → `close_all + open_all`
                        // and undo the kinds update.
                        if let Some(probe) = self.pending_probe.take() {
                            tracing::info!(
                                relay = %self.url,
                                sub_id = %probe.sub_id,
                                "cancelling in-flight probe due to UpdateKinds",
                            );
                            self.close_at_relay(&probe.sub_id).await;
                            self.metrics
                                .probe_in_flight
                                .with_label_values(&[self.url.as_str()])
                                .set(0);
                        }
                        self.kinds = kinds;
                        self.kind_slugs = kind_slugs;
                        self.floored_kinds.clear();
                        self.floor_retry_at = None;
                        self.close_all_forward_subs().await;
                        self.compute_initial_groups();
                        if let Err(e) = self.open_all_forward_subs().await {
                            tracing::warn!(relay = %self.url, error = %e, "failed to re-issue forward sub on reload");
                        }
                        if !backfill_kinds.is_empty() {
                            self.start_backfill_for_kinds(backfill_kinds).await;
                        }
                    }
                },
                Some(notif) = notifications.recv() => {
                    if let Err(e) = self.handle_notification(notif).await {
                        tracing::warn!(relay = %self.url, error = %e, "notification handler error");
                    }
                }
                _ = stuck_check.tick() => {
                    self.check_stuck_backfills().await;
                }
                _ = silence_check.tick() => {
                    self.check_connection_transitions().await;
                    self.tick_forward_sub_probe().await;
                    self.tick_forward_retries().await;
                }
                else => break,
            }
        }
    }

    fn compute_initial_groups(&mut self) {
        let kinds: Vec<u16> = self
            .kinds
            .iter()
            .copied()
            .filter(|k| !self.floored_kinds.contains(k))
            .collect();
        if kinds.is_empty() {
            self.kind_groups.clear();
            return;
        }
        // Start with one group; honour NIP-11 max_filters/max_subscriptions if
        // they're set conservatively. We treat max_subscriptions < 2 as a hint
        // that the relay can only hold one REQ — so we keep the single group
        // and skip kind-list halving until rejected. The brief asks us to
        // "plan for splitting" in this case; if NIP-11 advertises only 1 sub,
        // keeping a single group is exactly the right plan.
        self.kind_groups = vec![kinds];
        self.update_split_metric();
    }

    fn update_split_metric(&self) {
        self.metrics
            .relay_req_split_count
            .with_label_values(&[self.url.as_str()])
            .set(i64::try_from(self.kind_groups.len()).unwrap_or(i64::MAX));
    }

    /// Look up the slug for `kind` in the current `kind_slugs` map. Returns
    /// `""` when no slug is configured (e.g. for an unconfigured kind whose
    /// rejection is being recorded). Empty slug is the documented sentinel
    /// across the metric surface — `nozzl_config::is_valid_slug` guarantees
    /// real slugs are non-empty so the operator can distinguish the two
    /// unambiguously.
    fn slug_for(&self, kind: u16) -> &str {
        self.kind_slugs.get(&kind).map_or("", String::as_str)
    }

    fn set_status_metric(&self, state: RelayConnState) {
        self.metrics.set_relay_connection_state(&self.url, state);
    }

    async fn close_all_forward_subs(&mut self) {
        let ids: Vec<String> = self.forward_subs.keys().cloned().collect();
        for id in ids {
            self.close_at_relay(&id).await;
        }
        self.forward_subs.clear();
    }

    /// CLOSE `sub_id` on this relay only. `RelayPool::unsubscribe` sends the
    /// CLOSE to every relay in the pool, and every relay task holds its forward
    /// sub under the same id, so one relay's re-issue used to tear down every
    /// relay's forward sub: #39 (Forward-sub teardown is pool-wide). The relay's
    /// own `unsubscribe` reaches just this one, removes the id from its table
    /// (so `resubscribe()` does not replay it after a reconnect), and sends the
    /// CLOSE even when the library already dropped the id on the relay's
    /// CLOSED. nozzl's subscriptions are all targeted, which the pool never
    /// saves for inheritance, so nothing is left behind at the pool level.
    ///
    /// A forward id's CLOSE time is kept for [`FORWARD_CLOSE_ECHO_WINDOW`] so
    /// [`Self::handle_closed`] can tell a relay acknowledging it from a relay
    /// closing the sub opened after it.
    async fn close_at_relay(&mut self, sub_id: &str) {
        if sub_id.starts_with(FORWARD_SUB_BASE) {
            let now = Instant::now();
            self.forward_closed_by_us
                .retain(|_, at| now.saturating_duration_since(*at) < FORWARD_CLOSE_ECHO_WINDOW);
            self.forward_closed_by_us.insert(sub_id.to_string(), now);
        }
        match self.pool.relay(self.url.as_str()).await {
            Ok(relay) => {
                if let Err(e) = relay.unsubscribe(&SubscriptionId::new(sub_id)).await {
                    tracing::debug!(
                        relay = %self.url,
                        sub_id,
                        error = %e,
                        "CLOSE to this relay not sent",
                    );
                }
            }
            Err(e) => {
                tracing::debug!(
                    relay = %self.url,
                    sub_id,
                    error = %e,
                    "relay missing from pool; CLOSE not sent",
                );
            }
        }
    }

    /// Run on the 1 s tick: re-open the forward subs the relay closed once
    /// their back-off has elapsed, and retry floored kinds once their retry is
    /// due. Both are scheduled by [`Self::handle_closed`]. Neither runs while
    /// the relay is not connected: a REQ refused for a dead socket would leave
    /// a sub nozzl believes open and the library never registered. They stay
    /// due and run on the first tick after the reconnect.
    async fn tick_forward_retries(&mut self) {
        if self.last_observed_state != RelayConnState::Connected {
            return;
        }
        let now = Instant::now();
        let unconfirmed: Vec<String> = self
            .forward_echo_pending
            .iter()
            .filter(|(_, deadline)| now >= **deadline)
            .map(|(id, _)| id.clone())
            .collect();
        for id in unconfirmed {
            tracing::info!(
                relay = %self.url,
                sub_id = %id,
                "no EOSE or EVENT after a suspected echo; taking the empty CLOSED as the relay's close",
            );
            self.forward_closed_by_relay(&id, "").await;
        }
        if self.forward_reissue_at.is_some_and(|at| now >= at) {
            self.forward_reissue_at = None;
            tracing::info!(relay = %self.url, "re-issuing forward subs after back-off");
            if let Err(e) = self.open_forward_subs(true).await {
                // Try again shortly; the groups that did open are recorded.
                self.forward_reissue_at = Some(now + FORWARD_REISSUE_BACKOFF_INITIAL);
                tracing::warn!(relay = %self.url, error = %e, "failed to re-issue forward subs after back-off; retrying");
            }
        }
        if self.floor_retry_at.is_some_and(|at| now >= at) {
            self.floor_retry_at = None;
            if !self.floored_kinds.is_empty() {
                let kinds: Vec<u16> = self.floored_kinds.iter().copied().collect();
                tracing::warn!(
                    relay = %self.url,
                    kinds = ?kinds,
                    "retrying kinds the relay rejected at the single-kind floor",
                );
                // Each floored kind comes back as a group of its own, beside the
                // groups that work: merged into them, a second rejection would
                // re-run the whole split cascade, replaying every group each round.
                self.floored_kinds.clear();
                // Sub ids follow the group index, and a lone group is `forward`
                // rather than `forward-0`: only going from one group to more
                // renames an open sub. Otherwise open just the new groups.
                let renames = self.kind_groups.len() == 1;
                if renames {
                    self.close_all_forward_subs().await;
                }
                for kind in kinds {
                    self.kind_groups.push(vec![kind]);
                }
                self.update_split_metric();
                if let Err(e) = self.open_forward_subs(!renames).await {
                    // The back-off re-issue opens whatever group is still missing.
                    self.forward_reissue_at = Some(now + FORWARD_REISSUE_BACKOFF_INITIAL);
                    tracing::warn!(relay = %self.url, error = %e, "failed to open forward subs for the floor retry; retrying");
                }
            }
        }
    }

    async fn open_all_forward_subs(&mut self) -> Result<(), String> {
        self.open_forward_subs(false).await
    }

    /// Open one forward sub per kind group. With `only_missing`, skip the
    /// groups whose sub is still open, so re-opening the one a relay closed
    /// does not make the others replay too. A full open supersedes any
    /// pending back-off re-issue.
    async fn open_forward_subs(&mut self, only_missing: bool) -> Result<(), String> {
        if !only_missing {
            self.forward_reissue_at = None;
        }
        // Compute since across the union of kinds in this REQ. Per design, we
        // use the *minimum* checkpoint across the group's kinds so we don't
        // miss anything for any one kind (we'd rather refetch some than skip).
        for (idx, group) in self.kind_groups.iter().enumerate() {
            let sub_id = if idx == 0 && self.kind_groups.len() == 1 {
                FORWARD_SUB_BASE.to_string()
            } else {
                format!("{FORWARD_SUB_BASE}-{idx}")
            };
            if only_missing && self.forward_subs.contains_key(&sub_id) {
                continue;
            }

            // Look up min-since across this group's kinds.
            let mut min_since: Option<i64> = None;
            let now = now_secs();
            for k in group {
                let (since, backfill) = self.ckpt.since_and_backfill_state(&self.url, *k).await;
                match since {
                    Some(s) => {
                        min_since = Some(min_since.map_or(s, |cur| cur.min(s)));
                    }
                    None => {
                        if backfill == BackfillState::Complete {
                            // Backfill finished with zero events: relay has nothing for
                            // this kind. Use now as the lower bound so we only catch live
                            // events going forward; the first real event advances the
                            // checkpoint via ckpt.observe().
                            min_since = Some(min_since.map_or(now, |cur| cur.min(now)));
                        } else {
                            // Pending or InProgress: genuinely uncharted, must scan from
                            // the beginning.
                            min_since = None;
                            break;
                        }
                    }
                }
            }
            let since = since_with_safety_margin(min_since, self.safety_margin_seconds);
            let filter = forward_filter(group, since);

            tracing::info!(
                relay = %self.url,
                sub_id = %sub_id,
                kinds = ?group,
                since = ?since,
                "REQ open: forward subscription",
            );

            // Subscribe via the pool, scoped to this single relay.
            let res = self
                .pool
                .subscribe_with_id_to(
                    [self.url.as_str().to_string()],
                    SubscriptionId::new(&sub_id),
                    filter,
                    SubscribeOptions::default(),
                )
                .await;
            match res {
                Err(e) => return Err(e.to_string()),
                // A targeted REQ names one relay, ours. The pool files a send it
                // could not make under `failed` and still returns Ok; for a
                // back-off re-issue that must not record a sub never sent.
                Ok(output) if only_missing && !output.failed.is_empty() => {
                    return Err(format!("REQ not sent: {:?}", output.failed));
                }
                Ok(_) => {}
            }
            self.forward_subs.insert(
                sub_id.clone(),
                ForwardSub {
                    sub_id,
                    kinds: group.clone(),
                    // Diagnostic timestamp, initialised to "just opened". The
                    // probe's grace period is governed by the relay-wide
                    // `last_activity_at`, not this per-sub field.
                    last_event_at: Instant::now(),
                },
            );
        }
        Ok(())
    }

    async fn kick_off_initial_backfill(&mut self, scoped_kinds: Vec<u16>) {
        // If the caller asked for a scoped backfill (relay-add or kind-add),
        // honour that. Otherwise, walk every kind and start a backfill if its
        // checkpoint state is Pending or InProgress.
        let kinds_to_backfill: Vec<u16> = if scoped_kinds.is_empty() {
            let mut out = Vec::new();
            for k in &self.kinds {
                let st = self.ckpt.backfill_state(&self.url, *k).await;
                if !matches!(st, BackfillState::Complete) {
                    out.push(*k);
                }
            }
            out
        } else {
            scoped_kinds
        };
        if !kinds_to_backfill.is_empty() {
            self.start_backfill_for_kinds(kinds_to_backfill).await;
        }
    }

    async fn start_backfill_for_kinds(&mut self, kinds: Vec<u16>) {
        // For resumability, find the smallest `until_cursor` across these
        // kinds (so the next REQ will fetch the oldest still-needed page
        // across all of them). If any of them is Pending (no cursor), start
        // from now.
        let mut resume_until: Option<i64> = None;
        let mut all_pending = true;
        for k in &kinds {
            match self.ckpt.backfill_state(&self.url, *k).await {
                BackfillState::InProgress { until_cursor } => {
                    all_pending = false;
                    resume_until = Some(resume_until.map_or(until_cursor, |c| c.min(until_cursor)));
                }
                BackfillState::Pending | BackfillState::Complete => {}
            }
        }
        // Seed `until = now` on a fresh onboarding (all kinds Pending, no
        // persisted cursor). Without it the first page is
        // `{kinds, since:0, limit}` with NO `until` — which scores below
        // zapstore's `VagueFilters(3)` specificity floor (kinds+since = 2 < 3)
        // and is CLOSED "filters are too vague". A kind onboarded ALONE then
        // hits the single-kind floor in `handle_closed` and is abandoned with
        // no retry (issue #25); a multi-kind group only self-heals by accident
        // (the reject→split→re-invoke re-reads the cursor this call persists
        // below, so the retry carries `until=Some`). Seeding here fixes the
        // singleton case and eliminates the wasteful reject/split round-trips
        // for multi-kind onboarding on strict relays. It is semantically
        // identical to a relay's own "no `until` ⇒ default to now": the page
        // fetches `created_at ≤ now`, and the forward sub covers `> now`, so no
        // history is skipped. A resume already carries a persisted cursor, so
        // `until` is `Some` in every case.
        //
        // Location/timezone independence: `now_secs()` is UTC Unix epoch seconds
        // (NIP-01 `created_at` semantics) — no local-time conversion — so the
        // relative geography/timezone of nozzl and the relay is irrelevant. What
        // *could* differ is wall-clock SKEW between the two hosts, but it is
        // harmless here: backfill only walks BACKWARD from `until`, while the
        // `(until, live-edge)` window is owned by the forward sub — which on a
        // fresh onboarding runs `since=None` (a full scan that also captures
        // events dated after our `now`, i.e. skewed/future-dated ones), and once
        // a checkpoint exists carries the `safety_margin` to absorb edge skew.
        // So no event is skipped whichever host's clock is ahead. It also adds no
        // NEW clock dependency: the persisted `until_cursor` below was already
        // `now_secs()` pre-fix, so nozzl's clock already governed page 2 onward
        // and restart-resume; seeding page 1 merely makes them consistent.
        let now = now_secs();
        let until = if all_pending { Some(now) } else { resume_until };
        debug_assert!(
            until.is_some(),
            "until must be Some: fresh onboarding seeds now, resume carries a cursor",
        );

        // Seed (or reset) this run's completeness accounting. `start` is the
        // entry point for a fresh walk — a restart-resume re-walks the
        // remaining history under one new summary, which is the right scope:
        // the line reports what *this* process actually re-fetched.
        self.backfill_run_stats
            .insert(backfill_run_key(&kinds), BackfillRunStats::default());

        // No resume-time `until <= 0` guard: no code path persists a non-positive
        // cursor, and a stray one (corrupt checkpoint) self-heals via
        // `backfill_filter`'s clamp-to-1 and the `next_backfill_advance` floor terminal.
        let limit = self.effective_backfill_limit();
        let sub_id = format!("{BACKFILL_SUB_BASE}-{}", new_nonce());
        // `until` is always `Some` here (see above); persist it as the resume
        // cursor. `unwrap_or(now)` is dead-but-defensive — it keeps the persist
        // total without a panic if the invariant is ever violated.
        let until_cursor = until.unwrap_or(now);

        for k in &kinds {
            self.metrics
                .backfill_in_progress
                .with_label_values(&[self.url.as_str(), &k.to_string(), self.slug_for(*k)])
                .set(1);
            // Persist Pending → InProgress transition so a crash-restart can
            // resume from this cursor.
            self.ckpt
                .record_backfill(&self.url, *k, BackfillState::InProgress { until_cursor })
                .await;
        }

        let filter = backfill_filter(&kinds, until, limit);
        // Distinct "started" marker so a kind-group's lifetime can be measured
        // off the log alone: pair this with the matching `backfill complete`
        // line emitted by `handle_eose` for the same `kinds=` list. Subsequent
        // pages within the same group go through `REQ open: backfill next page`
        // (see `handle_eose`) and `REQ open: backfill page (re-issued)` (see
        // `reissue_backfill_page`); neither is a kind-group start, so neither
        // emits this marker. `until` is always `Some` now: `Some(now)` on a
        // fresh onboarding (all kinds Pending), or `Some(cursor)` from a
        // persisted checkpoint — a real restart resume, or the half of a
        // freshly-CLOSED filter-shape split inheriting its parent's cursor.
        tracing::info!(
            relay = %self.url,
            kinds = ?kinds,
            until = ?until,
            limit,
            "backfill started",
        );
        tracing::info!(
            relay = %self.url,
            sub_id = %sub_id,
            kinds = ?kinds,
            until = ?until,
            limit,
            "REQ open: onboarding backfill page",
        );
        let res = self
            .pool
            .subscribe_with_id_to(
                [self.url.as_str().to_string()],
                SubscriptionId::new(&sub_id),
                filter,
                SubscribeOptions::default(),
            )
            .await;
        if let Err(e) = res {
            tracing::warn!(relay = %self.url, error = %e, "failed to open backfill REQ");
            for k in &kinds {
                self.metrics
                    .backfill_in_progress
                    .with_label_values(&[self.url.as_str(), &k.to_string(), self.slug_for(*k)])
                    .set(0);
            }
            return;
        }
        self.active_backfills.insert(
            sub_id.clone(),
            BackfillSub {
                sub_id,
                kinds,
                received: 0,
                oldest_created_at: None,
                prev_oldest_created_at: None,
                short_streak: 0,
                limit,
                until,
                opened_at: Instant::now(),
            },
        );
    }

    fn effective_backfill_limit(&self) -> u32 {
        // The configured default for every relay. NIP-11 does advertise a
        // page clamp, `limitation.max_limit` (since 2023-04-08), but nozzl
        // never reads it: `nip11.rs` looks for the wrong key and fetches only
        // over ws://. #49 (NIP-11 (relay information) is never read) is the
        // fix; #32 (Backfill silently drops boundary-tie events) is what a
        // clamp below this default costs.
        self.default_limit.max(BACKFILL_MIN_LIMIT)
    }

    /// Bump the concurrent-pressure back-off window. Doubles up to
    /// [`MAX_PRESSURE_BACKOFF`]; the next backfill REQ open is deferred
    /// until [`PressureState::wait_required`] returns `None`.
    fn note_concurrent_pressure(&mut self, reason: &str) {
        self.pressure.note();
        tracing::warn!(
            relay = %self.url,
            reason,
            backoff_secs = self.pressure.backoff().as_secs(),
            "concurrency pressure detected; backing off before next backfill REQ",
        );
    }

    /// Sleep if a pressure backoff is currently active. Used on every path
    /// that opens a backfill REQ — both the EOSE-driven next-page open and
    /// `reissue_backfill_page`.
    async fn await_pressure_backoff(&self) {
        if let Some(wait) = self.pressure.wait_required(Instant::now()) {
            tracing::info!(
                relay = %self.url,
                wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                "deferring next backfill REQ for pressure backoff",
            );
            tokio::time::sleep(wait).await;
        }
    }

    /// Open a fresh backfill REQ at the given `until` cursor for `kinds`,
    /// preserving the existing `short_streak`. Used both by the
    /// stuck-detection path (a REQ that didn't EOSE in
    /// [`BACKFILL_REQ_TIMEOUT`]) and by the concurrent-pressure CLOSED path,
    /// where we don't want to lose the page's progress.
    async fn reissue_backfill_page(
        &mut self,
        kinds: Vec<u16>,
        until: Option<i64>,
        short_streak: u32,
        limit: u32,
    ) {
        // Honour an active back-off — sleep until the relay has had a moment
        // to drain in-flight subs.
        self.await_pressure_backoff().await;
        let new_id = format!("{BACKFILL_SUB_BASE}-{}", new_nonce());
        let filter = backfill_filter(&kinds, until, limit);
        tracing::info!(
            relay = %self.url,
            sub_id = %new_id,
            kinds = ?kinds,
            until = ?until,
            limit,
            short_streak,
            "REQ open: backfill page (re-issued)",
        );
        let res = self
            .pool
            .subscribe_with_id_to(
                [self.url.as_str().to_string()],
                SubscriptionId::new(&new_id),
                filter,
                SubscribeOptions::default(),
            )
            .await;
        if let Err(e) = res {
            tracing::warn!(
                relay = %self.url,
                error = %e,
                kinds = ?kinds,
                "failed to re-issue backfill page",
            );
            for k in &kinds {
                self.metrics
                    .backfill_in_progress
                    .with_label_values(&[self.url.as_str(), &k.to_string(), self.slug_for(*k)])
                    .set(0);
            }
            return;
        }
        // Refresh the per-kind in-progress gauges — a concurrent-pressure
        // CLOSED on a previous attempt may have flipped them off.
        for k in &kinds {
            self.metrics
                .backfill_in_progress
                .with_label_values(&[self.url.as_str(), &k.to_string(), self.slug_for(*k)])
                .set(1);
        }
        self.active_backfills.insert(
            new_id.clone(),
            BackfillSub {
                sub_id: new_id,
                kinds,
                received: 0,
                oldest_created_at: None,
                prev_oldest_created_at: None,
                short_streak,
                limit,
                until,
                opened_at: Instant::now(),
            },
        );
    }

    /// Poll the pool for the current connection status, update the gauge,
    /// and observe disconnect / reconnect transitions.
    ///
    /// Runs once per silence-check tick (1 s — see [`SILENCE_CHECK_INTERVAL`]).
    /// This is the fast path for `relay_connection_state`; the central
    /// acquirer also sweeps on a 30 s heartbeat but that's too coarse to
    /// catch a typical hourly reconnect cycle.
    ///
    /// Transition handling:
    ///  - `Connected (1) → !Connected (0|2)` increments
    ///    `relay_disconnects_total` and records the `Instant` of observation
    ///    in [`Self::disconnected_at`].
    ///  - `!Connected → Connected (1)` observes
    ///    `relay_reconnect_duration_seconds` with the wall-clock delta from
    ///    the stored disconnect instant, then clears it. If
    ///    [`Self::disconnected_at`] is `None` (initial bring-up), the
    ///    histogram is **not** observed — we don't want startup latency
    ///    polluting the reconnect-gap distribution.
    ///
    /// `pool.relay(url).await` returning `Err` (e.g. the relay was just
    /// removed) is logged at debug and treated as no-op.
    async fn check_connection_transitions(&mut self) {
        let curr = match self.pool.relay(self.url.as_str()).await {
            Ok(r) => conn_state(r.status()),
            Err(e) => {
                tracing::debug!(
                    relay = %self.url,
                    error = %e,
                    "pool.relay() lookup failed; skipping transition check",
                );
                return;
            }
        };
        self.set_status_metric(curr);

        let prev = self.last_observed_state;
        if prev == curr {
            return;
        }

        if prev == RelayConnState::Connected && curr != RelayConnState::Connected {
            // Connected → !Connected. This is the ONLY observed-transition
            // increment of `relay_disconnects_total`. The probe-timeout path
            // (search `relay_disconnects` in `tick_forward_sub_probe`)
            // increments the counter manually *instead* — it pre-sets
            // `last_observed_state = Disconnected` so this arm is suppressed on
            // the tick that follows a forced disconnect, counting each
            // disconnect exactly once. Keep the two in sync: if you change that
            // ordering, this arm will either double-count or miss the
            // probe-driven disconnects.
            self.metrics
                .relay_disconnects
                .with_label_values(&[self.url.as_str()])
                .inc();
            self.disconnected_at = Some(Instant::now());
            tracing::info!(
                relay = %self.url,
                prev_code = prev.code(),
                curr_code = curr.code(),
                "observed disconnect (Connected → !Connected); tracking reconnect duration",
            );
        } else if prev != RelayConnState::Connected && curr == RelayConnState::Connected {
            // !Connected → Connected
            let reconnected = self.disconnected_at.is_some();
            if let Some(disc_at) = self.disconnected_at.take() {
                #[allow(clippy::cast_precision_loss)]
                let seconds = disc_at.elapsed().as_secs_f64();
                self.metrics
                    .relay_reconnect_duration_seconds
                    .with_label_values(&[self.url.as_str()])
                    .observe(seconds);
                tracing::info!(
                    relay = %self.url,
                    reconnect_seconds = seconds,
                    "observed reconnect (!Connected → Connected)",
                );
            } else {
                // Initial bring-up — not a reconnect, not recorded.
                tracing::debug!(
                    relay = %self.url,
                    prev_code = prev.code(),
                    curr_code = curr.code(),
                    "observed initial connect; not recording histogram",
                );
            }
            // If a probe was in flight at the moment of a relay-initiated
            // disconnect, the reconnect has restarted the relay's view of
            // our subs (the lib's `resubscribe()` replays them). Collapse
            // "reconnect-while-probing" into `Timeout` semantics: the
            // probe's settlement (or lack of it) is no longer meaningful,
            // and the relay may have lost the forward-sub state we were
            // trying to verify, so defensively re-issue.
            if let Some(probe) = self.pending_probe.take() {
                tracing::warn!(
                    relay = %self.url,
                    sub_id = %probe.sub_id,
                    "in-flight probe interrupted by !Connected → Connected; treating as timeout",
                );
                self.close_at_relay(&probe.sub_id).await;
                self.metrics
                    .probe_total
                    .with_label_values(&[self.url.as_str(), "timeout"])
                    .inc();
                self.metrics
                    .probe_in_flight
                    .with_label_values(&[self.url.as_str()])
                    .set(0);
                // The relay restarted: defensively re-issue forward subs in
                // case the lib's resubscribe lost or re-mapped state.
                self.close_all_forward_subs().await;
                if let Err(e) = self.open_all_forward_subs().await {
                    tracing::warn!(
                        relay = %self.url,
                        error = %e,
                        "failed to re-issue forward subs after reconnect-while-probing",
                    );
                }
            }
            // A reconnect retries floored kinds at once: the rejection may have
            // belonged to the old connection (an upstream's CLOSED passed
            // through, a transient relay state). Not the initial bring-up: a
            // kind floored before it waits for its back-off like any other.
            if reconnected && !self.floored_kinds.is_empty() {
                self.floor_retry_at = Some(Instant::now());
            }
        }

        self.last_observed_state = curr;
    }

    /// Tick the subscription-dispatch liveness probe state machine. Runs
    /// once per `SILENCE_CHECK_INTERVAL` (1 s). Three branches:
    ///
    /// 1. If a probe is in flight and `probe_timeout` has elapsed since its
    ///    `sent_at`, treat the probe as `Timeout`: unsubscribe its `sub_id`
    ///    (load-bearing — see `nostr-relay-pool::Inner::resubscribe`),
    ///    increment counters, disconnect the relay AND explicitly re-arm it
    ///    through [`teardown_barrier_and_rearm`] — sequenced against the old
    ///    connection task's teardown, NOT a bare `connect_relay` (which
    ///    races the teardown and can silently no-op, wedging the relay
    ///    permanently `!Connected`; issue #8). Auto-reconnect does NOT cover
    ///    explicit `disconnect_relay` — `RelayStatus::Terminated` is a
    ///    terminal state in the lib's connect loop. Then refresh
    ///    `last_observed_state` so the next tick's trigger predicate
    ///    doesn't issue a second probe immediately, and return.
    /// 2. Otherwise, evaluate the trigger predicate. If all of (kill switch
    ///    off, no probe pending, at least one forward sub exists, relay-wide
    ///    dispatch silent past `forward_silence_threshold` — i.e. no event on
    ///    any of our subs / EOSE / CLOSED (so backfill traffic counts as
    ///    liveness, but ambient NOTICE/OK/AUTH does not) —
    ///    rate-limit OK, currently connected) hold, issue a probe.
    /// 3. Otherwise no-op.
    ///
    /// Settlement (alive) happens in the four notification entry points
    /// (`handle_event`, `RelayMessage::Event`, `RelayMessage::Closed`,
    /// `handle_eose`) via [`Self::on_probe_settled`] — *not* here.
    async fn tick_forward_sub_probe(&mut self) {
        if self.forward_silence_threshold.is_zero() {
            return; // kill switch — probe disabled entirely
        }
        let now = Instant::now();

        // ---- 1) Timeout check first ----
        if let Some(probe) = &self.pending_probe {
            if now.saturating_duration_since(probe.sent_at) >= self.probe_timeout {
                let sub_id = probe.sub_id.clone();
                let age_secs = now.saturating_duration_since(probe.sent_at).as_secs();
                tracing::warn!(
                    relay = %self.url,
                    sub_id = %sub_id,
                    age_secs,
                    "probe timed out; reconnecting relay",
                );
                // ORDER MATTERS: unsubscribe BEFORE disconnect so the
                // sub-id is removed from `atomic.subscriptions` and the
                // lib's `resubscribe()` (driven from `post_connection` on
                // the explicit `connect_relay` re-arm below) does not
                // replay a phantom probe REQ.
                self.close_at_relay(&sub_id).await;
                // Record outcome + clear `pending_probe` + drop the
                // in-flight gauge. Doing this through `on_probe_settled`
                // keeps the counter/gauge update in one place.
                self.on_probe_settled(ProbeOutcome::Timeout).await;
                // Inline the disconnect accounting that
                // `check_connection_transitions` would otherwise do on the
                // `Connected → !Connected` edge. We have to do it ourselves
                // because the status-cache refresh below sets
                // `last_observed_state = Disconnected` *before* the next tick
                // observes the real status — the transition tracker will see
                // `prev == Disconnected` already and its disconnect arm will
                // never fire, silently suppressing `relay_disconnects_total`.
                // Operators correlate
                // `probe_total{outcome="timeout"}` with `relay_disconnects_total`
                // in dashboards, so we increment it here. Storing
                // `disconnected_at = Some(now)` also lets the upcoming
                // reconnect feed `relay_reconnect_duration_seconds` correctly.
                self.metrics
                    .relay_disconnects
                    .with_label_values(&[self.url.as_str()])
                    .inc();
                self.disconnected_at = Some(now);
                if let Err(e) = self.pool.disconnect_relay(self.url.as_str()).await {
                    tracing::debug!(
                        relay = %self.url,
                        error = %e,
                        "disconnect_relay after probe timeout",
                    );
                }
                // `disconnect_relay` puts the relay in
                // `RelayStatus::Terminated`, which the lib's connect-loop
                // treats as terminal — auto-reconnect does NOT kick in (the
                // loop breaks when status is Terminated; friction-log entry
                // #5). The `RelayOptions::reconnect = true` setting only
                // governs *network-level* failures that bubble up through
                // `connect_and_run`; an explicit `disconnect_relay` is a
                // permanent kill (unless the lib loses its termination,
                // friction-log entry #11: then the old task reconnects
                // itself, and the barrier below times out to
                // `timeout_task_live`). We must re-arm the connection task
                // ourselves — and the re-arm must NOT race the old task's
                // teardown, which is exactly how issue #8's permanent
                // `!Connected` wedge happened (4 live occurrences).
                // `teardown_barrier_and_rearm` sequences the pair: await the
                // old task's exit via the fork-exposed
                // `is_connection_task_running()`, re-drive a non-connectable
                // status through `Terminated`, then connect. The lib's
                // `post_connection -> resubscribe()` then replays forward
                // subs and active backfills on the new WebSocket.
                //
                // Regression tests: T21
                // (`probe_timeout_followed_by_relay_reconnect_and_resubscribe`)
                // — observed live on 2026-05-23 when nos.lol stayed
                // Terminated for ~45 minutes after a probe timeout — and
                // `barrier_prevents_connect_relay_race_wedge` (the issue-#8
                // stress guard; sibling of the bug-present canary
                // `repro_connect_relay_race_wedge`).
                match self.pool.relay(self.url.as_str()).await {
                    Ok(relay) => {
                        let outcome = teardown_barrier_and_rearm(&relay).await;
                        self.metrics
                            .teardown_barrier_total
                            .with_label_values(&[self.url.as_str(), outcome.as_label()])
                            .inc();
                    }
                    Err(e) => {
                        // Should be unreachable — this relay task exists
                        // because the relay is in the pool. Fall back to the
                        // pre-barrier blind re-arm rather than doing nothing,
                        // and count it so `sum(teardown_barrier_total)` still
                        // equals `probe_total{outcome="timeout"}`.
                        tracing::error!(
                            relay = %self.url,
                            error = %e,
                            "relay missing from pool at probe-timeout re-arm; \
                             blind connect_relay fallback (no teardown barrier)",
                        );
                        self.metrics
                            .teardown_barrier_total
                            .with_label_values(&[self.url.as_str(), "fallback_blind"])
                            .inc();
                        if let Err(e) = self.pool.connect_relay(self.url.as_str()).await {
                            tracing::warn!(
                                relay = %self.url,
                                error = %e,
                                "connect_relay after probe timeout failed — relay \
                                 will not auto-recover until next process restart",
                            );
                        }
                    }
                }
                // Refresh the cached status IMMEDIATELY so the next
                // 1 s tick's trigger predicate sees `!Connected` and does
                // not fire another probe against the reconnecting relay
                // before `check_connection_transitions` observes the
                // state change.
                self.set_status_metric(RelayConnState::Disconnected);
                self.last_observed_state = RelayConnState::Disconnected;
                return; // don't evaluate the trigger predicate this tick
            }
        }

        // ---- 2) Trigger predicate ----
        // a) Kill switch already checked at the top.
        // b) No probe already pending.
        if self.pending_probe.is_some() {
            return;
        }
        // c) We only probe to keep a forward sub alive — nothing to do if
        //    there is none.
        if self.forward_subs.is_empty() {
            return;
        }
        // d) Relay-wide dispatch silence past threshold. We key on
        //    `last_activity_at` (any subscription-dispatch signal — an event on
        //    any of our subs, an EOSE, or a CLOSED; NOT ambient NOTICE/OK/AUTH)
        //    rather than per-forward-sub silence. During a long backfill the
        //    relay floods us with backfill
        //    traffic, so the connection is demonstrably alive even when the
        //    relay has deprioritized the forward sub. Probing there — and, on
        //    timeout, reconnecting — is a false positive: the reconnect's
        //    `resubscribe()` reflood overflows the fan-out channel, and because
        //    the backfill keeps the forward sub starved the probe re-fires,
        //    yielding a self-sustaining reconnect/reflood storm. Backfill
        //    traffic keeping `last_activity_at` fresh suppresses that. Once the
        //    relay is genuinely silent (backfill finished AND forward dead) the
        //    threshold elapses and the probe fires exactly as before — Bug-2
        //    detection intact, just deferred until there is no other evidence
        //    of life.
        if now.saturating_duration_since(self.last_activity_at) < self.forward_silence_threshold {
            return;
        }
        // e) Rate limit: anchor on issue time, not settlement.
        if let Some(last_at) = self.last_probe_attempt_at {
            if now.saturating_duration_since(last_at) < self.probe_min_interval {
                return;
            }
        }
        // f) Currently connected — skip while the pool is reconnecting.
        if self.last_observed_state != RelayConnState::Connected {
            return;
        }

        // ---- 3) Issue the probe ----
        self.issue_probe(now).await;
    }

    /// Issue a one-shot subscription-dispatch probe REQ. Updates
    /// `last_probe_attempt_at` unconditionally (the rate limit applies to
    /// attempts, not successes), and on success records the new pending
    /// probe and bumps `probe_in_flight = 1`.
    async fn issue_probe(&mut self, now: Instant) {
        let sub_id = format!("{PROBE_SUB_BASE}-{}", new_nonce());
        let filter = probe_filter();
        // Always record the attempt — even on send error — so the
        // rate-limit floor applies to attempts, not successes.
        self.last_probe_attempt_at = Some(now);
        tracing::debug!(
            relay = %self.url,
            sub_id = %sub_id,
            timeout_secs = self.probe_timeout.as_secs(),
            "REQ open: dispatch-liveness probe",
        );
        let res = self
            .pool
            .subscribe_with_id_to(
                [self.url.as_str().to_string()],
                SubscriptionId::new(&sub_id),
                filter,
                SubscribeOptions::default(),
            )
            .await;
        if let Err(e) = res {
            tracing::warn!(
                relay = %self.url,
                error = %e,
                "failed to issue dispatch-liveness probe",
            );
            // Leave `pending_probe = None` — there's nothing to settle.
            return;
        }
        self.pending_probe = Some(PendingProbe {
            sub_id,
            sent_at: now,
        });
        self.metrics
            .probe_in_flight
            .with_label_values(&[self.url.as_str()])
            .set(1);
    }

    /// Settle the currently-pending probe. Called from the four
    /// notification entry points when an event / EOSE / CLOSED matches the
    /// probe `sub_id`, and (with `Timeout`) from `tick_forward_sub_probe`.
    ///
    /// `Alive`: observe latency, increment `probe_total{outcome="alive"}`,
    /// unsubscribe the probe sub politely, clear `pending_probe`, and
    /// re-issue every forward sub (the actual recovery — equivalent to
    /// what the deleted asymmetric watchdog did when its gate fired).
    ///
    /// `Timeout`: counters + clear only. The timeout branch in
    /// `tick_forward_sub_probe` owns the disconnect + status refresh
    /// directly, so this variant exists for symmetry and for any future
    /// caller (none today — the reconnect-while-probing path inlines the
    /// equivalent steps because it also wants to force a forward-sub
    /// re-issue).
    async fn on_probe_settled(&mut self, outcome: ProbeOutcome) {
        let Some(probe) = self.pending_probe.take() else {
            // No pending probe — settlement signal was already consumed
            // (e.g. the alive path on `RelayMessage::Event` raced ahead of
            // a duplicate delivery via `handle_event`). Safe no-op.
            return;
        };
        match outcome {
            ProbeOutcome::Alive => {
                let elapsed = Instant::now().saturating_duration_since(probe.sent_at);
                #[allow(clippy::cast_precision_loss)]
                let elapsed_secs = elapsed.as_secs_f64();
                self.metrics
                    .probe_alive_latency_seconds
                    .with_label_values(&[self.url.as_str()])
                    .observe(elapsed_secs);
                self.metrics
                    .probe_total
                    .with_label_values(&[self.url.as_str(), "alive"])
                    .inc();
                self.metrics
                    .probe_in_flight
                    .with_label_values(&[self.url.as_str()])
                    .set(0);
                tracing::info!(
                    relay = %self.url,
                    sub_id = %probe.sub_id,
                    latency_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                    "probe settled (alive); re-issuing forward subs",
                );
                // Polite cleanup of the probe sub at the relay.
                self.close_at_relay(&probe.sub_id).await;
                // THE actual Bug-2 recovery: re-issue every forward sub
                // (close-then-open computes a fresh `since` from the
                // checkpoint minus the safety margin).
                self.close_all_forward_subs().await;
                if let Err(e) = self.open_all_forward_subs().await {
                    tracing::warn!(
                        relay = %self.url,
                        error = %e,
                        "failed to re-issue forward subs after alive probe",
                    );
                }
            }
            ProbeOutcome::Timeout => {
                self.metrics
                    .probe_total
                    .with_label_values(&[self.url.as_str(), "timeout"])
                    .inc();
                self.metrics
                    .probe_in_flight
                    .with_label_values(&[self.url.as_str()])
                    .set(0);
            }
        }
    }

    /// Find any backfill REQs that have been open longer than
    /// [`BACKFILL_REQ_TIMEOUT`] without an EOSE and re-issue them. Real-world
    /// relays sometimes silently drop subscriptions — typically after a
    /// concurrency-pressure NOTICE — leaving the backfill loop hung
    /// indefinitely. The timeout-and-retry keeps the cursor moving.
    async fn check_stuck_backfills(&mut self) {
        let now = Instant::now();
        let stuck: Vec<String> = self
            .active_backfills
            .iter()
            .filter(|(_, sub)| now.duration_since(sub.opened_at) >= BACKFILL_REQ_TIMEOUT)
            .map(|(sid, _)| sid.clone())
            .collect();
        for sid in stuck {
            let Some(sub) = self.active_backfills.remove(&sid) else {
                continue;
            };
            tracing::warn!(
                relay = %self.url,
                sub_id = %sid,
                kinds = ?sub.kinds,
                until = ?sub.until,
                received = sub.received,
                age_secs = now.duration_since(sub.opened_at).as_secs(),
                "backfill REQ stuck without EOSE; abandoning and re-issuing",
            );
            // Cancel the lingering sub so the relay frees its slot. Errors
            // are logged inside the pool — a missing sub is harmless.
            self.close_at_relay(&sid).await;
            // If we received some events, advance the cursor past the
            // oldest seen so we don't re-fetch them; otherwise retry the
            // same `until` cursor. This is a deliberately simpler advance than
            // the EOSE path's `next_backfill_advance`: the re-issued page
            // starts with no `prev_oldest`, so an inclusive retry would have
            // no stagnation guard and could re-read the same second for as
            // long as the relay keeps timing out there. The exclusive
            // `oldest - 1` retry can skip the rest of that second, a loss the
            // EOSE path no longer takes; it is the accepted cost on this rare
            // path.
            let next_until = match sub.oldest_created_at {
                Some(o) => Some(o.saturating_sub(1)),
                None => sub.until,
            };
            // Epoch-floor guard — the SAME terminal the EOSE path reaches via
            // `BackfillAdvance::Floor` (see `next_backfill_advance`), applied to
            // this independent cursor computation so it cannot bypass it. If advancing past the
            // oldest seen drops the cursor to `<= 0`, the walk has hit the
            // bottom: nothing is older than `created_at = 0`. Complete the group
            // instead of re-issuing a page at `until <= 0`. Without this, a stuck
            // page carrying a `created_at = 0` event (e.g. fountain's malformed
            // kind-9735 zaps, on a page that timed out before EOSE) would compute
            // `until = -1` here. `backfill_filter` would clamp that up to `until:1`
            // (no saw-tooth — the defense layer holds), but the group would never
            // complete and would keep re-issuing `until:1` pages; completing here
            // is correct and avoids that. Any `created_at = 0` events not yet
            // received on the timed-out page are unreachable regardless (a relay
            // reading `until:0` as "unset" can't be paginated below the floor).
            if matches!(next_until, Some(u) if u <= 0) {
                self.mark_backfill_complete(&sub.kinds).await;
                tracing::info!(
                    relay = %self.url,
                    kinds = ?sub.kinds,
                    oldest_created_at = ?sub.oldest_created_at,
                    "stuck backfill reached epoch floor (cursor would be <= 0); \
                     marking complete instead of re-issuing",
                );
                continue;
            }
            // If we made any progress, reset the empty-streak — the next
            // page is logically a fresh attempt.
            let short_streak = if sub.received > 0 {
                0
            } else {
                sub.short_streak
            };
            self.reissue_backfill_page(sub.kinds, next_until, short_streak, sub.limit)
                .await;
        }
    }

    async fn handle_notification(&mut self, notif: RelayPoolNotification) -> Result<(), String> {
        match notif {
            RelayPoolNotification::Event {
                subscription_id,
                event,
                ..
            } => self.handle_event(&subscription_id, *event).await,
            RelayPoolNotification::Message { message, .. } => {
                self.handle_message(message).await;
                Ok(())
            }
            RelayPoolNotification::Shutdown => Ok(()),
        }
    }

    /// Record relay-wide subscription-dispatch activity. Called from the same
    /// four points that settle a probe as Alive (an event on any of our subs,
    /// EOSE, CLOSED) — i.e. proof the relay is dispatching to *our*
    /// subscriptions, as opposed to ambient NOTICE/OK traffic, which is
    /// deliberately NOT treated as liveness (see the T15 integration test).
    /// This is what lets a long backfill's traffic keep the dispatch-liveness
    /// probe from false-firing while the forward sub is deprioritized.
    fn mark_dispatch_activity(&mut self) {
        self.last_activity_at = Instant::now();
    }

    async fn handle_event(
        &mut self,
        subscription_id: &SubscriptionId,
        event: Event,
    ) -> Result<(), String> {
        // Any event on any of our subs (forward OR backfill) is dispatch
        // activity — keeps the liveness probe suppressed while we're being served.
        self.mark_dispatch_activity();
        // Dispatch-liveness probe settlement (Alive). Route at the VERY
        // top, before the `forward_subs.get_mut` bump and the
        // `kinds.contains` check below — otherwise a probe event (kind =
        // `u16::MAX` = 65535, intentionally outside any configured kind
        // set) would increment `events_rejected{reason="kind-not-configured"}`
        // and pollute the rejection metric. Probe events are intentionally
        // not archived (no `EventEnvelope` produced).
        if self
            .pending_probe
            .as_ref()
            .is_some_and(|p| p.sub_id == subscription_id.as_str())
        {
            self.on_probe_settled(ProbeOutcome::Alive).await;
            return Ok(());
        }

        let kind = event.kind.as_u16();

        // Per-forward-sub `last_event_at` bookkeeping (diagnostic only since
        // the probe moved to the relay-wide `last_activity_at` set by
        // `mark_dispatch_activity` above). This path
        // (`RelayPoolNotification::Event`) only fires for events not yet seen
        // by the pool's pool-wide `MemoryDatabase` LRU; the wire-level bump on
        // the `RelayMessage::Event` arm of `handle_message` catches the rest.
        if let Some(sub) = self.forward_subs.get_mut(subscription_id.as_str()) {
            sub.last_event_at = Instant::now();
        }

        let _ = subscription_id; // remaining routing decisions live in `handle_message`

        // Defensive drop: kind not in configured set. By definition there's
        // no slug for this kind, so the label is the documented `""` sentinel.
        if !self.kinds.contains(&kind) {
            self.metrics
                .events_rejected
                .with_label_values(&[
                    self.url.as_str(),
                    &kind.to_string(),
                    "",
                    "kind-not-configured",
                ])
                .inc();
            tracing::debug!(
                relay = %self.url,
                kind,
                event_id = %short_id(&event.id.to_hex()),
                "event rejected: kind not configured",
            );
            return Ok(());
        }

        // Defensive drop: ephemeral kinds are not archived (relays don't
        // persist them). Replaceable, addressable, and regular all pass.
        let class = EventClass::classify(kind);
        if !matches!(
            class,
            EventClass::Replaceable | EventClass::Addressable | EventClass::Regular
        ) {
            self.metrics
                .events_rejected
                .with_label_values(&[
                    self.url.as_str(),
                    &kind.to_string(),
                    self.slug_for(kind),
                    "wrong-class",
                ])
                .inc();
            return Ok(());
        }

        // Per-event metric + checkpoint update.
        self.metrics
            .events_received
            .with_label_values(&[self.url.as_str(), &kind.to_string(), self.slug_for(kind)])
            .inc();
        let created_at = i64::try_from(event.created_at.as_secs()).unwrap_or(i64::MAX);
        self.ckpt
            .observe(&self.url, kind, created_at, &event.id)
            .await;

        // NOTE: Backfill page-state (received counter, oldest_created_at) is
        // intentionally NOT updated here. It's updated from the
        // `RelayMessage::Event` branch of `handle_message` instead. Reason:
        // `RelayPoolNotification::Event` deduplicates events by id at the
        // connection level — when forward and backfill subs on the same
        // socket both match an event, only the first sub_id observed gets
        // an `Event` notification (typically the forward sub, which opens
        // a few hundred microseconds before the backfill). Tracking
        // backfill state here would silently mis-complete every backfill
        // whose events are first observed via the forward sub. The
        // `Message` channel sees every relay observation regardless of
        // dedupe, so it's the correct hook for per-sub state.

        // Send to sink with backpressure (no drops). Look up slug from the
        // current map; this should always succeed because we already filtered
        // events by configured kind. If it doesn't, fall back to a stable
        // placeholder so we still archive the event under a self-evidently
        // wrong path (rather than crashing).
        let kind_slug = self.kind_slugs.get(&kind).cloned().unwrap_or_else(|| {
            tracing::warn!(
                relay = %self.url,
                kind,
                "no slug configured for kind; using placeholder"
            );
            format!("kind-{kind}-no-slug")
        });
        let env = EventEnvelope {
            relay: self.url.clone(),
            event,
            kind_slug,
        };
        if self.sink.send(env).await.is_err() {
            return Err("sink closed".into());
        }

        Ok(())
    }

    async fn handle_message(&mut self, message: RelayMessage<'static>) {
        match message {
            RelayMessage::Event {
                subscription_id,
                event,
            } => {
                let sid = subscription_id.as_ref().as_str();
                // Wire-level event delivery on any of our subs (forward OR
                // backfill) is dispatch activity — see `mark_dispatch_activity`.
                self.mark_dispatch_activity();
                // Dispatch-liveness probe settlement (Alive). Same early
                // return as the `handle_event` entry point — probe events
                // are intentionally dropped (not bumping `last_event_at`,
                // not touching `active_backfills`). Routes here as well
                // as in `handle_event` because the two paths fire
                // independently — the pool's LRU dedup suppresses one of
                // the two for repeats but on first-seen events both run.
                if self.pending_probe.as_ref().is_some_and(|p| p.sub_id == sid) {
                    self.on_probe_settled(ProbeOutcome::Alive).await;
                    return;
                }
                // Per-sub-observation tracking (see `handle_event` comment).
                // The `Message` channel fires for every relay-side EVENT
                // delivery, including duplicates that the lib's `Event`
                // notification deduplicates away. Update backfill page-state
                // AND (diagnostic) forward-sub `last_event_at` here;
                // persistence and metrics still happen in `handle_event`
                // exactly once per event id.
                //
                // The relay-wide dispatch-liveness bump (`mark_dispatch_activity`
                // at the top of this arm) is what matters for the probe, and
                // routing it through this wire-level channel is load-bearing:
                // it fires even when the pool's `MemoryDatabase` LRU dedups the
                // event before `RelayPoolNotification::Event`. Without wire-level
                // coverage, a forward sub whose stream is dominated by event ids
                // already seen via faster peer relays would falsely trip the
                // probe despite functioning correctly — `handle_event` only sees
                // first-seen events. Observed (against the old per-sub
                // `last_event_at` trigger) in production on 2026-05-09: every
                // relay's forward sub re-issued within 60 s of startup because
                // the dominant relay (damus) filled the LRU first and other
                // relays' redeliveries never reached `handle_event`.
                if let Some(sub) = self.forward_subs.get_mut(sid) {
                    sub.last_event_at = Instant::now();
                    self.forward_echo_pending.remove(sid);
                }
                if let Some(sub) = self.active_backfills.get_mut(sid) {
                    let kind = event.kind.as_u16();
                    if !self.kinds.contains(&kind) {
                        return;
                    }
                    let created_at = i64::try_from(event.created_at.as_secs()).unwrap_or(i64::MAX);
                    sub.received = sub.received.saturating_add(1);
                    sub.oldest_created_at = Some(
                        sub.oldest_created_at
                            .map_or(created_at, |c| c.min(created_at)),
                    );
                }
            }
            RelayMessage::Notice(s) => {
                let text = s.into_owned();
                tracing::info!(relay = %self.url, notice = %text, "NOTICE received");
                if rejection::is_concurrent_pressure(&text) {
                    // Concurrency / rate-limit pressure: halving filters
                    // would only multiply the open-sub count and amplify the
                    // problem. Bump the back-off window instead — the next
                    // backfill page will be deferred until in-flight REQs
                    // drain.
                    self.note_concurrent_pressure(&text);
                } else if rejection::is_rejection(&text) {
                    // Bare NOTICE doesn't tell us which sub it's for. Apply
                    // the split heuristic to any forward sub that's larger
                    // than 1 kind. (Backfill REQs are typically retried by
                    // explicit CLOSED.)
                    self.split_forward_subs_due_to_notice(&text).await;
                }
            }
            RelayMessage::Closed {
                subscription_id,
                message,
            } => {
                let sid = subscription_id.into_owned();
                let msg = message.into_owned();
                tracing::info!(
                    relay = %self.url,
                    sub_id = %sid,
                    reason = %msg,
                    "CLOSED received",
                );
                // A CLOSED for one of our subs is the relay responding to our
                // REQ — dispatch activity (see `mark_dispatch_activity`).
                self.mark_dispatch_activity();
                // Dispatch-liveness probe settlement (Alive). Mirrors NDK
                // convention — CLOSED-for-our-probe-sub is treated as
                // proof-of-dispatch (it covers relays that reject `limit:0`
                // as malformed). Skip the existing rejection / split logic
                // for this sid.
                if self
                    .pending_probe
                    .as_ref()
                    .is_some_and(|p| p.sub_id == sid.as_str())
                {
                    self.on_probe_settled(ProbeOutcome::Alive).await;
                    return;
                }
                self.handle_closed(sid.as_str(), &msg).await;
            }
            RelayMessage::EndOfStoredEvents(sub_id) => {
                let sid = sub_id.into_owned();
                tracing::debug!(relay = %self.url, sub_id = %sid, "EOSE received");
                self.handle_eose(sid.as_str()).await;
            }
            RelayMessage::Auth { .. } => {
                tracing::warn!(relay = %self.url, "AUTH skipped (NIP-42 not supported in v1)");
            }
            // OK / Count / Neg* are not relevant to a read-only acquirer.
            _ => {}
        }
    }

    async fn handle_closed(&mut self, sub_id: &str, reason: &str) {
        // Backfill REQ closed by the relay. If the close looks like a
        // "filters too broad" rejection, split the kind set in half and
        // re-issue two narrower backfills. Bottom out at one kind per REQ;
        // if a single-kind backfill *still* gets rejected, abandon backfill
        // for that kind on this relay until the next reconnect (forward sub
        // may still pick up new events for it).
        if let Some(sub) = self.active_backfills.remove(sub_id) {
            // The relay CLOSEd it: CLOSE it on our side too (see the forward
            // branch below for why), on this relay only.
            self.close_at_relay(sub_id).await;
            let kinds_in_sub = sub.kinds.clone();
            // Concurrency / rate-limit pressure: re-issue the same page
            // (same `until` cursor) after a back-off so we don't lose this
            // page's progress. Splitting the kind list would amplify open
            // subs and make pressure worse.
            if rejection::is_concurrent_pressure(reason) {
                self.note_concurrent_pressure(reason);
                tracing::warn!(
                    relay = %self.url,
                    sub_id,
                    reason,
                    kinds = ?kinds_in_sub,
                    backoff_secs = self.pressure.backoff().as_secs(),
                    "backfill REQ closed for concurrency pressure; will retry same cursor after back-off",
                );
                self.reissue_backfill_page(kinds_in_sub, sub.until, sub.short_streak, sub.limit)
                    .await;
                return;
            }
            if rejection::is_rejection(reason) {
                if kinds_in_sub.len() > 1 {
                    let mid = kinds_in_sub.len() / 2;
                    let (left, right) = kinds_in_sub.split_at(mid);
                    tracing::info!(
                        relay = %self.url,
                        sub_id,
                        reason,
                        kinds_before = ?kinds_in_sub,
                        left = ?left,
                        right = ?right,
                        "backfill REQ rejected; splitting kind list in half",
                    );
                    self.start_backfill_for_kinds(left.to_vec()).await;
                    self.start_backfill_for_kinds(right.to_vec()).await;
                    return;
                }
                // Single-kind floor: abandon backfill for this kind.
                let kind = kinds_in_sub[0];
                tracing::error!(
                    relay = %self.url,
                    kind,
                    reason,
                    "backfill REQ rejected at single-kind floor; abandoning backfill for this kind on this relay"
                );
                self.metrics
                    .events_rejected
                    .with_label_values(&[
                        self.url.as_str(),
                        &kind.to_string(),
                        self.slug_for(kind),
                        "backfill-rejected-at-floor",
                    ])
                    .inc();
                self.metrics
                    .backfill_in_progress
                    .with_label_values(&[self.url.as_str(), &kind.to_string(), self.slug_for(kind)])
                    .set(0);
                return;
            }
            // Non-rejection close (idle timeout, transient): just abandon
            // this REQ. The relay-task's reconnect / restart logic will
            // re-open backfill from the persisted `until_cursor` next time.
            tracing::warn!(
                relay = %self.url,
                sub_id,
                reason,
                kinds = ?kinds_in_sub,
                "backfill REQ closed by relay (non-rejection); abandoning page",
            );
            for k in &kinds_in_sub {
                self.metrics
                    .backfill_in_progress
                    .with_label_values(&[self.url.as_str(), &k.to_string(), self.slug_for(*k)])
                    .set(0);
            }
            return;
        }
        // An empty CLOSED for a forward id nozzl itself CLOSEd moments ago is most
        // likely the relay acknowledging that CLOSE (relay.conduit.market does),
        // not closing the sub re-opened under the same id since. Keep the sub,
        // but only on probation: unless an EOSE or an EVENT for the id arrives
        // within FORWARD_ECHO_CONFIRM, the tick treats it as the relay's close
        // after all, so a relay refusing REQs with an empty reason cannot hide.
        if reason.is_empty()
            && self.forward_subs.contains_key(sub_id)
            && self
                .forward_closed_by_us
                .get(sub_id)
                .is_some_and(|at| at.elapsed() < FORWARD_CLOSE_ECHO_WINDOW)
        {
            self.forward_closed_by_us.remove(sub_id);
            self.forward_echo_pending
                .insert(sub_id.to_string(), Instant::now() + FORWARD_ECHO_CONFIRM);
            self.metrics
                .forward_sub_closed
                .with_label_values(&[self.url.as_str(), "echo"])
                .inc();
            tracing::info!(
                relay = %self.url,
                sub_id,
                "empty CLOSED right after our own CLOSE of this id; taken as the relay acknowledging it, pending an EOSE",
            );
            return;
        }
        self.forward_closed_by_relay(sub_id, reason).await;
    }

    /// The relay closed forward sub `sub_id` (or a suspected echo of our own
    /// CLOSE was never confirmed): CLOSE it back, then split the group on a
    /// filter rejection, or re-open it after the back-off.
    async fn forward_closed_by_relay(&mut self, sub_id: &str, reason: &str) {
        self.forward_echo_pending.remove(sub_id);
        // If it was a forward sub: drop it and decide whether to halve.
        let was_forward = self.forward_subs.remove(sub_id).is_some();
        if !was_forward {
            return;
        }
        // The relay CLOSEd it: CLOSE it on our side too, on this relay only. A
        // relay may keep its own registration for a sub it CLOSEd
        // (an htree daemon's relay does when the CLOSED was an upstream's), and that
        // counts against its per-connection subscription cap.
        self.close_at_relay(sub_id).await;
        if rejection::is_rejection(reason) {
            // Find the offending group (sub_id is `forward` or `forward-N`).
            let group_idx = group_index_of(sub_id);
            self.split_forward_group_at(group_idx).await;
        } else {
            // Non-rejection close: re-issue the same forward sub after a
            // back-off. A re-issue already pending keeps its earlier time.
            let now = Instant::now();
            let wait = self.forward_reissue_backoff.next(now);
            let at = now + wait;
            self.forward_reissue_at = Some(self.forward_reissue_at.map_or(at, |t| t.min(at)));
            self.metrics
                .forward_sub_closed
                .with_label_values(&[self.url.as_str(), "reissue"])
                .inc();
            tracing::info!(
                relay = %self.url,
                sub_id,
                reason,
                backoff_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                "forward sub closed by the relay; re-issuing after back-off",
            );
        }
    }

    async fn split_forward_subs_due_to_notice(&mut self, _notice: &str) {
        // No sub-id in a NOTICE — split every group with > 1 kind.
        let mut splittable_indices: Vec<usize> = self
            .kind_groups
            .iter()
            .enumerate()
            .filter(|(_, g)| g.len() > 1)
            .map(|(i, _)| i)
            .collect();
        if splittable_indices.is_empty() {
            return;
        }
        // Drop existing forward subs and re-split.
        self.close_all_forward_subs().await;
        // Halve every multi-kind group.
        let mut new_groups = Vec::with_capacity(self.kind_groups.len() * 2);
        for (idx, group) in self.kind_groups.drain(..).enumerate() {
            if splittable_indices.first() == Some(&idx) {
                splittable_indices.remove(0);
                let mid = group.len() / 2;
                let (left, right) = group.split_at(mid);
                new_groups.push(left.to_vec());
                new_groups.push(right.to_vec());
            } else if group.len() > 1 {
                let mid = group.len() / 2;
                let (left, right) = group.split_at(mid);
                new_groups.push(left.to_vec());
                new_groups.push(right.to_vec());
            } else {
                new_groups.push(group);
            }
        }
        self.kind_groups = new_groups;
        self.update_split_metric();
        if let Err(e) = self.open_all_forward_subs().await {
            tracing::warn!(relay = %self.url, error = %e, "failed to open split forward subs");
        }
    }

    async fn split_forward_group_at(&mut self, group_idx: Option<usize>) {
        let Some(idx) = group_idx else {
            // Unknown group; conservatively halve everything as if NOTICE.
            self.metrics
                .forward_sub_closed
                .with_label_values(&[self.url.as_str(), "split"])
                .inc();
            self.split_forward_subs_due_to_notice("").await;
            return;
        };
        // Snapshot the current set of forward subs and replace the offending group.
        if idx >= self.kind_groups.len() {
            return;
        }
        let group = self.kind_groups[idx].clone();
        if group.len() == 1 {
            // Floor: stop subscribing to this kind on this relay, and retry it
            // after a back-off (or at once after a reconnect), so a relay whose
            // only kind is floored does not stay silent until a config change.
            let kind = group[0];
            let now = Instant::now();
            let wait = self.floor_retry_backoff.next(now);
            self.floor_retry_at = Some(now + wait);
            tracing::error!(
                relay = %self.url,
                kind,
                retry_in_secs = wait.as_secs(),
                "REQ rejected at single-kind floor; suppressing kind on this relay until the retry",
            );
            self.metrics
                .events_rejected
                .with_label_values(&[
                    self.url.as_str(),
                    &kind.to_string(),
                    self.slug_for(kind),
                    "kind-rejected-at-floor",
                ])
                .inc();
            self.metrics
                .forward_sub_closed
                .with_label_values(&[self.url.as_str(), "floor"])
                .inc();
            self.floored_kinds.insert(kind);
            self.kind_groups.remove(idx);
        } else {
            self.metrics
                .forward_sub_closed
                .with_label_values(&[self.url.as_str(), "split"])
                .inc();
            let mid = group.len() / 2;
            let left = group[..mid].to_vec();
            let right = group[mid..].to_vec();
            self.kind_groups.remove(idx);
            self.kind_groups.insert(idx, right);
            self.kind_groups.insert(idx, left);
            tracing::warn!(
                relay = %self.url,
                old_kinds = ?group,
                "auto-splitting REQ on rejection: halving kind list",
            );
        }
        // Re-issue all forward subs to keep state consistent.
        self.close_all_forward_subs().await;
        self.update_split_metric();
        if let Err(e) = self.open_all_forward_subs().await {
            tracing::warn!(relay = %self.url, error = %e, "failed to open subs after split");
        }
    }

    /// Record `Complete` for every kind in `kinds` and zero its in-progress
    /// gauge. This is the single terminal for a backfill group, reached three
    /// ways: the empty-page streak (history exhausted, `handle_eose`), the
    /// epoch-floor guard (`next_backfill_advance` → `Floor` in `handle_eose`),
    /// and the timeout-retry floor check in `check_stuck_backfills` (an
    /// abandoned page whose `oldest - 1` would drop to `<= 0`).
    async fn mark_backfill_complete(&mut self, kinds: &[u16]) {
        for k in kinds {
            self.ckpt
                .record_backfill(&self.url, *k, BackfillState::Complete)
                .await;
            self.metrics
                .backfill_in_progress
                .with_label_values(&[self.url.as_str(), &k.to_string(), self.slug_for(*k)])
                .set(0);
        }
        self.finish_backfill_run(kinds);
    }

    /// Emit the once-per-run backfill-completeness summary for `kinds` and drop
    /// the run's accumulated state. Called from `mark_backfill_complete` — i.e.
    /// from every terminal a backfill group can reach (empty-page streak,
    /// epoch-floor, stuck-page floor). The summary carries the page tallies
    /// (`full`/`partial`/`tie_stagnation`), total events, and the `created_at`
    /// span walked — the single line that answers "did this kind's walk cleanly
    /// cover its history?". It is once per
    /// run, not per page, so it does not flood the log. A run with no recorded
    /// stats (e.g. a kind already `Complete` at startup that never opened a
    /// page) is silent.
    fn finish_backfill_run(&mut self, kinds: &[u16]) {
        let key = backfill_run_key(kinds);
        let Some(stats) = self.backfill_run_stats.remove(&key) else {
            return;
        };
        tracing::info!(
            relay = %self.url,
            kinds = ?kinds,
            pages = stats.pages,
            full_pages = stats.full_pages,
            partial_pages = stats.partial_pages,
            tie_stagnations = stats.tie_stagnations,
            events = stats.events,
            newest_created_at = ?stats.newest_created_at,
            oldest_created_at = ?stats.oldest_created_at,
            "backfill run complete (completeness summary)",
        );
    }

    async fn handle_eose(&mut self, sub_id: &str) {
        // An EOSE is the relay completing one of our REQs (a backfill page,
        // the forward sub's initial scan, or a probe) — dispatch activity.
        // This is the key signal that keeps the probe suppressed across a long
        // backfill: every page EOSE refreshes liveness. See
        // `mark_dispatch_activity`.
        self.mark_dispatch_activity();
        // A forward sub kept through a suspected echo is confirmed alive.
        self.forward_echo_pending.remove(sub_id);
        // Dispatch-liveness probe settlement (Alive). NDK-style probes
        // expect a fast EOSE — that's the happy path. Route BEFORE the
        // backfill lookup so a probe sub_id never accidentally collides
        // with a backfill EOSE.
        if self
            .pending_probe
            .as_ref()
            .is_some_and(|p| p.sub_id == sub_id)
        {
            self.on_probe_settled(ProbeOutcome::Alive).await;
            return;
        }
        // Only backfill subs care about EOSE — the forward sub stays open.
        let Some(mut sub) = self.active_backfills.remove(sub_id) else {
            return;
        };
        // CLOSE this page's sub now that it has EOSE'd. A backfill filter's
        // `until` is pinned in the past, so the relay's live tail (the only
        // thing EOSE leaves open per NIP-01) can never match — keeping the
        // sub open is pure waste that burns a slot against the relay's
        // max-subscriptions cap (strfry/nos.lol: 20). Without this, every
        // page below opens a fresh `backfill-<nonce>` sub while the completed
        // ones linger, so a long backfill accumulates subs server-side and
        // self-inflicts a "too many concurrent REQs" throttle. Closing here —
        // before the next-page open below — holds us at one backfill sub per
        // relay at a time. `unsubscribe` queues a CLOSE frame ahead of the
        // next REQ on the same socket, so the relay frees the slot before the
        // new page registers. See issue #12.
        self.close_at_relay(sub_id).await;
        // EOSE is the page-completion signal. If pressure was observed during
        // this page, the backoff persists across the EOSE; otherwise it
        // decays. The actual sleep happens just before the next-page open
        // below — we don't sleep here because the dedup / completion logic
        // doesn't need to wait, and sleeping inside select! arms blocks every
        // other notification on this relay.
        let _ = self.pressure.on_eose(Instant::now());
        let received = sub.received;
        let limit = sub.limit;
        let oldest = sub.oldest_created_at;

        // A page is "empty" only when the relay returned zero events. The
        // earlier heuristic — `received < limit` — declared backfill done
        // after two near-full pages in a row, which on high-volume relays
        // (Damus, etc.) trips on timestamp-tie pagination quirks where the
        // relay returns 498-499 out of 500 even though years more history
        // are available. Empirically observed on relay.damus.io: backfill
        // walked back ~19 days and stopped on a 498/499 sequence while
        // millions of older events remained. Empty-page is the only
        // pagination-quirk-resistant signal that we've actually exhausted
        // history. The streak threshold is configurable via
        // `acquirer.backfill_empty_pages_to_complete` to also tolerate
        // transient empty bands and per-kind retention edges that can
        // produce a short run of zero-event pages above non-empty history.
        let was_empty = received == 0;
        if was_empty {
            sub.short_streak = sub.short_streak.saturating_add(1);
        } else {
            sub.short_streak = 0;
        }

        let empty_threshold = self.empty_pages_to_complete;
        tracing::info!(
            relay = %self.url,
            sub_id,
            received,
            limit,
            empty_streak = sub.short_streak,
            empty_threshold,
            oldest_created_at = ?oldest,
            "backfill page complete (EOSE)",
        );

        // Fold this page into the run's completeness accounting (consumed once,
        // at Complete, by `finish_backfill_run`). A tie-stagnation is a page
        // whose cursor could not advance: `next_backfill_advance` steps
        // `oldest - 1` past it, and the rest of that second is shed whenever the
        // relay holds more of it than one page carries (#18 (Backfill pagination
        // silently drops events when >limit (500) share a single created_at
        // second)). That is possible when the page was as large as this
        // relay's pages get: full, or as large as the run's largest page, the
        // size of a relay that clamps or cuts below `limit`. A smaller page that
        // stagnates, such as the re-read of a walk's last second, served the
        // whole second, so it is not counted. A page cut by a read budget below
        // the run's largest page can still shed uncounted (#53 (Backfill ignores
        // a pressure CLOSED that arrives after the page's EOSE)). No per-page log
        // line is added - this is silent accumulation + two cheap counters.
        let run_key = backfill_run_key(&sub.kinds);
        let largest_page = self
            .backfill_run_stats
            .get(&run_key)
            .map_or(0, |stats| stats.max_received);
        let tie_stagnation = is_tie_stagnation(
            received,
            limit,
            largest_page,
            oldest,
            sub.prev_oldest_created_at,
        );
        if let Some(stats) = self.backfill_run_stats.get_mut(&run_key) {
            stats.record_page(received, limit, oldest, tie_stagnation);
        }
        // Mirror the partial / tie-stagnation signals onto /metrics, per kind in
        // the page's group, so the summary is also dashboard-visible (and an
        // alert can fire on a dense kind shedding boundary bands mid-walk). A
        // partial page is `received < limit` (empty pages included).
        if received < limit {
            for k in &sub.kinds {
                self.metrics
                    .backfill_partial_pages
                    .with_label_values(&[self.url.as_str(), &k.to_string(), self.slug_for(*k)])
                    .inc();
            }
        }
        if tie_stagnation {
            for k in &sub.kinds {
                self.metrics
                    .backfill_tie_stagnations
                    .with_label_values(&[self.url.as_str(), &k.to_string(), self.slug_for(*k)])
                    .inc();
            }
        }

        // `empty_pages_to_complete` consecutive empty pages = no events
        // left before the cursor = backfill done.
        if sub.short_streak >= empty_threshold {
            self.mark_backfill_complete(&sub.kinds).await;
            tracing::info!(relay = %self.url, kinds = ?sub.kinds, "backfill complete");
            return;
        }

        // Decide the next cursor. The pagination rules (inclusive-on-advance,
        // stagnation fallback, empty-page step) AND the epoch-floor guard all
        // live in `next_backfill_advance` — see its doc for the full rationale,
        // including why a REQ with `until <= 0` must never be issued. The
        // cursor rules do not depend on whether the page was full.
        let next_until = match next_backfill_advance(oldest, sub.prev_oldest_created_at, sub.until)
        {
            BackfillAdvance::Floor => {
                // Reached the epoch floor: nothing older than created_at=0 can
                // exist. Complete the group (same terminal as the empty-page
                // streak) instead of issuing a doomed `until <= 0` page that
                // would saw-tooth the walk back to now (see `next_backfill_advance`).
                self.mark_backfill_complete(&sub.kinds).await;
                tracing::info!(
                    relay = %self.url,
                    kinds = ?sub.kinds,
                    "backfill reached epoch floor (next until <= 0); marking complete",
                );
                return;
            }
            BackfillAdvance::NoCursor => None,
            BackfillAdvance::Page(cursor) => Some(cursor),
        };
        let new_id = format!("{BACKFILL_SUB_BASE}-{}", new_nonce());
        // Persist the new cursor.
        if let Some(cursor) = next_until {
            for k in &sub.kinds {
                self.ckpt
                    .record_backfill(
                        &self.url,
                        *k,
                        BackfillState::InProgress {
                            until_cursor: cursor,
                        },
                    )
                    .await;
            }
        }

        let filter = backfill_filter(&sub.kinds, next_until, limit);
        // Honour an active concurrency-pressure back-off before opening the
        // next page. Without this gate the EOSE-driven page-advance loop
        // would re-fire instantly, amplifying the pressure that triggered
        // the NOTICE/CLOSED in the first place. See [`PressureState`].
        self.await_pressure_backoff().await;
        tracing::info!(
            relay = %self.url,
            sub_id = %new_id,
            kinds = ?sub.kinds,
            until = ?next_until,
            limit,
            short_streak = sub.short_streak,
            "REQ open: backfill next page",
        );
        let res = self
            .pool
            .subscribe_with_id_to(
                [self.url.as_str().to_string()],
                SubscriptionId::new(&new_id),
                filter,
                SubscribeOptions::default(),
            )
            .await;
        if let Err(e) = res {
            tracing::warn!(relay = %self.url, error = %e, "failed to open backfill next page");
            for k in &sub.kinds {
                self.metrics
                    .backfill_in_progress
                    .with_label_values(&[self.url.as_str(), &k.to_string(), self.slug_for(*k)])
                    .set(0);
            }
            return;
        }
        self.active_backfills.insert(
            new_id.clone(),
            BackfillSub {
                sub_id: new_id,
                kinds: sub.kinds,
                received: 0,
                oldest_created_at: None,
                prev_oldest_created_at: oldest,
                short_streak: sub.short_streak,
                limit,
                until: next_until,
                opened_at: Instant::now(),
            },
        );
    }
}

/// Where a backward-walking backfill goes after one EOSE page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillAdvance {
    /// Open the next page at this `until`. INVARIANT: always `> 0`.
    Page(i64),
    /// The walk reached the epoch floor — there is no event older than
    /// `created_at = 0`, so this is completion, not another page. The caller
    /// MUST mark the group Complete and must NOT build a filter from it.
    Floor,
    /// No cursor could be derived (an empty page with no prior `until`). Rare;
    /// the caller falls back to its existing no-cursor behavior. Not a floor.
    NoCursor,
}

/// Decide the next cursor for a backward-walking backfill from one EOSE page.
///
/// Backfill pagination walks *older* by lowering `until` (NIP-01: a page
/// returns `created_at <= until`). This function is the source of truth for the
/// EOSE-path cursor arithmetic — `handle_eose` calls it. One other site,
/// `check_stuck_backfills`, recomputes a cursor independently: an abandoned page
/// that never EOSE'd is re-issued with no `prev_oldest`, so it has no
/// stagnation guard and deliberately uses a simpler exclusive `oldest - 1`
/// retry. That path is NOT routed through here (the inclusive-hold rule would be
/// wrong for it), but it applies the same epoch-floor guard (below) so it
/// likewise can never issue `until <= 0`.
/// Keeping this function pure (no `self`, no I/O) makes the rules exhaustively
/// unit-testable and keeps the rationale below from drifting away from the code
/// during future refactors of the backfill machinery.
///
/// Inputs:
/// - `oldest`: the oldest `created_at` in the page just received (`None` if the
///   page was empty).
/// - `prev_oldest`: the previous page's `oldest` (stagnation detection).
/// - `sub_until`: the `until` this page was requested with.
///
/// Whether the page was full is deliberately NOT an input. Until 2026-10-12 a
/// page shorter than the requested `limit` stepped `oldest - 1`, on the reading
/// that the relay had nothing left at `oldest`, and skipped the rest of that
/// second whenever the reading was wrong (#32 (Backfill silently drops
/// boundary-tie events when a relay serves fewer events per REQ than
/// `backfill_default_limit`)). It was wrong in four ways seen live: a relay that
/// clamps below our limit without saying so (relay.conduit.market at 100, 70
/// events lost), ngit-grasp serving 489-499 of 500 at random, relay.damus.io
/// cutting pages on its per-IP read budget, and events the pool drops after
/// they took a slot in the relay's page (a bad id or signature, an expired
/// `expiration` tag, a deserialize failure).
///
/// Cursor rules:
/// - **Non-empty page that advanced** (`oldest != prev_oldest`), full or short:
///   hold *inclusively* at `oldest` so boundary events the relay did not serve
///   are re-requested on the next page; the pool's event-ID dedup discards
///   repeats. See `backfill_captures_all_events_at_boundary_timestamp`,
///   `backfill_captures_boundary_ties_when_the_relay_clamps_below_the_limit` and
///   `backfill_captures_boundary_ties_when_the_pool_drops_an_event_from_a_page`.
/// - **Page that stagnated** (`oldest == prev_oldest`): the inclusive retry
///   served nothing older than the second it re-read, so step strictly older
///   (`oldest - 1`). On a page as large as this relay's pages get (full, or the
///   size it clamps or cuts to) that means the second may hold more events than
///   one page carries, and the remainder is unrecoverable - Nostr filters have
///   no id-range cursor (#18 (Backfill pagination silently drops events when
///   `>limit (500)` share a single `created_at` second)); see
///   `docs/req-semantics.md` section Backfill cursor strategy and
///   `backfill_stagnation_guard_prevents_infinite_loop`. On a smaller page the
///   second was re-read whole: the re-read of a walk's last second costs one
///   page.
/// - **Empty page**: step below the `until` we asked for (`sub_until - 1`).
///
/// Termination: on a relay that honours `until` the cursor never rises, and it
/// holds still for at most one page: a page that serves nothing older than the
/// second it re-read stagnates and steps one second below, as an empty page
/// does. So the walk always reaches the empty-page streak or the floor.
///
/// THE EPOCH-FLOOR GUARD (issue #23 — do not remove). After computing the raw
/// next cursor by the rules above, if it would be `<= 0` the walk has reached
/// the bottom: nothing is older than `created_at = 0`. That is **completion**,
/// returned as [`BackfillAdvance::Floor`] — NOT another page. This guard exists
/// because issuing a REQ with `until <= 0` is catastrophic: a `created_at = 0`
/// event (e.g. a malformed kind-9735 zap receipt whose backend never set
/// `paid_at`; observed live on relay.fountain.fm) yields `oldest = 0` →
/// `next = -1` → `backfill_filter` would send `until:0`, which relays that read
/// `0` as "unset" answer with their NEWEST events, snapping the walk back to the
/// forward edge and looping forever (the 2026-06-15 fountain saw-tooth; the
/// `empty_streak` completion can never fire because every reset page is full).
/// Any relay serving a `created_at = 0` (or otherwise epoch-floored) event in a
/// backfilled kind hits this — it is not fountain-specific. Reaching the floor
/// here, before any filter is built, is the single guarantee against that class.
fn next_backfill_advance(
    oldest: Option<i64>,
    prev_oldest: Option<i64>,
    sub_until: Option<i64>,
) -> BackfillAdvance {
    let cursor_advanced = oldest != prev_oldest;
    let raw: i64 = match oldest {
        Some(o) if cursor_advanced => o,
        Some(o) => o.saturating_sub(1),
        None => match sub_until {
            Some(u) => u.saturating_sub(1),
            None => return BackfillAdvance::NoCursor,
        },
    };
    if raw <= 0 {
        BackfillAdvance::Floor
    } else {
        BackfillAdvance::Page(raw)
    }
}

/// Whether a page that stagnated (its oldest equal to the previous page's) may
/// have shed events, the count behind `tie_stagnations`: only when the page was
/// as large as this relay's pages get, full (`received >= limit`) or as large
/// as the run's largest page so far (`largest_page`, the size of a relay that
/// clamps or cuts below `limit`). A smaller page that stagnates, such as the
/// re-read of a walk's last second, served that whole second. A single event
/// never counts: a second holding one event is re-read alone. Two cases stay
/// wrong either way: a page cut by a read budget below the run's largest page
/// sheds uncounted, and a run whose whole history is one page of events at a
/// single second counts its re-read once.
fn is_tie_stagnation(
    received: u32,
    limit: u32,
    largest_page: u32,
    oldest: Option<i64>,
    prev_oldest: Option<i64>,
) -> bool {
    let as_large_as_pages_get = received >= limit || (received > 1 && received >= largest_page);
    as_large_as_pages_get && oldest.is_some() && oldest == prev_oldest
}

fn group_index_of(sub_id: &str) -> Option<usize> {
    if sub_id == FORWARD_SUB_BASE {
        Some(0)
    } else {
        sub_id
            .strip_prefix(FORWARD_SUB_BASE)
            .and_then(|s| s.strip_prefix('-'))
            .and_then(|s| s.parse::<usize>().ok())
    }
}

fn now_secs() -> i64 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    i64::try_from(secs).unwrap_or(i64::MAX)
}

/// Short, monotonic-ish nonce for backfill sub-ids. Doesn't need to be a UUID —
/// just unique within a relay-task lifetime. `Instant`-style monotonic via
/// `SystemTime` is sufficient and avoids pulling another dep.
fn new_nonce() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{nanos:x}")
}

fn short_id(hex: &str) -> &str {
    if hex.len() >= 8 { &hex[..8] } else { hex }
}

/// Map the upstream pool's [`RelayStatus`] onto the typed
/// [`RelayConnState`]. This is the single boundary between the
/// `nostr-relay-pool` status vocabulary and our metric domain enum; the
/// `0/1/2` wire encoding lives in [`RelayConnState::code`], not here.
///
/// Total by construction: a pure `match` with no fallible step, so it can
/// never fail to produce a state or panic. The exhaustive arms (no `_`
/// wildcard) are deliberate — `RelayStatus` is a plain, non-`#[non_exhaustive]`
/// enum, so if a future `nostr-relay-pool` upgrade adds a variant this stops
/// compiling and forces an explicit decision here. **Do not silence that with
/// a catch-all arm**: a wildcard would silently bucket an unknown state
/// (possibly a "connected"-like one) into `Disconnected` and skew health.
pub(crate) fn conn_state(status: RelayStatus) -> RelayConnState {
    match status {
        RelayStatus::Connected => RelayConnState::Connected,
        RelayStatus::Connecting | RelayStatus::Pending | RelayStatus::Initialized => {
            RelayConnState::Connecting
        }
        RelayStatus::Disconnected
        | RelayStatus::Terminated
        | RelayStatus::Banned
        | RelayStatus::Sleeping => RelayConnState::Disconnected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- BackfillRunStats: per-run completeness accounting -----

    #[test]
    fn run_stats_tallies_full_partial_and_stagnation_over_a_page_sequence() {
        // Synthetic walk: two full pages (one a tie-stagnation), one partial,
        // one empty terminator. The summary must split full vs partial, count
        // the single stagnation, sum events, and capture the created_at span.
        let mut s = BackfillRunStats::default();
        // Page 1: full, 500/500, oldest=2_000 (advanced from None).
        s.record_page(500, 500, Some(2_000), false);
        // Page 2: full, 500/500, oldest=1_000, but stagnated (cursor stuck).
        s.record_page(500, 500, Some(1_000), true);
        // Page 3: partial, 120/500, oldest=900.
        s.record_page(120, 500, Some(900), false);
        // Page 4: empty terminator, 0/500, no oldest.
        s.record_page(0, 500, None, false);

        assert_eq!(s.pages, 4, "every EOSE'd page is counted");
        assert_eq!(s.full_pages, 2, "two pages hit limit");
        assert_eq!(
            s.partial_pages, 2,
            "the 120/500 page and the empty page are both partial (received < limit)"
        );
        assert_eq!(s.tie_stagnations, 1, "exactly one full page stagnated");
        assert_eq!(s.events, 500 + 500 + 120, "events sum the wire receipts");
        // Span: newest is the first page's oldest (2_000), oldest is page 3's (900).
        assert_eq!(s.newest_created_at, Some(2_000));
        assert_eq!(s.oldest_created_at, Some(900));
    }

    #[test]
    fn run_stats_clean_solo_walk_has_zero_partials_until_the_terminator() {
        // The kind-7 control case: a clean dedicated walk is all-full pages
        // until the single empty terminator. partial_pages == 1 (the empty
        // terminator only) and tie_stagnations == 0 is the "came through clean"
        // signature the experiment looks for. A *rising* partial/stagnation
        // count mid-walk (full pages still ahead) is the gap signature instead.
        // (A real walk since 2026-10-12 also shows a short tail page and the
        // one re-read of its last second as partial; neither is a stagnation
        // that counts. This synthetic sequence has neither.)
        let mut s = BackfillRunStats::default();
        for i in 0..50 {
            // Strictly-decreasing oldest each page ⇒ no stagnation.
            s.record_page(500, 500, Some(10_000 - i), false);
        }
        s.record_page(0, 500, None, false); // exhausted

        assert_eq!(s.pages, 51);
        assert_eq!(s.full_pages, 50);
        assert_eq!(s.partial_pages, 1, "only the empty terminator is partial");
        assert_eq!(s.tie_stagnations, 0, "a clean walk never stagnates");
        assert_eq!(s.events, 50 * 500);
    }

    #[test]
    fn run_stats_tracks_the_largest_page() {
        // What a relay that clamps or cuts below `limit` serves per page: the
        // size `is_tie_stagnation` compares a stagnated page with.
        let mut s = BackfillRunStats::default();
        for received in [100, 87, 100, 1, 0] {
            s.record_page(received, 500, Some(1_000), false);
        }
        assert_eq!(s.max_received, 100);
    }

    #[test]
    fn tie_stagnation_counts_a_page_as_large_as_pages_get() {
        let (stuck, moved) = ((Some(900), Some(900)), (Some(900), Some(1_000)));
        // Full page stuck at its second: more than a page may share it (#18).
        assert!(is_tie_stagnation(500, 500, 500, stuck.0, stuck.1));
        // A relay clamping at 10 (we ask 100): a stuck 10-event page is as large
        // as its pages get, so the second may hold more (the review of the 2026-10-12
        // change: 20 events at one second, 10 shed).
        assert!(is_tie_stagnation(10, 100, 10, stuck.0, stuck.1));
        // The re-read of a walk's last second is smaller than the run's pages:
        // it served the whole second.
        assert!(!is_tie_stagnation(1, 500, 500, stuck.0, stuck.1));
        assert!(!is_tie_stagnation(3, 500, 451, stuck.0, stuck.1));
        // One event alone is never a shed, whatever the run's largest page.
        assert!(!is_tie_stagnation(1, 100, 1, stuck.0, stuck.1));
        // A page whose oldest moved is no stagnation, full or not.
        assert!(!is_tie_stagnation(500, 500, 500, moved.0, moved.1));
        assert!(!is_tie_stagnation(10, 100, 10, moved.0, moved.1));
        // An empty page is no stagnation.
        assert!(!is_tie_stagnation(0, 500, 500, None, None));
    }

    #[test]
    fn run_stats_empty_run_has_no_span() {
        // A run that opened a page but the relay had nothing: one empty page,
        // no created_at span. The summary still emits (pages=1) but the span
        // fields stay None.
        let mut s = BackfillRunStats::default();
        s.record_page(0, 500, None, false);
        assert_eq!(s.pages, 1);
        assert_eq!(s.partial_pages, 1);
        assert_eq!(s.events, 0);
        assert_eq!(s.newest_created_at, None);
        assert_eq!(s.oldest_created_at, None);
    }

    #[test]
    fn backfill_run_key_is_order_independent() {
        // The page chain re-issues fresh sub_ids but the kind set is stable.
        // The key must be identical regardless of the kinds' input order so a
        // run started with [7] and a page folded with [7] hit the same entry,
        // and a combined group [6,16] matches [16,6].
        assert_eq!(backfill_run_key(&[7]), backfill_run_key(&[7]));
        assert_eq!(backfill_run_key(&[6, 16]), backfill_run_key(&[16, 6]));
        assert_ne!(backfill_run_key(&[7]), backfill_run_key(&[6]));
    }

    // ----- next_backfill_advance: cursor rules + epoch-floor guard -----

    #[test]
    fn advance_page_that_moved_holds_inclusive() {
        // Oldest moved vs prev => hold inclusively at oldest so the relay can
        // serve boundary events it did not serve on this page (pool dedups
        // repeats). Full or short makes no difference, and the function no
        // longer takes it: a short page used to step oldest - 1 and skip the
        // rest of its oldest second (#32 (Backfill silently drops boundary-tie
        // events when a relay serves fewer events per REQ than
        // backfill_default_limit)). The integration tests pin the short cases.
        assert_eq!(
            next_backfill_advance(Some(900), Some(1_000), Some(1_000)),
            BackfillAdvance::Page(900)
        );
    }

    #[test]
    fn advance_first_page_holds_inclusive() {
        // The first page (and a re-issued one) has no prev: any oldest counts
        // as moved.
        assert_eq!(
            next_backfill_advance(Some(900), None, Some(1_000)),
            BackfillAdvance::Page(900)
        );
    }

    #[test]
    fn advance_page_that_stagnated_steps_older() {
        // oldest == prev => the inclusive retry served nothing older than the
        // second it re-read => oldest - 1. On a full page that is >limit events
        // at one second, the loss nothing can recover; on a short page the
        // second was re-read whole.
        assert_eq!(
            next_backfill_advance(Some(900), Some(900), Some(900)),
            BackfillAdvance::Page(899)
        );
    }

    #[test]
    fn advance_empty_page_steps_below_requested_until() {
        assert_eq!(
            next_backfill_advance(None, Some(900), Some(500)),
            BackfillAdvance::Page(499)
        );
    }

    #[test]
    fn advance_empty_page_with_no_until_is_no_cursor() {
        assert_eq!(
            next_backfill_advance(None, None, None),
            BackfillAdvance::NoCursor
        );
    }

    #[test]
    fn advance_floor_on_created_at_zero_page() {
        // THE BUG: oldest=0 => the inclusive cursor would be 0 => Floor, NOT a
        // page. until:0 is as dangerous as until:-1, which fountain.fm answered
        // with its newest events -> infinite saw-tooth.
        assert_eq!(
            next_backfill_advance(Some(0), Some(10), Some(10)),
            BackfillAdvance::Floor
        );
    }

    #[test]
    fn advance_floor_on_created_at_zero_stagnated_page() {
        // Stagnated at oldest=0 => next would be -1 => Floor.
        assert_eq!(
            next_backfill_advance(Some(0), Some(0), Some(0)),
            BackfillAdvance::Floor
        );
    }

    #[test]
    fn advance_holds_at_created_at_one_then_floors() {
        // oldest=1 moved => hold at 1 (a real page: until:1 re-reads that
        // second); when that page stagnates at 1, next = 0 => Floor.
        assert_eq!(
            next_backfill_advance(Some(1), Some(2), Some(2)),
            BackfillAdvance::Page(1)
        );
        assert_eq!(
            next_backfill_advance(Some(1), Some(1), Some(1)),
            BackfillAdvance::Floor
        );
    }

    #[test]
    fn advance_floor_on_empty_page_at_until_one() {
        // Empty page requested with until=1 ⇒ next = 0 ⇒ Floor.
        assert_eq!(
            next_backfill_advance(None, None, Some(1)),
            BackfillAdvance::Floor
        );
    }

    #[test]
    fn advance_normal_deep_cursor_is_a_page() {
        // Sanity: a deep positive cursor is always a Page, never a Floor.
        assert_eq!(
            next_backfill_advance(
                Some(1_700_000_000),
                Some(1_700_000_500),
                Some(1_700_000_500)
            ),
            BackfillAdvance::Page(1_700_000_000)
        );
        assert_eq!(
            next_backfill_advance(
                Some(1_700_000_000),
                Some(1_700_000_000),
                Some(1_700_000_000)
            ),
            BackfillAdvance::Page(1_699_999_999)
        );
    }

    #[test]
    fn group_index_parses_forward_base() {
        assert_eq!(group_index_of("forward"), Some(0));
    }

    #[test]
    fn group_index_parses_numeric_suffix() {
        assert_eq!(group_index_of("forward-0"), Some(0));
        assert_eq!(group_index_of("forward-1"), Some(1));
        assert_eq!(group_index_of("forward-42"), Some(42));
    }

    #[test]
    fn group_index_rejects_other_prefixes() {
        assert_eq!(group_index_of("backfill-1"), None);
        assert_eq!(group_index_of("forwardX"), None);
        assert_eq!(group_index_of("anything"), None);
        // Dispatch-liveness probe sub_ids must never be misclassified as
        // forward-sub groups — they share a relay-task scope and the
        // `Closed`/split logic in `handle_closed` would otherwise treat
        // a probe close as a forward-sub close with a numeric group
        // suffix. The "probe-" prefix is disjoint from "forward" so this
        // is naturally `None`; this case pins it so a future change to
        // `group_index_of` can't silently break the assumption.
        assert_eq!(group_index_of("probe-deadbeef"), None);
        // Bare "probe" (degenerate; we always append `-{nonce}`) — still
        // None, doesn't accidentally collide with `FORWARD_SUB_BASE`.
        assert_eq!(group_index_of("probe"), None);
    }

    #[test]
    fn status_metric_code_mapping() {
        // Pool status → typed domain enum …
        assert_eq!(
            conn_state(RelayStatus::Connected),
            RelayConnState::Connected
        );
        assert_eq!(
            conn_state(RelayStatus::Connecting),
            RelayConnState::Connecting
        );
        assert_eq!(conn_state(RelayStatus::Pending), RelayConnState::Connecting);
        assert_eq!(
            conn_state(RelayStatus::Initialized),
            RelayConnState::Connecting
        );
        assert_eq!(
            conn_state(RelayStatus::Disconnected),
            RelayConnState::Disconnected
        );
        assert_eq!(
            conn_state(RelayStatus::Terminated),
            RelayConnState::Disconnected
        );
        assert_eq!(
            conn_state(RelayStatus::Banned),
            RelayConnState::Disconnected
        );
        assert_eq!(
            conn_state(RelayStatus::Sleeping),
            RelayConnState::Disconnected
        );
        // … and the domain enum → stable 0/1/2 wire encoding.
        assert_eq!(RelayConnState::Disconnected.code(), 0);
        assert_eq!(RelayConnState::Connected.code(), 1);
        assert_eq!(RelayConnState::Connecting.code(), 2);
    }

    // ---- DecayingBackoff ----

    fn decaying_for_test() -> DecayingBackoff {
        DecayingBackoff::new(
            Duration::from_secs(2),
            Duration::from_secs(16),
            Duration::from_secs(60),
        )
    }

    #[test]
    fn decaying_backoff_starts_at_initial_and_doubles_to_the_cap() {
        let mut b = decaying_for_test();
        let t0 = Instant::now();
        let waits: Vec<u64> = (0..6)
            .map(|i| b.next(t0 + Duration::from_secs(i)).as_secs())
            .collect();
        assert_eq!(waits, vec![2, 4, 8, 16, 16, 16]);
    }

    #[test]
    fn decaying_backoff_starts_over_after_a_quiet_window() {
        let t0 = Instant::now();
        let mut b = decaying_for_test();
        b.next(t0);
        b.next(t0 + Duration::from_secs(1)); // 4 s now
        assert_eq!(
            b.next(t0 + Duration::from_secs(61)),
            Duration::from_secs(2),
            "60 s without an event resets to the initial wait"
        );
        let mut c = decaying_for_test();
        c.next(t0);
        assert_eq!(
            c.next(t0 + Duration::from_secs(59)),
            Duration::from_secs(4),
            "inside the window it keeps doubling"
        );
    }

    #[test]
    fn production_forward_reissue_backoff() {
        // The forward re-issue's shape on dev: 2 s, then doubling to 30 s.
        let mut b = DecayingBackoff::new(
            FORWARD_REISSUE_BACKOFF_INITIAL,
            FORWARD_REISSUE_BACKOFF_MAX,
            FORWARD_REISSUE_RESET_AFTER,
        );
        let t0 = Instant::now();
        let waits: Vec<u64> = (0..6)
            .map(|i| b.next(t0 + Duration::from_secs(i)).as_secs())
            .collect();
        assert_eq!(waits, vec![2, 4, 8, 16, 30, 30]);
    }

    #[test]
    fn production_floor_retry_backoff_holds_at_its_cap() {
        // A kind the relay never accepts, retried each time its wait is up:
        // 60 s doubling to 6 h, then every 6 h, never back to minutes.
        let mut b = floor_retry_backoff(Duration::from_secs(60));
        let mut t = Instant::now();
        let mut waits = Vec::new();
        for _ in 0..12 {
            let w = b.next(t);
            waits.push(w.as_secs());
            t += w;
        }
        assert_eq!(
            waits,
            vec![
                60, 120, 240, 480, 960, 1_920, 3_840, 7_680, 15_360, 21_600, 21_600, 21_600
            ]
        );
    }

    // ---- PressureState ----

    fn pressure_for_test() -> PressureState {
        // Smaller initial/max so doubling math is easy to reason about in tests.
        PressureState::new(Duration::from_millis(100), Duration::from_millis(800))
    }

    #[test]
    fn pressure_state_starts_idle() {
        let p = pressure_for_test();
        assert_eq!(p.backoff(), Duration::ZERO);
        // resume_at is ~now, so wait_required at "now" should be None.
        let now = Instant::now();
        // Either None, or Some(0..a few ns) — accept either.
        match p.wait_required(now) {
            None => {}
            Some(d) => assert!(
                d <= Duration::from_millis(1),
                "expected near-zero, got {d:?}"
            ),
        }
    }

    #[test]
    fn pressure_first_note_sets_initial_backoff() {
        let mut p = pressure_for_test();
        p.note();
        assert_eq!(p.backoff(), Duration::from_millis(100));
        assert!(p.wait_required(Instant::now()).is_some());
    }

    #[test]
    fn pressure_repeated_notes_double_up_to_max() {
        let mut p = pressure_for_test();
        p.note(); // 100
        assert_eq!(p.backoff(), Duration::from_millis(100));
        p.note(); // 200
        assert_eq!(p.backoff(), Duration::from_millis(200));
        p.note(); // 400
        assert_eq!(p.backoff(), Duration::from_millis(400));
        p.note(); // 800 (capped)
        assert_eq!(p.backoff(), Duration::from_millis(800));
        p.note(); // still 800
        assert_eq!(p.backoff(), Duration::from_millis(800));
    }

    #[test]
    fn pressure_eose_with_no_pressure_during_page_resets_backoff() {
        // Simulates a clean page (no NOTICE arrived during it). The relay
        // is healthy again; backoff decays to zero so the next page fires
        // promptly. This is the "happy path" steady state.
        let mut p = pressure_for_test();
        // Before the page, manually drive into a non-zero state to verify the
        // reset, without going through note() (which would set seen_during_page).
        p.backoff = Duration::from_millis(400);
        p.resume_at = Instant::now() + Duration::from_secs(60);
        // No call to note() during this page → seen_during_page stays false.
        let wait = p.on_eose(Instant::now());
        assert_eq!(wait, None, "clean EOSE must not request a wait");
        assert_eq!(p.backoff(), Duration::ZERO, "clean EOSE must reset backoff");
    }

    #[test]
    fn pressure_eose_with_pressure_during_page_keeps_backoff() {
        // Bug-1 invariant: a NOTICE during the page must NOT be wiped by the
        // EOSE that ends that page. Without this, the throttle is a no-op
        // (the original bug — see the integration test).
        let mut p = pressure_for_test();
        p.note();
        let backoff_after_note = p.backoff();
        let wait = p.on_eose(Instant::now());
        assert!(
            wait.is_some(),
            "EOSE-after-pressure must return a wait duration"
        );
        let returned_wait = wait.unwrap();
        // Wait should be roughly the configured backoff; allow slack for
        // the few microseconds elapsed inside the test.
        assert!(
            returned_wait <= backoff_after_note
                && returned_wait >= backoff_after_note.saturating_sub(Duration::from_millis(50)),
            "wait {returned_wait:?} should be ~{backoff_after_note:?}",
        );
        // Backoff itself isn't decayed by on_eose when pressure was seen —
        // it only decays via a clean EOSE (no pressure during page).
        assert_eq!(p.backoff(), backoff_after_note);
    }

    #[test]
    fn pressure_seen_during_page_is_cleared_after_eose() {
        // After on_eose returns, the per-page flag is reset so the next page
        // gets a fresh evaluation. If a NOTICE arrives during page 2, it
        // re-arms; if not, page 2's EOSE decays the backoff.
        let mut p = pressure_for_test();
        p.note(); // seen_during_page = true
        let _ = p.on_eose(Instant::now()); // clears seen_during_page
        // Now simulate a clean page 2: no note() between EOSEs.
        let wait = p.on_eose(Instant::now());
        assert_eq!(
            wait, None,
            "second EOSE without intervening pressure should be clean"
        );
        assert_eq!(p.backoff(), Duration::ZERO);
    }

    #[test]
    fn pressure_wait_required_decays_with_time() {
        let mut p = pressure_for_test();
        p.note(); // backoff = 100ms
        // At resume_at + 1s, no wait required.
        let later = p.resume_at + Duration::from_secs(1);
        assert_eq!(p.wait_required(later), None);
    }

    // ---- probe sub_id format ----

    /// The probe `sub_id` format is contractual: the four notification entry
    /// points (`handle_event`, `RelayMessage::Event`, `RelayMessage::Closed`,
    /// `handle_eose`) match on string equality with
    /// `PendingProbe::sub_id`, and `group_index_of` is brittle to anything
    /// that starts with `"forward"`. If someone ever changes
    /// `PROBE_SUB_BASE` to e.g. `"forward-probe"`, the parser would
    /// classify the probe as a forward-sub group and `handle_closed`
    /// would try to split on it. This test pins the disjoint prefix.
    #[test]
    fn probe_sub_base_is_disjoint_from_forward_sub_base() {
        assert!(
            !PROBE_SUB_BASE.starts_with(FORWARD_SUB_BASE),
            "PROBE_SUB_BASE {PROBE_SUB_BASE:?} must not share a prefix with FORWARD_SUB_BASE {FORWARD_SUB_BASE:?}"
        );
        // And the formatted sub_id is unambiguously a probe id (so the
        // entry-point routing strings are stable across nonces).
        let id = format!("{PROBE_SUB_BASE}-{}", new_nonce());
        assert!(id.starts_with("probe-"), "got {id}");
        assert_eq!(group_index_of(&id), None);
    }
}
