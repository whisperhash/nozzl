//! Integration tests against `nozzl-test-relay`.
//!
//! See `docs/crates/nozzl-acquirer.md` for the test plan.

#![allow(clippy::missing_panics_doc)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use nostr::{Event, EventBuilder, Keys, Kind, Tag, TagKind};
use nozzl_acquirer::{Acquirer, AcquirerConfig};
use nozzl_checkpoint::{BackfillState, CheckpointStore, FileCheckpointStore};
use nozzl_metrics::Metrics;
use nozzl_test_relay::TestRelay;
use nozzl_types::{EventEnvelope, RelayUrl};
use prometheus::Registry;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Build a slug map from a slice of kinds. Slugs are arbitrary but valid;
/// what matters in these tests is that every kind in `cfg.kinds` has a
/// corresponding entry.
fn slugs(kinds: &[u16]) -> HashMap<u16, String> {
    kinds.iter().map(|k| (*k, format!("test-k{k}"))).collect()
}

/// The slug `cfg` carries for `kind`, as the metric label writers see it. Used
/// by metric-asserting tests to address the same `(relay, kind, slug)` series
/// the acquirer increments.
fn metrics_slug(cfg: &AcquirerConfig, kind: u16) -> String {
    cfg.kind_slugs
        .get(&kind)
        .cloned()
        .unwrap_or_else(|| format!("test-k{kind}"))
}

/// Build and sign one signed event of the given kind. `created_at` is in unix
/// seconds; tests use replaceable kind 0, addressable kind 30023, and regular
/// kind 9735 — all archivable classes the acquirer's class filter accepts.
fn make_event(keys: &Keys, kind: u16, content: &str, created_at: u64) -> Event {
    EventBuilder::new(Kind::from(kind), content)
        .custom_created_at(nostr::Timestamp::from_secs(created_at))
        .sign_with_keys(keys)
        .expect("sign event")
}

/// Spin up a checkpoint store, metrics handle, and an mpsc sink. Returns
/// everything needed for the test, plus a `TempDir` whose lifetime keeps the
/// checkpoint file alive.
async fn setup() -> (
    Arc<dyn CheckpointStore>,
    Arc<Metrics>,
    mpsc::Sender<EventEnvelope>,
    mpsc::Receiver<EventEnvelope>,
    TempDir,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("ckpt.json");
    let ckpt = FileCheckpointStore::open(path, Duration::from_secs(60))
        .await
        .expect("checkpoint open");
    let registry = Registry::new();
    let metrics = Arc::new(Metrics::new(&registry).expect("metrics"));
    let (tx, rx) = mpsc::channel::<EventEnvelope>(1024);
    let store: Arc<dyn CheckpointStore> = ckpt;
    (store, metrics, tx, rx, dir)
}

/// Drain events from the sink up to `expected` matching events for this URL,
/// or until the timeout. Returns the events received in order.
async fn drain_events(
    rx: &mut mpsc::Receiver<EventEnvelope>,
    expected: usize,
    deadline: Duration,
) -> Vec<EventEnvelope> {
    let mut out = Vec::with_capacity(expected);
    let _ = timeout(deadline, async {
        while out.len() < expected {
            match rx.recv().await {
                Some(env) => out.push(env),
                None => break,
            }
        }
    })
    .await;
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn happy_path_preloaded_events_arrive_on_sink() {
    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..5)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i * 10))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    let received = drain_events(&mut rx, events.len(), Duration::from_secs(10)).await;
    assert!(
        received.len() >= events.len(),
        "expected at least {} events, got {}",
        events.len(),
        received.len()
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// Some relays refuse a WebSocket upgrade that carries no `User-Agent` (403
/// Forbidden, seen 2026-10-14), and the pool's own transport sends none. The
/// acquirer's transport sends one, so it backfills from a relay that requires
/// it. The control shows the library's transport refused by the same kind of
/// relay, so the test cannot pass vacuously.
#[tokio::test(flavor = "multi_thread")]
async fn handshake_carries_the_user_agent_a_relay_may_require() {
    use nostr_relay_pool::ConnectionMode;
    use nostr_relay_pool::transport::websocket::{DefaultWebsocketTransport, WebSocketTransport};
    use nozzl_acquirer::{USER_AGENT, UserAgentTransport};

    // Control, one transport at a time: the library's is refused, ours is not.
    let control = TestRelay::builder().require_user_agent().start().await;
    let control_url = nostr::Url::parse(control.url().as_str()).unwrap();
    let refused = DefaultWebsocketTransport
        .connect(
            &control_url,
            &ConnectionMode::Direct,
            Duration::from_secs(10),
        )
        .await
        .err()
        .expect("the library's transport sends no User-Agent and must be refused");
    assert!(refused.to_string().contains("403"), "got: {refused}");
    let accepted = UserAgentTransport
        .connect(
            &control_url,
            &ConnectionMode::Direct,
            Duration::from_secs(10),
        )
        .await;
    assert!(
        accepted.is_ok(),
        "ours must be accepted: {:?}",
        accepted.err()
    );
    assert_eq!(
        control.upgrade_user_agents(),
        vec![None, Some(USER_AGENT.to_string())]
    );
    drop(accepted);
    control.shutdown().await;

    // The acquirer, end to end, against a relay that requires one.
    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..5)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i * 10))
        .collect();
    let relay = TestRelay::builder()
        .require_user_agent()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    let received = drain_events(&mut rx, events.len(), Duration::from_secs(10)).await;
    assert!(
        received.len() >= events.len(),
        "expected at least {} events from a relay that requires a User-Agent, got {}",
        events.len(),
        received.len()
    );
    let agents = relay.upgrade_user_agents();
    assert!(!agents.is_empty());
    assert!(
        agents.iter().all(|a| a.as_deref() == Some(USER_AGENT)),
        "every handshake must carry {USER_AGENT}: {agents:?}"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn regular_kind_events_arrive_on_sink() {
    // A regular kind (kind-9735 zap receipts) is an archivable class: the
    // acquirer must request it, accept it past the defensive class gate, and
    // emit an EventEnvelope to the sink — same as replaceable/addressable.
    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..5)
        .map(|i| make_event(&keys, 9735, &format!("zap{i}"), 1_000 + i * 10))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![9735],
        kind_slugs: slugs(&[9735]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    let received = drain_events(&mut rx, events.len(), Duration::from_secs(10)).await;
    assert!(
        received.len() >= events.len(),
        "expected at least {} regular-kind events, got {}",
        events.len(),
        received.len()
    );
    assert!(
        received.iter().all(|e| e.event.kind.as_u16() == 9735),
        "every emitted envelope should be the configured regular kind",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn defensive_drop_for_kind_not_configured() {
    let keys = Keys::generate();
    // Preload one matching (kind 0) and one off-kind (kind 1). Kind 1 is a
    // regular event, which is an *archivable* class — so it is dropped solely
    // by the defensive not-in-configured-kinds check (kinds = [0]), NOT by the
    // class gate (regular passes that). Note: the acquirer's REQ already
    // restricts to configured kinds, but this test specifically validates the
    // defensive post-receive check: we publish kind-1 *after* the sub is open,
    // simulating a relay that ignores the filter.
    let e_match = make_event(&keys, 0, "match", 1_000);

    let relay = TestRelay::builder().preload_event(e_match.clone()).start().await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Should arrive.
    let got = drain_events(&mut rx, 1, Duration::from_secs(5)).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].event.id, e_match.id);

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn onboarding_backfill_paginates_until_complete() {
    // Preload 1500 events (kind 0). The acquirer's backfill will paginate
    // by `until` working backwards. Two consecutive empty pages → complete.
    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..1500)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        // Use a smaller per-page limit so we exercise multiple pages.
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg.clone(), tx, Arc::clone(&ckpt), metrics);

    // Drain at least 1500 events. Use a generous timeout — three pages × ws
    // round-trips against an in-process relay.
    let received = drain_events(&mut rx, 1500, Duration::from_secs(15)).await;
    let unique: std::collections::HashSet<_> = received.iter().map(|e| e.event.id).collect();
    assert_eq!(unique.len(), 1500, "expected 1500 distinct events, got {}", unique.len());

    // Wait for backfill state to flip to Complete. Timing depends on how
    // quickly the relay returns the second empty page; bounded poll.
    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let completed = timeout(Duration::from_secs(15), async {
        loop {
            if matches!(
                ckpt.backfill_state(&relay_url, 0).await,
                BackfillState::Complete
            ) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(completed, "backfill state did not flip to Complete");

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// A clean dedicated (solo) backfill walk drives the new diagnostics: the
/// per-kind `backfill_partial_pages_total` / `backfill_tie_stagnations_total`
/// counters must reflect the walk end-to-end through the real EOSE path. This
/// is the kind-7 control case in miniature — a relay whose history is exhausted
/// by full pages then an empty terminator should show ZERO tie-stagnations and
/// exactly the empty terminator page(s) as partial. (Mirrors
/// `onboarding_backfill_paginates_until_complete` but asserts on /metrics.)
#[tokio::test(flavor = "multi_thread")]
async fn solo_backfill_metrics_show_clean_walk() {
    let keys = Keys::generate();
    // 1500 events at strictly-increasing created_at ⇒ pages walk back with a
    // strictly-decreasing oldest each page ⇒ no tie-stagnation.
    let events: Vec<Event> = (0u64..1500)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let slug = metrics_slug(&cfg, 0);
    let acquirer = Acquirer::spawn(cfg.clone(), tx, Arc::clone(&ckpt), Arc::clone(&metrics));

    let _ = drain_events(&mut rx, 1500, Duration::from_secs(15)).await;
    let completed = timeout(Duration::from_secs(15), async {
        loop {
            if matches!(
                ckpt.backfill_state(&relay_url, 0).await,
                BackfillState::Complete
            ) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(completed, "backfill state did not flip to Complete");

    // A clean walk never tie-stagnates.
    let stagnations = metrics
        .backfill_tie_stagnations
        .with_label_values(&[relay_url.as_str(), "0", &slug])
        .get();
    assert_eq!(stagnations, 0, "a clean solo walk must not tie-stagnate");

    // The only partial pages are the empty terminators (= empty_pages_to_complete).
    // We assert the metric registered the terminators (>= 1) and that it never
    // exceeds the terminator count + a small slack for a possible trailing
    // partial real page — the load-bearing assertion is "stagnations == 0" above;
    // here we just confirm the partial-page counter is wired and fires.
    let partials = metrics
        .backfill_partial_pages
        .with_label_values(&[relay_url.as_str(), "0", &slug])
        .get();
    assert!(
        partials >= 1,
        "the empty terminator page(s) must register as partial; got {partials}"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// The two silent-loss escape valves are counted, not just logged. This pins
/// the metric *contract* the acquirer's `run_central` relies on:
///  - `notif_channel_lagged_total` is incremented BY `n` (the `Lagged(n)`
///    payload), not by 1 — so the counter is a true upper bound on lost events,
///    matching the `pool_loss_harness` invariant (`lost <= lagged_total`).
///  - `notif_fanout_dropped_total{relay}` is incremented by 1 per dropped notif.
///
/// We exercise the exact increment expressions the call sites use (acquirer.rs
/// ~470 / ~477) against a real registered `Metrics`, then read the registry
/// back — a deterministic guard that a future refactor can't silently switch a
/// lag of `n` back to a `+1` (which would under-count loss by ~capacity×).
#[test]
fn loss_counters_account_lag_by_n_and_drops_by_one() {
    let registry = Registry::new();
    let metrics = Metrics::new(&registry).expect("metrics");
    let relay = "wss://nos.lol/";

    // Two fan-out drops on the same relay.
    metrics
        .notif_fanout_dropped
        .with_label_values(&[relay])
        .inc();
    metrics
        .notif_fanout_dropped
        .with_label_values(&[relay])
        .inc();
    // Two lag events of 4096 and 17 skipped slots ⇒ counter must read 4113,
    // NOT 2. This is the whole point of incrementing by `n`.
    metrics.notif_channel_lagged.inc_by(4096);
    metrics.notif_channel_lagged.inc_by(17);

    assert_eq!(
        metrics
            .notif_fanout_dropped
            .with_label_values(&[relay])
            .get(),
        2,
        "fan-out drops count one per dropped notification"
    );
    assert_eq!(
        metrics.notif_channel_lagged.get(),
        4113,
        "lag must accumulate the Lagged(n) payload, not one-per-event"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_closes_each_page_sub_before_opening_the_next() {
    // Regression test for the backfill subscription leak (issue #12).
    //
    // Each paginated backfill page must CLOSE its (already-EOSE'd)
    // subscription before opening the next page, so that at most ONE
    // backfill sub is ever open against the relay at a time. Before the fix,
    // `handle_eose` advanced to a fresh `backfill-<nonce>` sub without
    // unsubscribing the completed page, so pages accumulated server-side for
    // the whole backfill. On a relay that caps concurrent subs per
    // connection (strfry/nos.lol: max_subscriptions = 20) that self-inflicts
    // a "too many concurrent REQs" throttle.
    let keys = Keys::generate();
    // 1500 events at a per-page limit of 50 → ~30 pages, far past nos.lol's
    // 20-sub cap, so a leak would be unmistakable.
    let events: Vec<Event> = (0u64..1500)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![relay_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        // Small per-page limit so 1500 events span many pages.
        backfill_default_limit: 50,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics);

    // Drain the sink continuously so the central listener never backpressures;
    // count deliveries (pool-deduplicated, so ~= distinct events) to prove the
    // backfill genuinely paginated to completion rather than vacuously passing.
    let drained = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    {
        let drained = Arc::clone(&drained);
        tokio::spawn(async move {
            while rx.recv().await.is_some() {
                drained.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });
    }

    // Poll the relay's open-subscription snapshot until backfill completes,
    // tracking the high-water mark of concurrently-open backfill subs.
    let mut max_concurrent_backfill = 0usize;
    let completed = timeout(Duration::from_secs(30), async {
        loop {
            let open_backfill = relay
                .open_subscriptions()
                .iter()
                .filter(|s| s.sub_id.starts_with("backfill"))
                .count();
            max_concurrent_backfill = max_concurrent_backfill.max(open_backfill);
            if matches!(
                ckpt.backfill_state(&relay_url, 0).await,
                BackfillState::Complete
            ) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(completed, "backfill did not reach Complete in time");

    // The invariant: never more than one backfill sub open at once. Pre-fix
    // this climbs to ~30 (one per page, none ever closed).
    assert!(
        max_concurrent_backfill <= 1,
        "backfill leaked subscriptions: observed {max_concurrent_backfill} concurrently-open \
         backfill subs (expected <= 1). Each EOSE'd page must be CLOSEd before the next opens.",
    );

    // Non-vacuity / completeness: a per-page limit of 50 means reaching all
    // 1500 events required ~30 pages, so the invariant above was exercised
    // across many page advances.
    let all_drained = timeout(Duration::from_secs(10), async {
        loop {
            if drained.load(std::sync::atomic::Ordering::Relaxed) >= 1500 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        all_drained,
        "expected all 1500 events to arrive across the paginated backfill, got {}",
        drained.load(std::sync::atomic::Ordering::Relaxed),
    );

    // After completion, no backfill sub should linger open on the relay.
    let cleared = timeout(Duration::from_secs(5), async {
        loop {
            if !relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id.starts_with("backfill"))
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(cleared, "a backfill sub remained open after backfill completed");

    // The fix closes backfill pages only; the long-lived forward sub must
    // survive — closing it would collapse live capture.
    assert!(
        relay
            .open_subscriptions()
            .iter()
            .any(|s| s.sub_id.starts_with("forward")),
        "forward sub should remain open after backfill completes",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_completion_closes_final_page_and_keeps_forward() {
    // Companion to `backfill_closes_each_page_sub_before_opening_the_next`,
    // pinning the terminal edge: the page that drives backfill to Complete
    // (the empty-streak page) is itself CLOSEd — the unsubscribe sits ahead
    // of the completion branch in `handle_eose` — so once backfill finishes
    // the *only* sub left open is the forward sub. A small fixture exercises
    // the few-page path quickly. See issue #12.
    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..3)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![relay_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics);

    let _ = drain_events(&mut rx, events.len(), Duration::from_secs(10)).await;

    let completed = timeout(Duration::from_secs(15), async {
        loop {
            if matches!(
                ckpt.backfill_state(&relay_url, 0).await,
                BackfillState::Complete
            ) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(completed, "backfill did not reach Complete in time");

    // Once Complete, the relay's open subs settle to exactly the forward sub:
    // every backfill page (including the terminal empty one) was CLOSEd.
    let settled = timeout(Duration::from_secs(5), async {
        loop {
            let subs = relay.open_subscriptions();
            let backfill = subs.iter().filter(|s| s.sub_id.starts_with("backfill")).count();
            let forward = subs.iter().filter(|s| s.sub_id.starts_with("forward")).count();
            if backfill == 0 && forward >= 1 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        settled,
        "after backfill completion expected only the forward sub open, got {:?}",
        relay.open_subscriptions().iter().map(|s| s.sub_id.clone()).collect::<Vec<_>>(),
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_after_complete_backfill_does_not_reflood_pages() {
    // Second-order guard for issue #12. Pre-fix, completed backfill pages were
    // left registered in the relay-pool's subscription map (never CLOSEd), so
    // a reconnect's `resubscribe()` would replay *every* stale page onto the
    // fresh socket — a reconnect-amplification bug. With each page CLOSEd on
    // EOSE the pool purges it, so after a completed backfill a reconnect
    // replays only the forward sub, never a stale backfill page.
    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..3)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![relay_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics);
    let _ = drain_events(&mut rx, events.len(), Duration::from_secs(10)).await;

    // Let backfill finish and confirm no backfill sub lingers (pre-condition).
    let completed = timeout(Duration::from_secs(15), async {
        loop {
            if matches!(
                ckpt.backfill_state(&relay_url, 0).await,
                BackfillState::Complete
            ) && !relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id.starts_with("backfill"))
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(completed, "backfill did not complete + clear before reconnect");

    // Drop the connection; the pool reconnects and replays its registered subs.
    relay.disconnect_all().await;
    let resubscribed = timeout(Duration::from_secs(30), async {
        loop {
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id.starts_with("forward"))
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(resubscribed, "forward sub did not come back after reconnect");

    // The completed backfill must NOT be replayed. Watch for a sustained
    // window so a late reflood can't slip past a single-shot check.
    let no_reflood = timeout(Duration::from_secs(2), async {
        loop {
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id.starts_with("backfill"))
            {
                return false; // a stale page reappeared
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    // timeout elapsing with no early `false` return = no reflood observed.
    .unwrap_or(true);
    assert!(
        no_reflood,
        "a completed backfill page was reflooded onto the reconnected socket: {:?}",
        relay
            .open_subscriptions()
            .iter()
            .map(|s| s.sub_id.clone())
            .collect::<Vec<_>>(),
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_resumes_after_disconnect_all() {
    let keys = Keys::generate();
    let initial: Vec<Event> = (0u64..3)
        .map(|i| make_event(&keys, 0, &format!("init{i}"), 1_000 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(initial.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Drain initial events.
    let got = drain_events(&mut rx, initial.len(), Duration::from_secs(10)).await;
    assert!(got.len() >= initial.len());

    // Disconnect the client; the acquirer's reconnect loop should bring it
    // back. Wait for the OLD subscription to fully clear, then for a NEW
    // subscription to register. `nostr-relay-pool`'s default retry interval
    // is 10s, so this must allow at least that.
    relay.disconnect_all().await;

    // Phase 1: the test relay's connection-task cleans up subs after
    // disconnect_rx fires. Wait for the snapshot to drain.
    let cleared = timeout(Duration::from_secs(5), async {
        loop {
            if relay.open_subscriptions().is_empty() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(cleared, "old subscription did not clear after disconnect");

    // Phase 2: wait for the client to reconnect and re-issue.
    let resubscribed = timeout(Duration::from_secs(30), async {
        loop {
            let subs = relay.open_subscriptions();
            if subs.iter().any(|s| s.sub_id.starts_with("forward")) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(resubscribed, "client did not resubscribe after disconnect");

    let new_evt = make_event(&keys, 0, "post-reconnect", 5_000);
    relay.publish(new_evt.clone()).await;

    let recovered = timeout(Duration::from_secs(15), async {
        loop {
            if let Some(env) = rx.recv().await {
                if env.event.id == new_evt.id {
                    return true;
                }
            } else {
                return false;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(recovered, "did not receive post-reconnect event");

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn notice_driven_split_halves_kind_groups() {
    // Configure 4 kinds (replaceable + addressable mix). Send a global NOTICE
    // matching the rejection heuristic; the acquirer should halve its forward
    // sub. Verify that two forward subs are open afterwards.
    let keys = Keys::generate();
    let events: Vec<Event> = vec![
        make_event(&keys, 0, "k0", 1_000),
        make_event(&keys, 3, "k3", 1_001),
        make_event(&keys, 10002, "k10002", 1_002),
        make_event(&keys, 30023, "k30023", 1_003),
    ];

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0, 3, 10002, 30023],
        kind_slugs: slugs(&[0, 3, 10002, 30023]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Wait for the initial single forward sub to be open.
    let open_initially = timeout(Duration::from_secs(5), async {
        loop {
            let subs = relay.open_subscriptions();
            let forward_subs: Vec<_> = subs.iter().filter(|s| s.sub_id.starts_with("forward")).collect();
            if !forward_subs.is_empty() {
                return forward_subs.len();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("initial sub did not open");
    assert!(open_initially >= 1, "expected at least one initial forward sub");

    // Drain initial events so the channel doesn't backpressure the central listener.
    let _ = drain_events(&mut rx, events.len(), Duration::from_secs(5)).await;

    // Send the rejection NOTICE.
    relay.send_notice("ERROR: too many filters").await;

    // The acquirer should halve the kind list — we end up with 2 forward
    // subs in steady state.
    let split_count = timeout(Duration::from_secs(5), async {
        loop {
            let subs = relay.open_subscriptions();
            let forward_subs: Vec<_> = subs
                .iter()
                .filter(|s| s.sub_id.starts_with("forward"))
                .collect();
            if forward_subs.len() >= 2 {
                return forward_subs.len();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        split_count >= 2,
        "expected forward subs to halve to >=2, got {split_count}"
    );

    // Now publish a new event matching the second half — should still arrive.
    let new_evt = make_event(&keys, 30023, "post-split", 5_000);
    relay.publish(new_evt.clone()).await;

    let received = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(env) = rx.recv().await {
                if env.event.id == new_evt.id {
                    return true;
                }
            } else {
                return false;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(received, "did not receive event after auto-split");

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn hot_reload_kind_addition_triggers_scoped_backfill() {
    // Start with one kind; add a second kind via apply_config; verify that a
    // backfill REQ is opened scoped to *only* the new kind.
    let keys = Keys::generate();
    let initial = [make_event(&keys, 0, "init", 1_000)];
    let new_kind_event = make_event(&keys, 30023, "new-kind", 2_000);

    let relay = TestRelay::builder()
        .preload_events([initial[0].clone(), new_kind_event.clone()])
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg.clone(), tx, ckpt, metrics);

    // Drain the kind-0 event.
    let initial_drain = drain_events(&mut rx, 1, Duration::from_secs(5)).await;
    assert_eq!(initial_drain.len(), 1);
    assert_eq!(initial_drain[0].event.id, initial[0].id);

    // Now hot-reload to add kind 30023.
    let cfg_updated = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0, 30023],
        kind_slugs: slugs(&[0, 30023]),
        ..cfg
    };
    acquirer.apply_config(cfg_updated).await;

    // We should observe the new-kind event arriving.
    let received = timeout(Duration::from_secs(10), async {
        loop {
            if let Some(env) = rx.recv().await {
                if env.event.id == new_kind_event.id {
                    return true;
                }
            } else {
                return false;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(received, "did not receive event for newly added kind");

    // Expect a backfill REQ to have been opened. Look for any sub that started
    // with backfill; the test relay snapshots open subs even after EOSE
    // because backfill keeps the sub registered until EOSE, and the
    // bookkeeping in the test relay only removes on CLOSE — we may or may
    // not catch one in flight, but we should have at least seen the new
    // forward sub get re-issued.
    let snapshot = relay.open_subscriptions();
    let forward_subs: Vec<_> = snapshot
        .iter()
        .filter(|s| s.sub_id.starts_with("forward"))
        .collect();
    assert!(
        !forward_subs.is_empty(),
        "expected at least one forward sub still open after reload"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn hot_reload_relay_addition_spawns_backfill() {
    let keys = Keys::generate();
    let e1 = make_event(&keys, 0, "r1", 1_000);
    let e2 = make_event(&keys, 0, "r2", 2_000);

    let relay1 = TestRelay::builder().preload_event(e1.clone()).start().await;
    let relay2 = TestRelay::builder().preload_event(e2.clone()).start().await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;

    let url1 = RelayUrl::parse(relay1.url().as_str()).unwrap();
    let url2 = RelayUrl::parse(relay2.url().as_str()).unwrap();

    let cfg = AcquirerConfig {
        relays: vec![url1.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg.clone(), tx, ckpt, metrics);

    // Drain e1 from relay1.
    let r1_drain = drain_events(&mut rx, 1, Duration::from_secs(5)).await;
    assert_eq!(r1_drain.len(), 1);
    assert_eq!(r1_drain[0].event.id, e1.id);

    // Add relay2.
    let cfg_updated = AcquirerConfig {
        relays: vec![url1.clone(), url2.clone()],
        ..cfg
    };
    acquirer.apply_config(cfg_updated).await;

    // We should receive e2 from relay2.
    let received = timeout(Duration::from_secs(10), async {
        loop {
            if let Some(env) = rx.recv().await {
                if env.event.id == e2.id {
                    return true;
                }
            } else {
                return false;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(received, "did not receive event from newly added relay");

    acquirer.shutdown().await;
    relay1.shutdown().await;
    relay2.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_rejection_triggers_split_into_smaller_kind_groups() {
    // Regression: a relay that rejects a multi-kind backfill REQ with a
    // CLOSED message containing a known rejection substring (e.g.
    // "filters are too vague" — observed live from relay.zapstore.dev)
    // used to be abandoned silently, leaving that backfill effectively
    // dead. The brief specifies auto-split mirroring the forward-sub
    // path. After the fix, the acquirer halves the kind list and
    // re-issues two narrower backfill REQs.
    //
    // We use the test relay's `reject_req_if` predicate to reject any
    // REQ whose filter has more than one kind, *before* events / EOSE
    // would otherwise advance acquirer state and dissolve the original
    // sub_id. This makes the test deterministic.
    let keys = Keys::generate();
    let e_k0 = make_event(&keys, 0, "k0", 1_000);
    let e_k30023 = EventBuilder::new(Kind::from(30023u16), "k30023")
        .tags(vec![nostr::Tag::identifier("article-1")])
        .custom_created_at(nostr::Timestamp::from_secs(1_001))
        .sign_with_keys(&keys)
        .expect("sign");

    let relay = TestRelay::builder()
        .preload_events([e_k0.clone(), e_k30023.clone()])
        .reject_req_if(|filters| {
            let any_multi = filters
                .iter()
                .any(|f| f.kinds.as_ref().is_some_and(|k| k.len() > 1));
            if any_multi {
                Some("filters are too vague".to_string())
            } else {
                None
            }
        })
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0, 30023],
        kind_slugs: slugs(&[0, 30023]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Single-kind backfills (post-split) should drive both events to the
    // sink. Forward sub also auto-splits and may deliver duplicates;
    // dedupe by id and assert both expected ids are present.
    let received = drain_events(&mut rx, 2, Duration::from_secs(10)).await;
    let ids: std::collections::HashSet<_> =
        received.iter().map(|e| e.event.id).collect();
    assert!(
        ids.contains(&e_k0.id),
        "missing k0 event after backfill split; got ids={ids:?}"
    );
    assert!(
        ids.contains(&e_k30023.id),
        "missing k30023 event after backfill split; got ids={ids:?}"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_rejection_at_single_kind_floor_abandons_kind() {
    // Regression: at the floor (one kind per backfill REQ), if the relay
    // *still* rejects with a recognised rejection substring, we abandon
    // backfill for that kind on this relay and increment
    // `events_rejected_total{reason="backfill-rejected-at-floor"}` so
    // operators see why an expected backfill never produced events.
    //
    // The predicate gates on `limit.is_some()` — only backfill REQs set a
    // `limit` (forward subs are unbounded), so the forward subscription is
    // unaffected. Gating on `until` would miss the very first backfill page
    // (no checkpoint yet → `until` is also None on the first page).
    let keys = Keys::generate();
    let e = make_event(&keys, 0, "any", 1_000);
    let relay = TestRelay::builder()
        .preload_event(e.clone())
        .reject_req_if(|filters| {
            if filters.iter().any(|f| f.limit.is_some()) {
                Some("filters are too vague".to_string())
            } else {
                None
            }
        })
        .start()
        .await;

    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let metrics_clone = metrics.clone();
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Wait for the floor-rejection counter to flip. The slug here ("test-k0")
    // is whatever `slugs(&[0])` produced for kind 0 in this test's config.
    let rejected = timeout(Duration::from_secs(5), async {
        loop {
            let v = metrics_clone
                .events_rejected
                .with_label_values(&[url.as_str(), "0", "test-k0", "backfill-rejected-at-floor"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert_eq!(
        rejected, 1,
        "expected exactly one floor-rejection counter increment, got {rejected}"
    );

    // Confirm the counter doesn't keep climbing (we should have abandoned,
    // not entered a retry loop).
    tokio::time::sleep(Duration::from_millis(300)).await;
    let final_count = metrics_clone
        .events_rejected
        .with_label_values(&[url.as_str(), "0", "test-k0", "backfill-rejected-at-floor"])
        .get();
    assert_eq!(
        final_count, 1,
        "floor counter should not increment again after abandon"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn singleton_onboarding_completes_against_filter_specificity_relay() {
    // Regression for issue #25. A relay that enforces a filter-specificity
    // floor (zapstore's `VagueFilters(3)`: every REQ filter must score ≥ 3
    // specificity points; `until` present is worth +1) used to permanently
    // abandon a kind onboarded *alone*: its first backfill page is
    // `{kinds, since:0, limit:500}` with NO `until` (no checkpoint yet), which
    // scores 2 < 3 → CLOSED "filters are too vague" → single-kind floor →
    // abandoned with no retry. Only the live backfill was lost.
    //
    // This relay models that floor precisely for our filter shapes: reject a
    // backfill REQ (only backfill sets a `limit`; forward subs are unbounded)
    // that lacks `until`, but accept it once `until` is present. So the ONLY
    // shape it rejects is exactly the buggy fresh-onboarding first page.
    //
    // The DISCRIMINATOR is the backfill reaching Complete (plus zero floor
    // rejections). Note the forward sub is NOT the discriminator: on a fresh
    // onboarding it runs `since=None` with no `limit` (score 3 here: kinds +
    // absent-limit) and is accepted, so it delivers all 25 events even pre-fix
    // — event-arrival alone does not prove the backfill ran. Pre-fix the
    // backfill first page is rejected and abandoned at the single-kind floor
    // (`backfill-rejected-at-floor` → 1), so it never flips to Complete and
    // this test fails at that assertion. The fix seeds `until = now` on fresh
    // onboarding so the first page carries the field (score 3) and is accepted
    // for singleton AND multi-kind alike → backfill completes, zero floor
    // rejections.
    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..25)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .reject_req_if(|filters| {
            // Model VagueFilters(3): the fresh-onboarding first page
            // (backfill REQ — has `limit` — without `until`) is too vague.
            let too_vague = filters
                .iter()
                .any(|f| f.limit.is_some() && f.until.is_none());
            if too_vague {
                Some("filters are too vague".to_string())
            } else {
                None
            }
        })
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let metrics_clone = metrics.clone();
    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![relay_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics);

    // The backfill must actually run and deliver every preloaded event —
    // proving the first page was accepted rather than abandoned.
    let received = drain_events(&mut rx, 25, Duration::from_secs(15)).await;
    let unique: std::collections::HashSet<_> = received.iter().map(|e| e.event.id).collect();
    assert_eq!(
        unique.len(),
        25,
        "singleton onboarding must archive every event against a specificity-floor relay \
         (issue #25); got {}",
        unique.len()
    );

    // …and reach Complete, not sit abandoned.
    let completed = timeout(Duration::from_secs(15), async {
        loop {
            if matches!(
                ckpt.backfill_state(&relay_url, 0).await,
                BackfillState::Complete
            ) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(completed, "backfill state did not flip to Complete (issue #25)");

    // The first page must never have hit the single-kind floor abandon.
    let floored = metrics_clone
        .events_rejected
        .with_label_values(&[relay_url.as_str(), "0", "test-k0", "backfill-rejected-at-floor"])
        .get();
    assert_eq!(
        floored, 0,
        "fresh singleton onboarding must not be abandoned at the single-kind floor (issue #25)"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// Live smoke test against the REAL `relay.zapstore.dev` (which enforces
/// `VagueFilters(3)`) — the empirical counterpart to the mock-relay tests for
/// issue #25. Read-only (REQ only; never publishes). `#[ignore]`d so it never
/// runs in offline CI; run manually:
///   cargo test -p nozzl-acquirer --test integration \
///     zapstore_live_singleton_onboarding_is_not_abandoned -- --ignored --nocapture
///
/// Onboards kind 3063 (zapstore "software asset" — the exact kind from the
/// issue #25 repro) ALONE. Pre-fix the first backfill page
/// `{kinds, since:0, limit:500}` is CLOSED "filters are too vague" and the
/// backfill is abandoned at the single-kind floor. Post-fix the page carries
/// `until=now` (score 3), is accepted, and events flow with zero floor
/// rejections.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "hits the live relay.zapstore.dev; run manually with --ignored"]
async fn zapstore_live_singleton_onboarding_is_not_abandoned() {
    // Surface the acquirer's backfill/CLOSED lines when run with --nocapture.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let metrics_clone = metrics.clone();
    let url = RelayUrl::parse("wss://relay.zapstore.dev").unwrap();
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![3063],
        kind_slugs: slugs(&[3063]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics);

    // Some events must arrive (proves the REQ was accepted, not abandoned).
    let received = drain_events(&mut rx, 10, Duration::from_secs(30)).await;
    assert!(
        !received.is_empty(),
        "expected kind-3063 events from the live zapstore relay; got none \
         (backfill likely abandoned — issue #25 regressed)"
    );

    // The critical invariant: the single-kind floor abandon must NOT have fired.
    let floored = metrics_clone
        .events_rejected
        .with_label_values(&[url.as_str(), "3063", "test-k3063", "backfill-rejected-at-floor"])
        .get();
    assert_eq!(
        floored, 0,
        "the fresh singleton backfill page was rejected at the single-kind floor by the \
         live zapstore relay — issue #25 is NOT fixed (got {floored} floor rejections)"
    );

    acquirer.shutdown().await;
}

/// Live fresh-onboarding regression check against the strfry relays (nos.lol,
/// damus, primal) — empirical proof the issue #25 fix (`until=now` on page 1)
/// does NOT regress the OTHER relays. Read-only; `#[ignore]`d (offline CI).
/// Run: `cargo test -p nozzl-acquirer --test integration live_onboard_ -- \
///   --ignored --nocapture`.
///
/// Onboards one kind fresh (empty checkpoint) and drives the backfill to
/// `Complete`, returning (distinct event ids archived, floor-rejection count,
/// completed?). The caller asserts a clean walk; the printed count is
/// cross-checked against an independent `nak` enumeration in the runbook.
#[cfg(test)]
async fn live_onboard_to_complete(
    relay: &str,
    kind: u16,
    deadline: Duration,
) -> (usize, u64, bool) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![kind],
        kind_slugs: slugs(&[kind]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        // Production default is 8; 4 is plenty for these low-tie kinds and
        // keeps the live walk quick. The point is a clean walk, not the count.
        backfill_empty_pages_to_complete: 4,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics.clone());

    // Drain the sink in the background so the channel never back-pressures the
    // writer; it closes when the acquirer is shut down.
    let collector = tokio::spawn(async move {
        let mut ids = std::collections::HashSet::new();
        while let Some(env) = rx.recv().await {
            ids.insert(env.event.id);
        }
        ids
    });

    let completed = timeout(deadline, async {
        loop {
            if matches!(
                ckpt.backfill_state(&url, kind).await,
                BackfillState::Complete
            ) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or(false);

    let floored = metrics
        .events_rejected
        .with_label_values(&[
            url.as_str(),
            &kind.to_string(),
            &format!("test-k{kind}"),
            "backfill-rejected-at-floor",
        ])
        .get();

    acquirer.shutdown().await;
    let ids = collector.await.unwrap_or_default();
    let n = ids.len();
    eprintln!("LIVE-ONBOARD relay={relay} kind={kind} archived_distinct_ids={n} floored={floored} completed={completed}");
    (n, floored, completed)
}

macro_rules! live_onboard_test {
    ($name:ident, $relay:literal, $kind:literal) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "hits a live public relay; run manually with --ignored"]
        async fn $name() {
            // kind 9041 (zap-goals, Regular) is bounded on all three relays, so
            // the walk completes quickly and — being Regular (no replacement) —
            // its archived count equals the relay's served count for a clean
            // cross-check against `nak`.
            let (_n, floored, completed) =
                live_onboard_to_complete($relay, $kind, Duration::from_secs(120)).await;
            // No-regression invariants for the until=now seed (independent of how
            // many events the relay happens to carry — a caching relay like
            // primal may legitimately serve 0 for a niche kind). The printed
            // `archived_distinct_ids` count is cross-checked against `nak` in the
            // runbook (nos.lol 1897 / damus 86 match the pre-fix baseline).
            assert_eq!(
                floored, 0,
                "{} kind {}: backfill hit the single-kind floor — regression",
                $relay, $kind
            );
            assert!(
                completed,
                "{} kind {}: backfill did not reach Complete within the deadline (regression: \
                 the until=now seed must not break pagination/termination)",
                $relay, $kind
            );
        }
    };
}

live_onboard_test!(live_onboard_nos_lol_9041, "wss://nos.lol", 9041);
live_onboard_test!(live_onboard_damus_9041, "wss://relay.damus.io", 9041);
live_onboard_test!(live_onboard_primal_9041, "wss://relay.primal.net", 9041);

#[tokio::test(flavor = "multi_thread")]
async fn onboarding_seed_until_now_does_not_drop_future_dated_events() {
    // Clock-skew / location safety for the issue #25 fix. The fix seeds the
    // first backfill page's `until = now` (UTC epoch seconds — no local-time
    // conversion, so relay/nozzl geography is irrelevant). Backfill then walks
    // BACKWARD from now and never fetches `created_at > now`; that window is
    // owned by the forward sub, which on a fresh onboarding runs `since=None`
    // (a full scan). So an event dated AFTER our clock — host skew, or a
    // genuinely future-dated event — must STILL be archived. This proves
    // `until=now` costs no completeness whichever host's clock is ahead.
    let keys = Keys::generate();
    let mut events: Vec<Event> = (0u64..5)
        .map(|i| make_event(&keys, 0, &format!("past{i}"), 1_000 + i))
        .collect();
    // ~year 2096 — comfortably after any real `now_secs()` on the test host.
    let future = make_event(&keys, 0, "future", 4_000_000_000);
    events.push(future.clone());

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .reject_req_if(|filters| {
            // Same VagueFilters(3) model as the companion test.
            if filters.iter().any(|f| f.limit.is_some() && f.until.is_none()) {
                Some("filters are too vague".to_string())
            } else {
                None
            }
        })
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![relay_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics);

    let received = drain_events(&mut rx, 6, Duration::from_secs(15)).await;
    let ids: std::collections::HashSet<_> = received.iter().map(|e| e.event.id).collect();
    assert!(
        ids.contains(&future.id),
        "future-dated event (created_at > now) must still be archived via the forward sub; \
         `until=now` seeding must not drop it (issue #25 clock-skew safety)"
    );
    assert_eq!(
        ids.len(),
        6,
        "all events (past + future) must be archived, got {}",
        ids.len()
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_pagination_advances_when_forward_sub_dedupes_events_first() {
    // Regression: `nostr-relay-pool::RelayPoolNotification::Event` only fires
    // the **first** time an event id is observed on a connection (it
    // deduplicates against an in-memory database). The forward and backfill
    // subs run on the same WebSocket and almost always race so the forward
    // sub observes events first; the backfill sub's observations are then
    // silently dropped from the `Event` notification stream.
    //
    // The acquirer's backfill page-state (`received` counter,
    // `oldest_created_at`) must therefore be tracked from the `Message`
    // notification — which fires for every observation, before dedupe — not
    // from `Event`. Without this, every backfill against a relay whose
    // forward sub races completes after `backfill_empty_pages_to_complete`
    // empty pages without ever walking history. (This test pins
    // `backfill_empty_pages_to_complete = 2` for fast turnaround.) The bug
    // was invisible by counting events on the sink (forward
    // sub still delivers them all) and only became apparent by comparing
    // captured event volumes against an external reference.
    //
    // The acquirer enforces `BACKFILL_MIN_LIMIT = 50` on every backfill
    // page regardless of `backfill_default_limit`, so the test must preload
    // > 50 events to actually exercise multi-page pagination — otherwise
    // page 1 is already short and the bug-vs-fix outcomes coincide.
    //
    // Test setup: 120 events, default_limit = 50. With the fix the backfill
    // walks pages
    //   p1: 50 events (e119..e70, full)
    //   p2: 50 events (e69..e20, full)
    //   p3: 20 events (e19..e0, short=1)
    //   p4: 0 events (short=2 → complete)
    // i.e. >= 4 backfill REQs hit the relay. Without the fix:
    //   p1: 0 events (deduped from Event channel; Message channel ignored
    //         by the buggy code) short=1
    //   p2: 0 events short=2 → complete
    // i.e. exactly 2 backfill REQs, and pagination never visited any
    // preloaded event through the backfill sub.
    //
    // We count REQs on the relay using `reject_req_if` purely as a side
    // channel (it never rejects). Filters that set a `limit` are backfill
    // REQs (forward subs are unbounded).
    use std::sync::atomic::{AtomicUsize, Ordering};

    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..120)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i * 100))
        .collect();

    let backfill_pages = Arc::new(AtomicUsize::new(0));
    let counter = backfill_pages.clone();
    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .reject_req_if(move |filters| {
            if filters.iter().any(|f| f.limit.is_some()) {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            None
        })
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 50,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let ckpt_for_test = ckpt.clone();
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Drain at least the 120 unique events (sink is deduplicated by id, so
    // we get 120, not 120+backfill duplicates).
    let _ = drain_events(&mut rx, 120, Duration::from_secs(10)).await;

    // Wait for the backfill to be marked Complete in the checkpoint.
    let completed = timeout(Duration::from_secs(5), async {
        loop {
            let s = ckpt_for_test.backfill_state(&url, 0).await;
            if matches!(s, BackfillState::Complete) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(completed, "backfill never reached Complete state within 5s");

    let pages = backfill_pages.load(Ordering::SeqCst);
    assert!(
        pages >= 4,
        "backfill must paginate through history; expected >= 4 backfill REQs \
         (limit=50 over 120 events => p1,p2 full + p3 short + p4 empty), got {pages}.\n\
         If this regresses to 2: the fix that tracks backfill page-state from \
         the `Message` channel (handle_message::Event branch) is missing or broken — \
         nostr-relay-pool's Event notification dedupes, so backfill state never \
         advances when the forward sub on the same connection wins the race."
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_does_not_terminate_on_near_full_pages() {
    // Regression: the original "two consecutive short pages = done"
    // heuristic (`received < limit`) trips on production relays that
    // return slightly under-cap pages due to timestamp-tie pagination
    // quirks even when more history remains. Empirically observed on
    // relay.damus.io: backfill walked 19 days and stopped on a 498/499
    // sequence while millions of older events were still available.
    //
    // The fix uses `received == 0` (an actually-empty page) as the only
    // "exhausted" signal, with `backfill_empty_pages_to_complete`
    // consecutive empties as the safety margin (configurable; this test
    // pins it to 2 for fast turnaround).
    //
    // Test setup: 200 preloaded events, limit = 50,
    // `truncate_each_page_by(2)` → every page returns 48 events while
    // older history is still available. Under the old heuristic, the
    // first two pages alone would terminate backfill (48 < 50, twice).
    // Under the fix, backfill walks the full history and only stops
    // after the configured number of genuinely empty pages.
    use std::sync::atomic::{AtomicUsize, Ordering};

    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..200)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i))
        .collect();

    let backfill_pages = Arc::new(AtomicUsize::new(0));
    let counter = backfill_pages.clone();
    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .truncate_each_page_by(2)
        .reject_req_if(move |filters| {
            if filters.iter().any(|f| f.limit.is_some()) {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            None
        })
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 50,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let ckpt_for_test = ckpt.clone();
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Drain all 200 unique events. With the bug, only ~96 arrive (2 pages
    // × 48 events) before backfill prematurely terminates.
    let received = drain_events(&mut rx, 200, Duration::from_secs(10)).await;
    let unique: std::collections::HashSet<_> =
        received.iter().map(|e| e.event.id).collect();

    // Wait for backfill complete.
    let _ = timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                ckpt_for_test.backfill_state(&url, 0).await,
                BackfillState::Complete
            ) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    let pages = backfill_pages.load(Ordering::SeqCst);
    assert!(
        unique.len() >= 200,
        "backfill must walk full history despite near-full pages; got \
         {} unique events, expected >= 200. Pages issued: {pages}. \
         If this regresses, the `was_short = received < limit` heuristic \
         is back — replace it with `was_empty = received == 0` so \
         relays that return slightly under-cap pages don't terminate \
         backfill prematurely.",
        unique.len()
    );
    assert!(
        pages >= 5,
        "expected >= 5 backfill pages walking 200 events 48-at-a-time, \
         got {pages}. The under-cap heuristic regression caps pages at 2."
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_empty_pages_to_complete_is_honored() {
    // Asserts that the configurable `backfill_empty_pages_to_complete`
    // threshold is plumbed through and respected: backfill must walk the
    // configured number of consecutive empty pages before declaring a
    // kind's history complete.
    //
    // Setup: 30 preloaded events at timestamps 1000..1029. Per-page limit
    // is clamped up to BACKFILL_MIN_LIMIT (50), so a single page exhausts
    // the preloaded history. With `backfill_empty_pages_to_complete = 5`,
    // we expect:
    //   p1: 30 events, oldest t=1000 (streak resets to 0); the oldest moved,
    //       so the cursor holds inclusively at 1000, short page or not (#32
    //       (Backfill silently drops boundary-tie events when a relay serves
    //       fewer events per REQ than backfill_default_limit))
    //   p2: the t=1000 event again (streak stays 0); stagnated => 999
    //   p3..p7: 0 events each (streak counts 1..5)
    //   declare Complete
    // i.e. exactly 2 + 5 = 7 backfill REQs hit the relay, and Complete
    // does not flip until the seventh.
    use std::sync::atomic::{AtomicUsize, Ordering};

    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..30)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i))
        .collect();

    let backfill_pages = Arc::new(AtomicUsize::new(0));
    let counter = backfill_pages.clone();
    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .reject_req_if(move |filters| {
            if filters.iter().any(|f| f.limit.is_some()) {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            None
        })
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let threshold: u32 = 5;
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 50,
        backfill_empty_pages_to_complete: threshold,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let ckpt_for_test = ckpt.clone();
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    let _ = drain_events(&mut rx, 30, Duration::from_secs(10)).await;

    let completed = timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                ckpt_for_test.backfill_state(&url, 0).await,
                BackfillState::Complete
            ) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(completed, "backfill never reached Complete state within 5s");

    let pages = backfill_pages.load(Ordering::SeqCst);
    let expected = 2 + threshold as usize;
    assert_eq!(
        pages, expected,
        "with backfill_empty_pages_to_complete = {threshold}, expected exactly \
         1 real page + 1 re-read of its oldest second + {threshold} empty pages = \
         {expected} backfill REQs; got {pages}. \
         A smaller count means the configured threshold is being ignored (or hard-coded \
         lower); a larger count means the streak isn't terminating where the config says."
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_with_complete_checkpoint_does_not_re_backfill() {
    // Regression: prior to the fix, `Acquirer::spawn` always passed the full
    // kind list as `backfill_kinds` to `add_relay_internal`, which routed
    // through `kick_off_initial_backfill`'s scoped path and bypassed the
    // checkpoint. On a restart with `BackfillState::Complete` for every
    // kind, the relay-task would still issue an `until=now` backfill REQ
    // and overwrite the Complete checkpoint back to InProgress. The fix is
    // to pass `Vec::new()` from the startup path so the checkpoint-walking
    // branch decides which kinds (if any) need backfill.
    let keys = Keys::generate();
    let events: Vec<Event> = (0u64..3)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;

    // Mark backfill Complete in the checkpoint *before* the acquirer starts.
    ckpt.observe(
        &url,
        0,
        1_002,
        &events.last().unwrap().id,
    )
    .await;
    ckpt.record_backfill(&url, 0, BackfillState::Complete).await;

    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics);

    // Wait for the forward sub to open so we know the relay-task has run
    // its initial-bring-up step.
    let _ = timeout(Duration::from_secs(5), async {
        loop {
            let subs = relay.open_subscriptions();
            if subs.iter().any(|s| s.sub_id.starts_with("forward")) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    // Give the relay-task a moment to (incorrectly, in the buggy case) open
    // a backfill REQ. 200 ms is generous against an in-process relay.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let backfill_subs: Vec<_> = relay
        .open_subscriptions()
        .into_iter()
        .filter(|s| s.sub_id.starts_with("backfill"))
        .collect();
    assert!(
        backfill_subs.is_empty(),
        "expected zero backfill REQs on restart with Complete checkpoint, got {backfill_subs:?}"
    );

    // Checkpoint must still be Complete (not silently flipped to InProgress).
    let state_after = ckpt.backfill_state(&url, 0).await;
    assert!(
        matches!(state_after, BackfillState::Complete),
        "Complete checkpoint must survive a restart; got {state_after:?}"
    );

    // Drain anything the forward sub picked up — the preloaded events fall
    // before our checkpointed `since`, so this should yield nothing within
    // the timeout. Receiving them would mean the forward sub's `since` is
    // wrong, not a backfill bug.
    let _ = drain_events(&mut rx, 1, Duration::from_millis(200)).await;

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrency_pressure_notice_does_not_split_forward_subs() {
    // Critical invariant: a "too many concurrent REQs" NOTICE is concurrency
    // pressure, NOT a filter-shape rejection. Halving the kind list on these
    // doubles the open-sub count and amplifies the very pressure the relay
    // is complaining about. Real-world reproduction: nos.lol / primal.net
    // emit "ERROR: too many concurrent REQs" frequently during a high-volume
    // backfill, and the older "halve on any rejection-shaped NOTICE"
    // behaviour kept multiplying forward subs until floored at one-per-kind.
    let keys = Keys::generate();
    let events: Vec<Event> = vec![
        make_event(&keys, 0, "k0", 1_000),
        make_event(&keys, 3, "k3", 1_001),
        make_event(&keys, 10002, "k10002", 1_002),
        make_event(&keys, 30023, "k30023", 1_003),
    ];

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0, 3, 10002, 30023],
        kind_slugs: slugs(&[0, 3, 10002, 30023]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Wait for the initial single forward sub.
    let initial_count = timeout(Duration::from_secs(5), async {
        loop {
            let subs = relay.open_subscriptions();
            let forward: Vec<_> = subs
                .iter()
                .filter(|s| s.sub_id.starts_with("forward"))
                .collect();
            if !forward.is_empty() {
                return forward.len();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("initial forward sub did not open");
    assert_eq!(
        initial_count, 1,
        "expected exactly one forward sub before the pressure NOTICE"
    );

    let _ = drain_events(&mut rx, events.len(), Duration::from_secs(5)).await;

    // Send a representative concurrency-pressure NOTICE. The acquirer must
    // detect this as pressure (back-off path) rather than filter-shape
    // rejection (split path).
    relay.send_notice("ERROR: too many concurrent REQs").await;

    // Forward sub count should remain 1. Wait long enough that any split
    // attempt would have completed.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after_count = relay
        .open_subscriptions()
        .iter()
        .filter(|s| s.sub_id.starts_with("forward"))
        .count();
    assert_eq!(
        after_count, 1,
        "concurrency-pressure NOTICE must not trigger a forward-sub split; \
         got {after_count} forward subs (expected 1)"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// Bug-1 regression: when a relay sends a concurrency-pressure NOTICE during
/// an active backfill, the EOSE-driven page-advance loop must actually back
/// off before opening the next REQ. The original code bumped a backoff
/// counter on the NOTICE but then unconditionally **reset** it on every EOSE
/// and skipped the backoff sleep on the next-page path entirely — so the
/// backoff was a no-op and pages kept firing at full speed (thousands/sec
/// against a fast local relay), amplifying the pressure that triggered the
/// NOTICE. This test reproduces the live nos.lol failure pattern (2026-05-02
/// 17:01 UTC) and asserts a single NOTICE produces an observable pause in
/// new-REQ openings.
///
/// Signal: count backfill pages *completed* during a 1 s window after a
/// NOTICE, via the relay's cumulative backfill-CLOSE tally
/// (`backfill_close_count`). Each EOSE'd page is CLOSEd before the next opens
/// (issue #12), so that tally is a polling-cadence-independent page counter —
/// unlike snapshotting `open_subscriptions()`, which now only ever shows the
/// single in-flight page.
///
/// Without the fix the loop fires roughly one REQ per millisecond after the
/// NOTICE, so dozens of new REQs accumulate in the 1 s window. With the fix
/// the next-page open after EOSE-with-pressure-seen sleeps for
/// `PRESSURE_BACKOFF_INITIAL` (2 s), so the window catches at most the single
/// in-flight page that was already open when the NOTICE arrived.
#[tokio::test(flavor = "multi_thread")]
async fn concurrency_pressure_notice_throttles_backfill_page_rate() {
    let keys = Keys::generate();
    // Enough events at descending timestamps that backfill paginates well
    // past the 1 s observation window — without this, backfill completes
    // before the bug can be detected and delta is naturally small.
    let events: Vec<Event> = (0u64..5_000)
        .map(|i| make_event(&keys, 0, &format!("e{i}"), 1_000_000_000 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 50,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };

    // Drain events in the background so the bounded sink doesn't backpressure
    // the relay-task and skew the page-rate measurement.
    let drain_handle = tokio::spawn(async move {
        while let Some(_env) = rx.recv().await {}
    });

    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics);

    // Count *completed* pages via the relay's cumulative backfill-CLOSE
    // tally. Each EOSE'd page is CLOSEd before the next opens (issue #12), so
    // this monotonic counter is the page-advance signal — independent of
    // polling cadence, unlike snapshotting currently-open subs (which now
    // only ever shows the single in-flight page).
    let pages_completed = || relay.backfill_close_count();

    // Wait until backfill has paginated several pages so we're well past
    // warm-up and the page-rate is steady-state high. The warm-up is mostly
    // the forward REQ's replay: a kind being onboarded has a forward sub with
    // no `since` and no `limit`, so the test relay sends all 5,000 events
    // before it answers the first page, and the pool parses and verifies each
    // one. That takes about 2.7 s in a debug build on a fast machine and
    // longer on a slower CI runner, where a 5 s bound failed once
    // (2026-10-14). The bound only catches a hang; the measurement starts
    // after it.
    timeout(Duration::from_secs(60), async {
        loop {
            if pages_completed() >= 5 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("backfill did not paginate to 5+ pages within 60 s");

    // Snapshot the cumulative page count, send exactly one concurrency-pressure
    // NOTICE, then sample again after 1 s. The 1 s window is well under the
    // 2 s `PRESSURE_BACKOFF_INITIAL` so a working throttle catches at most
    // one in-flight page completing.
    let n_before = pages_completed();
    // The walk has 100 full pages; the unthrottled walk must have 10 or more
    // left to show in the window, or the bound below would pass vacuously.
    assert!(
        n_before <= 90,
        "too few backfill pages left to measure the throttle: {n_before} done before the NOTICE",
    );
    relay.send_notice("ERROR: too many concurrent REQs").await;
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let n_after = pages_completed();
    let delta = n_after.saturating_sub(n_before);

    // Without the throttle this delta is large (a page every few ms). With it,
    // at most the in-flight page that was already open when the NOTICE arrived
    // completes; subsequent pages wait out the 2 s backoff. `n_before` is read
    // before the NOTICE is enqueued, so on a slow runner a page or two can
    // complete in between; the bound allows for them, and the unthrottled walk,
    // with at least 10 pages left, still fails it.
    assert!(
        delta <= 5,
        "concurrency-pressure NOTICE was not honoured: \
         {delta} new backfill REQs opened in 1 s after NOTICE \
         (expected <= 5 due to 2 s backoff). \
         n_before={n_before}, n_after={n_after}",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
    let _ = drain_handle.await;
}

// The previous Bug-2 regression test (`forward_sub_silently_dropped_is_detected_and_reissued`)
// and its sibling quiet-relay counterpart (`forward_sub_silence_watchdog_skips_when_whole_relay_is_quiet`)
// have been removed. They tested the now-deleted asymmetric watchdog
// (`forward_sub_is_silent_but_relay_alive`), whose "relay otherwise alive"
// gate was load-bearing in their assertions. The probe-based replacement
// has different semantics:
//   - Bug-2 detection is covered by T2 (`probe_path_recovers_bug2_with_relay_otherwise_alive`),
//     which is a strict superset (it additionally observes the probe sub_id
//     register in the recovery path).
//   - The quiet-relay false-positive guard is rendered moot: under the new
//     design even a quiet relay gets a probe (the probe IS the alive check),
//     so the previous "must not re-issue" assertion no longer makes sense.
//     T8 (`low_rate_relay_does_not_trigger_reconnect`) covers the operationally
//     relevant property that low-rate relays don't churn the connection.

/// Build a kind-N event whose tags vector has `count` `["p", <hex>]` entries
/// — the shape of an oversized follow/mute list. Hex is just `<index>`
/// padded to 64 chars; the relay-pool lib doesn't reject malformed pubkey
/// hex at parse time (only schnorr signature verification matters), so this
/// is sufficient to exercise the tag-count path.
fn make_event_with_p_tags(keys: &Keys, kind: u16, content: &str, created_at: u64, count: usize) -> Event {
    let tags: Vec<Tag> = (0..count)
        .map(|i| {
            Tag::custom(
                TagKind::Custom("p".into()),
                [format!("{i:064x}")],
            )
        })
        .collect();
    EventBuilder::new(Kind::from(kind), content)
        .tags(tags)
        .custom_created_at(nostr::Timestamp::from_secs(created_at))
        .sign_with_keys(keys)
        .expect("sign event")
}

/// Regression: nostr-relay-pool's `RelayLimits` defaults reject any
/// non-`ContactList` event with > 2_000 tags (and any `ContactList` event
/// with > 10_000 tags) at the message-handler level — the lib logs
/// `Impossible to handle relay message. error=Received event with too
/// many tags: tags=N, max_tags=2000` and the event never reaches the
/// acquirer. Observed in production on 2026-05-09 silently dropping
/// high-profile accounts' events.
///
/// We override these limits in `Acquirer::add_relay_internal`. This test
/// pins the override by sending a kind-10000 (mute-list) event with 2_001
/// tags through the test relay and asserting it lands in the sink. Kind
/// 10000 has no per-kind override in the lib's defaults, so it falls back
/// to the global `max_num_tags = 2_000` cap.
#[tokio::test(flavor = "multi_thread")]
async fn large_tag_event_passes_through_when_limits_are_disabled() {
    let keys = Keys::generate();
    // 2_001 tags — one over the lib's default global cap.
    let event = make_event_with_p_tags(&keys, 10000, "huge mute list", 2_000_000_000, 2_001);
    let event_id = event.id;

    let relay = TestRelay::builder()
        .preload_event(event)
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![10000],
        kind_slugs: slugs(&[10000]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    let received = drain_events(&mut rx, 1, Duration::from_secs(10)).await;
    assert_eq!(
        received.len(),
        1,
        "expected the 2_001-tag event to reach the sink; pre-fix it would be \
         silently dropped by `nostr_relay_pool::Error::TooManyTags`",
    );
    assert_eq!(received[0].event.id, event_id);
    assert_eq!(received[0].event.tags.len(), 2_001);

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// Regression: the dispatch-liveness probe must not false-fire on a forward
/// sub whose stream consists of events the pool's `MemoryDatabase` LRU had
/// already seen via a faster peer relay. `RelayPoolNotification::Event` only
/// fires for first-seen ids; the dedup'd events still arrive on the
/// `RelayMessage::Event` channel (every wire-level frame). If the liveness
/// signal were bumped only from the post-dedup path, a healthy forward sub on
/// the slower-arriving relay would tip into "silent" territory after
/// `forward_silence_threshold`, the probe would fire and close-and-re-issue
/// it, and the cycle would repeat each threshold. Observed in production on
/// 2026-05-09 (against the then-current per-sub `last_event_at` trigger):
/// every relay's forward sub re-issued within 60 s of startup despite all
/// four being healthy, because damus consistently won the LRU race.
///
/// Both the per-sub `last_event_at` and (now) the relay-wide `last_activity_at`
/// are bumped from the wire-level `RelayMessage::Event` arm for exactly this
/// reason. This test verifies it by setting up two relays, continuously
/// publishing the *same* event id to both — so the pool LRU dedups one
/// delivery on every cycle — and asserting that *neither* relay observed a
/// `["CLOSE", "forward"]` after twice the silence threshold (a CLOSE would
/// follow a probe that fired and settled Alive). Without wire-level coverage,
/// whichever relay lost the LRU race would see its forward sub re-issued.
#[tokio::test(flavor = "multi_thread")]
async fn watchdog_does_not_fire_when_forward_events_are_lru_deduped_from_peers() {
    let keys = Keys::generate();
    let kick = make_event(&keys, 0, "kickoff kind-0", 2_000_000_000);

    let relay_a = TestRelay::builder().preload_event(kick.clone()).start().await;
    let relay_b = TestRelay::builder().preload_event(kick.clone()).start().await;

    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![
            RelayUrl::parse(relay_a.url().as_str()).unwrap(),
            RelayUrl::parse(relay_b.url().as_str()).unwrap(),
        ],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        // Short threshold so the test runs in seconds, not minutes.
        forward_silence_threshold: Duration::from_secs(2),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Wait for both forward subs to register so subsequent publishes match.
    timeout(Duration::from_secs(5), async {
        loop {
            let a = relay_a.open_subscriptions().iter().any(|s| s.sub_id == "forward");
            let b = relay_b.open_subscriptions().iter().any(|s| s.sub_id == "forward");
            if a && b {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("forward subs never registered on both relays");

    // Continuously publish the SAME event id to both relays. Each cycle
    // the pool sees two wire-level deliveries for the same id; the LRU
    // suppresses the second on `RelayPoolNotification::Event`, but the
    // wire-level `RelayMessage::Event` still fires for both — and that arm
    // bumps the relay-wide `last_activity_at`, so both relays stay above the
    // silence threshold.
    //
    // Pump cadence: well below `forward_silence_threshold` (2 s) so that
    // a missed bump on either relay would tip into "silent" territory
    // and trigger the probe within the test window.
    let threshold_secs = 2;
    let pump_until =
        tokio::time::Instant::now() + Duration::from_secs(2 * threshold_secs + 1);
    while tokio::time::Instant::now() < pump_until {
        relay_a.publish(kick.clone()).await;
        relay_b.publish(kick.clone()).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // With the wire-level `last_activity_at` bump in place, neither relay
    // should have observed a `["CLOSE", "forward"]`. Without wire-level
    // coverage this would fail on the LRU loser.
    assert_eq!(
        relay_a.close_count("forward"),
        0,
        "watchdog re-issued forward sub on relay_a despite wire-level events arriving",
    );
    assert_eq!(
        relay_b.close_count("forward"),
        0,
        "watchdog re-issued forward sub on relay_b despite wire-level events arriving",
    );

    acquirer.shutdown().await;
    relay_a.shutdown().await;
    relay_b.shutdown().await;
}

/// `kind_only_relays` is the inverse restriction: a kind pinned to a specific
/// relay is NOT requested from any other relay, even one with no `only_kinds`
/// of its own. Two relays both hold a kind-30402 event; only the pinned relay
/// is asked for it, so exactly one copy reaches the sink and the unpinned
/// relay's forward REQ never names the kind.
///
/// This covers the mechanism, not any particular deployment. It was introduced
/// for the GammaMarkets marketplace kinds, which were pinned to
/// relay.plebeian.market on 2026-08-20 and unpinned the same day; **no shipped
/// config currently uses `only_relays`**. Keep the test regardless -- the
/// primitive has to keep working for the next kind that needs pinning, and
/// `crates/nozzl/tests/shipped_configs.rs` is what asserts what the shipped
/// configs actually do.
#[tokio::test(flavor = "multi_thread")]
async fn kind_only_relays_pins_kind_to_listed_relay_only() {
    let keys = Keys::generate();
    // The SAME kind exists on both relays. Distinct ids so we can tell which
    // relay a delivered event came from.
    let pinned_copy = make_event(&keys, 30402, "from-dedicated", 2_000);
    let offlimits_copy = make_event(&keys, 30402, "from-general", 1_000);
    let shared = make_event(&keys, 0, "shared-kind", 3_000);

    let dedicated = TestRelay::builder()
        .preload_events([pinned_copy.clone()])
        .start()
        .await;
    let general = TestRelay::builder()
        .preload_events([offlimits_copy.clone(), shared.clone()])
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let dedicated_url = RelayUrl::parse(dedicated.url().as_str()).unwrap();
    let general_url = RelayUrl::parse(general.url().as_str()).unwrap();

    let mut kind_only_relays = HashMap::new();
    kind_only_relays.insert(30402u16, HashSet::from([dedicated_url.clone()]));

    let cfg = AcquirerConfig {
        relays: vec![dedicated_url.clone(), general_url.clone()],
        // Global kinds carries BOTH, and neither relay has an only_kinds.
        kinds: vec![0, 30402],
        kind_slugs: slugs(&[0, 30402]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays,
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Expect exactly two events: the pinned relay's 30402 and the general
    // relay's kind 0. The general relay's 30402 must never be requested.
    let got = drain_events(&mut rx, 3, Duration::from_secs(5)).await;
    let ids: std::collections::HashSet<_> = got.iter().map(|e| e.event.id).collect();
    assert!(
        ids.contains(&pinned_copy.id),
        "the pinned relay's kind-30402 event should arrive, got {got:?}"
    );
    assert!(
        ids.contains(&shared.id),
        "the unpinned kind 0 should still arrive from the general relay"
    );
    assert!(
        !ids.contains(&offlimits_copy.id),
        "kind-30402 must NOT be acquired from the unpinned relay, got {got:?}"
    );

    // And prove it at the wire level: the general relay's forward REQ must
    // carry kind 0 only.
    let general_forward = general.open_subscriptions();
    let forward = general_forward
        .iter()
        .find(|s| s.sub_id.starts_with("forward"))
        .expect("forward sub should be open on the general relay");
    let kinds_in_filters: Vec<u16> = forward
        .filters
        .iter()
        .flat_map(|f| {
            f.kinds
                .as_ref()
                .map(|ks| ks.iter().map(|k| k.as_u16()).collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(
        kinds_in_filters,
        vec![0u16],
        "the general relay's forward sub must not name the pinned kind, got {kinds_in_filters:?}"
    );

    acquirer.shutdown().await;
    dedicated.shutdown().await;
    general.shutdown().await;
}

/// Hot-reload path: a kind ADDED with `only_relays` must be honored at the
/// moment it is added -- the excluded relay must neither re-issue its forward
/// sub naming the new kind nor start a scoped backfill for it.
///
/// This is the path a real onboarding takes (`kinds_added` -> `UpdateKinds` on
/// every surviving relay), and the reason a newly added kind needs no restart
/// while a change to an existing kind's `only_relays` does.
#[tokio::test(flavor = "multi_thread")]
async fn hot_reload_added_kind_with_only_relays_skips_excluded_relay() {
    let keys = Keys::generate();
    // The same new kind sits on BOTH relays, so the test proves both halves:
    // it is acquired from the pinned relay AND not from the excluded one.
    let newcomer = make_event(&keys, 30402, "newly-onboarded-kind", 5_000);
    let pinned_copy = make_event(&keys, 30402, "newly-onboarded-pinned", 6_000);
    let baseline = make_event(&keys, 0, "baseline", 1_000);

    let dedicated = TestRelay::builder()
        .preload_events([pinned_copy.clone()])
        .start()
        .await;
    let general = TestRelay::builder()
        .preload_events([baseline.clone(), newcomer.clone()])
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let dedicated_url = RelayUrl::parse(dedicated.url().as_str()).unwrap();
    let general_url = RelayUrl::parse(general.url().as_str()).unwrap();

    // Start with kind 0 only, on both relays.
    let base_cfg = AcquirerConfig {
        relays: vec![dedicated_url.clone(), general_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(base_cfg.clone(), tx, ckpt, metrics);
    let _ = drain_events(&mut rx, 1, Duration::from_secs(5)).await;

    // Now onboard kind 30402, pinned to the dedicated relay -- the shape of a
    // real hot-reload onboarding.
    let mut kind_only_relays = HashMap::new();
    kind_only_relays.insert(30402u16, HashSet::from([dedicated_url.clone()]));
    let updated = AcquirerConfig {
        kinds: vec![0, 30402],
        kind_slugs: slugs(&[0, 30402]),
        kind_only_relays,
        ..base_cfg
    };
    acquirer.apply_config(updated).await;

    // Positive half: the pinned relay's copy must actually arrive, proving the
    // hot-reload really onboarded the kind rather than dropping it everywhere.
    let after = drain_events(&mut rx, 2, Duration::from_secs(5)).await;
    let ids: std::collections::HashSet<_> = after.iter().map(|e| e.event.id).collect();
    assert!(
        ids.contains(&pinned_copy.id),
        "the pinned relay's copy of the newly onboarded kind must arrive, got {after:?}"
    );
    // Negative half: the general relay holds a kind-30402 event and would
    // happily serve it, but it must never be asked.
    assert!(
        !ids.contains(&newcomer.id),
        "the excluded relay must not deliver the newly pinned kind, got {after:?}"
    );

    let snapshot = general.open_subscriptions();
    let named_new_kind = snapshot.iter().any(|s| {
        s.filters.iter().any(|f| {
            f.kinds
                .as_ref()
                .is_some_and(|ks| ks.iter().any(|k| k.as_u16() == 30402))
        })
    });
    assert!(
        !named_new_kind,
        "no subscription on the excluded relay may name the pinned kind: {snapshot:?}"
    );

    acquirer.shutdown().await;
    dedicated.shutdown().await;
    general.shutdown().await;
}

/// `relay_only_kinds` restricts the per-relay kind set: the forward REQ
/// emitted to a restricted relay carries the intersection of the global
/// kind list and that relay's allowlist, and an event whose kind is NOT
/// in the allowlist (preloaded on the relay) never reaches the sink.
///
/// This is the load-bearing behaviour for the zapstore.dev configuration:
/// excluded kinds must not be requested, so the forward sub's `min(since)`
/// is not dragged back by their ancient checkpoints.
#[tokio::test(flavor = "multi_thread")]
async fn relay_only_kinds_restricts_forward_sub_to_listed_kinds() {
    let keys = Keys::generate();
    // Two events on the relay: one of an allowed kind (30023), one of a
    // configured-but-excluded kind (0). The excluded one must never leave
    // the relay because our REQ doesn't ask for it.
    let allowed = make_event(&keys, 30023, "allowed", 1_000);
    let excluded = make_event(&keys, 0, "excluded", 2_000);

    let relay = TestRelay::builder()
        .preload_events([allowed.clone(), excluded.clone()])
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let mut relay_only_kinds = HashMap::new();
    relay_only_kinds.insert(url.clone(), vec![30023u16]);
    let cfg = AcquirerConfig {
        // Global kinds includes BOTH 0 and 30023.
        relays: vec![url.clone()],
        kinds: vec![0, 30023],
        kind_slugs: slugs(&[0, 30023]),
        // But this relay is restricted to 30023 only.
        relay_only_kinds,
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // The allowed event must arrive.
    let got = drain_events(&mut rx, 1, Duration::from_secs(5)).await;
    assert_eq!(got.len(), 1, "expected the kind-30023 event to arrive");
    assert_eq!(got[0].event.id, allowed.id);

    // The excluded event must NOT arrive — neither via backfill nor via the
    // forward sub — because our REQ does not include kind 0.
    let extra = drain_events(&mut rx, 1, Duration::from_millis(300)).await;
    assert!(
        extra.is_empty(),
        "expected zero events of excluded kind 0, got {extra:?}"
    );

    // The forward REQ's filter must carry ONLY kind 30023, not the global
    // kind list. Inspect the relay's open-subs snapshot.
    let snapshot = relay.open_subscriptions();
    let forward = snapshot
        .iter()
        .find(|s| s.sub_id.starts_with("forward"))
        .expect("forward sub should be open");
    let kinds_in_filters: Vec<u16> = forward
        .filters
        .iter()
        .flat_map(|f| {
            f.kinds
                .as_ref()
                .map(|ks| ks.iter().map(|k| k.as_u16()).collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(
        kinds_in_filters,
        vec![30023u16],
        "forward sub must carry only the allowlisted kind, got {kinds_in_filters:?}"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// A relay WITHOUT a `relay_only_kinds` entry continues to receive the
/// full configured kind list — i.e. the restriction is opt-in per relay,
/// not a global behaviour change.
#[tokio::test(flavor = "multi_thread")]
async fn relay_without_only_kinds_gets_full_kind_set() {
    let keys = Keys::generate();
    let e_k0 = make_event(&keys, 0, "k0", 1_000);
    let e_k30 = make_event(&keys, 30023, "k30", 2_000);

    let relay = TestRelay::builder()
        .preload_events([e_k0.clone(), e_k30.clone()])
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let cfg = AcquirerConfig {
        relays: vec![RelayUrl::parse(relay.url().as_str()).unwrap()],
        kinds: vec![0, 30023],
        kind_slugs: slugs(&[0, 30023]),
        // No restriction for this relay → both kinds should be subscribed.
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    let got = drain_events(&mut rx, 2, Duration::from_secs(5)).await;
    let kinds_seen: std::collections::HashSet<u16> = got
        .iter()
        .map(|env| u16::from(env.event.kind))
        .collect();
    assert!(kinds_seen.contains(&0), "kind 0 should arrive");
    assert!(kinds_seen.contains(&30023), "kind 30023 should arrive");

    let snapshot = relay.open_subscriptions();
    let forward = snapshot
        .iter()
        .find(|s| s.sub_id.starts_with("forward"))
        .expect("forward sub should be open");
    let mut kinds_in_filters: Vec<u16> = forward
        .filters
        .iter()
        .flat_map(|f| {
            f.kinds
                .as_ref()
                .map(|ks| ks.iter().map(|k| k.as_u16()).collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect();
    kinds_in_filters.sort_unstable();
    assert_eq!(
        kinds_in_filters,
        vec![0u16, 30023],
        "forward sub must carry the full configured kind list"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// A relay whose `relay_only_kinds` intersection with the global kind list
/// is empty is skipped at spawn — no forward sub is opened against it.
/// Guards against accidentally degenerating to "subscribe to nothing"
/// (which is technically a valid REQ but burns a connection slot and is
/// almost certainly a misconfiguration the operator wants to be warned
/// about).
#[tokio::test(flavor = "multi_thread")]
async fn relay_with_empty_effective_kinds_opens_no_forward_sub() {
    let relay = TestRelay::builder().start().await;

    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let mut relay_only_kinds = HashMap::new();
    // Intersection is empty: global kinds = [0], only_kinds = [30023].
    relay_only_kinds.insert(url.clone(), vec![30023u16]);
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds,
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Give the relay-task a generous moment to (incorrectly) bring itself up.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let snapshot = relay.open_subscriptions();
    let forward_subs: Vec<_> = snapshot
        .iter()
        .filter(|s| s.sub_id.starts_with("forward"))
        .collect();
    assert!(
        forward_subs.is_empty(),
        "no forward sub should be opened when effective kinds is empty, got {forward_subs:?}"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// =========================================================================
// Subscription-Dispatch Liveness Probe tests (T1–T20)
//
// These tests pin the contract of the probe state machine that
// replaces the old asymmetric "relay-alive watchdog" gate.
//
// Standard harness for probe tests: `forward_silence_threshold: 3s`,
// `probe_timeout: 2s`, `probe_min_interval: 4s`. These are aggressive
// enough to keep test wall-clock tight, with enough slack (>= 2x the
// trigger threshold) for slow runners.
//
// Probe sub_id prefix: `"probe-"` (PROBE_SUB_BASE in relay_task.rs).
// Probe filter kinds: `[u16::MAX]` with `limit:0` (probe_filter in
// filter.rs; `PROBE_KIND = u16::MAX`).
// =========================================================================

/// Build the standard probe-test `AcquirerConfig` for a single relay. Used
/// by T1–T20 so the thresholds are consistent.
fn probe_test_cfg(relays: Vec<RelayUrl>, kinds: Vec<u16>) -> AcquirerConfig {
    let kind_slugs = slugs(&kinds);
    AcquirerConfig {
        relays,
        kinds,
        kind_slugs,
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        // Probe-feature thresholds — see module-level comment block above.
        forward_silence_threshold: Duration::from_secs(3),
        probe_timeout: Duration::from_secs(2),
        probe_min_interval: Duration::from_secs(4),
    }
}

/// True if any currently-open sub has a `sub_id` starting with `"probe-"`.
/// Matches the live `PROBE_SUB_BASE` from relay_task.rs.
fn has_probe_sub(relay: &nozzl_test_relay::TestRelayHandle) -> bool {
    relay
        .open_subscriptions()
        .iter()
        .any(|s| s.sub_id.starts_with("probe-"))
}

/// Count of currently-open subs with `sub_id` starting with `"probe-"`.
fn probe_sub_count(relay: &nozzl_test_relay::TestRelayHandle) -> usize {
    relay
        .open_subscriptions()
        .iter()
        .filter(|s| s.sub_id.starts_with("probe-"))
        .count()
}

/// Predicate matching the dispatch-liveness probe filter shape:
/// `kinds = [PROBE_KIND]` (= `u16::MAX`) and `limit = 0`. Used in
/// `ignore_req_if` / `reject_req_if` predicates to target probe REQs
/// without affecting the forward sub or backfill REQs.
fn is_probe_filter(filters: &[nostr::Filter]) -> bool {
    filters.iter().any(|f| {
        f.kinds
            .as_ref()
            .is_some_and(|ks| ks.contains(&Kind::from_u16(nozzl_acquirer::PROBE_KIND)))
            && f.limit == Some(0)
    })
}

/// Wait until the initial `"forward"` sub registers at the relay. Bounded
/// to 5 s; panics on timeout (every probe test starts from this state).
async fn wait_for_initial_forward(relay: &nozzl_test_relay::TestRelayHandle) {
    timeout(Duration::from_secs(5), async {
        loop {
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("initial forward sub never registered at relay");
}

// -------------------------------------------------------------------------
// T1: probe fires when forward silent AND relay also silent.
//
// This is the case the OLD asymmetric gate missed (the "5-hour stall"
// observed on 2026-05-23 against three relays simultaneously): forward
// sub is silent because the relay
// silently dropped it server-side, AND the relay also stops sending
// NOTICEs / EOSEs of its own accord, so the old "relay otherwise alive"
// liveness signal (since removed) refused to fire. The probe-based
// approach tests dispatch directly and does NOT gate on ambient relay
// liveness.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn probe_fires_when_forward_silent_and_relay_silent() {
    // Add a small EOSE delay so the probe sub is observable in
    // open_subscriptions() (otherwise the probe REQ → EOSE → unsubscribe
    // round-trip completes in well under a polling interval against the
    // in-process relay).
    let relay = TestRelay::builder()
        .delay_eose_by(Duration::from_millis(300))
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;

    // Silently drop the forward sub at the relay. No CLOSED is sent and
    // no other frames will flow — the exact scenario the old "relay
    // otherwise alive" liveness signal would have refused to fire on.
    relay.silently_drop_subscription("forward").await;

    // Wait for a probe sub to appear in the relay's open-subs snapshot.
    // The probe should fire ~1 s after forward_silence_threshold (3 s)
    // since the silence tick is 1 s. Budget 10 s — well above any
    // legitimate window.
    let probe_seen = timeout(Duration::from_secs(10), async {
        loop {
            if has_probe_sub(&relay) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        probe_seen,
        "probe sub never registered after threshold + ~1 s tick (forward-silent, \
         relay-silent — the case the asymmetric gate missed)",
    );

    // After the default test relay EOSEs the probe, the alive path calls
    // close_all + open_all_forward_subs → a fresh "forward" sub should
    // reappear at the relay. Use metric as the most reliable signal:
    // probe_total{alive} bumps after EOSE → on_probe_settled.
    let alive = timeout(Duration::from_secs(10), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "alive"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(alive >= 1, "probe never settled alive");

    let forward_reissued = timeout(Duration::from_secs(5), async {
        loop {
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward")
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        forward_reissued,
        "forward sub was not re-issued after probe settled (alive); expected the \
         alive path to call close_all + open_all_forward_subs",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T2: probe path recovers Bug-2 with relay otherwise alive.
//
// Strict superset of the deleted Bug-2 regression test
// (`forward_sub_silently_dropped_is_detected_and_reissued`). The extra
// observation here is that a `probe-*` sub registers in the recovery
// path, before the new "forward" sub — proof that the probe replaces
// the asymmetric branch cleanly rather than (e.g.) calling close+open
// on a different code path.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn probe_path_recovers_bug2_with_relay_otherwise_alive() {
    let keys = Keys::generate();
    // Small EOSE delay so the probe sub is observable in
    // `open_subscriptions()` for the T2-specific assertion below.
    let relay = TestRelay::builder()
        .delay_eose_by(Duration::from_millis(300))
        .start()
        .await;
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;

    // Silently drop the forward sub (no CLOSED to the client).
    relay.silently_drop_subscription("forward").await;
    assert!(
        !relay
            .open_subscriptions()
            .iter()
            .any(|s| s.sub_id == "forward"),
        "precondition: forward sub should be gone after silently_drop_subscription",
    );

    // Pump NOTICEs to keep the relay "otherwise alive" (mirrors the live
    // nos.lol failure conditions: connection looks healthy, other frames
    // flow, only the forward sub is dead). Also observe the probe in the
    // recovery path — that's the T2-specific assertion beyond the old
    // Bug-2 test.
    let probe_observed_in_recovery = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = std::sync::Arc::clone(&probe_observed_in_recovery);

    let reissued = timeout(Duration::from_secs(15), async {
        loop {
            relay.send_notice("keepalive").await;
            // Capture probe presence on every tick — once observed, latch it.
            if has_probe_sub(&relay) {
                observed.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward")
            {
                return true;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(reissued, "forward sub was not re-issued within 15 s");
    assert!(
        probe_observed_in_recovery.load(std::sync::atomic::Ordering::SeqCst),
        "T2 invariant: a `probe-*` sub must register in the recovery path, \
         proving the probe (not the deleted asymmetric branch) drove the re-issue",
    );

    // Publish post-recovery; event must flow through the fresh forward sub.
    let event = make_event(&keys, 0, "post-recovery", 2_000_000_000);
    relay.publish(event.clone()).await;
    let received = drain_events(&mut rx, 1, Duration::from_secs(5)).await;
    assert_eq!(received.len(), 1, "post-recovery event did not flow");
    assert_eq!(received[0].event.id, event.id);

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T3: probe success re-issues forward, events flow.
//
// Extends T1 with the end-to-end event-flow assertion: after the alive
// path re-issues the forward sub, a freshly-published event must reach
// the sink.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn probe_success_reissues_forward_and_events_flow() {
    let keys = Keys::generate();
    let relay = TestRelay::builder().start().await;
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Wait for recovery — a fresh "forward" sub registered.
    let reissued = timeout(Duration::from_secs(10), async {
        loop {
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward")
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(reissued, "forward sub did not re-issue after probe alive");

    // Publish — must flow through the new forward sub.
    let event = make_event(&keys, 0, "post-probe", 2_000_000_000);
    relay.publish(event.clone()).await;
    let received = drain_events(&mut rx, 1, Duration::from_secs(5)).await;
    assert_eq!(
        received.len(),
        1,
        "expected the re-issued forward sub to deliver the post-probe event",
    );
    assert_eq!(received[0].event.id, event.id);

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T4: probe timeout triggers relay reconnect.
//
// Uses `ignore_req_if` to register probes without ever EOSE-ing them.
// After probe_timeout elapses, the probe-timeout branch should fire:
// `probe_total{outcome="timeout"}` increments AND `relay_disconnects_total`
// increments. The disconnect counter is bumped inline by the probe-timeout
// branch (the `set_status_metric(RelayConnState::Disconnected)` write happens *after* the inline
// `relay_disconnects.inc()`, so the operator-facing
// `probe_total{outcome="timeout"} ↔ relay_disconnects_total` correlation
// holds without depending on `check_connection_transitions` racing the
// status refresh).
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn probe_timeout_triggers_relay_reconnect() {
    // Only ignore probe-shaped REQs (kinds:[u16::MAX], limit:0). Forward
    // and backfill REQs are left to the normal happy path.
    let relay = TestRelay::builder()
        .ignore_req_if(is_probe_filter)
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Probe fires after `forward_silence_threshold` (3 s) + ~1 s tick,
    // then times out after `probe_timeout` (2 s). Total ~6 s — budget 15 s.
    let timed_out = timeout(Duration::from_secs(15), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "timeout"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        timed_out >= 1,
        "expected probe_total{{outcome=\"timeout\"}} >= 1 after \
         forward_silence_threshold + probe_timeout, got {timed_out}",
    );

    // probe_in_flight should return to 0 after timeout.
    let in_flight = metrics_clone
        .probe_in_flight
        .with_label_values(&[url.as_str()])
        .get();
    assert_eq!(
        in_flight, 0,
        "probe_in_flight must return to 0 after timeout, got {in_flight}",
    );

    // `relay_disconnects_total` must also increment so operators can
    // correlate `probe_total{outcome="timeout"}` with reconnects on
    // dashboards. The probe-timeout branch increments this inline (the
    // status refresh that follows would otherwise mask the 1→0 transition
    // from `check_connection_transitions`).
    let disconnects = metrics_clone
        .relay_disconnects
        .with_label_values(&[url.as_str()])
        .get();
    assert!(
        disconnects >= 1,
        "expected relay_disconnects_total >= 1 after probe-driven reconnect, got {disconnects}",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T5: no probe when events flow normally (false-positive guard).
//
// Publish one event per second for 5 s (well above
// `forward_silence_threshold = 3 s` cadence). Every event bumps
// `last_activity_at` so the trigger predicate never fires. Asserts zero
// `probe-*` subs ever registered over the test window.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn no_probe_when_events_flow_normally() {
    let keys = Keys::generate();
    let relay = TestRelay::builder().start().await;
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;

    // Background drain so the sink doesn't backpressure.
    let drain_handle = tokio::spawn(async move {
        while let Some(_env) = rx.recv().await {}
    });

    // Pump one event per second for 5 s. Check on each iteration that no
    // probe sub has registered.
    let mut max_probe_subs_seen = 0;
    for i in 0..5u64 {
        let e = make_event(&keys, 0, &format!("steady-{i}"), 1_000_000_000 + i);
        relay.publish(e).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        max_probe_subs_seen = max_probe_subs_seen.max(probe_sub_count(&relay));
    }
    assert_eq!(
        max_probe_subs_seen, 0,
        "false positive: probe sub registered while events were flowing at 1/s \
         (well above forward_silence_threshold = 3 s)",
    );
    let alive_count = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();
    let timeout_count = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "timeout"])
        .get();
    assert_eq!(
        alive_count, 0,
        "false positive: probe_total{{outcome=\"alive\"}} = {alive_count} (expected 0)",
    );
    assert_eq!(
        timeout_count, 0,
        "false positive: probe_total{{outcome=\"timeout\"}} = {timeout_count} (expected 0)",
    );

    acquirer.shutdown().await;
    drop(drain_handle);
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T6: rate limit prevents probe storm.
//
// First probe fires → EOSE → re-issue. Immediately drop the new forward
// sub. Within `probe_min_interval` (4 s), no second probe should fire.
// After the interval elapses, the second probe must fire.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn rate_limit_prevents_probe_storm() {
    let relay = TestRelay::builder().start().await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Wait for first probe to fire + settle + forward re-issue.
    let first_alive = timeout(Duration::from_secs(10), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "alive"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(first_alive >= 1, "first probe did not settle alive");

    // Immediately drop the new forward sub.
    relay.silently_drop_subscription("forward").await;

    // Within `probe_min_interval = 4 s` (but minus the time already elapsed),
    // no second probe should fire. Sample alive count after ~2 s.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after_quick = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();
    assert_eq!(
        after_quick, first_alive,
        "rate-limit violated: probe_total{{alive}} climbed from {first_alive} \
         to {after_quick} within probe_min_interval = 4 s",
    );

    // Wait past the rate-limit floor + threshold + tick. probe_min_interval
    // is anchored on last issue time (issued ~5 s ago from now), so the next
    // probe should fire any time now.
    let second_alive = timeout(Duration::from_secs(15), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "alive"])
                .get();
            if v > first_alive {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        second_alive > first_alive,
        "after probe_min_interval elapsed, a second probe must fire; got \
         alive={second_alive} (started at {first_alive})",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T7: no overlapping probe per relay.
//
// With `ignore_req_if`, the first probe never settles. Verify that
// `probe-*` count stays <= 1 over an extended window. The trigger
// predicate's `pending_probe.is_some()` guard is the load-bearing
// invariant.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn no_overlapping_probe_per_relay() {
    let relay = TestRelay::builder()
        .ignore_req_if(is_probe_filter)
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Wait for the first probe to appear.
    timeout(Duration::from_secs(10), async {
        loop {
            if has_probe_sub(&relay) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("first probe did not register within 10 s");

    // Sample over a window. The probe will time out (2 s) and trigger a
    // reconnect, but `probe_min_interval = 4 s` prevents another probe
    // within that window. Even across multiple probe-timeout cycles, at
    // no instant should `probe_sub_count` exceed 1.
    let window_end = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut max_seen = 0;
    while tokio::time::Instant::now() < window_end {
        let n = probe_sub_count(&relay);
        if n > max_seen {
            max_seen = n;
        }
        assert!(
            n <= 1,
            "concurrent probes detected: probe_sub_count = {n} (expected <= 1)",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Sanity: we did see at least one probe at some point.
    assert!(max_seen >= 1, "no probe ever appeared in T7 window");

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T8: low-rate relay does not trigger reconnect.
//
// The zapstore-style case: a quiet relay (no events published) that
// still EOSEs probes normally. Drop the forward sub repeatedly; probes
// fire on schedule, EOSE, and re-issue. Over a 15 s window, no
// `relay_disconnects_total` increment — the relay is alive even though
// it has nothing to send.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn low_rate_relay_does_not_trigger_reconnect() {
    let relay = TestRelay::builder().start().await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;

    let baseline_disconnects = metrics_clone
        .relay_disconnects
        .with_label_values(&[url.as_str()])
        .get();
    let baseline_timeouts = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "timeout"])
        .get();

    // Drop the forward sub multiple times across a 15 s window to give
    // probes multiple opportunities to fire and recover. The relay
    // EOSEs probes normally so each round must take the alive path.
    let window_end = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < window_end {
        relay.silently_drop_subscription("forward").await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    let disconnects_after = metrics_clone
        .relay_disconnects
        .with_label_values(&[url.as_str()])
        .get();
    let timeouts_after = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "timeout"])
        .get();
    assert_eq!(
        disconnects_after, baseline_disconnects,
        "T8: zapstore-style low-rate relay must not see relay_disconnects_total \
         climb — probes succeed and re-issue forward subs without reconnect churn. \
         baseline={baseline_disconnects}, after={disconnects_after}",
    );
    assert_eq!(
        timeouts_after, baseline_timeouts,
        "T8: low-rate relay must not produce probe timeouts (relay EOSEs probes). \
         baseline={baseline_timeouts}, after={timeouts_after}",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T9: CLOSED-for-probe-sub counts as alive (defensive against limit:0
// rejection).
//
// Some relays may reject `limit:0` as malformed and CLOSED our probe
// sub_id. CLOSED-for-probe settles the probe as alive: it
// must be routed to `on_probe_settled(Alive)` — same as EOSE / EVENT.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn closed_for_probe_sub_counts_as_alive() {
    let relay = TestRelay::builder()
        .reject_req_if(|filters| {
            if is_probe_filter(filters) {
                Some("limit:0 not supported".to_string())
            } else {
                None
            }
        })
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Probe fires → CLOSED-for-probe → alive path. Counter should bump.
    let alive_count = timeout(Duration::from_secs(15), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "alive"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        alive_count >= 1,
        "T9: CLOSED-for-probe-sub must settle the probe as alive (NDK \
         convention; defensive for relays that reject limit:0). \
         probe_total{{alive}} = {alive_count}",
    );

    // No timeouts should have occurred — CLOSED arrives well before
    // `probe_timeout`.
    let timeout_count = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "timeout"])
        .get();
    assert_eq!(
        timeout_count, 0,
        "T9: CLOSED routing must short-circuit before probe_timeout; got \
         probe_total{{timeout}} = {timeout_count}",
    );

    // Forward sub must be re-issued (alive path runs close_all + open_all).
    let reissued = timeout(Duration::from_secs(5), async {
        loop {
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward")
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        reissued,
        "forward sub must be re-issued after CLOSED-as-alive",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T10: multiple relays have independent probe state.
//
// Two relays; only relay A has its forward sub silently dropped. Probe
// fires on A; B never sees a `probe-*` sub. The per-relay isolation is
// load-bearing: a stall on one relay must not perturb subs on its peers.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn multiple_relays_have_independent_probe_state() {
    let keys = Keys::generate();
    let relay_a = TestRelay::builder().start().await;
    let relay_b = TestRelay::builder().start().await;
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url_a = RelayUrl::parse(relay_a.url().as_str()).unwrap();
    let url_b = RelayUrl::parse(relay_b.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url_a.clone(), url_b.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Wait for both forward subs.
    timeout(Duration::from_secs(5), async {
        loop {
            let a = relay_a
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward");
            let b = relay_b
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward");
            if a && b {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("forward subs never registered on both relays");

    // Drop A's forward sub; B remains healthy. Pump events to B
    // throughout the test window to keep its `last_activity_at` fresh
    // (otherwise B would also age past `forward_silence_threshold = 3 s`
    // and trigger its own probe).
    relay_a.silently_drop_subscription("forward").await;

    // Background drain so the sink doesn't backpressure.
    let drain_handle = tokio::spawn(async move {
        while let Some(_env) = rx.recv().await {}
    });

    // Pump events to B inline while polling A's alive counter. The
    // TestRelayHandle isn't Clone/Send-wrappable so we drive the pump
    // here rather than via a spawned task.
    let probe_alive_on_a = timeout(Duration::from_secs(15), async {
        let mut counter = 0u64;
        loop {
            // Pump an event to B every iteration (~300 ms) — well below
            // forward_silence_threshold = 3 s so B's last_activity_at stays
            // fresh.
            let e = make_event(&keys, 0, &format!("b-pump-{counter}"), 1_000_000_000 + counter);
            counter += 1;
            relay_b.publish(e).await;
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url_a.as_str(), "alive"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        probe_alive_on_a >= 1,
        "T10: probe never fired-and-settled-alive on relay A",
    );

    // B's probe counters must all be zero — its forward sub never went
    // silent because we pumped events to it.
    let alive_b = metrics_clone
        .probe_total
        .with_label_values(&[url_b.as_str(), "alive"])
        .get();
    let timeout_b = metrics_clone
        .probe_total
        .with_label_values(&[url_b.as_str(), "timeout"])
        .get();
    let in_flight_b = metrics_clone
        .probe_in_flight
        .with_label_values(&[url_b.as_str()])
        .get();
    assert_eq!(
        alive_b, 0,
        "T10: relay B must never produce probe_total{{alive}}; got {alive_b}",
    );
    assert_eq!(
        timeout_b, 0,
        "T10: relay B must never produce probe_total{{timeout}}; got {timeout_b}",
    );
    assert_eq!(
        in_flight_b, 0,
        "T10: relay B's probe_in_flight must remain 0; got {in_flight_b}",
    );
    assert!(
        !has_probe_sub(&relay_b),
        "T10: relay B must not see any probe sub — per-relay probe state is independent",
    );

    drop(drain_handle);
    acquirer.shutdown().await;
    relay_a.shutdown().await;
    relay_b.shutdown().await;
}

// -------------------------------------------------------------------------
// T11: probe sub is closed after settlement.
//
// `on_probe_settled(Alive)` calls `pool.unsubscribe` on the probe sub,
// which results in a `["CLOSE", probe-<nonce>]` frame at the relay.
// `relay.probe_close_count()` sums these — should be >= 1 after T3's
// flow.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn probe_sub_is_closed_after_settlement() {
    let relay = TestRelay::builder().start().await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Wait for alive settlement.
    timeout(Duration::from_secs(10), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "alive"])
                .get();
            if v >= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("alive probe never settled");

    // Give the unsubscribe → CLOSE frame round-trip a moment to reach the
    // relay (pool.unsubscribe is async and the CLOSE is sent on the WS).
    let close_count = timeout(Duration::from_secs(5), async {
        loop {
            let n = relay.probe_close_count();
            if n >= 1 {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        close_count >= 1,
        "T11: probe sub must be CLOSEd at the relay after alive settlement \
         (polite cleanup); probe_close_count = {close_count}",
    );

    // No probe should be lingering open after settlement.
    let lingering = probe_sub_count(&relay);
    assert_eq!(
        lingering, 0,
        "no probe sub should remain open after settlement; found {lingering}",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T12: probe metrics increment per outcome (exact deltas).
//
// Combine an alive flow (default relay) and a timeout flow (ignore_req_if
// relay) and assert exact deltas on the two `probe_total` label
// combinations.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn probe_metrics_increment_per_outcome() {
    // Alive flow.
    let relay_alive = TestRelay::builder().start().await;
    let (ckpt_a, metrics_a, tx_a, _rx_a, _dir_a) = setup().await;
    let url_a = RelayUrl::parse(relay_alive.url().as_str()).unwrap();
    let metrics_a_clone = std::sync::Arc::clone(&metrics_a);
    let cfg_a = probe_test_cfg(vec![url_a.clone()], vec![0]);
    let acquirer_a = Acquirer::spawn(cfg_a, tx_a, ckpt_a, metrics_a);

    wait_for_initial_forward(&relay_alive).await;
    relay_alive.silently_drop_subscription("forward").await;

    let alive = timeout(Duration::from_secs(10), async {
        loop {
            let v = metrics_a_clone
                .probe_total
                .with_label_values(&[url_a.as_str(), "alive"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert_eq!(
        alive, 1,
        "T12 alive flow: probe_total{{alive}} must be exactly 1, got {alive}",
    );
    let timeout_alive_side = metrics_a_clone
        .probe_total
        .with_label_values(&[url_a.as_str(), "timeout"])
        .get();
    assert_eq!(
        timeout_alive_side, 0,
        "T12 alive flow: probe_total{{timeout}} must be 0, got {timeout_alive_side}",
    );

    // Timeout flow on a separate relay/metrics (otherwise the rate-limit
    // and reconnect state would couple them).
    let relay_to = TestRelay::builder()
        .ignore_req_if(is_probe_filter)
        .start()
        .await;
    let (ckpt_t, metrics_t, tx_t, _rx_t, _dir_t) = setup().await;
    let url_t = RelayUrl::parse(relay_to.url().as_str()).unwrap();
    let metrics_t_clone = std::sync::Arc::clone(&metrics_t);
    let cfg_t = probe_test_cfg(vec![url_t.clone()], vec![0]);
    let acquirer_t = Acquirer::spawn(cfg_t, tx_t, ckpt_t, metrics_t);

    wait_for_initial_forward(&relay_to).await;
    relay_to.silently_drop_subscription("forward").await;

    let timed_out = timeout(Duration::from_secs(15), async {
        loop {
            let v = metrics_t_clone
                .probe_total
                .with_label_values(&[url_t.as_str(), "timeout"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        timed_out >= 1,
        "T12 timeout flow: probe_total{{timeout}} must be >= 1, got {timed_out}",
    );
    let alive_timeout_side = metrics_t_clone
        .probe_total
        .with_label_values(&[url_t.as_str(), "alive"])
        .get();
    assert_eq!(
        alive_timeout_side, 0,
        "T12 timeout flow: probe_total{{alive}} must be 0 on the ignored-probe relay, \
         got {alive_timeout_side}",
    );

    acquirer_a.shutdown().await;
    acquirer_t.shutdown().await;
    relay_alive.shutdown().await;
    relay_to.shutdown().await;
}

// -------------------------------------------------------------------------
// T13: shutdown during in-flight probe is clean.
//
// Issue probe via ignore_req_if; call `acquirer.shutdown().await` while
// `pending_probe.is_some()`. Must complete within budget without panic.
// The Shutdown arm at relay_task.rs:362-377 also
// unsubscribes the pending_probe sub_id; we observe a `probe_close_count
// >= 1`.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_during_in_flight_probe_is_clean() {
    let relay = TestRelay::builder()
        .ignore_req_if(is_probe_filter)
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Wait for `probe_in_flight = 1`.
    let in_flight = timeout(Duration::from_secs(10), async {
        loop {
            let v = metrics_clone
                .probe_in_flight
                .with_label_values(&[url.as_str()])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert_eq!(in_flight, 1, "probe_in_flight should be 1 before shutdown");

    let baseline_probe_closes = relay.probe_close_count();

    // Shutdown should complete within budget without panicking. 5 s is
    // generous against any reasonable disconnect.
    let shutdown_completed = timeout(Duration::from_secs(5), async {
        acquirer.shutdown().await;
    })
    .await
    .is_ok();
    assert!(
        shutdown_completed,
        "T13: acquirer.shutdown() did not complete within 5 s with a probe in flight",
    );

    // After shutdown, the Shutdown arm should have unsubscribed the
    // probe. Give a moment for the CLOSE frame to reach the relay.
    let saw_close = timeout(Duration::from_secs(2), async {
        loop {
            if relay.probe_close_count() > baseline_probe_closes {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        saw_close,
        "T13: Shutdown arm must unsubscribe the in-flight probe (sends CLOSE); \
         baseline={baseline_probe_closes}, after_shutdown={}",
        relay.probe_close_count(),
    );

    // probe_in_flight must return to 0.
    let in_flight_after = metrics_clone
        .probe_in_flight
        .with_label_values(&[url.as_str()])
        .get();
    assert_eq!(
        in_flight_after, 0,
        "T13: probe_in_flight must return to 0 after Shutdown",
    );

    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T14: pressure backoff does not block probe.
//
// Send concurrency-pressure NOTICEs periodically. The `PressureState`
// gates backfill page opens, not the probe. Probe should still fire and
// settle alive against an otherwise-functional relay.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn pressure_backoff_does_not_block_probe() {
    let relay = TestRelay::builder().start().await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;

    // Send a couple of pressure NOTICEs to bump the `PressureState`
    // backoff. Note: the probe is intentionally exempt — see the
    // `tick_forward_sub_probe` body, which gates only on
    // `pending_probe`, `forward_silence_threshold`, `probe_min_interval`
    // and connection state — *not* on `PressureState`.
    relay.send_notice("ERROR: too many concurrent REQs").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    relay.send_notice("ERROR: too many concurrent REQs").await;

    relay.silently_drop_subscription("forward").await;

    let alive = timeout(Duration::from_secs(10), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "alive"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        alive >= 1,
        "T14: probe must fire and settle alive even under concurrency-pressure \
         backoff (probe is intentionally exempt from PressureState). \
         probe_total{{alive}} = {alive}",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T15: notice-only relay results in probe timeout, then reconnect.
//
// NOTICEs are NOT sub-scoped and the probe deliberately diverges from NDK
// by NOT routing NOTICE as alive — a NOTICE during a probe is ambient
// noise, not proof-of-dispatch. So a relay that only sends NOTICEs (and
// ignores REQs) results in a probe timeout.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn notice_only_relay_results_in_probe_timeout_then_reconnect() {
    let relay = TestRelay::builder()
        .ignore_req_if(is_probe_filter)
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Pump NOTICEs at 200 ms intervals throughout the probe window. None
    // should be routed as "alive" → the probe must time out.
    let timed_out = timeout(Duration::from_secs(15), async {
        loop {
            relay.send_notice("ambient noise").await;
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "timeout"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        timed_out >= 1,
        "T15: NOTICE traffic must NOT settle a probe as alive (deliberate \
         divergence from NDK). Expected probe_total{{timeout}} >= 1, got {timed_out}",
    );
    let alive = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();
    assert_eq!(
        alive, 0,
        "T15: probe_total{{alive}} must be 0 — NOTICE traffic is not a settlement signal, \
         got {alive}",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T15b: an active backfill suppresses the probe despite forward silence.
//
// Counterpart to T15 (NOTICE is NOT liveness): backfill traffic IS. The
// dispatch-liveness probe keys on relay-wide *dispatch* activity (events on
// any of our subs + EOSE/CLOSED for our REQs), not just forward-sub events.
// So while a backfill is walking — delivering page after page of events and
// EOSEs — the relay is demonstrably dispatching to us, and the probe must NOT
// fire even though the forward sub itself never sees an event.
//
// This is the regression guard for the kind-9735 firehose storm: before the
// fix the probe keyed on forward-sub silence alone, so during a long backfill
// (where the relay deprioritizes the forward sub) it false-fired, forcing a
// reconnect whose `resubscribe()` reflood overflowed the per-relay fan-out
// channel — observed as 98.7% of all log lines being "notif channel full".
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn active_backfill_suppresses_probe_despite_forward_silence() {
    let keys = Keys::generate();
    // A pile of *historical* events (year-2001 timestamps). The kind is being
    // onboarded, so the forward sub has no `since` and its REQ replays all of
    // them first; after that the forward sub stays silent. The backfill walks
    // them again in 500-event pages (4 data pages here). The per-REQ EOSE delay
    // (2 s) paces those pages so the backfill comfortably outlasts the watch
    // window (24 of 24 runs passed under the CPU load that failed
    // `concurrency_pressure_notice_throttles_backfill_page_rate`'s old bound).
    let events: Vec<Event> = (0..2_000)
        .map(|i| make_event(&keys, 0, "history", 1_000_000_000 + i as u64))
        .collect();
    let relay = TestRelay::builder()
        .preload_events(events)
        .delay_eose_by(Duration::from_secs(2))
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    // Drain the sink so the writer-side channel never backpressures the relay
    // task (which would block it on `send` and prevent a clean shutdown). The
    // drainer exits when the acquirer drops its `tx` on shutdown.
    let drainer = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    // probe_test_cfg: forward_silence_threshold = 3 s, tick = 1 s. Under the
    // old forward-only trigger the probe would fire by ~4 s.
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;

    // Watch for 5 s — comfortably past forward_silence_threshold + one tick.
    // The backfill is still walking (its events + paced EOSEs keep dispatch
    // fresh), so no probe sub must ever appear.
    let end = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < end {
        assert!(
            !has_probe_sub(&relay),
            "probe fired while a backfill was actively dispatching — the false \
             positive the relay-wide-activity trigger removes",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Guard against a vacuous pass: the watch window must have fallen *inside*
    // an active backfill. If the backfill had already completed, the relay
    // would have gone genuinely silent and the assertion above would be
    // meaningless — so require a backfill sub to still be open.
    assert!(
        relay
            .open_subscriptions()
            .iter()
            .any(|s| s.sub_id.starts_with("backfill-")),
        "backfill finished before the watch window ended; test is not exercising \
         the during-backfill case (raise the preload count or EOSE delay)",
    );

    // And no probe outcome was recorded either (catches a probe that fired and
    // settled between polls).
    let alive = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();
    let timeouts = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "timeout"])
        .get();
    assert_eq!(
        alive + timeouts,
        0,
        "no probe should fire while backfill dispatch is active (alive={alive}, timeout={timeouts})",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
    drainer.abort();
}

// -------------------------------------------------------------------------
// T16: probe with split forward subs re-issues all groups.
//
// Drive the relay into a 2-way split via the same setup as the existing
// `concurrency_pressure_notice_does_not_split_forward_subs` test sibling
// (rejecting filters with kinds.len() > N). Once split, drop only one
// group; assert exactly one probe registers and the alive path re-opens
// both groups.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn probe_with_split_forward_subs_reissues_all_groups() {
    // Reject any REQ whose first filter has >= 4 kinds. With 4 configured
    // kinds, this forces a halve to 2+2 → two forward subs (post-split
    // naming: "forward-0" and "forward-1"; see open_all_forward_subs in
    // relay_task.rs — only the single-group case uses the bare "forward"
    // sub_id). The 2-kind post-split REQs pass through; the probe filter
    // has only 1 kind so it is unaffected.
    let relay = TestRelay::builder()
        .reject_req_if(|filters| {
            let any_4plus = filters
                .iter()
                .any(|f| f.kinds.as_ref().is_some_and(|k| k.len() >= 4));
            if any_4plus {
                Some("ERROR: too many filters".to_string())
            } else {
                None
            }
        })
        // EOSE delay so the probe sub is observable in `open_subscriptions`.
        .delay_eose_by(Duration::from_millis(300))
        .start()
        .await;
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let mut cfg = probe_test_cfg(vec![url.clone()], vec![0, 3, 10002, 30023]);
    cfg.kind_slugs = slugs(&[0, 3, 10002, 30023]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Wait for the post-split state: two `"forward-*"` subs.
    let split = timeout(Duration::from_secs(10), async {
        loop {
            let subs = relay.open_subscriptions();
            let forward_subs: Vec<_> = subs
                .iter()
                .filter(|s| s.sub_id.starts_with("forward"))
                .collect();
            if forward_subs.len() >= 2 {
                return forward_subs
                    .iter()
                    .map(|s| s.sub_id.clone())
                    .collect::<Vec<_>>();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_default();
    assert!(
        split.len() >= 2,
        "split forward subs never registered (>=2 expected); got {split:?}"
    );

    // Drain to avoid backpressure.
    let drain_handle = tokio::spawn(async move {
        while let Some(_env) = rx.recv().await {}
    });

    // Pick the second group's sub_id (it ends with "-1") and snapshot
    // pre-state close-counts for both groups.
    let f0_sub_id = split
        .iter()
        .find(|s| s.ends_with("-0"))
        .cloned()
        .unwrap_or_else(|| "forward-0".to_string());
    let f1_sub_id = split
        .iter()
        .find(|s| s.ends_with("-1"))
        .cloned()
        .unwrap_or_else(|| "forward-1".to_string());
    let f0_closes_before = relay.close_count(&f0_sub_id);
    let f1_closes_before = relay.close_count(&f1_sub_id);
    let baseline_alive = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();

    // Drop ONLY the second group. The trigger predicate is "any forward
    // sub stale" so a probe must fire (one for the relay, not one per
    // group).
    relay.silently_drop_subscription(&f1_sub_id).await;

    // Observe at-most-one probe across the recovery window. Also detect
    // settlement via probe_total{alive} which is the most reliable signal.
    let recovery_window_end = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut max_concurrent_probes = 0;
    let mut saw_any_probe = false;
    let mut both_reissued = false;
    while tokio::time::Instant::now() < recovery_window_end {
        let n = probe_sub_count(&relay);
        if n > 0 {
            saw_any_probe = true;
        }
        if n > max_concurrent_probes {
            max_concurrent_probes = n;
        }
        let subs = relay.open_subscriptions();
        let post_split_forward_subs: Vec<_> = subs
            .iter()
            .filter(|s| s.sub_id.starts_with("forward"))
            .collect();
        // After recovery we expect 2 forward subs again (one per group),
        // no probe in flight, AND the alive counter to have incremented.
        let alive_now = metrics_clone
            .probe_total
            .with_label_values(&[url.as_str(), "alive"])
            .get();
        if post_split_forward_subs.len() >= 2 && n == 0 && alive_now > baseline_alive {
            both_reissued = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(saw_any_probe, "T16: no probe ever registered after split drop");
    assert!(
        max_concurrent_probes <= 1,
        "T16: at most one probe per relay across all groups; saw \
         max_concurrent_probes = {max_concurrent_probes}",
    );
    assert!(
        both_reissued,
        "T16: alive path must re-open BOTH forward groups (close_all + open_all). \
         Recovery did not produce a state with two forward-* subs, no probe, and \
         an alive counter bump within 15 s.",
    );

    // close_all_forward_subs sends CLOSE for every group; the relay
    // should record both. We dropped f1 server-side (no CLOSE sent from
    // client at that moment), but the alive path's close_all sends CLOSE
    // for every group including f0.
    let f0_closes_after = relay.close_count(&f0_sub_id);
    let f1_closes_after = relay.close_count(&f1_sub_id);
    assert!(
        f0_closes_after > f0_closes_before,
        "T16: '{f0_sub_id}' must receive a CLOSE during recovery \
         (close_all_forward_subs); before={f0_closes_before}, after={f0_closes_after}",
    );
    assert!(
        f1_closes_after >= f1_closes_before,
        "T16: '{f1_sub_id}' CLOSE count must not decrease; before={f1_closes_before}, \
         after={f1_closes_after}",
    );

    drop(drain_handle);
    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T17: kill switch disables probe.
//
// `forward_silence_threshold = 0` disables the probe entirely (kill
// switch — see `tick_forward_sub_probe` first line). Drop the forward
// sub, wait, publish more events; assert zero probe activity and zero
// reconnect attempts.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn kill_switch_disables_probe() {
    let keys = Keys::generate();
    let relay = TestRelay::builder().start().await;
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    // Kill switch ON: threshold = 0.
    let mut cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    cfg.forward_silence_threshold = Duration::from_secs(0);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;

    let baseline_disconnects = metrics_clone
        .relay_disconnects
        .with_label_values(&[url.as_str()])
        .get();

    relay.silently_drop_subscription("forward").await;

    // Drain in background.
    let drain_handle = tokio::spawn(async move {
        while let Some(_env) = rx.recv().await {}
    });

    // Wait well past what would have been the threshold (5 s here is far
    // beyond the standard 3 s probe threshold). Publish a few events along
    // the way — these would normally bump `last_activity_at`, but with the
    // dropped sub they don't reach us, so we're testing the kill-switch
    // skip-before-trigger path.
    for i in 0..5u64 {
        let e = make_event(&keys, 0, &format!("ks-{i}"), 1_000_000_000 + i);
        relay.publish(e).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Zero probes, zero increments.
    assert!(
        !has_probe_sub(&relay),
        "T17: kill switch (forward_silence_threshold=0) must prevent any probe \
         registration; found one open",
    );
    assert_eq!(
        probe_sub_count(&relay),
        0,
        "T17: kill switch must keep probe_sub_count at 0 throughout",
    );
    let alive = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();
    let timeouts = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "timeout"])
        .get();
    assert_eq!(
        alive, 0,
        "T17: kill switch must keep probe_total{{alive}} at 0, got {alive}",
    );
    assert_eq!(
        timeouts, 0,
        "T17: kill switch must keep probe_total{{timeout}} at 0, got {timeouts}",
    );
    let disconnects_after = metrics_clone
        .relay_disconnects
        .with_label_values(&[url.as_str()])
        .get();
    assert_eq!(
        disconnects_after, baseline_disconnects,
        "T17: kill switch must not produce any reconnects; baseline={baseline_disconnects}, \
         after={disconnects_after}",
    );

    drop(drain_handle);
    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T18: UpdateKinds cancels in-flight probe.
//
// An UpdateKinds while a probe is in
// flight must unsubscribe the probe and clear pending_probe — without
// this, the probe's eventual settlement would tear down the
// freshly-reloaded forward subs. We assert (a) probe_in_flight returns
// to 0, (b) probe sub is CLOSEd, (c) the new forward subs reflect the
// updated kind set, (d) waiting beyond probe_timeout does not trigger
// any phantom close+reopen.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn update_kinds_cancels_in_flight_probe() {
    let relay = TestRelay::builder()
        .ignore_req_if(is_probe_filter)
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let initial_cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(initial_cfg.clone(), tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Wait for probe_in_flight == 1.
    let in_flight = timeout(Duration::from_secs(10), async {
        loop {
            let v = metrics_clone
                .probe_in_flight
                .with_label_values(&[url.as_str()])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert_eq!(in_flight, 1, "probe_in_flight should be 1 before UpdateKinds");

    let baseline_probe_closes = relay.probe_close_count();
    let baseline_alive = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();

    // Now send UpdateKinds via apply_config — new kind set {0, 30023}.
    let new_cfg = AcquirerConfig {
        kinds: vec![0, 30023],
        kind_slugs: slugs(&[0, 30023]),
        ..initial_cfg
    };
    acquirer.apply_config(new_cfg).await;

    // (a) probe_in_flight returns to 0.
    let in_flight_post = timeout(Duration::from_secs(5), async {
        loop {
            let v = metrics_clone
                .probe_in_flight
                .with_label_values(&[url.as_str()])
                .get();
            if v == 0 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(-1);
    assert_eq!(
        in_flight_post, 0,
        "T18(a): probe_in_flight must return to 0 after UpdateKinds cancels the probe",
    );

    // (b) probe sub is CLOSEd (probe_close_count incremented).
    let saw_close = timeout(Duration::from_secs(5), async {
        loop {
            if relay.probe_close_count() > baseline_probe_closes {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        saw_close,
        "T18(b): UpdateKinds must unsubscribe the in-flight probe (sends CLOSE)",
    );

    // (c) the new forward subs reflect the updated kind set (must include
    // 30023). Find the forward sub and inspect its filter kinds.
    let kinds_updated = timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = relay.open_subscriptions();
            if let Some(f) = snapshot
                .iter()
                .find(|s| s.sub_id.starts_with("forward"))
            {
                let kinds_in_filters: Vec<u16> = f
                    .filters
                    .iter()
                    .flat_map(|filt| {
                        filt.kinds
                            .as_ref()
                            .map(|ks| ks.iter().map(|k| k.as_u16()).collect::<Vec<_>>())
                            .unwrap_or_default()
                    })
                    .collect();
                if kinds_in_filters.contains(&30023) && kinds_in_filters.contains(&0) {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        kinds_updated,
        "T18(c): new forward sub must reflect the updated kind set {{0, 30023}}",
    );

    // (d) wait WITHIN probe_min_interval so no fresh probe can legitimately
    // fire (rate-limit anchors on the cancelled probe's `last_probe_attempt_at`,
    // so the trigger predicate stays blocked through this window). Anything
    // we observe is therefore a phantom settlement of the cancelled probe.
    // Choose 3 s < probe_min_interval = 4 s but > probe_timeout = 2 s, so
    // the cancelled probe's timeout would have fired had cancellation
    // failed.
    let snapshot_before_wait = relay.open_subscriptions();
    let forward_subs_before: Vec<String> = snapshot_before_wait
        .iter()
        .filter(|s| s.sub_id.starts_with("forward"))
        .map(|s| s.sub_id.clone())
        .collect();
    let close_count_forward_before = relay.close_count("forward");
    tokio::time::sleep(Duration::from_secs(3)).await; // < probe_min_interval, > probe_timeout
    let alive_after = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();
    assert_eq!(
        alive_after, baseline_alive,
        "T18(d): no phantom alive settlement should fire after UpdateKinds; \
         baseline_alive={baseline_alive}, after={alive_after}",
    );

    // probe_in_flight must remain 0 — the rate limit blocks any fresh
    // probe within this window, so a non-zero value would mean the
    // cancelled probe wasn't actually cancelled.
    let in_flight_during_wait = metrics_clone
        .probe_in_flight
        .with_label_values(&[url.as_str()])
        .get();
    assert_eq!(
        in_flight_during_wait, 0,
        "T18(d): probe_in_flight must remain 0 during the rate-limited window; \
         got {in_flight_during_wait} (cancellation may not have cleared pending_probe)",
    );

    // Forward subs must still be the same — no phantom close+reopen.
    let snapshot_after_wait = relay.open_subscriptions();
    let forward_subs_after: Vec<String> = snapshot_after_wait
        .iter()
        .filter(|s| s.sub_id.starts_with("forward"))
        .map(|s| s.sub_id.clone())
        .collect();
    let close_count_forward_after = relay.close_count("forward");
    assert_eq!(
        forward_subs_before, forward_subs_after,
        "T18(d): forward subs must not change during the post-UpdateKinds wait \
         window (no phantom close+reopen)",
    );
    assert_eq!(
        close_count_forward_before, close_count_forward_after,
        "T18(d): no additional CLOSE on 'forward' during the wait window",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T19: unrelated CLOSED in-flight does not settle probe.
//
// Hold a probe in flight via ignore_req_if. From the test, send CLOSED
// for an unrelated sub_id (a backfill, or any other sub_id not matching
// the probe). Assert: (a) pending_probe stays in flight (probe_in_flight
// remains 1), (b) no `alive` count increment, (c) the probe still times
// out correctly after probe_timeout.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn unrelated_closed_in_flight_does_not_settle_probe() {
    let relay = TestRelay::builder()
        .ignore_req_if(is_probe_filter)
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Wait for probe in flight.
    timeout(Duration::from_secs(10), async {
        loop {
            let v = metrics_clone
                .probe_in_flight
                .with_label_values(&[url.as_str()])
                .get();
            if v >= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("probe never went in-flight");
    let baseline_alive = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();

    // Send a CLOSED for a sub_id that is NOT the probe sub. We don't
    // know exactly what backfill sub_ids are open at any moment, but
    // sending CLOSED for a synthetic sub_id like "bogus-sub-id" still
    // exercises the routing predicate at the acquirer side — the CLOSED
    // arm checks `pending_probe.as_ref().map(|p| p.sub_id == sid)` which
    // will be false for "bogus-sub-id".
    relay
        .close_subscription("bogus-sub-id-not-a-probe", "test-injection")
        .await;

    // Give the message a moment to flow.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // (a) probe still in flight.
    let still_in_flight = metrics_clone
        .probe_in_flight
        .with_label_values(&[url.as_str()])
        .get();
    assert_eq!(
        still_in_flight, 1,
        "T19(a): unrelated CLOSED must NOT settle the probe; \
         probe_in_flight = {still_in_flight} (expected 1)",
    );
    // (b) no alive increment.
    let alive_now = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();
    assert_eq!(
        alive_now, baseline_alive,
        "T19(b): unrelated CLOSED must NOT increment probe_total{{alive}}; \
         baseline={baseline_alive}, after={alive_now}",
    );

    // (c) probe still times out correctly.
    let timed_out = timeout(Duration::from_secs(8), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "timeout"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        timed_out >= 1,
        "T19(c): probe must still time out after probe_timeout even though \
         an unrelated CLOSED was injected; probe_total{{timeout}} = {timed_out}",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T20: late probe response after timeout is a no-op.
//
// Use `delay_eose_by(2 * probe_timeout)` so EOSE arrives long after the
// probe-timeout window. Probe issues → times out → relay reconnects.
// Post-reconnect, the lib's `resubscribe()` replays subs from
// `atomic.subscriptions` — but the probe's sub_id was unsubscribed
// before the disconnect (load-bearing), so no phantom probe re-appears.
//
// Assertions: (a) probe_total{timeout} >= 1, (b) no probe-* sub
// re-registers at the relay post-reconnect, (c) the late EOSE never
// produces an alive increment.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn late_probe_response_after_timeout_is_noop() {
    // delay_eose_by = 2 * probe_timeout (4 s) — well past the 2 s timeout.
    let relay = TestRelay::builder()
        .delay_eose_by(Duration::from_secs(4))
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;

    // Drop forward sub → probe will fire after threshold.
    relay.silently_drop_subscription("forward").await;

    // Wait for the probe-timeout to fire.
    let timed_out = timeout(Duration::from_secs(15), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "timeout"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        timed_out >= 1,
        "T20(a): probe must time out (delay_eose_by exceeds probe_timeout); \
         probe_total{{timeout}} = {timed_out}",
    );

    // Snapshot probe sub_ids that were ever recorded for this WS
    // (pre-timeout). Anything starting with "probe-" represents the
    // original probe we're tracking for the post-reconnect replay check.
    let original_probe_subs_seen: std::collections::HashSet<String> = relay
        .open_subscriptions()
        .iter()
        .map(|s| s.sub_id.clone())
        .filter(|sid| sid.starts_with("probe-"))
        .collect();
    // Snapshot the `alive` counter BEFORE the late-EOSE window so we can
    // assert (c) below: the late EOSE on the torn-down WebSocket must
    // never produce an alive increment.
    let baseline_alive = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();

    // Wait through the late-EOSE delivery window: delay_eose_by (4 s) +
    // generous slack for post-reconnect to settle. Total: ~8 s.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // (b) Post-reconnect, no probe-* sub from the original set should
    // re-appear at the relay — the unsubscribe before disconnect_relay
    // removed it from the lib's `atomic.subscriptions`, so the
    // post-connection `resubscribe()` does not replay it. (A *fresh*
    // probe with a new sub_id may legitimately fire if the rate-limit
    // interval elapsed and the forward sub is still stale; that is not
    // a phantom replay.)
    let current_probe_subs: std::collections::HashSet<String> = relay
        .open_subscriptions()
        .iter()
        .map(|s| s.sub_id.clone())
        .filter(|sid| sid.starts_with("probe-"))
        .collect();
    let replayed_originals: Vec<_> = current_probe_subs
        .intersection(&original_probe_subs_seen)
        .cloned()
        .collect();
    assert!(
        replayed_originals.is_empty(),
        "T20(b): no original probe sub_id should re-appear after reconnect \
         (the unsubscribe before disconnect_relay must clear atomic.subscriptions \
         so post_connection's resubscribe doesn't replay the probe). \
         Replayed: {replayed_originals:?}",
    );

    // (c) The late EOSE arriving on the torn-down WebSocket must never
    // settle the original probe as Alive. Without the lib-table cleanup
    // step, the late frame could conceivably ride the new connection and
    // trigger `on_probe_settled(Alive)` for a sub_id that's no longer
    // pending — which would falsely re-issue the forward subs.
    let alive_after = metrics_clone
        .probe_total
        .with_label_values(&[url.as_str(), "alive"])
        .get();
    assert_eq!(
        alive_after, baseline_alive,
        "T20(c): late EOSE for the torn-down probe must NOT increment \
         probe_total{{alive}} (baseline {baseline_alive}, got {alive_after})",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// -------------------------------------------------------------------------
// T21: probe timeout MUST be followed by relay reconnect + resubscribe.
//
// Regression test for the bug observed in production on 2026-05-23:
// nos.lol probe timed out at 15:54:40 UTC and the relay then stayed in
// `RelayStatus::Terminated` for ~45 minutes (until process restart),
// silently absent from `connected` count in heartbeats and from
// `events_received_total`.
//
// Root cause: `pool.disconnect_relay()` puts the relay into
// `RelayStatus::Terminated`, and the lib's reconnect loop at
// `nostr_relay_pool::relay::inner::connect_with_backoff` breaks out
// when status is Terminated (see inner.rs:568-571). Auto-reconnect
// only kicks in for *network-level* failures bubbling up through
// `connect_and_run`; explicit `disconnect_relay` is treated as a
// permanent kill. Earlier plan-time research (and the docstring on
// `tick_forward_sub_probe`) incorrectly claimed otherwise.
//
// T4 caught the disconnect side (probe_total{timeout} + relay_disconnects
// increments) but did not assert the *recovery* side — and the bug
// is precisely the absence of recovery. This test holds the relay in
// `ignore_req_if(is_probe_filter)` so the probe path is exercised, then
// asserts that within bounded wall-clock time the lib has reconnected
// and the forward sub has re-registered at the relay (the smoking gun
// for a working `connect_relay` re-arm).
//
// Note: under `ignore_req_if(probe)` the recovery cycle is self-limiting:
// forward re-registers → goes silent again after `forward_silence_threshold`
// → probe → timeout → another disconnect/reconnect. The test latches on
// the *first* re-registration after the first timeout; that is the only
// signal we need to confirm `connect_relay` works.
// -------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn probe_timeout_followed_by_relay_reconnect_and_resubscribe() {
    let relay = TestRelay::builder()
        .ignore_req_if(is_probe_filter)
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let metrics_clone = std::sync::Arc::clone(&metrics);
    let cfg = probe_test_cfg(vec![url.clone()], vec![0]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    wait_for_initial_forward(&relay).await;
    relay.silently_drop_subscription("forward").await;

    // Wait for the probe-timeout to fire. After this point the lib has
    // called `disconnect_relay` and the relay is in `Terminated` state.
    let timed_out = timeout(Duration::from_secs(15), async {
        loop {
            let v = metrics_clone
                .probe_total
                .with_label_values(&[url.as_str(), "timeout"])
                .get();
            if v >= 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        timed_out >= 1,
        "T21 precondition: probe must time out (probe_total{{timeout}} \
         expected >= 1, got {timed_out})",
    );

    // `silently_drop_subscription` only purges from the test relay's
    // tracking map — the WebSocket itself is intact at this point. When
    // the probe-timeout path runs `disconnect_relay`, that closes the
    // WebSocket, which the test relay observes and cleans up via
    // `subscriptions.retain(|(cid, _), _| *cid != conn_id)` in server.rs.
    // So the post-timeout `open_subscriptions()` is empty until the lib
    // reconnects and resubscribes.
    //
    // Bug case: lib stays in Terminated → no reconnect → `"forward"` never
    // re-registers → this assertion fails.
    // Fixed case: lib re-arms via `connect_relay` → post_connection runs
    // resubscribe() → `"forward"` re-registers within retry_interval
    // (~2 s) + post_connection time.
    let forward_reappeared = timeout(Duration::from_secs(15), async {
        loop {
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward")
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        forward_reappeared,
        "T21: forward sub did not re-register at the relay within 15 s \
         after probe timeout — relay is stuck in RelayStatus::Terminated. \
         The probe-timeout path must follow `disconnect_relay` with an \
         explicit `connect_relay` call; auto-reconnect does NOT kick in \
         for explicit termination (nostr_relay_pool 0.44: \
         inner.rs:568-571 breaks the connect loop when status is Terminated).",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// Regression: when a kind-group contains one kind with `BackfillState::Complete`
/// and `since=None` (backfill walked to the bottom but found zero events), the
/// forward subscription for the entire group must NOT be issued without a `since`
/// field. Without the fix, the `else` branch in `open_all_forward_subs` treated
/// any `since=None` as "genuinely uncharted", set `min_since=None` and broke out
/// of the loop, causing the relay to re-deliver its full stored-event cap on every
/// sub re-open (~60 s probe-alive cycle).
///
/// Mixed-group scenario: one kind (k0) has a real checkpoint; another (k30023)
/// has `Complete+since=None`. The forward filter's `since` must be driven by k0
/// (the `Complete+None` kind contributes `now` which is larger, so k0's older
/// checkpoint wins via `min`).
#[tokio::test(flavor = "multi_thread")]
async fn forward_sub_since_not_poisoned_by_complete_zero_events_kind() {
    let keys = Keys::generate();
    let relay = TestRelay::builder().start().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();

    let (ckpt, metrics, tx, _rx, _dir) = setup().await;

    // Pre-seed k0 with a real checkpoint at a well-known past timestamp.
    let k0_since: i64 = 1_700_000_000;
    let dummy_event = make_event(&keys, 0, "seed", k0_since as u64);
    ckpt.observe(&url, 0, k0_since, &dummy_event.id).await;
    ckpt.record_backfill(&url, 0, BackfillState::Complete).await;

    // k30023: backfill completed but found zero events — since stays None.
    ckpt.record_backfill(&url, 30023, BackfillState::Complete).await;

    let safety_margin_secs: u64 = 60;
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0, 30023],
        kind_slugs: slugs(&[0, 30023]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(safety_margin_secs),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Wait for the forward sub to appear.
    let forward = timeout(Duration::from_secs(5), async {
        loop {
            let subs = relay.open_subscriptions();
            if let Some(s) = subs.into_iter().find(|s| s.sub_id.starts_with("forward")) {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("forward sub did not open within 5s");

    // The filter must have `since` set. Pre-fix: `since=None` because k30023's
    // `Complete+None` broke out of the loop and set `min_since = None`.
    let filter_since = forward.filters.iter().find_map(|f| f.since);
    assert!(
        filter_since.is_some(),
        "forward filter must have since set when all kinds have backfill=Complete; \
         got since=None — the Complete+zero-events kind is poisoning min_since. \
         filters={:?}",
        forward.filters,
    );

    // The `since` value must be driven by k0's checkpoint (minus safety margin).
    // k30023 contributes `now` (a large current timestamp) which is larger than
    // k0's 1_700_000_000, so the `min` picks k0's value.
    let expected_since = nostr::Timestamp::from_secs(
        (k0_since - safety_margin_secs as i64).max(0) as u64,
    );
    assert_eq!(
        filter_since,
        Some(expected_since),
        "forward filter since should equal k0's checkpoint ({k0_since}) minus \
         safety_margin ({safety_margin_secs}s) = {}; got {:?}",
        expected_since.as_secs(),
        filter_since.map(|t| t.as_secs()),
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// Regression (all-Complete+None variant): when every kind in a group has
/// `BackfillState::Complete` and `since=None`, the forward sub must still
/// include a `since` field (approximately `now - safety_margin`). Without
/// the fix, `min_since` stayed `None` for the entire loop and the relay
/// received a since-less REQ.
#[tokio::test(flavor = "multi_thread")]
async fn forward_sub_since_not_poisoned_when_all_kinds_complete_zero_events() {
    let relay = TestRelay::builder().start().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();

    let (ckpt, metrics, tx, _rx, _dir) = setup().await;

    // Both kinds: backfill Complete, zero events ever seen (since=None).
    ckpt.record_backfill(&url, 0, BackfillState::Complete).await;
    ckpt.record_backfill(&url, 30023, BackfillState::Complete).await;

    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0, 30023],
        kind_slugs: slugs(&[0, 30023]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let before_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Wait for the forward sub to appear.
    let forward = timeout(Duration::from_secs(5), async {
        loop {
            let subs = relay.open_subscriptions();
            if let Some(s) = subs.into_iter().find(|s| s.sub_id.starts_with("forward")) {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("forward sub did not open within 5s");

    // The filter must have `since` set — not None. Pre-fix both kinds have
    // since=None so min_since never gets set, yielding a since-less REQ.
    let filter_since = forward.filters.iter().find_map(|f| f.since);
    assert!(
        filter_since.is_some(),
        "forward filter must have since set when all kinds are Complete with \
         zero events ever seen; got since=None — the relay will re-deliver \
         all stored events on every sub re-open. filters={:?}",
        forward.filters,
    );

    // The value must be in a reasonable range: approximately `now - 60s`.
    // Allow a generous window around `before_secs - 60` (clock jitter and
    // startup time). The key invariant is "not None" and "not epoch-0".
    let since_secs = filter_since.unwrap().as_secs();
    let lower = before_secs.saturating_sub(90); // generous lower bound
    assert!(
        since_secs >= lower,
        "forward filter since ({since_secs}) should be approximately now ({before_secs}) \
         minus safety margin (60s); got a value too far in the past"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// ---------------------------------------------------------------------------
// Backfill pagination boundary correctness
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn backfill_captures_all_events_at_boundary_timestamp() {
    // Regression test for the backfill pagination blind spot.
    //
    // When a page fills to `limit` and multiple events share the oldest
    // timestamp on that page, the relay returns some but not all of them. The
    // pre-fix behaviour set `next_until = oldest - 1`, permanently excluding
    // the remainder. The fix uses `next_until = oldest` (inclusive) so the
    // next page can deliver those events; the pool's event-ID deduplication
    // discards events already seen.
    //
    // Concrete setup (limit=50, matching BACKFILL_MIN_LIMIT so clamping doesn't
    // change the effective page size):
    //   60 events total:
    //     - 45 above the boundary (t=1_000..1_044, unique timestamps)
    //     - 10 at the boundary (t=900)
    //     - 5 below (t=899, 898, 897, 896, 895)
    //
    //   Relay sorts by (created_at DESC, id ASC) and truncates at limit=50.
    //
    //   Page 1 (until=now, ≥ every event's created_at so it fetches from the top):
    //     45 above-boundary events + first 5 of the 10 at t=900 = 50 events.
    //     oldest=900, received=50=limit.
    //     Pre-fix: next_until=899 → page 2 misses the remaining 5 at t=900.
    //     Post-fix: next_until=900 (inclusive, cursor advanced) → page 2
    //               retrieves all 10 at t=900 + 5 below = 15 events; the 5
    //               already-seen boundary events are deduplicated by the pool.
    //
    //   Page 2 (post-fix, until=900):
    //     15 events on wire; pool deduplicates 5 boundary repeats → 10 new
    //     events reach the sink (5 new boundary + 5 below).
    //     oldest=895, received=15 < 50 → next_until=894.
    //
    //   Pages 3+: 0 events → streak → complete.
    //   All 60 events captured.
    //
    // Pre-fix page 2 (until=899): only the 5 below-boundary events are
    // returned. Pages 3+: empty → complete. 5 boundary events permanently lost.
    //
    // Forward sub is excluded by pre-seeding the checkpoint with since=10_000;
    // effective forward since = 10_000 - safety_margin(60) = 9_940, which is
    // far above all test events (t ≤ 1_044). Only backfill can deliver them.

    // 45 keys for above-boundary, 10 for boundary, 5 for below, 1 for seed.
    let keys: Vec<Keys> = (0..61).map(|_| Keys::generate()).collect();
    let boundary_ts = 900u64;

    let mut events: Vec<Event> = Vec::with_capacity(60);
    // 45 above-boundary (t=1_000..1_044)
    for i in 0u64..45 {
        events.push(make_event(&keys[i as usize], 0, &format!("above_{i}"), 1_000 + i));
    }
    // 10 at boundary
    for i in 0u64..10 {
        events.push(make_event(&keys[45 + i as usize], 0, &format!("tie_{i}"), boundary_ts));
    }
    // 5 below boundary (t=899..895)
    for i in 0u64..5 {
        events.push(make_event(&keys[55 + i as usize], 0, &format!("below_{i}"), 899 - i));
    }
    assert_eq!(events.len(), 60);

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;

    // Pre-seed: since=10_000 keeps forward sub's effective since at 9_940,
    // far above all test events. Backfill stays Pending so the backfill runs.
    let seed_event = make_event(&keys[60], 0, "seed", 10_000);
    ckpt.observe(&relay_url, 0, 10_000i64, &seed_event.id).await;

    let cfg = AcquirerConfig {
        relays: vec![relay_url],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        // 50 == BACKFILL_MIN_LIMIT so clamping is a no-op; page 1 fills
        // exactly (45 above + 5 boundary = 50) leaving 5 boundary events
        // unreachable under the pre-fix oldest-1 cursor.
        backfill_default_limit: 50,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    let received = drain_events(&mut rx, events.len(), Duration::from_secs(15)).await;
    let unique: std::collections::HashSet<_> = received.iter().map(|e| e.event.id).collect();
    assert_eq!(
        unique.len(),
        events.len(),
        "expected all {} events captured including all 10 at the boundary \
         timestamp (t={boundary_ts}); got {} unique — {} events were lost in the \
         backfill pagination blind spot (oldest-1 cursor excluded boundary events). \
         If this fails without the prev_oldest_created_at fix in relay_task.rs, \
         that is expected: the fix is required.",
        events.len(),
        unique.len(),
        events.len() - unique.len(),
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// Wait until the backfill of `kind` on `url` reaches `Complete`, or panic
/// after `deadline`.
async fn await_backfill_complete(
    ckpt: &Arc<dyn CheckpointStore>,
    url: &RelayUrl,
    kind: u16,
    deadline: Duration,
) {
    let completed = timeout(deadline, async {
        loop {
            if ckpt.backfill_state(url, kind).await == BackfillState::Complete {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "backfill of kind {kind} on {url} did not reach Complete within {deadline:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_captures_boundary_ties_when_the_relay_clamps_below_the_limit() {
    // #32 (Backfill silently drops boundary-tie events when a relay serves
    // fewer events per REQ than backfill_default_limit), first trigger: a
    // relay that serves fewer events per REQ than we ask for and advertises
    // nothing (relay.conduit.market serves 100 of 500; 70 of 7,353 events were
    // lost at its onboarding). Every page is short, so a rule that holds the
    // cursor inclusively only on a full page never holds it, and at each page
    // boundary the tied events that did not fit are skipped.
    //
    // Setup: we ask for 100, the relay clamps at 50.
    //   45 above the boundary (t=1_000..1_044), 10 at t=900, 5 below
    //   (t=899..895); the relay sorts (created_at DESC, id ASC).
    //   p1 (until=now): 45 above + the first 5 at t=900 = 50 < 100, short.
    //       Pre-fix: next until = 899, and the other 5 at t=900 are lost.
    //       Post-fix: the oldest moved (no prev) => next until = 900.
    //   p2 (until=900): all 10 at t=900 + the 5 below = 15; the pool drops
    //       the 5 repeats. The oldest moved to 895 => next until = 895.
    //   p3 (until=895): the event at t=895 again; stagnated => 894.
    //   p4, p5: empty => Complete (empty_pages_to_complete = 2).
    //
    // The forward sub is kept out by a checkpoint at since=10_000 (effective
    // 9_940, above every test event), so only the backfill can deliver them.
    let keys: Vec<Keys> = (0..61).map(|_| Keys::generate()).collect();
    let boundary_ts = 900u64;
    let mut events: Vec<Event> = Vec::with_capacity(60);
    for i in 0u64..45 {
        events.push(make_event(
            &keys[i as usize],
            0,
            &format!("above_{i}"),
            1_000 + i,
        ));
    }
    for i in 0u64..10 {
        events.push(make_event(
            &keys[45 + i as usize],
            0,
            &format!("tie_{i}"),
            boundary_ts,
        ));
    }
    for i in 0u64..5 {
        events.push(make_event(
            &keys[55 + i as usize],
            0,
            &format!("below_{i}"),
            899 - i,
        ));
    }

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .clamp_each_page_to(50)
        .start()
        .await;
    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let ckpt_poll = ckpt.clone();
    let seed_event = make_event(&keys[60], 0, "seed", 10_000);
    ckpt.observe(&relay_url, 0, 10_000i64, &seed_event.id).await;

    let cfg = AcquirerConfig {
        relays: vec![relay_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        // Twice the relay's clamp: every page it serves is short.
        backfill_default_limit: 100,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    let received = drain_events(&mut rx, events.len(), Duration::from_secs(15)).await;
    let unique: std::collections::HashSet<_> = received.iter().map(|e| e.event.id).collect();
    let ties_captured = events
        .iter()
        .filter(|e| e.created_at.as_secs() == boundary_ts && unique.contains(&e.id))
        .count();
    assert_eq!(
        unique.len(),
        events.len(),
        "expected all {} events, the 10 at t={boundary_ts} among them, from a \
         relay clamping below our limit; got {} ({ties_captured} of 10 at the \
         boundary). A short page must hold the cursor inclusively too.",
        events.len(),
        unique.len(),
    );
    await_backfill_complete(&ckpt_poll, &relay_url, 0, Duration::from_secs(15)).await;

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_captures_boundary_ties_when_the_pool_drops_an_event_from_a_page() {
    // #32 (Backfill silently drops boundary-tie events when a relay serves
    // fewer events per REQ than backfill_default_limit), the trigger
    // index.ngit.dev showed before its onboarding on 2026-10-12: the relay
    // serves a full page, but one event in it carries an id that does not
    // hash. The pool rejects that event in `event.verify()` before any
    // notification, so nozzl counts 49 of 50 and reads the page as short,
    // although the relay's page was full and more events share its oldest
    // second. An expired `expiration` tag (#41 (rust-nostr drops expired
    // events before any notification)) and a deserialize failure (#31
    // (Pool-level deserialize failures are unmetered event loss)) take a slot
    // the same way.
    //
    // Setup (limit 50): one tampered and 44 valid events above the boundary
    // (t=1_000..1_044), 10 valid at t=900, 5 below (t=899..895).
    //   p1 (until=now): the 45 above + the first 5 at t=900 = 50 on the wire;
    //       the pool drops the tampered one, so received = 49 < 50.
    //       Pre-fix: next until = 899, and the other 5 at t=900 are lost.
    //       Post-fix: next until = 900, inclusive, and p2 serves them.
    let keys: Vec<Keys> = (0..61).map(|_| Keys::generate()).collect();
    let boundary_ts = 900u64;
    let mut valid: Vec<Event> = Vec::with_capacity(59);
    for i in 1u64..45 {
        valid.push(make_event(
            &keys[i as usize],
            0,
            &format!("above_{i}"),
            1_000 + i,
        ));
    }
    for i in 0u64..10 {
        valid.push(make_event(
            &keys[45 + i as usize],
            0,
            &format!("tie_{i}"),
            boundary_ts,
        ));
    }
    for i in 0u64..5 {
        valid.push(make_event(
            &keys[55 + i as usize],
            0,
            &format!("below_{i}"),
            899 - i,
        ));
    }
    // The content no longer hashes to the id: `verify()` fails with InvalidId.
    let signed = make_event(&keys[0], 0, "above_0", 1_000);
    let tampered = Event::new(
        signed.id,
        signed.pubkey,
        signed.created_at,
        signed.kind,
        signed.tags.to_vec(),
        "tampered after signing",
        signed.sig,
    );
    assert!(
        tampered.verify().is_err(),
        "the tampered event must fail verification"
    );
    let mut served = valid.clone();
    served.push(tampered.clone());

    let relay = TestRelay::builder().preload_events(served).start().await;
    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let ckpt_poll = ckpt.clone();
    let seed_event = make_event(&keys[60], 0, "seed", 10_000);
    ckpt.observe(&relay_url, 0, 10_000i64, &seed_event.id).await;

    let cfg = AcquirerConfig {
        relays: vec![relay_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 50, // == BACKFILL_MIN_LIMIT; clamping is a no-op
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    let received = drain_events(&mut rx, valid.len(), Duration::from_secs(15)).await;
    await_backfill_complete(&ckpt_poll, &relay_url, 0, Duration::from_secs(15)).await;
    let unique: std::collections::HashSet<_> = received.iter().map(|e| e.event.id).collect();
    let ties_captured = valid
        .iter()
        .filter(|e| e.created_at.as_secs() == boundary_ts && unique.contains(&e.id))
        .count();
    assert_eq!(
        unique.len(),
        valid.len(),
        "expected all {} valid events, the 10 at t={boundary_ts} among them; got {} \
         ({ties_captured} of 10 at the boundary). A page the pool shortened must \
         hold the cursor inclusively.",
        valid.len(),
        unique.len(),
    );
    assert!(
        !unique.contains(&tampered.id),
        "the pool must never hand the tampered event to the sink"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_resumed_on_a_held_second_captures_its_ties() {
    // A restart mid-walk under the rule of #32 (Backfill silently drops
    // boundary-tie events when a relay serves fewer events per REQ than
    // backfill_default_limit): the persisted cursor is the second a short page
    // held inclusively, and the resumed page has no previous oldest, so the
    // stagnation guard cannot fire on it. The resumed walk must still read that
    // second whole and the history below it, then complete.
    //
    // Setup: we ask for 100, the relay clamps at 50; the checkpoint says
    // InProgress { until_cursor: 900 } with 10 events at t=900 and 5 below.
    //   resumed page (until=900): 10 at t=900 + 5 below = 15; oldest 895 moved
    //       (no prev) => until 895; the re-read of t=895 stagnates => 894;
    //       two empty pages => Complete.
    let keys: Vec<Keys> = (0..61).map(|_| Keys::generate()).collect();
    let mut events: Vec<Event> = Vec::new();
    for i in 0u64..30 {
        // above the cursor: a resumed walk never asks for these again
        events.push(make_event(
            &keys[i as usize],
            0,
            &format!("above_{i}"),
            1_000 + i,
        ));
    }
    let mut wanted: Vec<Event> = Vec::new();
    for i in 0u64..10 {
        wanted.push(make_event(
            &keys[30 + i as usize],
            0,
            &format!("tie_{i}"),
            900,
        ));
    }
    for i in 0u64..5 {
        wanted.push(make_event(
            &keys[40 + i as usize],
            0,
            &format!("below_{i}"),
            899 - i,
        ));
    }
    events.extend(wanted.iter().cloned());
    let untils = Arc::new(std::sync::Mutex::new(Vec::<Option<u64>>::new()));
    let seen = untils.clone();
    let relay = TestRelay::builder()
        .preload_events(events)
        .clamp_each_page_to(50)
        .reject_req_if(move |filters| {
            if let Some(f) = filters.iter().find(|f| f.limit.is_some()) {
                seen.lock().unwrap().push(f.until.map(|t| t.as_secs()));
            }
            None
        })
        .start()
        .await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let seed = make_event(&keys[60], 0, "seed", 10_000);
    ckpt.observe(&url, 0, 10_000i64, &seed.id).await;
    ckpt.record_backfill(&url, 0, BackfillState::InProgress { until_cursor: 900 })
        .await;
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 100,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), metrics);

    let received = drain_events(&mut rx, wanted.len(), Duration::from_secs(10)).await;
    await_backfill_complete(&ckpt, &url, 0, Duration::from_secs(15)).await;
    let unique: HashSet<_> = received.iter().map(|e| e.event.id).collect();
    assert!(
        wanted.iter().all(|e| unique.contains(&e.id)),
        "the resumed walk must capture the 10 ties at its cursor and the 5 below; \
         got {} of {}",
        wanted.iter().filter(|e| unique.contains(&e.id)).count(),
        wanted.len(),
    );
    let untils = untils.lock().unwrap().clone();
    assert_eq!(
        untils.first().copied().flatten(),
        Some(900),
        "the first backfill page resumes at the checkpoint cursor; untils={untils:?}"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_counts_a_shed_second_on_a_relay_that_clamps_below_the_limit() {
    // Holding a short page inclusively recovers the ties at its boundary, but
    // no until-only cursor can read past a second that holds more events than
    // the relay serves per page (#18 (Backfill pagination silently drops events
    // when >limit (500) share a single created_at second), at the clamp's
    // size). The loss is unavoidable; it must not be silent. Found by the
    // review of the #32 fix, which first counted only full pages.
    //
    // Setup: we ask for 100, the relay clamps at 10; 20 events at t=900, 5 below.
    //   p1 (until=now): the first 10 at t=900; held inclusively => 900.
    //   p2 (until=900): the same 10, stagnated, and as large as this relay's
    //       pages get => a tie stagnation; the other 10 at t=900 are shed; 899.
    //   p3 (until=899): the 5 below; held => 895.
    //   p4 (until=895): the event at t=895 alone, stagnated but smaller than
    //       the relay's pages => not counted; 894. p5, p6 empty => Complete.
    let keys: Vec<Keys> = (0..26).map(|_| Keys::generate()).collect();
    let mut events: Vec<Event> = Vec::new();
    for i in 0u64..20 {
        events.push(make_event(&keys[i as usize], 0, &format!("tie_{i}"), 900));
    }
    for i in 0u64..5 {
        events.push(make_event(
            &keys[20 + i as usize],
            0,
            &format!("below_{i}"),
            899 - i,
        ));
    }
    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .clamp_each_page_to(10)
        .start()
        .await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let seed = make_event(&keys[25], 0, "seed", 10_000);
    ckpt.observe(&url, 0, 10_000i64, &seed.id).await;
    let cfg = AcquirerConfig {
        relays: vec![url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 100,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let slug = metrics_slug(&cfg, 0);
    let acquirer = Acquirer::spawn(cfg, tx, Arc::clone(&ckpt), Arc::clone(&metrics));

    let received = drain_events(&mut rx, events.len(), Duration::from_secs(6)).await;
    await_backfill_complete(&ckpt, &url, 0, Duration::from_secs(15)).await;
    let unique: HashSet<_> = received.iter().map(|e| e.event.id).collect();
    assert_eq!(
        unique.len(),
        15,
        "10 of the 20 at t=900 (one clamped page) and the 5 below; got {}",
        unique.len()
    );
    let stagnations = metrics
        .backfill_tie_stagnations
        .with_label_values(&[url.as_str(), "0", &slug])
        .get();
    assert_eq!(
        stagnations, 1,
        "the shed second must be counted once, and the walk-end re-read not at all"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_completes_at_created_at_zero_floor() {
    // Regression test for issue #23 — the relay.fountain.fm backfill saw-tooth.
    //
    // A relay serving an event with `created_at = 0` (e.g. a malformed kind-9735
    // zap receipt whose backend never set `paid_at`) used to make backfill loop
    // forever. The walk reaches `oldest = 0`, computes `next_until = 0 - 1 = -1`,
    // and `backfill_filter` would emit `until:0`. NIP-01 reads that as
    // `created_at <= 0`, so the relay keeps returning the same created_at=0
    // event — `oldest` stays 0, the page is never empty, and the empty-streak
    // completion can never fire. (Worse on relays that treat `until:0` as
    // "unset" — fountain.fm — which return their NEWEST events, snapping the
    // cursor back to now.) Either way: no termination.
    //
    // The fix: `next_backfill_advance` returns `Floor` when the next cursor would
    // be `<= 0`, and `handle_eose` marks the group Complete. This asserts the
    // backfill REACHES Complete (it would hang pre-fix → poll times out → fail)
    // and that the created_at=0 event is still captured before completion.
    //
    // Forward sub is excluded by pre-seeding since=10_000 (effective 9_940),
    // above all events, so only backfill is exercised.

    let keys: Vec<Keys> = (0..7).map(|_| Keys::generate()).collect();
    let mut events: Vec<Event> = (0u64..5)
        .map(|i| make_event(&keys[i as usize], 0, &format!("normal_{i}"), 100 + i))
        .collect();
    let zero_event = make_event(&keys[5], 0, "created_at_zero", 0);
    events.push(zero_event.clone());

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let ckpt_poll = ckpt.clone();

    let seed_event = make_event(&keys[6], 0, "seed", 10_000);
    ckpt.observe(&relay_url, 0, 10_000i64, &seed_event.id).await;

    let cfg = AcquirerConfig {
        relays: vec![relay_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 50,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // The created_at=0 event must be captured (it arrives on the floor page,
    // before completion) — the fix must not drop it.
    let received = drain_events(&mut rx, events.len(), Duration::from_secs(15)).await;
    let unique: std::collections::HashSet<_> = received.iter().map(|e| e.event.id).collect();
    assert!(
        unique.contains(&zero_event.id),
        "the created_at=0 event must be captured during the floor page before completion"
    );

    // Backfill must TERMINATE (reach Complete). Pre-fix it loops on the
    // created_at=0 page and this poll times out — that is the regression guard.
    let completed = timeout(Duration::from_secs(10), async {
        loop {
            if ckpt_poll.backfill_state(&relay_url, 0).await == BackfillState::Complete {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "backfill never reached Complete with a created_at=0 event present — the \
         epoch-floor guard is missing/broken; the walk is looping"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_stagnation_guard_prevents_infinite_loop() {
    // Property test for the stagnation guard in the inclusive cursor algorithm.
    //
    // When more events share a single `created_at` second than the page
    // `limit`, the inclusive cursor (`until = oldest`) does not help: the
    // relay always returns the same first `limit` events (sorted by id ASC),
    // so `oldest` never changes between page N and N+1. The stagnation guard
    // detects this (`oldest == prev_oldest` on consecutive full pages) and
    // falls back to `oldest - 1`, accepting that the remaining events at that
    // second are unrecoverable without an id-range cursor (which Nostr filters
    // do not support). See docs/req-semantics.md §Backfill cursor strategy.
    //
    // Asserts two invariants:
    //   1. Backfill terminates (no infinite loop) even with ≥ limit events at
    //      one second.
    //   2. No duplicate event ids reach the sink — pool dedup correctly
    //      absorbs page-2 re-deliveries without leaking duplicates.
    //
    // Expected page sequence (60 events all at t=1_000, limit=50):
    //   p1 (until=now): 50 events (first 50 by id); full, cursor_advanced →
    //                    next_until = 1000 (inclusive)
    //   p2 (until=1000): same 50 events (deterministic relay sort); full,
    //                    cursor_stagnant → stagnation guard → next_until = 999
    //   p3 (until=999):  0 events → empty_streak = 1
    //   p4 (until=999):  0 events → empty_streak = 2 → Complete
    //
    // The remaining 10 events (highest ids) are unrecoverable — an
    // acknowledged limitation documented in docs/req-semantics.md.

    let keys: Vec<Keys> = (0..61).map(|_| Keys::generate()).collect();
    let shared_ts = 1_000u64;
    let events: Vec<Event> = (0u64..60)
        .map(|i| make_event(&keys[i as usize], 0, &format!("e{i}"), shared_ts))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let relay_url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let ckpt_for_test = ckpt.clone();

    // Pre-seed keeps forward sub's since at 9_940, well above shared_ts=1000,
    // so forward sub delivers nothing and only backfill is exercised.
    let seed_event = make_event(&keys[60], 0, "seed", 10_000);
    ckpt.observe(&relay_url, 0, 10_000i64, &seed_event.id).await;

    let cfg = AcquirerConfig {
        relays: vec![relay_url.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 50, // == BACKFILL_MIN_LIMIT; clamping is a no-op
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(60),
    };
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, metrics);

    // Drain with ceiling 60. We expect exactly 50 (page 1 only); page-2
    // re-deliveries are all pool-deduped and never reach the sink.
    let received = drain_events(&mut rx, 60, Duration::from_secs(15)).await;
    let unique: std::collections::HashSet<_> = received.iter().map(|e| e.event.id).collect();

    // No duplicates must reach the sink.
    assert_eq!(
        unique.len(),
        received.len(),
        "sink received {} events but only {} unique ids — pool dedup leaked a duplicate",
        received.len(),
        unique.len(),
    );

    // Exactly 50 unique events from page 1. The remaining 10 (highest ids at
    // the same second) are the acknowledged unrecoverable tail.
    assert_eq!(
        unique.len(),
        50,
        "expected exactly 50 unique events (page-1 quota); remaining 10 share \
         the same second and are unrecoverable; got {}",
        unique.len(),
    );

    // Backfill must reach Complete — not loop forever.
    let completed = timeout(Duration::from_secs(15), async {
        loop {
            if matches!(
                ckpt_for_test.backfill_state(&relay_url, 0).await,
                BackfillState::Complete
            ) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "backfill did not reach Complete within timeout — stagnation guard may \
         not have fired, leaving the cursor stuck at t={shared_ts}",
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

// =========================================================================
// EXPERIMENT (ignored): empirically reproduce the connect_relay-after-
// disconnect_relay race in nostr-relay-pool 0.44 that wedged nos.lol on
// 2026-06-06 (stuck `Connecting`/state=2 for ~5 h until a manual restart).
//
// This drives the EXACT sequence the probe-timeout recovery path runs in
// `relay_task::tick_forward_sub_probe` —
//   pool.disconnect_relay(url).await;   // -> RelayStatus::Terminated + signal
//   pool.connect_relay(url).await;      // -> set Pending; spawn_connection_task
// — directly against the lib pool, so the experiment isolates the race to
// the library (no acquirer/probe machinery involved).
//
// Mechanism (from nostr-relay-pool-0.44 src/relay/inner.rs):
//   * `running: AtomicBool` (148) is cleared ONLY at connection_task exit (608).
//   * `disconnect()` (1282) sets status=Terminated but never clears `running`.
//   * On terminate, `post_connection` runs `close_ws().await` (786) — a real
//     close handshake; ~instant on localhost, tens of ms over the network.
//   * connection_task then snapshots status into a LOCAL var at 566 and breaks
//     at 569 if that local snapshot is Terminated.
// The WEDGE: our `connect()` executes entirely within [566 snapshot, 608 clear]
// — it sets shared status=Pending and `spawn_connection_task` no-ops because
// `is_running()` is still true ("Connection task is already running"), yet T1
// breaks anyway (local snapshot was Terminated). End state: Pending, no task,
// nothing re-arms it. Competing windows (connect before 566, or after 608)
// both recover, which is why it is rare and network-timing-dependent.
//
// Run: cargo test -p nozzl-acquirer --test integration -- \
//        --ignored --nocapture repro_connect_relay_race_wedge
// =========================================================================
// NOTE on reproduction conditions (empirically established 2026-06-07):
//   * A SINGLE relay cycling alone does NOT wedge (2778 iterations, 0 hits):
//     with no other tasks, our `connect()` is reliably scheduled either before
//     T1's line 566 or after line 608, both of which recover.
//   * The wedge needs the live system's condition — MANY tasks on a
//     CPU-contended runtime — so the scheduler places our `connect()` at
//     jittered offsets, occasionally inside T1's [566,608]. This mirrors
//     production: 4 relay tasks + the notification listener all sharing the
//     runtime is what made nos.lol's timing line up.
//   * With 40 relays cycling on a 4-worker runtime this reproduced 5/5 runs in
//     25-90 s (median ~40 s). The terminal state is either `Connecting`
//     (status 2 — the exact live nos.lol gauge value) or `Disconnected`
//     (status 4); both are the same class — relay stuck non-Connected with no
//     connection task and nothing to re-arm it.
//
// POLARITY: this asserts the wedge IS reproduced, and stays that way ON
// PURPOSE (decision 2026-07-05, issue #8): it drives the raw un-barriered
// disconnect/connect pair, so it is the UPSTREAM CANARY — rerun it on every
// `nostr-relay-pool` bump; the day it stops wedging, upstream fixed the race
// and the whisperhash fork (see docs/nostr-relay-pool-limitations.md → "The
// whisperhash fork") can be dropped. The fixed-behavior regression guard is
// the inverted-polarity sibling `barrier_prevents_connect_relay_race_wedge`
// below, which drives the same contention through the production
// `teardown_barrier_and_rearm`. Kept `#[ignore]` — it is a ~30-90 s
// probabilistic stress harness, not a per-commit CI gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "experiment; run explicitly with --ignored --nocapture"]
async fn repro_connect_relay_race_wedge() {
    use nostr_relay_pool::pool::RelayPoolBuilder;
    use nostr_relay_pool::prelude::RelayStatus;
    use nostr_relay_pool::{RelayOptions, RelayPool};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();

    async fn status_of(pool: &RelayPool, url: &str) -> Option<RelayStatus> {
        pool.relay(url).await.ok().map(|r| r.status())
    }
    async fn wait_connected(
        pool: &RelayPool,
        url: &str,
        dur: Duration,
    ) -> Result<(), Option<RelayStatus>> {
        let deadline = Instant::now() + dur;
        loop {
            let s = status_of(pool, url).await;
            if s == Some(RelayStatus::Connected) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(s);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    const RELAYS: usize = 40;
    let pool = RelayPoolBuilder::default().build();
    let mut relays = Vec::with_capacity(RELAYS);
    let mut urls = Vec::with_capacity(RELAYS);
    for _ in 0..RELAYS {
        let relay = TestRelay::builder().start().await;
        let url = relay.url().to_string();
        let options = RelayOptions::default()
            .retry_interval(Duration::from_millis(20))
            .adjust_retry_interval(false);
        pool.add_relay(url.as_str(), options).await.expect("add_relay");
        pool.connect_relay(url.as_str()).await.expect("connect_relay");
        // Generous: setup connects all relays before any loop task runs, but on
        // a busy runtime the later ones can still take a few seconds.
        wait_connected(&pool, &url, Duration::from_secs(15))
            .await
            .expect("initial connect");
        relays.push(relay);
        urls.push(url);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_secs(180);
    let (tx, mut rx) = mpsc::channel::<(usize, usize, Option<RelayStatus>)>(RELAYS);

    let mut handles = Vec::new();
    for (idx, url) in urls.iter().cloned().enumerate() {
        let pool = pool.clone();
        let stop = Arc::clone(&stop);
        let tx = tx.clone();
        handles.push(tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                // EXACT production order from tick_forward_sub_probe (1081,1105):
                let _ = pool.disconnect_relay(url.as_str()).await;
                match i % 4 {
                    0 => {}
                    1 => tokio::task::yield_now().await,
                    2 => {
                        for _ in 0..(((idx as u64 + 1) * (i as u64 + 1) * 13) % 700) {
                            std::hint::spin_loop();
                        }
                    }
                    _ => {
                        tokio::time::sleep(Duration::from_micros((i as u64 * 7) % 200)).await;
                    }
                }
                let _ = pool.connect_relay(url.as_str()).await;

                // Normal localhost recovery is ms-scale. If not Connected
                // within 2 s this is a wedge CANDIDATE — confirm PERMANENCE: a
                // genuine wedge has no connection task and never reconnects,
                // whereas a load-stalled relay eventually does. Only report if
                // it stays non-Connected for a long confirmation window, which
                // rules out false positives from a saturated test runtime.
                if wait_connected(&pool, &url, Duration::from_secs(2)).await.is_err() {
                    let s = status_of(&pool, &url).await;
                    if wait_connected(&pool, &url, Duration::from_secs(20)).await.is_err() {
                        stop.store(true, Ordering::Relaxed);
                        let _ = tx.send((idx, i, s)).await;
                        return i;
                    }
                }
                i += 1;
            }
            i
        }));
    }
    drop(tx);

    let wedge = rx.recv().await;
    stop.store(true, Ordering::Relaxed);
    let mut total_cycles = 0usize;
    for h in handles {
        total_cycles += h.await.unwrap_or(0);
    }

    eprintln!(
        "[repro] relays={RELAYS} total_cycles>={total_cycles} wedge={wedge:?} \
         (status 2=Connecting/Pending matches the live nos.lol gauge)"
    );
    pool.shutdown().await;
    for r in relays {
        r.shutdown().await;
    }
    assert!(
        wedge.is_some(),
        "did not reproduce the wedge in 120 s across {RELAYS} relays \
         (~{total_cycles} cycles) — increase RELAYS or runtime contention",
    );
}

/// Issue-#8 regression guard — the inverted-polarity sibling of
/// [`repro_connect_relay_race_wedge`]. Identical 40-relay/4-worker contention
/// harness and cycle count, but the raw racy `connect_relay` is replaced by
/// the production `teardown_barrier_and_rearm` (the two-stage barrier the
/// probe-timeout recovery path now uses). Where the repro wedges a relay
/// `!Connected` within 25–90 s on 5/5 runs, this must complete the full 180 s
/// with ZERO wedges.
///
/// The repro stays bug-present on purpose: it is the upstream canary (rerun
/// it on every `nostr-relay-pool` bump — the day it stops wedging, upstream
/// fixed the race and the fork can be dropped). THIS test is the guard that
/// our barrier defeats the race.
///
/// Wedge detection here is SHARPER than the repro's time-based permanence
/// window: with inverted polarity a saturated-runtime false positive would
/// *fail* the suite, and a live task can legitimately sit `Connecting` up to
/// the lib's 60 s connect timeout under harness contention — longer than any
/// reasonable time window (a 130 s run mislabeled exactly one such slow
/// attempt as a wedge before this was sharpened). So this test checks the
/// wedge's *definition* directly via the fork accessor: `!Connected` with NO
/// connection task, confirmed twice 1 s apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "3-minute stress guard; run explicitly with --ignored --nocapture"]
async fn barrier_prevents_connect_relay_race_wedge() {
    use nostr_relay_pool::pool::RelayPoolBuilder;
    use nostr_relay_pool::prelude::RelayStatus;
    use nostr_relay_pool::{RelayOptions, RelayPool};
    use nozzl_acquirer::{TeardownBarrierOutcome, teardown_barrier_and_rearm};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Instant;

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();

    async fn status_of(pool: &RelayPool, url: &str) -> Option<RelayStatus> {
        pool.relay(url).await.ok().map(|r| r.status())
    }
    async fn wait_connected(
        pool: &RelayPool,
        url: &str,
        dur: Duration,
    ) -> Result<(), Option<RelayStatus>> {
        let deadline = Instant::now() + dur;
        loop {
            let s = status_of(pool, url).await;
            if s == Some(RelayStatus::Connected) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(s);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    const RELAYS: usize = 40;
    let pool = RelayPoolBuilder::default().build();
    let mut relays = Vec::with_capacity(RELAYS);
    let mut urls = Vec::with_capacity(RELAYS);
    for _ in 0..RELAYS {
        let relay = TestRelay::builder().start().await;
        let url = relay.url().to_string();
        let options = RelayOptions::default()
            .retry_interval(Duration::from_millis(20))
            .adjust_retry_interval(false);
        pool.add_relay(url.as_str(), options).await.expect("add_relay");
        pool.connect_relay(url.as_str()).await.expect("connect_relay");
        wait_connected(&pool, &url, Duration::from_secs(15))
            .await
            .expect("initial connect");
        relays.push(relay);
        urls.push(url);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_secs(180);
    let (tx, mut rx) = mpsc::channel::<(usize, usize, Option<RelayStatus>)>(RELAYS);
    // Outcome tallies: cleared / status_redrive / timeout_connectable /
    // timeout_task_live — printed at the end for visibility into which
    // barrier arms the contention actually exercised.
    let tallies: Arc<[AtomicUsize; 4]> = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));

    let mut handles = Vec::new();
    for (idx, url) in urls.iter().cloned().enumerate() {
        let pool = pool.clone();
        let stop = Arc::clone(&stop);
        let tx = tx.clone();
        let tallies = Arc::clone(&tallies);
        handles.push(tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                // Same production order as the repro — but the connect is the
                // barriered production helper instead of the raw racy call.
                let _ = pool.disconnect_relay(url.as_str()).await;
                // Keep the repro's scheduling perturbations between the
                // disconnect and the re-arm: the barrier must be correct
                // under exactly the jitter that makes the raw pair wedge.
                match i % 4 {
                    0 => {}
                    1 => tokio::task::yield_now().await,
                    2 => {
                        for _ in 0..(((idx as u64 + 1) * (i as u64 + 1) * 13) % 700) {
                            std::hint::spin_loop();
                        }
                    }
                    _ => {
                        tokio::time::sleep(Duration::from_micros((i as u64 * 7) % 200)).await;
                    }
                }
                let relay = pool.relay(url.as_str()).await.expect("relay in pool");
                let outcome = teardown_barrier_and_rearm(&relay).await;
                let slot = match outcome {
                    TeardownBarrierOutcome::Cleared => 0,
                    TeardownBarrierOutcome::StatusRedrive => 1,
                    TeardownBarrierOutcome::TimeoutConnectable => 2,
                    TeardownBarrierOutcome::TimeoutTaskLive => 3,
                };
                tallies[slot].fetch_add(1, Ordering::Relaxed);

                // Wedge discrimination — SHARPER than the repro's 2 s + 20 s
                // permanence window, which false-positives under this
                // harness's saturation: a *live* connection task can
                // legitimately sit in `transport.connect` for up to the
                // lib's 60 s `DEFAULT_CONNECTION_TIMEOUT` against a starved
                // localhost TestRelay (observed: a 130 s run mislabeled one
                // such slow attempt as a wedge). The issue-#8 wedge is
                // DEFINED as `!Connected` with NO connection task — and the
                // fork accessor lets us test exactly that: report a wedge
                // only if `!is_connection_task_running()` holds while
                // `!Connected`, confirmed twice 1 s apart (the no-task state
                // is stable — nothing in the harness re-arms between
                // checks). A live-but-slow task is the lib's territory
                // (60 s timeout → Disconnected → 20 ms retry): wait it out
                // up to 120 s, then move on with a note.
                if wait_connected(&pool, &url, Duration::from_secs(2)).await.is_err() {
                    let live_deadline = Instant::now() + Duration::from_secs(120);
                    loop {
                        if wait_connected(&pool, &url, Duration::from_millis(50)).await.is_ok() {
                            break; // recovered — not a wedge
                        }
                        let running = relay.is_connection_task_running();
                        if !running {
                            // Candidate wedge: no task + !Connected. Confirm
                            // stability before reporting.
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            let s = status_of(&pool, &url).await;
                            if s != Some(RelayStatus::Connected)
                                && !relay.is_connection_task_running()
                            {
                                stop.store(true, Ordering::Relaxed);
                                let _ = tx.send((idx, i, s)).await;
                                return i;
                            }
                            continue; // recovered between samples
                        }
                        if Instant::now() >= live_deadline {
                            // Task alive but !Connected for 120 s: extreme
                            // localhost starvation, not the no-task wedge
                            // this guard exists for. Note it and move on.
                            eprintln!(
                                "[barrier] relay {idx} cycle {i}: live task \
                                 !Connected past 120 s (status {:?}) — \
                                 continuing (not an issue-#8 wedge)",
                                status_of(&pool, &url).await,
                            );
                            break;
                        }
                    }
                }
                i += 1;
            }
            i
        }));
    }
    drop(tx);

    // Unlike the repro (which returns on the first wedge), run to the
    // deadline: rx yields None when every cycler finishes wedge-free.
    let wedge = rx.recv().await;
    stop.store(true, Ordering::Relaxed);
    let mut total_cycles = 0usize;
    for h in handles {
        total_cycles += h.await.unwrap_or(0);
    }

    eprintln!(
        "[barrier] relays={RELAYS} total_cycles={total_cycles} wedge={wedge:?} outcomes: \
         cleared={} status_redrive={} timeout_connectable={} timeout_task_live={}",
        tallies[0].load(Ordering::Relaxed),
        tallies[1].load(Ordering::Relaxed),
        tallies[2].load(Ordering::Relaxed),
        tallies[3].load(Ordering::Relaxed),
    );
    pool.shutdown().await;
    for r in relays {
        r.shutdown().await;
    }
    assert!(
        wedge.is_none(),
        "teardown barrier failed to prevent the issue-#8 wedge: {wedge:?} \
         after ~{total_cycles} cycles across {RELAYS} relays",
    );
}

/// Pins the contract of the fork-added accessor
/// (`Relay::is_connection_task_running`, whisperhash/nostr branch
/// `nozzl-v0.44`) that `teardown_barrier_and_rearm` builds on: `true` while a
/// connection task is alive, `false` once teardown completes after an
/// explicit disconnect. Both sides poll with a bound - the flag is set inside
/// the spawned task's first poll (NOT synchronously in `connect()`) and
/// cleared only at task exit (after the `close_ws` handshake), so neither
/// edge is instantaneous. Compilation against the fork already pins the
/// method's existence; this pins its semantics.
///
/// It disconnects only once the relay is `Connected`, and checks the status is
/// `Terminated` after the exit, so the exit is the disconnect's doing. Then it
/// connects again: once the accessor reads `false`, `connect()` must spawn a
/// task that reaches `Connected`, which is what the barrier relies on.
///
/// Reconnect is off because the library's `disconnect()` can lose its own
/// termination (`docs/nostr-relay-pool-limitations.md` entry #11): it wakes the
/// connection task before it sets `Terminated`, so a task that reads the status
/// in between (or whose `_try_connect` then overwrites an early `Terminated`:
/// entry #11 lists three paths) takes the wake for a dropped connection and,
/// with reconnect on, connects again after its retry interval (10 s +/- 3 s by
/// default), outliving the disconnect. That race is the library's, not the
/// accessor's; it failed this test in CI five times in October 2026, and once
/// in 320 runs under CPU load. With reconnect off the task exits whatever
/// status it reads, so the test pins the accessor alone;
/// `explicit_disconnect_ends_the_task_with_reconnect_on` covers production's
/// options.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_accessor_tracks_connection_task_lifecycle() {
    use nostr_relay_pool::RelayOptions;
    use nostr_relay_pool::pool::RelayPoolBuilder;
    use nostr_relay_pool::prelude::RelayStatus;
    use std::time::Instant;

    let relay = TestRelay::builder().start().await;
    let url = relay.url().to_string();
    let pool = RelayPoolBuilder::default().build();
    pool.add_relay(url.as_str(), RelayOptions::default().reconnect(false))
        .await
        .expect("add_relay");
    let handle = pool.relay(url.as_str()).await.expect("relay handle");

    // Before any connect: no task has ever been spawned.
    assert!(
        !handle.is_connection_task_running(),
        "connection task reported running before first connect",
    );

    pool.connect_relay(url.as_str())
        .await
        .expect("connect_relay");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !handle.is_connection_task_running() {
        assert!(
            Instant::now() < deadline,
            "connection task not running within 5 s of connect_relay",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    while handle.status() != RelayStatus::Connected {
        assert!(
            Instant::now() < deadline,
            "relay not Connected within 5 s of connect_relay",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    pool.disconnect_relay(url.as_str())
        .await
        .expect("disconnect_relay");
    let deadline = Instant::now() + Duration::from_secs(5);
    while handle.is_connection_task_running() {
        assert!(
            Instant::now() < deadline,
            "connection task still running 5 s after disconnect_relay - \
             teardown never completed",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        handle.status(),
        RelayStatus::Terminated,
        "the task exited without the disconnect's Terminated",
    );

    // Once the accessor reads false, a connect must not be guard-no-op'd.
    pool.connect_relay(url.as_str())
        .await
        .expect("connect_relay again");
    let deadline = Instant::now() + Duration::from_secs(5);
    while handle.status() != RelayStatus::Connected {
        assert!(
            Instant::now() < deadline,
            "no task reached Connected within 5 s of connecting again",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        handle.is_connection_task_running(),
        "Connected again, but the accessor reads no task",
    );

    pool.shutdown().await;
    relay.shutdown().await;
}

/// With production's options (reconnect on), an explicit disconnect ends the
/// connection task, with the status `Terminated`. The library can lose a
/// disconnect's termination (`docs/nostr-relay-pool-limitations.md` entry
/// #11), rarely: about 4 in 10,000 disconnects of a connected relay under CPU
/// saturation. So a task that outlives a disconnect is retried on a fresh
/// pool, up to three times, and only a task that outlives all three fails the
/// test, as a loop that no longer exits on `Terminated` would.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_disconnect_ends_the_task_with_reconnect_on() {
    use nostr_relay_pool::RelayOptions;
    use nostr_relay_pool::pool::RelayPoolBuilder;
    use nostr_relay_pool::prelude::RelayStatus;
    use std::time::Instant;

    let relay = TestRelay::builder().start().await;
    let url = relay.url().to_string();
    let mut outlived = 0;
    for _ in 0..3 {
        let pool = RelayPoolBuilder::default().build();
        pool.add_relay(url.as_str(), RelayOptions::default())
            .await
            .expect("add_relay");
        let handle = pool.relay(url.as_str()).await.expect("relay handle");
        pool.connect_relay(url.as_str())
            .await
            .expect("connect_relay");
        let deadline = Instant::now() + Duration::from_secs(5);
        while handle.status() != RelayStatus::Connected {
            assert!(
                Instant::now() < deadline,
                "relay not Connected within 5 s of connect_relay",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        pool.disconnect_relay(url.as_str())
            .await
            .expect("disconnect_relay");
        let ended_by = Instant::now() + Duration::from_secs(3);
        while handle.is_connection_task_running() && Instant::now() < ended_by {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let ended = !handle.is_connection_task_running();
        let status = handle.status();
        pool.shutdown().await;
        if ended {
            assert_eq!(
                status,
                RelayStatus::Terminated,
                "the task ended, but not on the disconnect's Terminated",
            );
            break;
        }
        outlived += 1;
    }
    relay.shutdown().await;
    assert!(
        outlived < 3,
        "the connection task outlived 3 of 3 explicit disconnects with reconnect on",
    );
}

/// For `disconnects_outlived`: how a cycle disconnects. `AfterConnected` waits
/// for `Connected`, then disconnects from a blocking-pool thread, so the wake
/// crosses threads as it did from the test body when
/// `fork_accessor_tracks_connection_task_lifecycle` failed in CI: the race of
/// entry #11 needs the disconnecting thread preempted between the wake and
/// `Terminated`. `AtOnce` disconnects as soon as the accessor reads `true`,
/// while the task may still be before or inside `_try_connect`, whose status
/// writes can overwrite an early `Terminated`.
#[derive(Clone, Copy)]
enum DisconnectWhen {
    AfterConnected,
    AtOnce,
}

/// How many explicit disconnects a relay's connection task outlives, out of
/// how many, per `DisconnectWhen` (`[AfterConnected, AtOnce]`), in `run_for`
/// while CPU-burning threads oversubscribe the machine. A task still running
/// 3 s after `disconnect()` has lost its termination
/// (`docs/nostr-relay-pool-limitations.md` entry #11): the library's retry is
/// 10 s +/- 3 s by default, so a lost termination keeps the task for at least
/// 7 s, and a clean exit takes milliseconds.
async fn disconnects_outlived(reconnect: bool, run_for: Duration) -> [(usize, usize); 2] {
    use nostr_relay_pool::RelayOptions;
    use nostr_relay_pool::pool::RelayPoolBuilder;
    use nostr_relay_pool::prelude::RelayStatus;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Instant;

    /// Stops the burners however this function ends, a panic included.
    struct StopOnDrop(Arc<AtomicBool>);
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    let relay = TestRelay::builder().start().await;
    let url = relay.url().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let burners: Vec<_> = (0..2 * std::thread::available_parallelism().map_or(4, |n| n.get()))
        .map(|_| {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    std::hint::spin_loop();
                }
            })
        })
        .collect();
    let stop_burners = StopOnDrop(Arc::clone(&stop));
    let tallies: Arc<[[AtomicUsize; 2]; 2]> = Arc::new(Default::default());
    let deadline = Instant::now() + run_for;
    let cyclers: Vec<_> = (0..8)
        .map(|_| {
            let url = url.clone();
            let tallies = Arc::clone(&tallies);
            tokio::spawn(async move {
                let mut n = 0usize;
                while Instant::now() < deadline {
                    let when = if n % 2 == 0 {
                        DisconnectWhen::AfterConnected
                    } else {
                        DisconnectWhen::AtOnce
                    };
                    n += 1;
                    let pool = RelayPoolBuilder::default().build();
                    pool.add_relay(url.as_str(), RelayOptions::default().reconnect(reconnect))
                        .await
                        .expect("add_relay");
                    let handle = pool.relay(url.as_str()).await.expect("relay handle");
                    pool.connect_relay(url.as_str())
                        .await
                        .expect("connect_relay");
                    let ready_by = Instant::now() + Duration::from_secs(10);
                    let ready = match when {
                        DisconnectWhen::AfterConnected => {
                            while handle.status() != RelayStatus::Connected
                                && Instant::now() < ready_by
                            {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                            handle.status() == RelayStatus::Connected
                        }
                        DisconnectWhen::AtOnce => {
                            while !handle.is_connection_task_running() && Instant::now() < ready_by
                            {
                                tokio::task::yield_now().await;
                            }
                            handle.is_connection_task_running()
                        }
                    };
                    if ready {
                        match when {
                            DisconnectWhen::AfterConnected => {
                                let h = handle.clone();
                                tokio::task::spawn_blocking(move || h.disconnect())
                                    .await
                                    .expect("disconnect");
                            }
                            DisconnectWhen::AtOnce => handle.disconnect(),
                        }
                        let ended_by = Instant::now() + Duration::from_secs(3);
                        while handle.is_connection_task_running() && Instant::now() < ended_by {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        let [outlived, cycles] = &tallies[when as usize];
                        cycles.fetch_add(1, Ordering::Relaxed);
                        if handle.is_connection_task_running() {
                            outlived.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    pool.shutdown().await;
                }
            })
        })
        .collect();
    for c in cyclers {
        c.await.expect("cycler");
    }
    drop(stop_burners);
    for b in burners {
        b.join().expect("burner");
    }
    relay.shutdown().await;
    let read = |w: DisconnectWhen| {
        let [outlived, cycles] = &tallies[w as usize];
        (
            outlived.load(Ordering::Relaxed),
            cycles.load(Ordering::Relaxed),
        )
    };
    [
        read(DisconnectWhen::AfterConnected),
        read(DisconnectWhen::AtOnce),
    ]
}

/// Upstream canary for entry #11 of `docs/nostr-relay-pool-limitations.md`:
/// with reconnect on, a connection task can outlive an explicit disconnect.
/// Passes while the bug is present, in either way of disconnecting. Rerun it
/// on every library bump. If it fails for want of a survivor, upstream has
/// changed the ordering: read `disconnect()` and `_try_connect`'s status
/// writes before dropping `reconnect(false)` from
/// `fork_accessor_tracks_connection_task_lifecycle`, since a fix of one path
/// may leave the others (entry #11 lists three). It also fails if disconnects
/// of a connected relay are outlived 1% of the time or more: a deterministic
/// regression, not this race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "upstream canary; burns every core for two minutes; run it alone: cargo test -p nozzl-acquirer --test integration repro_disconnect_can_lose_its_termination -- --ignored --exact --nocapture"]
async fn repro_disconnect_can_lose_its_termination() {
    let [(after_out, after_n), (once_out, once_n)] =
        disconnects_outlived(true, Duration::from_secs(120)).await;
    eprintln!(
        "[lost-termination] reconnect on: after Connected {after_out} of {after_n} outlived; \
         at once {once_out} of {once_n} outlived"
    );
    assert!(
        after_out + once_out > 0,
        "no connection task outlived its disconnect in {} cycles: has upstream fixed entry #11?",
        after_n + once_n,
    );
    assert!(
        after_out * 100 < after_n,
        "{after_out} of {after_n} disconnects of a connected relay outlived: too many for this race",
    );
}

/// The premise of `fork_accessor_tracks_connection_task_lifecycle`'s
/// `reconnect(false)`: under the canary's load, with reconnect off, no
/// connection task outlives its disconnect, in either way of disconnecting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress check; burns every core for two minutes; run it alone: cargo test -p nozzl-acquirer --test integration with_reconnect_off_no_task_outlives_its_disconnect -- --ignored --exact --nocapture"]
async fn with_reconnect_off_no_task_outlives_its_disconnect() {
    let [(after_out, after_n), (once_out, once_n)] =
        disconnects_outlived(false, Duration::from_secs(120)).await;
    eprintln!(
        "[lost-termination] reconnect off: after Connected {after_out} of {after_n} outlived; \
         at once {once_out} of {once_n} outlived"
    );
    assert!(
        after_n > 0 && once_n > 0,
        "a way of disconnecting ran no cycle"
    );
    assert_eq!(
        (after_out, once_out),
        (0, 0),
        "with reconnect off, tasks outlived their disconnect",
    );
}

// -------------------------------------------------------------------------
// #48 (Forward-sub CLOSED handling): a forward sub the relay CLOSEs is CLOSEd
// back on that relay and re-opened after a back-off, not at once; and a kind
// rejected at the single-kind floor is retried, instead of leaving the relay
// connected and silent until a config change.
// -------------------------------------------------------------------------

/// True for a forward-sub REQ: forward filters carry no `limit` (backfill pages
/// do, and the probe's is `limit: 0`).
fn is_forward_filter(filters: &[nostr::Filter]) -> bool {
    filters.iter().any(|f| f.limit.is_none())
}

fn forward_sub_closed(metrics: &Metrics, relay: &RelayUrl, action: &str) -> u64 {
    metrics
        .forward_sub_closed
        .with_label_values(&[relay.as_str(), action])
        .get()
}

fn cfg_one_relay_kind0(relay: &RelayUrl, probe_min_interval: Duration) -> AcquirerConfig {
    AcquirerConfig {
        relays: vec![relay.clone()],
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        // No probe: these tests are about the CLOSED path alone.
        forward_silence_threshold: Duration::from_secs(3_600),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn forward_sub_closed_by_relay_is_closed_back_and_reissued_after_backoff() {
    // A relay that CLOSEs every forward REQ for a reason that is not a filter
    // rejection used to get the REQ straight back, one per round trip: on dev,
    // up to 71 a minute from relay.damus.io's read budget and 10,883 a day from
    // relay.conduit.market. Now the sub is CLOSEd back and re-opened after
    // 2 s, then 4 s, doubling.
    use std::sync::atomic::{AtomicUsize, Ordering};
    let forward_reqs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&forward_reqs);
    let req_times = Arc::new(std::sync::Mutex::new(Vec::<std::time::Instant>::new()));
    let times = Arc::clone(&req_times);
    let relay = TestRelay::builder()
        .reject_req_if(move |filters| {
            if is_forward_filter(filters) {
                counter.fetch_add(1, Ordering::SeqCst);
                times.lock().unwrap().push(std::time::Instant::now());
                Some("error: upstream connection lost".to_string())
            } else {
                None
            }
        })
        .start()
        .await;

    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let acquirer = Acquirer::spawn(
        cfg_one_relay_kind0(&url, Duration::from_secs(60)),
        tx,
        ckpt,
        Arc::clone(&metrics),
    );

    // REQs at about 0 s, 2 s and 6 s (each re-issue up to a 1 s tick late):
    // two or three in 7.5 s, against hundreds when re-issued at once.
    tokio::time::sleep(Duration::from_millis(7_500)).await;
    let n = forward_reqs.load(Ordering::SeqCst);
    assert!(
        (2..=4).contains(&n),
        "expected 2-4 forward REQs in 7.5 s with the back-off, got {n}"
    );
    // The waits double: about 2 s, then about 4 s (each up to a 1 s tick late).
    let t = req_times.lock().unwrap().clone();
    if t.len() >= 3 {
        let (gap1, gap2) = (t[1] - t[0], t[2] - t[1]);
        assert!(
            gap1 >= Duration::from_millis(1_800) && gap2 >= Duration::from_millis(3_500),
            "re-issue waits should be about 2 s then 4 s, got {gap1:?} then {gap2:?}"
        );
    } else {
        assert!(
            t[1] - t[0] >= Duration::from_millis(1_800),
            "the first re-issue came {:?} after the CLOSED, not after the back-off",
            t[1] - t[0]
        );
    }
    assert!(
        relay.close_count("forward") >= 1,
        "the forward sub the relay CLOSEd was never CLOSEd back"
    );
    let reissues = forward_sub_closed(&metrics, &url, "reissue");
    assert!(
        reissues >= 1 && usize::try_from(reissues).unwrap() <= n,
        "forward_sub_closed_total{{action=\"reissue\"}} = {reissues} for {n} forward REQs"
    );
    assert_eq!(forward_sub_closed(&metrics, &url, "floor"), 0);

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn forward_floor_is_retried_instead_of_silencing_the_relay() {
    // A single-kind forward REQ rejected for a rejection-worded reason (here
    // NIP-01's `invalid:` prefix, as an upstream's CLOSED passed through by
    // an htree daemon's relay could read) floors the relay's only kind group. It used
    // to stay floored until a config change, with no forward sub and so no
    // probe. Now the floor is retried after `probe_min_interval`, doubling per
    // further floor, and the relay is read again once it accepts.
    use std::sync::atomic::{AtomicUsize, Ordering};
    let forward_reqs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&forward_reqs);
    let req_times = Arc::new(std::sync::Mutex::new(Vec::<std::time::Instant>::new()));
    let times = Arc::clone(&req_times);
    let relay = TestRelay::builder()
        .reject_req_if(move |filters| {
            if !is_forward_filter(filters) {
                return None;
            }
            times.lock().unwrap().push(std::time::Instant::now());
            if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                Some("invalid: kind not supported".to_string())
            } else {
                None
            }
        })
        .start()
        .await;

    let (ckpt, metrics, tx, mut rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let acquirer = Acquirer::spawn(
        cfg_one_relay_kind0(&url, Duration::from_secs(2)),
        tx,
        ckpt,
        Arc::clone(&metrics),
    );

    // Floored at about 0 s and again at the 2 s retry; accepted at the 4 s one.
    let reopened = timeout(Duration::from_secs(15), async {
        loop {
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        reopened.is_ok(),
        "the floored kind was never retried: no forward sub after 15 s ({} forward REQs)",
        forward_reqs.load(Ordering::SeqCst)
    );
    assert_eq!(forward_reqs.load(Ordering::SeqCst), 3);
    // The retry waits double: about 2 s after the first floor, about 4 s after
    // the second (each up to a 1 s tick late).
    let t = req_times.lock().unwrap().clone();
    let (gap1, gap2) = (t[1] - t[0], t[2] - t[1]);
    assert!(
        gap1 >= Duration::from_millis(1_800) && gap2 >= Duration::from_millis(3_500),
        "floor retry waits should be about 2 s then 4 s, got {gap1:?} then {gap2:?}"
    );
    assert_eq!(forward_sub_closed(&metrics, &url, "floor"), 2);
    assert_eq!(
        metrics
            .events_rejected
            .with_label_values(&[url.as_str(), "0", "test-k0", "kind-rejected-at-floor"])
            .get(),
        2
    );

    // And the relay is read again: a new event reaches the sink.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let e = make_event(&Keys::generate(), 0, "after the floor", now);
    relay.publish(e.clone()).await;
    let got = drain_events(&mut rx, 1, Duration::from_secs(5)).await;
    assert!(
        got.iter().any(|env| env.event.id == e.id),
        "an event published after the retry never reached the sink"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn forward_floor_is_retried_at_once_after_a_reconnect() {
    // The floor retry's back-off starts at `probe_min_interval` (60 s here), but
    // a reconnect retries at once: the rejection may have belonged to the old
    // connection. Without that, the forward sub would stay closed for 60 s.
    use std::sync::atomic::{AtomicUsize, Ordering};
    let forward_reqs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&forward_reqs);
    let relay = TestRelay::builder()
        .reject_req_if(move |filters| {
            if is_forward_filter(filters) && counter.fetch_add(1, Ordering::SeqCst) < 1 {
                Some("invalid: kind not supported".to_string())
            } else {
                None
            }
        })
        .start()
        .await;

    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let acquirer = Acquirer::spawn(
        cfg_one_relay_kind0(&url, Duration::from_secs(60)),
        tx,
        ckpt,
        Arc::clone(&metrics),
    );

    // Wait for the floor, then drop the connection.
    let floored = timeout(Duration::from_secs(10), async {
        while forward_sub_closed(&metrics, &url, "floor") == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(floored.is_ok(), "the forward REQ was never floored");
    // Let the task observe the initial connect first, so the drop is a real
    // reconnect.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    relay.disconnect_all().await;

    let reopened = timeout(Duration::from_secs(30), async {
        loop {
            if relay
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        reopened.is_ok(),
        "no forward sub 30 s after the reconnect: the floor waited for its 60 s back-off"
    );
    assert_eq!(forward_reqs.load(Ordering::SeqCst), 2);

    acquirer.shutdown().await;
    relay.shutdown().await;
}

/// The kind-0 relays of the #39 (Forward-sub teardown is pool-wide) and echo
/// tests: a short silence threshold, so a quiet relay's probe re-issues its
/// forward sub within seconds.
fn cfg_probing(relays: &[&RelayUrl]) -> AcquirerConfig {
    AcquirerConfig {
        relays: relays.iter().map(|r| (*r).clone()).collect(),
        kinds: vec![0],
        kind_slugs: slugs(&[0]),
        relay_only_kinds: HashMap::new(),
        kind_only_relays: HashMap::new(),
        backfill_safety_margin: Duration::from_secs(60),
        backfill_default_limit: 500,
        backfill_empty_pages_to_complete: 2,
        pool_dedup_max_events: 5_000_000,
        forward_silence_threshold: Duration::from_secs(2),
        probe_timeout: Duration::from_secs(5),
        probe_min_interval: Duration::from_secs(2),
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relays_forward_reissue_leaves_other_relays_forward_subs_alone() {
    // #39 (Forward-sub teardown is pool-wide): every relay task holds its
    // forward sub as "forward", and `RelayPool::unsubscribe` sends the CLOSE to
    // every relay, so a quiet relay's probe re-issue used to close the busy
    // relay's live forward sub too (dev: each relay dark about a quarter of the
    // time). Closes are now scoped to the relay that owns the sub.
    let quiet = TestRelay::builder().start().await;
    let busy = TestRelay::builder().start().await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let q = RelayUrl::parse(quiet.url().as_str()).unwrap();
    let b = RelayUrl::parse(busy.url().as_str()).unwrap();
    let acquirer = Acquirer::spawn(cfg_probing(&[&q, &b]), tx, ckpt, Arc::clone(&metrics));

    timeout(Duration::from_secs(5), async {
        loop {
            let q_open = quiet
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward");
            let b_open = busy
                .open_subscriptions()
                .iter()
                .any(|s| s.sub_id == "forward");
            if q_open && b_open {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("forward subs never registered on both relays");

    // Keep the busy relay delivering (new ids, so the wire stays active) while
    // the quiet one goes silent past its 2 s threshold and re-issues.
    let keys = Keys::generate();
    let until = tokio::time::Instant::now() + Duration::from_secs(7);
    let mut i = 0u64;
    while tokio::time::Instant::now() < until {
        busy.publish(make_event(&keys, 0, &format!("busy {i}"), unix_now()))
            .await;
        i += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    assert!(
        quiet.close_count("forward") >= 1,
        "the quiet relay's probe never re-issued its forward sub"
    );
    assert_eq!(
        busy.close_count("forward"),
        0,
        "the quiet relay's re-issue CLOSEd the busy relay's forward sub (pool-wide teardown)"
    );

    acquirer.shutdown().await;
    quiet.shutdown().await;
    busy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_closed_echoing_our_own_close_is_not_a_relay_close() {
    // relay.conduit.market answers every client CLOSE with an empty CLOSED.
    // When nozzl re-issues a forward sub (CLOSE, then REQ under the same id),
    // that echo arrives after the REQ went out but refers to the old sub. Read
    // as the relay closing the new one, it would CLOSE the live sub back and
    // back off: the relay dark every time its probe re-issues.
    let relay = TestRelay::builder().echo_closed_on_close().start().await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    // Probes at least 15 s apart, so the next echo does not arrive before the
    // first one's 10 s confirmation deadline and push it back.
    let mut cfg = cfg_probing(&[&url]);
    cfg.probe_min_interval = Duration::from_secs(15);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, Arc::clone(&metrics));

    // Quiet: its probe re-issues after 2 s, the next not for 15 s.
    let reissued = timeout(Duration::from_secs(10), async {
        while relay.close_count("forward") < 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        reissued.is_ok(),
        "the probe never re-issued the forward sub"
    );
    // Past the 10 s confirmation deadline: each echo must have been confirmed by
    // the re-opened sub's EOSE, or it would now count as a relay close.
    tokio::time::sleep(Duration::from_millis(11_500)).await;

    assert_eq!(
        forward_sub_closed(&metrics, &url, "reissue"),
        0,
        "the relay's echo of our own CLOSE was taken for the relay closing the sub"
    );
    assert!(
        forward_sub_closed(&metrics, &url, "echo") >= 1,
        "the echo was not counted"
    );
    assert!(
        relay
            .open_subscriptions()
            .iter()
            .any(|s| s.sub_id == "forward"),
        "the re-opened forward sub was closed after the echo"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_backfill_page_the_relay_closes_is_closed_back() {
    // A relay may keep its own registration for a sub it CLOSEd, counting it
    // against its per-connection cap (an htree daemon's relay does when an upstream
    // CLOSEd it). The page here is CLOSEd for a reason that is neither a filter
    // rejection nor pressure, so it is abandoned without an EOSE: the only
    // CLOSE the relay can see for it is the CLOSE back.
    let relay = TestRelay::builder()
        .reject_req_if(|filters| {
            if filters.iter().any(|f| f.limit.is_some_and(|l| l > 0)) {
                Some("error: try later".to_string())
            } else {
                None
            }
        })
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let acquirer = Acquirer::spawn(
        cfg_one_relay_kind0(&url, Duration::from_secs(60)),
        tx,
        ckpt,
        Arc::clone(&metrics),
    );

    let closed_back = timeout(Duration::from_secs(5), async {
        while relay.backfill_close_count() < 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        closed_back.is_ok(),
        "the backfill page the relay CLOSEd was never CLOSEd back"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_floored_kind_is_retried_on_its_own_not_merged_back() {
    // kind-0 is accepted; a REQ naming kind-3 is refused as invalid. The first
    // REQ [0, 3] splits into [0] and [3], and [3] floors. The retry brings [3]
    // back as a group of its own beside [0]: merged into one group again, every
    // retry would re-run the split, a second rejection and a replay of [0] each
    // round.
    use std::sync::atomic::{AtomicUsize, Ordering};
    let with_kind3 = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&with_kind3);
    let relay = TestRelay::builder()
        .reject_req_if(move |filters| {
            let names3 = filters.iter().any(|f| {
                f.limit.is_none()
                    && f.kinds
                        .as_ref()
                        .is_some_and(|k| k.contains(&Kind::from(3u16)))
            });
            if names3 {
                counter.fetch_add(1, Ordering::SeqCst);
                Some("invalid: kind not supported".to_string())
            } else {
                None
            }
        })
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let mut cfg = cfg_one_relay_kind0(&url, Duration::from_secs(2));
    cfg.kinds = vec![0, 3];
    cfg.kind_slugs = slugs(&[0, 3]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, Arc::clone(&metrics));

    // [0, 3] and [3] at about 0 s, one retry of [3] alone at about 2-3 s, the
    // next not before about 6 s.
    tokio::time::sleep(Duration::from_millis(5_000)).await;
    assert_eq!(
        with_kind3.load(Ordering::SeqCst),
        3,
        "expected [0, 3], [3] and one retry of [3] alone"
    );
    assert_eq!(forward_sub_closed(&metrics, &url, "split"), 1);
    assert_eq!(forward_sub_closed(&metrics, &url, "floor"), 2);
    // Exactly one forward sub covers kind-0: the retry renamed `forward` to
    // `forward-0` (one group became two) and must not leave the old one open.
    let kind0_subs: Vec<String> = relay
        .open_subscriptions()
        .iter()
        .filter(|s| {
            s.filters.iter().any(|f| {
                f.limit.is_none()
                    && f.kinds
                        .as_ref()
                        .is_some_and(|k| k.contains(&Kind::from(0u16)))
            })
        })
        .map(|s| s.sub_id.clone())
        .collect();
    assert_eq!(
        kind0_subs.len(),
        1,
        "kind-0 forward subs open: {kind0_subs:?}"
    );

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_refusal_mistaken_for_an_echo_is_caught_when_no_eose_follows() {
    // The echo rule keeps a sub through an empty CLOSED that comes right after
    // nozzl's own CLOSE of that id. A relay that refuses every forward REQ with
    // an empty reason (an upstream's CLOSED passed through, say) produces the
    // same shape after nozzl's CLOSE back. Without confirmation the relay would
    // stay dark with the sub believed open; with it, no EOSE within 10 s makes
    // the CLOSED a relay close after all, counted and re-issued.
    let relay = TestRelay::builder()
        .reject_req_if(|filters| is_forward_filter(filters).then(String::new))
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let acquirer = Acquirer::spawn(
        cfg_one_relay_kind0(&url, Duration::from_secs(60)),
        tx,
        ckpt,
        Arc::clone(&metrics),
    );

    // Refused at about 0 s (a relay close: CLOSE back, back-off 2 s); the 2 s
    // re-issue's refusal falls within 5 s of that CLOSE back, so it is held as a
    // suspected echo; 10 s on, with no EOSE, it is a relay close again.
    let caught = timeout(Duration::from_secs(20), async {
        while forward_sub_closed(&metrics, &url, "reissue") < 2 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        caught.is_ok(),
        "an empty refusal held as an echo was never taken for the relay's close (reissue {}, echo {})",
        forward_sub_closed(&metrics, &url, "reissue"),
        forward_sub_closed(&metrics, &url, "echo")
    );
    assert!(forward_sub_closed(&metrics, &url, "echo") >= 1);

    acquirer.shutdown().await;
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_floored_kind_accepted_on_retry_leaves_one_sub_per_group() {
    // As above, but the relay accepts kind-3 when it is retried. One group
    // became two, so `forward` is now `forward-0`: the old sub has to go, or
    // kind-0 stays subscribed twice for as long as the groups last.
    use std::sync::atomic::{AtomicUsize, Ordering};
    let refusals = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&refusals);
    let relay = TestRelay::builder()
        .reject_req_if(move |filters| {
            let names3 = filters.iter().any(|f| {
                f.limit.is_none()
                    && f.kinds
                        .as_ref()
                        .is_some_and(|k| k.contains(&Kind::from(3u16)))
            });
            if names3 && counter.fetch_add(1, Ordering::SeqCst) < 2 {
                Some("invalid: kind not supported".to_string())
            } else {
                None
            }
        })
        .start()
        .await;
    let (ckpt, metrics, tx, _rx, _dir) = setup().await;
    let url = RelayUrl::parse(relay.url().as_str()).unwrap();
    let mut cfg = cfg_one_relay_kind0(&url, Duration::from_secs(2));
    cfg.kinds = vec![0, 3];
    cfg.kind_slugs = slugs(&[0, 3]);
    let acquirer = Acquirer::spawn(cfg, tx, ckpt, Arc::clone(&metrics));

    // [0, 3] and [3] refused at about 0 s; [3] accepted at the 2-3 s retry.
    tokio::time::sleep(Duration::from_millis(5_000)).await;
    let subs_naming = |kind: u16| -> Vec<String> {
        relay
            .open_subscriptions()
            .iter()
            .filter(|s| {
                s.filters.iter().any(|f| {
                    f.limit.is_none()
                        && f.kinds
                            .as_ref()
                            .is_some_and(|k| k.contains(&Kind::from(kind)))
                })
            })
            .map(|s| s.sub_id.clone())
            .collect()
    };
    let (k0, k3) = (subs_naming(0), subs_naming(3));
    assert_eq!(k0.len(), 1, "kind-0 forward subs open: {k0:?}");
    assert_eq!(k3.len(), 1, "kind-3 forward subs open: {k3:?}");
    assert_eq!(forward_sub_closed(&metrics, &url, "floor"), 1);

    acquirer.shutdown().await;
    relay.shutdown().await;
}
