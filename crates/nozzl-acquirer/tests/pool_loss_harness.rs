//! Stress harness that pins down the *exact* event-loss mechanism in the
//! vendored `nostr-relay-pool` 0.44 `RelayPool` under a firehose, and proves
//! the invariant that justifies our production conclusion.
//!
//! ## Why this exists
//!
//! The 2026-06-18 onboarding write-up originally claimed events were "dropped
//! inside the vendored RelayPool" under firehose load, with the high-level
//! `RelayPoolNotification::Event` API as a silent black box. That was
//! hand-waving. This harness replaces it with a mechanism and a falsifiable
//! invariant.
//!
//! ## What the source says
//!
//! nozzl builds the pool with a `MemoryDatabase { events: false }`, which is a
//! bare `LruCache<EventId, ()>` id-tracker. In `relay/inner.rs::handle_event_msg`
//! an id only becomes `Saved` via `save_event`'s `put`, and that `put` is
//! *immediately* followed by emitting the `Event` notification. So **every
//! accepted unique id emits at least one `Event` notification** — there is no
//! silent pre-notification drop path for an accepted event.
//!
//! The *only* place a unique accepted event can vanish before nozzl sees it is
//! the pool's external notification fan-out, a **`tokio::sync::broadcast`
//! channel of capacity 4096** (`DEFAULT_NOTIFICATION_CHANNEL_SIZE`). A broadcast
//! channel overwrites its oldest entry when full; a receiver that has fallen
//! behind by more than the capacity observes `RecvError::Lagged(n)` and those
//! `n` items are gone *for that receiver*. Crucially, tokio broadcast **never
//! drops silently**: every skipped item is accounted for by a `Lagged(n)`.
//!
//! ## The invariant these tests establish
//!
//! For a single firehose relay delivering `N` unique events:
//!   1. A consumer that keeps up sees **all `N`** ids and reports **zero**
//!      `Lagged` → no loss.
//!   2. A deliberately slow consumer loses some ids, but the number lost is
//!      **always `<=` the total `Lagged` count it observed** → loss never
//!      escapes the `Lagged` signal.
//!
//! Contrapositive of (2): **zero `Lagged` ⟹ zero loss.** The live
//! deployment logged zero `"acquirer notifications channel lagged"` and zero
//! `"dropping notif"` during the onboarding, so by this invariant no events
//! were lost in the pool — which is why the measured archive gap is now
//! attributed to verification-side artifacts, not in-pool loss.

#![allow(clippy::missing_panics_doc)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use nostr::{Event, EventBuilder, EventId, Filter, Keys, Kind};
use nostr_database::{MemoryDatabase, MemoryDatabaseOptions};
use nostr_relay_pool::pool::RelayPoolBuilder;
use nostr_relay_pool::{RelayOptions, RelayPool, RelayPoolNotification, SubscribeOptions};
use nozzl_test_relay::TestRelay;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::timeout;

/// Mirror of `Acquirer::new`'s pool construction (acquirer.rs): a
/// dedup-only `MemoryDatabase` (`events: false`) and the default
/// `notification_channel_size` (4096). Keeping this identical is the whole
/// point — we are characterizing the *production* pool, not a toy.
fn build_pool_like_nozzl(pool_dedup_max_events: usize) -> RelayPool {
    let db = MemoryDatabase::with_opts(MemoryDatabaseOptions {
        events: false,
        max_events: Some(pool_dedup_max_events.max(1)),
    });
    RelayPoolBuilder {
        __database: Arc::new(db),
        ..RelayPoolBuilder::default()
    }
    .build()
}

/// `n` distinct, validly-signed kind-1 events. Distinct content ⟹ distinct
/// ids, so the pool's id-tracker treats every one as `NotExistent` exactly
/// once (no replaceable/coordinate collapsing for a regular kind).
fn make_unique_events(n: usize) -> Vec<Event> {
    let keys = Keys::generate();
    (0..n)
        .map(|i| {
            EventBuilder::new(Kind::from(1u16), format!("firehose-{i}"))
                .custom_created_at(nostr::Timestamp::from_secs(1_700_000_000 + i as u64))
                .sign_with_keys(&keys)
                .expect("sign event")
        })
        .collect()
}

#[derive(Debug, Default)]
struct DrainResult {
    /// Distinct event ids delivered via `RelayPoolNotification::Event`.
    seen_event_ids: HashSet<EventId>,
    /// Sum of all `Lagged(n)` observed on the broadcast receiver.
    lagged_total: u64,
    /// Count of `Event` notifications (with duplicates, for diagnostics).
    event_notifs: u64,
    /// Count of `Message` notifications (every wire message, incl. EOSE).
    message_notifs: u64,
}

/// Drain a *pre-subscribed* broadcast receiver until the firehose goes idle
/// for `idle`. `per_notif_delay` injects artificial slowness to provoke
/// `Lagged`; `None` = drain as fast as physically possible.
///
/// The receiver MUST be obtained before `subscribe`, otherwise it misses the
/// events sent before it registered with the broadcast channel.
async fn drain(
    mut rx: tokio::sync::broadcast::Receiver<RelayPoolNotification>,
    per_notif_delay: Option<Duration>,
    idle: Duration,
) -> DrainResult {
    let mut out = DrainResult::default();
    loop {
        match timeout(idle, rx.recv()).await {
            // No traffic for `idle`: the firehose is drained.
            Err(_elapsed) => break,
            Ok(Ok(notif)) => {
                match notif {
                    RelayPoolNotification::Event { event, .. } => {
                        out.seen_event_ids.insert(event.id);
                        out.event_notifs += 1;
                    }
                    RelayPoolNotification::Message { .. } => out.message_notifs += 1,
                    RelayPoolNotification::Shutdown => break,
                }
                if let Some(d) = per_notif_delay {
                    tokio::time::sleep(d).await;
                }
            }
            // The decisive signal: the channel overwrote items we hadn't read.
            Ok(Err(RecvError::Lagged(n))) => out.lagged_total += n,
            Ok(Err(RecvError::Closed)) => break,
        }
    }
    out
}

/// Stand up the firehose relay + nozzl-shaped pool, subscribe, and drain.
async fn run_firehose(n: usize, per_notif_delay: Option<Duration>) -> (usize, DrainResult) {
    let events = make_unique_events(n);
    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let pool = build_pool_like_nozzl(5_000_000);
    let relay_url = relay.url();
    pool.add_relay(relay_url.as_str(), RelayOptions::default())
        .await
        .expect("add relay");
    pool.connect().await;
    pool.wait_for_connection(Duration::from_secs(5)).await;

    // Subscribe AFTER taking the receiver so nothing is missed.
    let rx = pool.notifications();
    pool.subscribe(
        vec![Filter::new().kind(Kind::from(1u16))],
        SubscribeOptions::default(),
    )
    .await
    .expect("subscribe");

    let result = drain(rx, per_notif_delay, Duration::from_millis(600)).await;

    pool.disconnect().await;
    relay.shutdown().await;
    (n, result)
}

/// (1) A consumer that keeps up archives **every** event and reports **zero**
/// lag. This is the contrapositive anchor: it shows the pool does NOT silently
/// drop accepted events — when nothing lags, nothing is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fast_consumer_loses_nothing_and_never_lags() {
    let (n, r) = run_firehose(8_000, None).await;

    eprintln!(
        "[fast] n={n} seen={} lagged={} event_notifs={} message_notifs={}",
        r.seen_event_ids.len(),
        r.lagged_total,
        r.event_notifs,
        r.message_notifs
    );
    assert_eq!(
        r.lagged_total, 0,
        "a keep-up consumer must never lag (event_notifs={}, message_notifs={})",
        r.event_notifs, r.message_notifs
    );
    assert_eq!(
        r.seen_event_ids.len(),
        n,
        "zero lag must mean zero loss: saw {}/{} unique ids",
        r.seen_event_ids.len(),
        n
    );
}

/// (2) A deliberately slow consumer DOES lose events under the firehose — but
/// the count lost is always covered by the `Lagged` total. Loss never escapes
/// the signal, so the signal is a complete accounting of loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_consumer_loss_is_fully_accounted_by_lagged() {
    let n = 8_000;
    // ~500µs/notif (rounds up to the OS timer tick) is ~10x slower than the
    // pool's verify-bound producer, so the 4096-deep ring overflows mid-burst.
    let (_, r) = run_firehose(n, Some(Duration::from_micros(500))).await;

    let lost = n - r.seen_event_ids.len();

    eprintln!(
        "[slow] n={n} seen={} lost={lost} lagged={} event_notifs={} message_notifs={}",
        r.seen_event_ids.len(),
        r.lagged_total,
        r.event_notifs,
        r.message_notifs
    );
    assert!(
        r.lagged_total > 0,
        "slow consumer should have provoked Lagged (saw {}/{} ids, lost {lost})",
        r.seen_event_ids.len(),
        n
    );
    assert!(
        lost > 0,
        "slow consumer should have actually lost events (lagged_total={})",
        r.lagged_total
    );
    // The core invariant: every lost event is covered by the Lagged signal.
    // (Lagged counts skipped Event AND Message slots, so it is an upper bound
    // on lost unique ids — loss can never exceed what Lagged reported.)
    assert!(
        r.lagged_total >= lost as u64,
        "loss ({lost}) must never exceed the Lagged accounting ({})",
        r.lagged_total
    );
}
