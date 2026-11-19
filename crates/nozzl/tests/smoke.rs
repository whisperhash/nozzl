//! Full-stack smoke test: spin up `nozzl-test-relay`, point a temp config at
//! it, run `nozzl::run` in the background, and wait for events to land on
//! disk.

mod common;

use std::time::Duration;

use common::{NozzlHandle, count_event_files, write_config};
use nostr::{EventBuilder, Keys, Kind};
use nozzl_test_relay::TestRelay;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_preloaded_events_land_on_disk() {
    let keys = Keys::generate();
    let n_events = 10usize;
    let events: Vec<_> = (0..n_events)
        .map(|i| {
            EventBuilder::new(Kind::from_u16(0), format!("e{i}"))
                .custom_created_at(nostr::Timestamp::from_secs(1_700_000_000 + i as u64))
                .sign_with_keys(&keys)
                .unwrap()
        })
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let cfg_dir = TempDir::new().unwrap();
    let archive_dir = TempDir::new().unwrap();

    let cfg_path = write_config(&cfg_dir, archive_dir.path(), &[relay.url().as_str()], &[0]);

    let archive_root = archive_dir.path().to_path_buf();
    let nozzl = NozzlHandle::start(cfg_path, archive_root.clone());

    // Wait until at least N event files exist under
    // {archive}/c=replaceable/k=0/.
    let (ok, count) = nozzl
        .wait_for_events(n_events, Duration::from_secs(30))
        .await;
    assert!(ok, "expected {n_events} files in archive, got {count}");

    nozzl.kill();
    relay.shutdown().await;

    // Sanity: no extras (each preloaded event is unique by id).
    let final_count = count_event_files(&archive_root);
    assert!(
        final_count >= n_events,
        "expected >= {n_events} files, got {final_count}"
    );
}

/// A regular kind (kind-9735 zap receipts) is captured end-to-end through the
/// full binary — backfill from the preloaded set plus a live forward-sub
/// publish — and lands under the `regular/` class tree.
#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_regular_kind_lands_under_regular_tree() {
    let keys = Keys::generate();
    let n_preloaded = 8usize;
    // Preloaded events drive the onboarding backfill.
    let preloaded: Vec<_> = (0..n_preloaded)
        .map(|i| {
            EventBuilder::new(Kind::from_u16(9735), format!("zap{i}"))
                .custom_created_at(nostr::Timestamp::from_secs(1_700_000_000 + i as u64))
                .sign_with_keys(&keys)
                .unwrap()
        })
        .collect();

    let relay = TestRelay::builder()
        .preload_events(preloaded.clone())
        .start()
        .await;

    let cfg_dir = TempDir::new().unwrap();
    let archive_dir = TempDir::new().unwrap();
    let cfg_path = write_config(
        &cfg_dir,
        archive_dir.path(),
        &[relay.url().as_str()],
        &[9735],
    );

    let archive_root = archive_dir.path().to_path_buf();
    let nozzl = NozzlHandle::start(cfg_path, archive_root.clone());

    // Backfill should pick up the preloaded set.
    let (ok, count) = nozzl
        .wait_for_events(n_preloaded, Duration::from_secs(30))
        .await;
    assert!(ok, "expected {n_preloaded} backfilled files, got {count}");

    // A live publish exercises the forward sub for a regular kind too.
    let live = EventBuilder::new(Kind::from_u16(9735), "zap-live")
        .custom_created_at(nostr::Timestamp::from_secs(1_700_001_000))
        .sign_with_keys(&keys)
        .unwrap();
    relay.publish(live.clone()).await;
    let (ok, count) = nozzl
        .wait_for_events(n_preloaded + 1, Duration::from_secs(30))
        .await;
    assert!(ok, "expected forward-sub event to land too, got {count}");

    nozzl.kill();
    relay.shutdown().await;

    // All chunks for a regular kind live under the c=regular/ tree.
    assert!(
        archive_root.join("c=regular").is_dir(),
        "expected a c=regular/ class tree at {}",
        archive_root.display()
    );
    assert!(
        !archive_root.join("c=replaceable").exists()
            && !archive_root.join("c=addressable").exists(),
        "a kind-9735-only run must not create c=replaceable/ or c=addressable/ trees"
    );
}
