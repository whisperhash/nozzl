//! Hot-reload integration test: start with one kind, after ingestion is
//! observed, rewrite `kinds.toml` to include a second kind, and verify the
//! second wave of events lands too.

mod common;

use std::time::Duration;

use common::{NozzlHandle, write_config, write_kinds};
use nostr::{EventBuilder, Keys, Kind};
use nozzl_test_relay::TestRelay;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn add_kind_triggers_backfill_for_new_kind() {
    let keys = Keys::generate();

    // Two cohorts of preloaded events: kind 0 (initially configured) and
    // kind 30023 (added after reload).
    let kind0_events: Vec<_> = (0..5u64)
        .map(|i| {
            EventBuilder::new(Kind::from_u16(0), format!("k0-{i}"))
                .custom_created_at(nostr::Timestamp::from_secs(1_700_000_000 + i))
                .sign_with_keys(&keys)
                .unwrap()
        })
        .collect();
    let kind_addr_events: Vec<_> = (0..5u64)
        .map(|i| {
            EventBuilder::new(Kind::from_u16(30023), format!("k30023-{i}"))
                .custom_created_at(nostr::Timestamp::from_secs(1_700_000_100 + i))
                .sign_with_keys(&keys)
                .unwrap()
        })
        .collect();

    let mut all = kind0_events.clone();
    all.extend(kind_addr_events.clone());

    let relay = TestRelay::builder().preload_events(all).start().await;

    let cfg_dir = TempDir::new().unwrap();
    let archive_dir = TempDir::new().unwrap();

    let cfg_path = write_config(
        &cfg_dir,
        archive_dir.path(),
        &[relay.url().as_str()],
        &[0],
    );

    let archive_root = archive_dir.path().to_path_buf();
    let nozzl = NozzlHandle::start(cfg_path, archive_root);

    // Phase 1: wait for the initial 5 kind-0 events.
    let initial_target = kind0_events.len();
    let (phase1_ok, phase1_count) = nozzl
        .wait_for_events(initial_target, Duration::from_secs(30))
        .await;
    assert!(
        phase1_ok,
        "initial kind-0 ingestion did not complete; got {phase1_count}"
    );

    // Phase 2: rewrite kinds.toml to include 30023. The watcher should
    // pick this up within a couple of seconds (debounce + filesystem
    // notification latency), trigger a scoped backfill for kind 30023,
    // and the events land.
    write_kinds(&cfg_dir, &[0, 30023]);

    let total_target = kind0_events.len() + kind_addr_events.len();
    let (phase2_ok, phase2_count) = nozzl
        .wait_for_events(total_target, Duration::from_secs(30))
        .await;
    assert!(
        phase2_ok,
        "post-reload kind-30023 ingestion did not complete; got {phase2_count}"
    );

    nozzl.kill();
    relay.shutdown().await;
}
