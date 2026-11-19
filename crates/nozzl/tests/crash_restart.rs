//! Crash-and-restart integration test.
//!
//! Phase 1: run nozzl, ingest a batch of events, kill the instance.
//! Phase 2: restart nozzl with the same config + archive directory. On
//! restart, any events still in the per-(kind, shard) WAL are replayed into
//! fresh chunks; checkpoint resume avoids re-fetching the already-acknowledged
//! backfill cursor. Together this ensures no event loss across crashes and no
//! duplicate work for events already durable on disk.
//!
//! The phase-1 → phase-2 transition uses [`common::NozzlHandle::kill`], which
//! drops nozzl's dedicated runtime — atomically terminating every task it
//! spawned. This is critical: a bare `JoinHandle::abort()` leaves detached
//! children (writer, forward sub) alive, which previously latched onto
//! phase-2 events published before the restart and produced a spurious
//! double-archive.

mod common;

use std::time::Duration;

use common::{NozzlHandle, count_event_files, write_config};
use nostr::{Event, EventBuilder, Keys, Kind};
use nozzl_test_relay::TestRelay;
use tempfile::TempDir;

fn make(keys: &Keys, kind: u16, content: &str, ts: u64) -> Event {
    EventBuilder::new(Kind::from_u16(kind), content)
        .custom_created_at(nostr::Timestamp::from_secs(ts))
        .sign_with_keys(keys)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_resume_after_restart() {
    let keys = Keys::generate();
    let phase1_events: Vec<Event> = (0..5u64)
        .map(|i| make(&keys, 0, &format!("p1-{i}"), 1_700_000_000 + i))
        .collect();
    let phase2_events: Vec<Event> = (0..5u64)
        .map(|i| make(&keys, 0, &format!("p2-{i}"), 1_700_000_100 + i))
        .collect();

    let relay = TestRelay::builder()
        .preload_events(phase1_events.clone())
        .start()
        .await;

    let cfg_dir = TempDir::new().unwrap();
    let archive_dir = TempDir::new().unwrap();

    let cfg_path = write_config(
        &cfg_dir,
        archive_dir.path(),
        &[relay.url().as_str()],
        &[0],
    );

    let archive_root = archive_dir.path().to_path_buf();

    // ---- Phase 1: ingest the first 5 events, then kill. ----
    let nozzl = NozzlHandle::start(cfg_path.clone(), archive_root.clone());

    let (phase1_ok, phase1_count) = nozzl
        .wait_for_events(phase1_events.len(), Duration::from_secs(30))
        .await;
    assert!(phase1_ok, "phase 1 ingestion incomplete; got {phase1_count}");

    // Give the checkpoint flusher a chance to write (interval = 1s in
    // common::write_config).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Faithful crash: drops nozzl's runtime, atomically killing the writer
    // / forward sub / checkpoint flusher. Phase 1's forward sub is gone
    // before the phase-2 publishes below, so the relay cannot deliver
    // phase-2 events to a "zombie" phase-1 connection.
    nozzl.kill();

    let count_after_phase1 = count_event_files(&archive_root);

    // ---- Phase 2: publish 5 more events, restart nozzl. ----
    for e in &phase2_events {
        relay.publish(e.clone()).await;
    }

    let nozzl2 = NozzlHandle::start(cfg_path.clone(), archive_root.clone());

    // The restart should pick up only the new events (the existing ones are
    // already in the archive and will be deduped by PutMode::Create).
    let target = phase1_events.len() + phase2_events.len();
    let (phase2_ok, final_count) = nozzl2.wait_for_events(target, Duration::from_secs(30)).await;
    assert!(
        phase2_ok,
        "phase 2 ingestion incomplete; got {final_count} of {target}"
    );

    // No double-archive: total file count should be exactly `target`.
    // The archive's PutMode::Create makes this true regardless of whether
    // the relay re-emits the phase-1 events.
    assert_eq!(
        final_count, target,
        "expected exactly {target} files (no double-archive), got {final_count}; phase1 had {count_after_phase1}"
    );

    nozzl2.kill();
    relay.shutdown().await;
}
