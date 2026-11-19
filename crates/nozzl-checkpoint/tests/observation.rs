//! Behavior tests for `observe` / `since` / `backfill_state` /
//! `record_backfill`. These exercise pure in-memory semantics; the file format
//! tests live in `file_format.rs`.

use std::sync::Arc;
use std::time::Duration;

use nozzl_checkpoint::{BackfillState, CheckpointStore, FileCheckpointStore};
use nozzl_types::{EventId, RelayUrl};

const FOREVER: Duration = Duration::from_secs(60 * 60 * 24);

fn evid(suffix: u8) -> EventId {
    let hex = format!("{:0>64x}", suffix);
    EventId::from_hex(&hex).unwrap()
}

#[tokio::test]
async fn since_is_none_for_unseen_coord() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();
    assert_eq!(store.since(&r, 0).await, None);
}

#[tokio::test]
async fn since_returns_max_observed() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();

    store.observe(&r, 0, 100, &evid(1)).await;
    store.observe(&r, 0, 300, &evid(3)).await;
    store.observe(&r, 0, 200, &evid(2)).await;

    assert_eq!(store.since(&r, 0).await, Some(300));
}

#[tokio::test]
async fn observe_does_not_regress_on_older_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();

    store.observe(&r, 30023, 500, &evid(5)).await;
    store.observe(&r, 30023, 100, &evid(1)).await;
    assert_eq!(store.since(&r, 30023).await, Some(500));

    // Equal `created_at` also doesn't regress (strictly-greater rule).
    store.observe(&r, 30023, 500, &evid(7)).await;
    assert_eq!(store.since(&r, 30023).await, Some(500));
}

#[tokio::test]
async fn distinct_relays_and_kinds_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r1 = RelayUrl::parse("wss://r1").unwrap();
    let r2 = RelayUrl::parse("wss://r2").unwrap();

    store.observe(&r1, 0, 100, &evid(1)).await;
    store.observe(&r1, 3, 200, &evid(2)).await;
    store.observe(&r2, 0, 300, &evid(3)).await;

    assert_eq!(store.since(&r1, 0).await, Some(100));
    assert_eq!(store.since(&r1, 3).await, Some(200));
    assert_eq!(store.since(&r2, 0).await, Some(300));
    assert_eq!(store.since(&r2, 3).await, None);
}

#[tokio::test]
async fn backfill_default_is_pending() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();
    assert_eq!(
        store.backfill_state(&r, 30063).await,
        BackfillState::Pending
    );
}

#[tokio::test]
async fn record_backfill_overwrites() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();

    store
        .record_backfill(&r, 30063, BackfillState::InProgress { until_cursor: 1000 })
        .await;
    assert_eq!(
        store.backfill_state(&r, 30063).await,
        BackfillState::InProgress { until_cursor: 1000 }
    );

    store
        .record_backfill(&r, 30063, BackfillState::Complete)
        .await;
    assert_eq!(
        store.backfill_state(&r, 30063).await,
        BackfillState::Complete
    );
}

/// Hammer `observe` from many tasks with random timestamps and confirm the
/// final `since` is the maximum across all writers.
#[tokio::test]
async fn concurrent_observe_converges_to_max() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();

    const TASKS: usize = 32;
    const OBS_PER_TASK: i64 = 250;

    let mut handles = Vec::with_capacity(TASKS);
    for t in 0..TASKS {
        let store = Arc::clone(&store);
        let r = r.clone();
        handles.push(tokio::spawn(async move {
            // Each task writes a deterministic, distinct sequence of timestamps
            // covering [t*OBS_PER_TASK, (t+1)*OBS_PER_TASK).
            for i in 0..OBS_PER_TASK {
                let ts = (t as i64) * OBS_PER_TASK + i;
                store.observe(&r, 0, ts, &evid((ts & 0xff) as u8)).await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let expected_max = (TASKS as i64) * OBS_PER_TASK - 1;
    assert_eq!(store.since(&r, 0).await, Some(expected_max));
}

/// Concurrent monotonically-decreasing observations across many tasks should
/// still converge to the highest single observation each task issued first.
#[tokio::test]
async fn concurrent_observe_with_descending_writers() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();

    const TASKS: usize = 16;
    const OBS_PER_TASK: i64 = 100;
    const HIGH: i64 = 10_000;

    let mut handles = Vec::with_capacity(TASKS);
    for t in 0..TASKS {
        let store = Arc::clone(&store);
        let r = r.clone();
        handles.push(tokio::spawn(async move {
            // First, a high stamp; then a long descending tail. The high one
            // must "win" regardless of interleaving.
            store.observe(&r, 0, HIGH + t as i64, &evid(t as u8)).await;
            for i in (0..OBS_PER_TASK).rev() {
                store.observe(&r, 0, i, &evid((i & 0xff) as u8)).await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let expected_max = HIGH + (TASKS as i64) - 1;
    assert_eq!(store.since(&r, 0).await, Some(expected_max));
}

#[tokio::test]
async fn since_snapshot_is_empty_when_nothing_observed() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    assert!(store.since_snapshot().await.is_empty());
}

#[tokio::test]
async fn since_snapshot_skips_coords_without_observations() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();

    // record_backfill creates an entry with `since: None`.
    store
        .record_backfill(&r, 30063, BackfillState::InProgress { until_cursor: 1 })
        .await;
    // observe creates an entry with `since: Some(_)`.
    store.observe(&r, 0, 100, &evid(1)).await;

    let snap = store.since_snapshot().await;
    assert_eq!(snap.len(), 1, "only the observed coord should be reported");
    assert_eq!(snap[0].1, 0);
    assert_eq!(snap[0].2, 100);
}

#[tokio::test]
async fn since_snapshot_returns_all_observed_relay_kind_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r1 = RelayUrl::parse("wss://r1").unwrap();
    let r2 = RelayUrl::parse("wss://r2").unwrap();

    store.observe(&r1, 0, 100, &evid(1)).await;
    store.observe(&r1, 3, 200, &evid(2)).await;
    store.observe(&r2, 0, 300, &evid(3)).await;
    // A subsequent older observation must not regress; snapshot reflects max.
    store.observe(&r1, 0, 50, &evid(4)).await;

    let mut snap = store.since_snapshot().await;
    snap.sort_by(|a, b| (a.0.as_str(), a.1).cmp(&(b.0.as_str(), b.1)));

    assert_eq!(snap.len(), 3);
    assert_eq!(snap[0].0.as_str(), "wss://r1/");
    assert_eq!(snap[0].1, 0);
    assert_eq!(snap[0].2, 100);
    assert_eq!(snap[1].0.as_str(), "wss://r1/");
    assert_eq!(snap[1].1, 3);
    assert_eq!(snap[1].2, 200);
    assert_eq!(snap[2].0.as_str(), "wss://r2/");
    assert_eq!(snap[2].1, 0);
    assert_eq!(snap[2].2, 300);
}

#[tokio::test]
async fn since_snapshot_reflects_concurrent_observations() {
    // After concurrent writers settle, the snapshot must report the max
    // `since` per (relay, kind) — same invariant as `since(...)`.
    let dir = tempfile::tempdir().unwrap();
    let store = FileCheckpointStore::open(dir.path().join("c.json"), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();

    const TASKS: usize = 16;
    const OBS_PER_TASK: i64 = 100;

    let mut handles = Vec::with_capacity(TASKS);
    for t in 0..TASKS {
        let store = Arc::clone(&store);
        let r = r.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..OBS_PER_TASK {
                let ts = (t as i64) * OBS_PER_TASK + i;
                store.observe(&r, 7, ts, &evid((ts & 0xff) as u8)).await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let snap = store.since_snapshot().await;
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].2, (TASKS as i64) * OBS_PER_TASK - 1);
}

#[tokio::test]
async fn shutdown_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.json");
    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://x").unwrap();
    store.observe(&r, 0, 1, &evid(1)).await;

    Arc::clone(&store).shutdown().await.unwrap();
    Arc::clone(&store).shutdown().await.unwrap();
    assert!(path.exists(), "shutdown should have flushed at least once");
}

#[tokio::test]
async fn shutdown_flushes_pending_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.json");
    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://relay.example").unwrap();
    store.observe(&r, 10002, 42, &evid(7)).await;
    Arc::clone(&store).shutdown().await.unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        v["relays"]["wss://relay.example/"]["kinds"]["10002"]["since"],
        serde_json::json!(42_i64)
    );
}
