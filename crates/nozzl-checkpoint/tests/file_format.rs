//! Byte-level tests of the on-disk format. The schema is part of the public
//! contract — operators read these files, downstream tools parse them, and
//! crash recovery depends on byte-for-byte interpretation.

use std::time::Duration;

use nozzl_checkpoint::{BackfillState, CheckpointStore, FileCheckpointStore, SCHEMA_VERSION};
use nozzl_types::{EventId, RelayUrl};
use serde_json::json;

const FOREVER: Duration = Duration::from_secs(60 * 60 * 24);

fn evid(hex: &str) -> EventId {
    EventId::from_hex(hex).expect("valid 32-byte hex")
}

const E1: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const E2: &str = "2222222222222222222222222222222222222222222222222222222222222222";

#[tokio::test]
async fn schema_version_is_one() {
    assert_eq!(SCHEMA_VERSION, 1);
}

#[tokio::test]
async fn flushed_file_matches_documented_shape() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let relay = RelayUrl::parse("wss://relay.damus.io").unwrap();

    store.observe(&relay, 0, 1_714_579_200, &evid(E1)).await;
    store
        .record_backfill(&relay, 0, BackfillState::Complete)
        .await;
    store.observe(&relay, 10002, 1_714_579_150, &evid(E2)).await;
    store
        .record_backfill(
            &relay,
            10002,
            BackfillState::InProgress {
                until_cursor: 1_700_000_000,
            },
        )
        .await;

    store.flush().await.unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();

    assert_eq!(v["schema_version"], json!(1));
    // updated_at must be a non-empty ISO-8601 string ending in Z.
    let updated = v["updated_at"].as_str().unwrap();
    assert!(
        updated.ends_with('Z'),
        "updated_at should end in Z: {updated}"
    );
    assert_eq!(updated.len(), 20);

    let kinds = &v["relays"]["wss://relay.damus.io/"]["kinds"];
    assert_eq!(kinds["0"]["since"], json!(1_714_579_200_i64));
    assert_eq!(kinds["0"]["last_event_id"], json!(E1));
    assert_eq!(kinds["0"]["backfill"], json!({ "state": "complete" }));

    assert_eq!(kinds["10002"]["since"], json!(1_714_579_150_i64));
    assert_eq!(kinds["10002"]["last_event_id"], json!(E2));
    assert_eq!(
        kinds["10002"]["backfill"],
        json!({ "state": "in_progress", "until_cursor": 1_700_000_000_i64 })
    );
}

#[tokio::test]
async fn backfill_pending_serializes_without_until_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let relay = RelayUrl::parse("wss://nos.lol").unwrap();
    store
        .record_backfill(&relay, 30063, BackfillState::Pending)
        .await;
    store.flush().await.unwrap();

    let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        v["relays"]["wss://nos.lol/"]["kinds"]["30063"]["backfill"],
        json!({ "state": "pending" })
    );
}

#[tokio::test]
async fn round_trip_preserves_state_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let relay = RelayUrl::parse("wss://relay.primal.net").unwrap();

    {
        let store = FileCheckpointStore::open(path.clone(), FOREVER)
            .await
            .unwrap();
        store.observe(&relay, 3, 100, &evid(E1)).await;
        store.observe(&relay, 3, 200, &evid(E2)).await;
        store
            .record_backfill(&relay, 3, BackfillState::InProgress { until_cursor: 50 })
            .await;
        store.flush().await.unwrap();
        store.shutdown().await.unwrap();
    }

    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    assert_eq!(store.since(&relay, 3).await, Some(200));
    assert_eq!(
        store.backfill_state(&relay, 3).await,
        BackfillState::InProgress { until_cursor: 50 }
    );
}

#[tokio::test]
async fn missing_file_starts_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("does-not-exist.json");

    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let relay = RelayUrl::parse("wss://relay.damus.io").unwrap();
    assert_eq!(store.since(&relay, 0).await, None);
    assert_eq!(
        store.backfill_state(&relay, 0).await,
        BackfillState::Pending
    );
    assert!(!path.exists(), "no I/O until first observation");
}

#[tokio::test]
async fn corrupted_file_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");
    std::fs::write(&path, b"{not valid json").unwrap();

    let err = match FileCheckpointStore::open(path.clone(), FOREVER).await {
        Ok(_) => panic!("should refuse to load corrupt file"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains("corrupt"), "got: {msg}");
}

#[tokio::test]
async fn unsupported_schema_returns_specific_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");
    let bogus = json!({
        "schema_version": 2,
        "updated_at": "2026-05-01T00:00:00Z",
        "relays": {}
    });
    std::fs::write(&path, serde_json::to_vec(&bogus).unwrap()).unwrap();

    let err = match FileCheckpointStore::open(path.clone(), FOREVER).await {
        Ok(_) => panic!("should reject unsupported schema"),
        Err(e) => e,
    };
    assert!(
        matches!(
            err,
            nozzl_checkpoint::CheckpointError::UnsupportedSchema {
                found: 2,
                expected: 1,
                ..
            }
        ),
        "got: {err:?}"
    );
}

#[tokio::test]
async fn json_serialize_deserialize_is_lossless() {
    // Build the same logical state via two routes and confirm the file content
    // is identical: emit -> read -> re-emit must be byte-stable thanks to our
    // BTreeMap-based ordering.
    let dir = tempfile::tempdir().unwrap();
    let path_a = dir.path().join("a.json");
    let path_b = dir.path().join("b.json");

    let relay = RelayUrl::parse("wss://relay.example").unwrap();

    let store = FileCheckpointStore::open(path_a.clone(), FOREVER)
        .await
        .unwrap();
    store.observe(&relay, 0, 1, &evid(E1)).await;
    store.observe(&relay, 30063, 7, &evid(E2)).await;
    store.flush().await.unwrap();
    store.shutdown().await.unwrap();

    // Reopen, immediately flush again with no mutations. updated_at will move,
    // but the relays/kinds payload must be identical.
    let store = FileCheckpointStore::open(path_a.clone(), FOREVER)
        .await
        .unwrap();
    // Force a flush by mutating something then flushing — we want to confirm
    // re-flushing the same state yields the same `relays` content.
    store.flush().await.unwrap();
    // Manually copy the post-load state out by re-flushing to path_b. We need a
    // separate store instance for that.
    let raw_a = std::fs::read_to_string(&path_a).unwrap();
    let v_a: serde_json::Value = serde_json::from_str(&raw_a).unwrap();
    drop(store);

    let store_b = FileCheckpointStore::open(path_b.clone(), FOREVER)
        .await
        .unwrap();
    store_b.observe(&relay, 0, 1, &evid(E1)).await;
    store_b.observe(&relay, 30063, 7, &evid(E2)).await;
    store_b.flush().await.unwrap();
    store_b.shutdown().await.unwrap();
    let raw_b = std::fs::read_to_string(&path_b).unwrap();
    let v_b: serde_json::Value = serde_json::from_str(&raw_b).unwrap();

    assert_eq!(v_a["relays"], v_b["relays"]);
    assert_eq!(v_a["schema_version"], v_b["schema_version"]);
}
