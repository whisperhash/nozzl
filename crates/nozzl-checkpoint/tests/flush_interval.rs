//! Verifies the background flush task fires on the configured interval and
//! does no I/O when state is unchanged.

use std::sync::Arc;
use std::time::Duration;

use nozzl_checkpoint::{CheckpointStore, FileCheckpointStore};
use nozzl_types::{EventId, RelayUrl};

fn evid(s: u8) -> EventId {
    let hex = format!("{:0>64x}", s);
    EventId::from_hex(&hex).unwrap()
}

#[tokio::test]
async fn idle_store_does_not_create_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), Duration::from_millis(20))
        .await
        .unwrap();

    // Wait through several flush intervals with no observations.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!path.exists(), "no I/O without mutations");

    Arc::clone(&store).shutdown().await.unwrap();
    // Shutdown is forced — it always flushes once at the end, so by here the
    // file may or may not exist depending on whether any state changed. With
    // no observations it'd flush an empty document, which is allowed.
}

#[tokio::test]
async fn background_flush_fires_after_observation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), Duration::from_millis(30))
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://relay.example").unwrap();
    store.observe(&r, 0, 1, &evid(1)).await;

    // Poll up to 1s for the background task to flush.
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        path.exists(),
        "background flush should have written the file"
    );

    let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(v["schema_version"], serde_json::json!(1));

    Arc::clone(&store).shutdown().await.unwrap();
}

#[tokio::test]
async fn no_redundant_io_after_settling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), Duration::from_millis(25))
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://relay.example").unwrap();
    store.observe(&r, 0, 1, &evid(1)).await;

    // Wait for the first background flush to land.
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while std::time::Instant::now() < deadline && !path.exists() {
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert!(path.exists());

    let mtime_before = std::fs::metadata(&path).unwrap().modified().unwrap();

    // Sleep through several intervals with no further mutations.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mtime_after = std::fs::metadata(&path).unwrap().modified().unwrap();

    assert_eq!(
        mtime_before, mtime_after,
        "idle ticks must not re-write the file"
    );

    Arc::clone(&store).shutdown().await.unwrap();
}
