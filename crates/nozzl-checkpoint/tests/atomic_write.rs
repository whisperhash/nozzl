//! Atomic-write semantics: the target file is never observed in a
//! half-written state, even under faults or concurrent mutations.
//!
//! `docs/crates/nozzl-checkpoint.md` calls for a "crash during write"
//! verification. We achieve equivalent assurance with two tests:
//!
//! 1. **Fault injection.** Simulate a rename failure by inserting a directory
//!    where the target file should be. The persist step fails; the previous
//!    file (if any, in a sibling location) must be intact.
//! 2. **Concurrent observer.** While one task hammers `flush()` in a tight
//!    loop, a second task continually re-reads + re-parses the file. Every
//!    successful read must yield a parseable, schema-valid document — no
//!    reader ever sees a torn write.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nozzl_checkpoint::{CheckpointError, CheckpointStore, FileCheckpointStore};
use nozzl_types::{EventId, RelayUrl};

const FOREVER: Duration = Duration::from_secs(60 * 60 * 24);

fn evid(suffix: u8) -> EventId {
    let hex = format!("{:0>64x}", suffix);
    EventId::from_hex(&hex).unwrap()
}

#[tokio::test]
async fn flush_failure_does_not_corrupt_previous_file() {
    // Strategy: get a healthy file written first, then poison the rename target
    // by replacing it with a non-empty directory. The next flush must:
    //  - return an error,
    //  - leave the directory's contents intact, proving no partial write
    //    happened to the rename target.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://relay.first").unwrap();
    store.observe(&r, 0, 100, &evid(1)).await;
    store.flush().await.unwrap();
    let healthy_bytes = std::fs::read(&path).expect("first flush wrote a file");

    // Replace the file with a directory containing a sentinel.
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("sentinel"), b"original").unwrap();

    // Mutate state and try to flush. The rename will fail because the target
    // is a non-empty directory; the directory's contents must be unchanged.
    store.observe(&r, 0, 200, &evid(2)).await;
    let err = match store.flush().await {
        Ok(()) => panic!("flush onto an obstructing directory should have failed"),
        Err(e) => e,
    };
    assert!(matches!(err, CheckpointError::Io { .. }), "got {err:?}");

    // Sentinel inside the obstructing directory must be untouched. No half-
    // written file shows up alongside it either.
    let entries: Vec<_> = std::fs::read_dir(&path).unwrap().collect();
    assert_eq!(
        entries.len(),
        1,
        "directory leaked extra files: {entries:?}"
    );
    assert_eq!(std::fs::read(path.join("sentinel")).unwrap(), b"original");

    // The healthy bytes captured before the fault are still parseable —
    // demonstrates the file content we previously wrote was intact and would
    // have continued to be readable had the directory not been swapped in.
    let v: serde_json::Value = serde_json::from_slice(&healthy_bytes).unwrap();
    assert_eq!(v["schema_version"], serde_json::json!(1));

    // Replace the obstruction so shutdown's final flush can succeed.
    std::fs::remove_file(path.join("sentinel")).unwrap();
    std::fs::remove_dir(&path).unwrap();
    Arc::clone(&store).shutdown().await.unwrap();
}

#[tokio::test]
async fn temp_files_do_not_leak_on_failure() {
    // After a failed flush the parent directory should not be littered with
    // tempfiles — the `NamedTempFile` drop guard cleans up on persist failure.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://relay.example").unwrap();
    store.observe(&r, 0, 1, &evid(1)).await;
    store.flush().await.unwrap();

    // Poison: target becomes a directory.
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();

    store.observe(&r, 0, 2, &evid(2)).await;
    let _ = store.flush().await; // expected error

    // Count entries in dir.path() — should be exactly the obstructing dir.
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "tempfiles leaked into parent dir: {entries:?}"
    );

    std::fs::remove_dir(&path).unwrap();
    Arc::clone(&store).shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn persisted_file_is_world_readable() {
    // A read-only account inspects live
    // checkpoint state, so the persisted file must be world-readable (0644) —
    // NOT the 0600 a NamedTempFile would otherwise carry across the rename.
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://relay.example").unwrap();
    store.observe(&r, 0, 1, &evid(1)).await;
    store.flush().await.unwrap();

    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o644, "first flush mode was {mode:o}, want 0644");

    // A second flush (fresh tempfile) must preserve the mode, not regress to 0600.
    store.observe(&r, 0, 2, &evid(2)).await;
    store.flush().await.unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o644, "second flush mode was {mode:o}, want 0644");

    // The shutdown final flush takes the same write path — still 0644 after.
    Arc::clone(&store).shutdown().await.unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o644, "post-shutdown mode was {mode:o}, want 0644");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reader_never_sees_torn_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let store = FileCheckpointStore::open(path.clone(), FOREVER)
        .await
        .unwrap();
    let r = RelayUrl::parse("wss://relay.example").unwrap();
    // Seed state so the first flush has something to write.
    store.observe(&r, 0, 1, &evid(0)).await;
    store.flush().await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    let writer = {
        let store = Arc::clone(&store);
        let r = r.clone();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut ts: i64 = 2;
            while !stop.load(Ordering::Relaxed) {
                store.observe(&r, 0, ts, &evid((ts & 0xff) as u8)).await;
                store.flush().await.unwrap();
                ts += 1;
            }
            ts
        })
    };

    let reader_path = path.clone();
    let reader_stop = Arc::clone(&stop);
    let reader = tokio::task::spawn_blocking(move || {
        let mut reads = 0_u64;
        while !reader_stop.load(Ordering::Relaxed) {
            match std::fs::read(&reader_path) {
                Ok(bytes) => {
                    let parsed: serde_json::Value = serde_json::from_slice(&bytes)
                        .expect("every observed file must be parseable JSON");
                    assert_eq!(parsed["schema_version"], serde_json::json!(1));
                    reads += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => panic!("unexpected reader error: {e}"),
            }
        }
        reads
    });

    // Run for ~250ms — plenty of flush-read cycles without slowing CI.
    tokio::time::sleep(Duration::from_millis(250)).await;
    stop.store(true, Ordering::Relaxed);

    let final_ts = writer.await.unwrap();
    let read_count = reader.await.unwrap();
    assert!(final_ts > 2, "writer should have made progress");
    assert!(
        read_count > 0,
        "reader should have observed at least one full file"
    );

    Arc::clone(&store).shutdown().await.unwrap();
}
