use super::*;
use crate::reader;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};
use object_store::local::LocalFileSystem;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path as ObjPath,
};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::time::timeout;

/// Flush permits for a single-writer test: wide enough never to be the thing
/// under test. Tests that care about the bound build their own narrow
/// semaphore, and the archive-level bound is covered in `demux::tests`.
fn test_permits() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(ChunkWriterConfig::default().chunk_flush_concurrency))
}

fn make_event(kind: u16, content: &str) -> Event {
    let keys = Keys::generate();
    EventBuilder::new(Kind::from_u16(kind), content)
        .custom_created_at(Timestamp::from_secs(1_700_000_000))
        .sign_with_keys(&keys)
        .unwrap()
}

fn make_addressable(d: &str) -> Event {
    let keys = Keys::generate();
    EventBuilder::new(Kind::from_u16(30063), "release")
        .tags(vec![Tag::identifier(d)])
        .custom_created_at(Timestamp::from_secs(1_700_000_000))
        .sign_with_keys(&keys)
        .unwrap()
}

/// Standard test rig: a temp dir with a `LocalFileSystem` rooted at it,
/// plus a WAL root subdir. Returns `(store, wal_root, chunks_root)`.
fn rig() -> (
    Arc<dyn ObjectStore>,
    std::path::PathBuf,
    std::path::PathBuf,
    tempfile::TempDir,
) {
    let dir = tempdir().unwrap();
    let chunks_root = dir.path().to_path_buf();
    let store = Arc::new(LocalFileSystem::new_with_prefix(&chunks_root).unwrap()) as Arc<dyn ObjectStore>;
    let wal_root = dir.path().to_path_buf();
    (store, wal_root, chunks_root, dir)
}

#[tokio::test]
async fn writer_publishes_chunk_on_force_flush() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig::default();
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(
        key.clone(),
        "profile-metadata".into(),
        cfg,
        wal_root,
        store.clone(),
        rx,
        None,
        test_permits(),
    ));

    // The writer's chunk path is keyed by pubkey shard '0', but our random
    // events will have arbitrary first hex chars. For this test we don't
    // care about shard correctness — the writer just dumps whatever comes
    // through its channel into one chunk.
    for _ in 0..5 {
        tx.send(WriterMessage::Event {
            event: Box::new(make_event(0, "hi")),
            ingested_at: 1_700_000_000,
        })
        .await
        .unwrap();
    }
    tx.send(WriterMessage::ForceFlush).await.unwrap();
    // Drop tx and await task — task only exits after processing all
    // queued messages, which guarantees the ForceFlush has landed.
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let events = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(events.len(), 5);
}

#[tokio::test]
async fn writer_rolls_on_max_rows() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig {
        chunk_max_rows: 3,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));
    for i in 0..10 {
        tx.send(WriterMessage::Event {
            event: Box::new(make_event(0, &format!("e{i}"))),
            ingested_at: 1_700_000_000 + i,
        })
        .await
        .unwrap();
    }
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    // 10 events at 3-per-chunk = 4 chunks (3+3+3+1 from final shutdown
    // flush).
    let chunks: Vec<_> = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|s| s == "parquet")
        })
        .collect();
    assert_eq!(
        chunks.len(),
        4,
        "expected 4 chunks for 10 events @ max_rows=3, got {chunks:?}"
    );

    let all = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(all.len(), 10);
}

#[tokio::test]
async fn writer_replays_wal_on_restart() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig::default();
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    // Simulate a "crashed previous run" by writing events directly to the
    // WAL with no chunk publish. This precisely models a kill that left
    // events in the WAL but never produced a chunk — without racing the
    // task abort against in-flight WAL appends.
    let wal_file = wal_path(&wal_root, key.class, key.kind, key.shard).unwrap();
    tokio::fs::create_dir_all(wal_file.parent().unwrap()).await.unwrap();
    let mut wal = Wal::open(&wal_file).await.unwrap();
    let originals: Vec<Event> = (0..5).map(|i| make_event(0, &format!("e{i}"))).collect();
    for (i, e) in originals.iter().enumerate() {
        wal.append(1_700_000_000 + i as u64, e).await.unwrap();
    }
    drop(wal);

    // No chunks exist yet — only the WAL.
    let post_crash_chunks: Vec<_> = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|s| s == "parquet")
        })
        .collect();
    assert_eq!(post_crash_chunks.len(), 0);

    // Restart: writer replays the WAL into its builder and publishes a
    // chunk immediately (trigger=replay). The ForceFlush below is now
    // redundant — kept as a sanity check that ForceFlush on an
    // already-empty builder is a safe no-op — but the chunk has already
    // landed by the time it's sent. See `replay_publishes_chunk_immediately_without_force_flush`
    // for the test that specifically pins the replay-flush behavior.
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));
    tx.send(WriterMessage::ForceFlush).await.unwrap();
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let all = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(
        all.len(),
        5,
        "all 5 pre-crash events should be recovered via WAL replay"
    );
    let recovered_ids: std::collections::HashSet<_> =
        all.iter().map(|r| r.event.id).collect();
    for e in &originals {
        assert!(recovered_ids.contains(&e.id));
    }
}

#[tokio::test]
async fn writer_dedup_within_batch() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig::default();
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    let e = make_event(0, "duplicate-me");
    // Send the same event 3 times. Only the first should be recorded.
    for _ in 0..3 {
        tx.send(WriterMessage::Event {
            event: Box::new(e.clone()),
            ingested_at: 1_700_000_000,
        })
        .await
        .unwrap();
    }
    tx.send(WriterMessage::ForceFlush).await.unwrap();
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let all = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(all.len(), 1, "in-batch dedup must drop repeats");
}

#[tokio::test]
async fn addressable_writer_writes_under_addressable_root() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig::default();
    let key = WriterKey {
        class: EventClass::Addressable,
        kind: 30063,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(
        key,
        "zapstore-app-releases".into(),
        cfg,
        wal_root,
        store,
        rx,
        None,
        test_permits(),
    ));
    tx.send(WriterMessage::Event {
        event: Box::new(make_addressable("myapp@1.0")),
        ingested_at: 1_700_000_000,
    })
    .await
    .unwrap();
    tx.send(WriterMessage::ForceFlush).await.unwrap();
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let chunks: Vec<_> = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|s| s == "parquet")
        })
        .collect();
    assert_eq!(chunks.len(), 1);
    let path_str = chunks[0].to_string_lossy();
    assert!(
        path_str.contains("c=addressable/k=30063/p=0/"),
        "chunk path should reflect addressable layout: got {path_str}"
    );

    let all = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].d_tag.as_deref(), Some("myapp@1.0"));
}

// ------------------------------------------------------------------
// Buffer-age trigger tests
//
// These tests use real wall-clock with short budgets (1 s buffer age,
// a few seconds total per test). Virtual time via `tokio::time::pause`
// does not compose well with the writer's `tokio::fs` IO path: under
// paused time the writer can suspend on a blocking-pool wake-up that
// never registers as a runtime deadline, leaving the writer stuck mid
// WAL append. Real time sidesteps that entirely — the writer's
// `wal_handle.append` completes in milliseconds, and a 1-second age
// budget plus a 1-second poll cadence keeps these tests under ~3 s each.
// ------------------------------------------------------------------

/// Polls the chunk directory on a 100ms cadence until at least `n`
/// chunks exist, or until the real-clock budget runs out.
/// Published chunks under `root`, by the same definition `wait_for_chunks`
/// polls for. Shared so a test can assert an *absence* at a point in time
/// without re-deriving what counts as a chunk.
fn count_chunks(root: &std::path::Path) -> usize {
    walkdir(root)
        .into_iter()
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|s| s == "parquet")
        })
        .count()
}

async fn wait_for_chunks(
    root: &std::path::Path,
    n: usize,
    budget: Duration,
) -> usize {
    let deadline = std::time::Instant::now() + budget;
    loop {
        let count = count_chunks(root);
        if count >= n {
            return count;
        }
        assert!(std::time::Instant::now() < deadline, "expected at least {n} chunks within {budget:?}, got {count}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Test 1: the buffer-age trigger fires under sustained traffic.
///
/// Under continuous traffic — one event every 100 ms wall-clock for
/// ~3 s — the old idle trigger would never have fired (`last_event_at`
/// refreshed on every push). The buffer-age trigger MUST fire because
/// the clock is keyed off the *first* push, not the latest. We assert
/// at least one mid-stream roll lands while the channel is still
/// producing events.
#[tokio::test]
async fn rolls_on_buffer_age_under_sustained_traffic() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig {
        chunk_max_buffer_seconds: 1,
        // Jitter off: these tests assert timing against a 1 s budget, whose
        // 10% default window (100 ms) would silently eat their stated slack.
        // The jitter is exercised end to end by `buffer_age_roll_uses_the_jittered_deadline`.
        chunk_buffer_age_jitter: 0.0,
        chunk_max_rows: usize::MAX,
        chunk_max_size_bytes: usize::MAX,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(64);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // Sustained traffic: 30 events over ~3 s. The age threshold (1 s) is
    // reached partway through, with events still arriving — exactly the
    // regime the old idle trigger failed on. The producer task holds tx
    // open for the full ~3 s so the test asserts mid-stream chunks land
    // while traffic is sustained, not as a shutdown flush.
    let producer = tokio::spawn(async move {
        for i in 0..30 {
            tx.send(WriterMessage::Event {
                event: Box::new(make_event(0, &format!("e{i}"))),
                ingested_at: 1_700_000_000 + i,
            })
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tx
    });

    // Expect at least one chunk mid-stream while the producer is still
    // running. The writer's 1 s age-tick + 1 s threshold puts the
    // first roll near wall-clock t≈1–2 s; the producer runs ~3 s total.
    let mid_run_chunks = wait_for_chunks(&chunks_root, 1, Duration::from_secs(4)).await;
    assert!(
        mid_run_chunks >= 1,
        "expected mid-stream buffer-age roll under sustained traffic; got {mid_run_chunks} chunks"
    );

    // Drain the producer and let the writer shut down.
    let tx = producer.await.unwrap();
    drop(tx);
    timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();

    let all = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(all.len(), 30, "all 30 events must be archived");
}

/// Test 2: equivalent of the old idle test. One event, no further activity,
/// expect a roll after the configured buffer age elapses.
#[tokio::test]
async fn rolls_after_buffer_age_with_single_event() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig {
        chunk_max_buffer_seconds: 1,
        // Jitter off: these tests assert timing against a 1 s budget, whose
        // 10% default window (100 ms) would silently eat their stated slack.
        // The jitter is exercised end to end by `buffer_age_roll_uses_the_jittered_deadline`.
        chunk_buffer_age_jitter: 0.0,
        chunk_max_rows: usize::MAX,
        chunk_max_size_bytes: usize::MAX,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(4);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "solo")),
        ingested_at: 1_700_000_000,
    })
    .await
    .unwrap();

    // The 1-second age tick + 1-second threshold means the chunk lands
    // within ~1-2 s. Budget 4 s for CI slack.
    wait_for_chunks(&chunks_root, 1, Duration::from_secs(4)).await;

    drop(tx);
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 1);
}

/// Test 3: the buffer-age clock starts on the first push since the last
/// flush, NOT at writer startup. Sit idle for longer than the threshold;
/// no chunk should appear. Then push and confirm a roll lands.
#[tokio::test]
async fn buffer_clock_starts_on_first_push_not_at_startup() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig {
        chunk_max_buffer_seconds: 1,
        // Jitter off: these tests assert timing against a 1 s budget, whose
        // 10% default window (100 ms) would silently eat their stated slack.
        // The jitter is exercised end to end by `buffer_age_roll_uses_the_jittered_deadline`.
        chunk_buffer_age_jitter: 0.0,
        chunk_max_rows: usize::MAX,
        chunk_max_size_bytes: usize::MAX,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(4);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // 2.5 s of nothing — channel empty. The age tick fires every 1 s
    // but should find `buffer_started_at = None` and do nothing.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let parquet_count = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|s| s == "parquet")
        })
        .count();
    assert_eq!(
        parquet_count, 0,
        "no chunks should exist before any push (clock must not start at task startup)"
    );

    // Now push one event. The chunk should land within ~1-2 s.
    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "first")),
        ingested_at: 1_700_000_000,
    })
    .await
    .unwrap();
    wait_for_chunks(&chunks_root, 1, Duration::from_secs(4)).await;

    drop(tx);
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
}

/// Test 4: after a flush, the clock resets. The next push starts a fresh
/// budget; the second roll must NOT happen immediately just because the
/// previous burst's clock was past threshold.
#[tokio::test]
async fn buffer_clock_resets_after_flush() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig {
        chunk_max_buffer_seconds: 1,
        // Jitter off: these tests assert timing against a 1 s budget, whose
        // 10% default window (100 ms) would silently eat their stated slack.
        // The jitter is exercised end to end by `buffer_age_roll_uses_the_jittered_deadline`.
        chunk_buffer_age_jitter: 0.0,
        chunk_max_rows: usize::MAX,
        chunk_max_size_bytes: usize::MAX,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(4);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // Push #1 → wait for first roll.
    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "first")),
        ingested_at: 1_700_000_000,
    })
    .await
    .unwrap();
    let chunks_after_first = wait_for_chunks(&chunks_root, 1, Duration::from_secs(4)).await;
    assert_eq!(chunks_after_first, 1);

    // Push #2 immediately. If the clock had NOT reset, the next age-tick
    // would flush right away because `started.elapsed()` would still be
    // well past 1 s from event #1's perspective.
    //
    // Record the wall-clock *before* the send so the elapsed window we
    // verify is a strict overestimate of the elapsed time the writer
    // sees (`buffer_started_at` is set inside the writer task on
    // `builder.push`, which happens some unknown ms after `.await`). This
    // means an age_tick firing during the verification window saw at most
    // `push2_at.elapsed()` of buffer age — strictly less than 1 s.
    let push2_at = std::time::Instant::now();
    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "second")),
        ingested_at: 1_700_000_005,
    })
    .await
    .unwrap();

    // Verify the clock reset: across the entire 600 ms window after
    // push #2, chunk count must remain 1. Repeated 100 ms polls catch a
    // spurious flush from ANY age_tick that fires during the window, not
    // just one at a single sampled instant — that's the regression the
    // single-sample 300 ms assertion could have missed on a slow runner.
    // The 600 ms budget stays well below the 1 s age threshold, leaving
    // ~400 ms of slack for `tokio::time::sleep` overrun + writer ingest
    // latency before we'd risk a false-positive flush.
    while push2_at.elapsed() < Duration::from_millis(600) {
        let mid_count = walkdir(&chunks_root)
            .into_iter()
            .filter(|p| {
                p.extension()
                    .and_then(|e| e.to_str())
                    .is_some_and(|s| s == "parquet")
            })
            .count();
        assert_eq!(
            mid_count, 1,
            "second event's chunk must wait for max_buffer to elapse since its OWN push \
             (elapsed since push: {:?})",
            push2_at.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Eventually the second roll lands.
    wait_for_chunks(&chunks_root, 2, Duration::from_secs(4)).await;

    drop(tx);
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 2);
}

/// Regression: size threshold still triggers a roll before the buffer-age
/// timer would. Real wall-clock test (no virtual time) — the size limit
/// trips in microseconds; no clock advance needed.
#[tokio::test]
async fn size_threshold_still_rolls_before_buffer_age() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig {
        // Plenty of time budget; we want size to win.
        chunk_max_buffer_seconds: 3600,
        chunk_max_size_bytes: 4096,
        chunk_max_rows: usize::MAX,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // Each event's estimated_bytes ≈ id+pubkey+sig+content ≈ 256 + content.
    // ~1500-byte content per event ⇒ ~1800 bytes/event ⇒ 4096-byte
    // threshold trips by the 3rd push.
    let big = "x".repeat(1500);
    for i in 0..4 {
        tx.send(WriterMessage::Event {
            event: Box::new(make_event(0, &format!("{i}-{big}"))),
            ingested_at: 1_700_000_000 + i,
        })
        .await
        .unwrap();
    }
    drop(tx);
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();

    // At least 2 chunks: one size-driven mid-stream + the shutdown flush.
    let chunks = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|s| s == "parquet")
        })
        .count();
    assert!(
        chunks >= 2,
        "expected at least one size-driven mid-stream roll plus a shutdown flush, got {chunks}"
    );
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 4);
}

// ------------------------------------------------------------------
// Replay-flush tests
//
// On writer startup, any rows recovered from the WAL should be flushed
// to a chunk immediately rather than waiting for the buffer-age timer.
// Under the default chunk_max_buffer_seconds = 3600 the latter would
// otherwise mean a 1 h post-restart visibility lag for recovered data.
// ------------------------------------------------------------------

/// Replay path: a non-empty WAL at startup must produce a chunk without
/// any incoming events, force-flush, or shutdown signal. Holds tx open
/// while polling so the chunk landing cannot be attributed to the
/// channel-closed shutdown flush.
#[tokio::test]
async fn replay_publishes_chunk_immediately_without_force_flush() {
    let (store, wal_root, chunks_root, _dir) = rig();
    // Long buffer-age budget: if replay-flush were broken, a buffer-age
    // roll would not arrive within the test's wall-clock budget, and
    // wait_for_chunks would panic.
    let cfg = ChunkWriterConfig {
        chunk_max_buffer_seconds: 3600,
        chunk_max_rows: usize::MAX,
        chunk_max_size_bytes: usize::MAX,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    // Pre-populate the WAL — simulates a crash that left rows un-rolled.
    let wal_file = wal_path(&wal_root, key.class, key.kind, key.shard).unwrap();
    tokio::fs::create_dir_all(wal_file.parent().unwrap()).await.unwrap();
    let mut wal = Wal::open(&wal_file).await.unwrap();
    let originals: Vec<Event> = (0..5).map(|i| make_event(0, &format!("e{i}"))).collect();
    for (i, e) in originals.iter().enumerate() {
        wal.append(1_700_000_000 + i as u64, e).await.unwrap();
    }
    drop(wal);

    // No chunks yet — only the WAL has data.
    assert_eq!(
        walkdir(&chunks_root)
            .into_iter()
            .filter(|p| p.extension().and_then(|e| e.to_str()).is_some_and(|s| s == "parquet"))
            .count(),
        0
    );

    // Spawn the writer; HOLD tx so neither a ForceFlush nor a
    // channel-close shutdown flush can be blamed for the chunk landing.
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // The replay flush should land in O(WAL replay + serialize + PUT) —
    // single-digit ms in this rig. 5 s of budget is plenty even on a
    // very slow CI runner.
    wait_for_chunks(&chunks_root, 1, Duration::from_secs(5)).await;

    // Mid-test, the chunk on disk holds all 5 replayed events.
    let mid = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(mid.len(), 5, "replay chunk should contain all 5 WAL rows");
    let mid_ids: std::collections::HashSet<_> = mid.iter().map(|r| r.event.id).collect();
    for e in &originals {
        assert!(mid_ids.contains(&e.id), "missing event {} from replay chunk", e.id);
    }

    // Clean shutdown. Builder is empty post-replay; no shutdown flush
    // should produce a second chunk.
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let chunks_final = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| p.extension().and_then(|e| e.to_str()).is_some_and(|s| s == "parquet"))
        .count();
    assert_eq!(
        chunks_final, 1,
        "exactly one chunk should exist (the replay chunk); no spurious shutdown chunk"
    );
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 5);
}

/// Negative case: an empty (or missing) WAL must NOT produce a spurious
/// replay chunk. Without this guard the writer would publish empty
/// chunks on every restart and pollute the partition tree.
#[tokio::test]
async fn empty_wal_does_not_produce_replay_chunk() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig {
        chunk_max_buffer_seconds: 3600,
        chunk_max_rows: usize::MAX,
        chunk_max_size_bytes: usize::MAX,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    // Deliberately no WAL pre-population — the file does not exist yet.

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // Give the writer a generous window to (a) finish replay (no-op),
    // (b) skip the replay flush (builder is empty), (c) enter the main
    // loop. If a spurious chunk were going to land it would do so well
    // within 500 ms.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mid = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| p.extension().and_then(|e| e.to_str()).is_some_and(|s| s == "parquet"))
        .count();
    assert_eq!(mid, 0, "no chunk should be emitted when replay produced no rows");

    // Channel-close shutdown is also a no-op on an empty builder.
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let after = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| p.extension().and_then(|e| e.to_str()).is_some_and(|s| s == "parquet"))
        .count();
    assert_eq!(after, 0, "no chunk should exist after a clean shutdown with empty builder");
}

/// After a replay flush, the next push starts a fresh buffer-age clock —
/// i.e. the WAL was truncated and the new event's age begins at *its*
/// push, not at the (now-flushed) replay's notional start. This pins the
/// "replay flush clears `buffer_started_at`" contract.
#[tokio::test]
async fn buffer_clock_starts_fresh_after_replay_flush() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig {
        chunk_max_buffer_seconds: 1,
        // Jitter off: these tests assert timing against a 1 s budget, whose
        // 10% default window (100 ms) would silently eat their stated slack.
        // The jitter is exercised end to end by `buffer_age_roll_uses_the_jittered_deadline`.
        chunk_buffer_age_jitter: 0.0,
        chunk_max_rows: usize::MAX,
        chunk_max_size_bytes: usize::MAX,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    // Pre-populate WAL.
    let wal_file = wal_path(&wal_root, key.class, key.kind, key.shard).unwrap();
    tokio::fs::create_dir_all(wal_file.parent().unwrap()).await.unwrap();
    let mut wal = Wal::open(&wal_file).await.unwrap();
    let originals: Vec<Event> = (0..3).map(|i| make_event(0, &format!("e{i}"))).collect();
    for (i, e) in originals.iter().enumerate() {
        wal.append(1_700_000_000 + i as u64, e).await.unwrap();
    }
    drop(wal);

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // Wait for the replay chunk.
    wait_for_chunks(&chunks_root, 1, Duration::from_secs(4)).await;

    // Push #1 right after replay. If `buffer_started_at` had NOT been
    // cleared on the replay flush, the next age-tick would publish this
    // event almost immediately (the stale clock's elapsed() is already
    // past 1 s by the time we get here). Assert the clock is fresh by
    // checking no second chunk appears within 300 ms of the push.
    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "post-replay")),
        ingested_at: 1_700_000_100,
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let count_after_push = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| p.extension().and_then(|e| e.to_str()).is_some_and(|s| s == "parquet"))
        .count();
    assert_eq!(
        count_after_push, 1,
        "post-replay push must reset the buffer clock; second chunk should NOT have landed within 300 ms"
    );

    // Eventually the post-replay event's chunk lands once its own 1 s
    // budget elapses.
    wait_for_chunks(&chunks_root, 2, Duration::from_secs(4)).await;

    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 4);
}

// ------------------------------------------------------------------
// Failed-flush tests
//
// Inject `put_opts` failures via the `FaultyStore` wrapper, and assert
// the writer's in-memory state (builder rows, `batch_ids`,
// `buffer_started_at`) is preserved across the failure so the next size
// / rows / age trigger retries the same rows against the same dedup
// frame. Before the serialize-from-borrow reorder these properties did
// not hold — the rows were dropped from memory on any pre-PUT failure
// and recoverable only via WAL replay on next restart.
// ------------------------------------------------------------------

/// Wraps an inner `ObjectStore` and fails the first `fail_first_n`
/// `put_opts` calls with a synthetic `Generic` error. Every other method
/// delegates to the inner store.
struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    fail_first_n: AtomicUsize,
}

impl FaultyStore {
    fn new(inner: Arc<dyn ObjectStore>, fail_first_n: usize) -> Self {
        Self {
            inner,
            fail_first_n: AtomicUsize::new(fail_first_n),
        }
    }
}

impl std::fmt::Debug for FaultyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultyStore").finish()
    }
}

impl std::fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FaultyStore")
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let prev = self.fail_first_n.load(Ordering::SeqCst);
        if prev > 0 {
            self.fail_first_n.fetch_sub(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "faulty-store",
                source: "injected put failure".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjPath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjPath>,
        offset: &ObjPath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A failed PUT must NOT discard the in-memory rows. After a single
/// injected failure, the next size-driven roll retries the same rows
/// (plus any that arrived in the meantime) and the chunk on disk
/// contains every event the writer ever received.
#[tokio::test]
async fn flush_failure_preserves_builder_for_retry() {
    let dir = tempdir().unwrap();
    let chunks_root = dir.path().to_path_buf();
    let inner: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&chunks_root).unwrap());
    let store: Arc<dyn ObjectStore> = Arc::new(FaultyStore::new(inner, 1));
    let wal_root = dir.path().to_path_buf();

    // Small chunk_max_rows so every push past 5 triggers a roll attempt.
    let cfg = ChunkWriterConfig {
        chunk_max_rows: 5,
        chunk_max_size_bytes: usize::MAX,
        chunk_max_buffer_seconds: 3600,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // 5 events → first roll attempt → injected put failure → builder
    // retains all 5 rows.
    for i in 0..5 {
        tx.send(WriterMessage::Event {
            event: Box::new(make_event(0, &format!("e{i}"))),
            ingested_at: 1_700_000_000 + i,
        })
        .await
        .unwrap();
    }

    // Give the writer a moment to process and observe the failed flush.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let chunks_mid = walkdir(&chunks_root)
        .into_iter()
        .filter(|p| p.extension().and_then(|e| e.to_str()).is_some_and(|s| s == "parquet"))
        .count();
    assert_eq!(
        chunks_mid, 0,
        "first PUT was injected to fail; no chunk should be on disk yet"
    );

    // One more event triggers another size roll. The store now succeeds.
    // The retry chunk must contain ALL 6 events (the original 5 +
    // this one) — proving the original rows survived in the builder.
    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "e5-after-failure")),
        ingested_at: 1_700_000_010,
    })
    .await
    .unwrap();

    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let all = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(
        all.len(),
        6,
        "post-retry chunk(s) must contain the 5 events from the failed batch + the trigger event; got {}",
        all.len()
    );
}

/// A failed flush must NOT clear `batch_ids`. If it did, re-delivery of
/// an in-flight event from upstream (relay-pool LRU eviction, etc.)
/// could duplicate-archive that event into the same chunk.
#[tokio::test]
async fn flush_failure_does_not_clear_batch_ids() {
    let dir = tempdir().unwrap();
    let chunks_root = dir.path().to_path_buf();
    let inner: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&chunks_root).unwrap());
    let store: Arc<dyn ObjectStore> = Arc::new(FaultyStore::new(inner, 1));
    let wal_root = dir.path().to_path_buf();

    let cfg = ChunkWriterConfig {
        chunk_max_rows: 3,
        chunk_max_size_bytes: usize::MAX,
        chunk_max_buffer_seconds: 3600,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // Three distinct events trigger a roll. PUT is injected to fail.
    let e0 = make_event(0, "alpha");
    let e1 = make_event(0, "beta");
    let e2 = make_event(0, "gamma");
    for (i, e) in [&e0, &e1, &e2].into_iter().enumerate() {
        tx.send(WriterMessage::Event {
            event: Box::new(e.clone()),
            ingested_at: 1_700_000_000 + i as u64,
        })
        .await
        .unwrap();
    }

    // Wait for the writer to observe the failure.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Resend `e1` — same event id, post-failure. If `batch_ids` had been
    // erroneously cleared on the failed flush, the writer would treat it
    // as a fresh event and push it into the builder a second time,
    // producing a duplicate row in the eventual chunk. If `batch_ids`
    // survived the failure (the correct behavior), the resend is
    // deduped before it reaches the builder.
    tx.send(WriterMessage::Event {
        event: Box::new(e1.clone()),
        ingested_at: 1_700_000_010,
    })
    .await
    .unwrap();

    // Trigger the next roll (the store will succeed this time).
    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "delta")),
        ingested_at: 1_700_000_011,
    })
    .await
    .unwrap();

    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let all = reader::read_all_events(&chunks_root).unwrap();
    let ids: std::collections::HashSet<_> = all.iter().map(|r| r.event.id).collect();
    let total_rows = all.len();
    let unique_ids = ids.len();
    assert_eq!(unique_ids, 4, "expected 4 unique event ids (alpha, beta, gamma, delta)");
    assert_eq!(
        total_rows, unique_ids,
        "each event should appear exactly once on disk; got {total_rows} rows for {unique_ids} unique ids \
         — the duplicate `beta` resend must have been deduped by surviving batch_ids"
    );
}

/// A failed flush must NOT clear `buffer_started_at`. Otherwise the
/// retry on the next age tick is gated on a fresh clock and a writer
/// stuck on a small buffer past the budget would never roll until new
/// traffic arrives. With the clock preserved, the next age tick fires
/// the retry against the same buffered rows.
#[tokio::test]
async fn flush_failure_preserves_buffer_started_at() {
    let dir = tempdir().unwrap();
    let chunks_root = dir.path().to_path_buf();
    let inner: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&chunks_root).unwrap());
    let store: Arc<dyn ObjectStore> = Arc::new(FaultyStore::new(inner, 1));
    let wal_root = dir.path().to_path_buf();

    // Tight 1 s age budget, no size/rows triggers. The only way a flush
    // happens is via the buffer-age tick — first attempt fails, second
    // attempt (next tick) succeeds.
    let cfg = ChunkWriterConfig {
        chunk_max_buffer_seconds: 1,
        // Jitter off: these tests assert timing against a 1 s budget, whose
        // 10% default window (100 ms) would silently eat their stated slack.
        // The jitter is exercised end to end by `buffer_age_roll_uses_the_jittered_deadline`.
        chunk_buffer_age_jitter: 0.0,
        chunk_max_rows: usize::MAX,
        chunk_max_size_bytes: usize::MAX,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(key, "profile-metadata".into(), cfg, wal_root, store, rx, None, test_permits()));

    // One event. The buffer-age clock starts on this push.
    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "solo")),
        ingested_at: 1_700_000_000,
    })
    .await
    .unwrap();

    // ~1 s later the first age tick fires; PUT fails. ~1 s after that
    // the second tick fires; PUT succeeds. Budget 5 s of wall-clock
    // for CI slack.
    wait_for_chunks(&chunks_root, 1, Duration::from_secs(5)).await;

    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 1);
}

/// Successful flush increments `chunks_published_total` with the
/// `(class, kind, slug, shard)` labels of the writer key.
#[tokio::test]
async fn flush_increments_chunks_published_counter() {
    let (store, wal_root, _chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig::default();
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(nozzl_metrics::Metrics::new(&registry).unwrap());

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(
        key.clone(),
        "profile-metadata".into(),
        cfg,
        wal_root,
        store,
        rx,
        Some(metrics.clone()),
        test_permits(),
    ));

    for _ in 0..3 {
        tx.send(WriterMessage::Event {
            event: Box::new(make_event(0, "x")),
            ingested_at: 1_700_000_000,
        })
        .await
        .unwrap();
    }
    tx.send(WriterMessage::ForceFlush).await.unwrap();
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let count = metrics
        .chunks_published
        .with_label_values(&["replaceable", "0", "profile-metadata", "0"])
        .get();
    assert_eq!(count, 1, "expected exactly one chunk published");

    // Sibling label sets should be at zero — confirms label values come from
    // the writer (class from the key, slug from the spawn arg) rather than being
    // hard-coded.
    let wrong_class = metrics
        .chunks_published
        .with_label_values(&["addressable", "0", "profile-metadata", "0"])
        .get();
    assert_eq!(wrong_class, 0);
}

/// Addressable kinds carry the `addressable` class label.
#[tokio::test]
async fn flush_labels_addressable_class() {
    let (store, wal_root, _chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig::default();
    let key = WriterKey {
        class: EventClass::Addressable,
        kind: 30063,
        shard: PubkeyShard::from_pubkey_hex("a").unwrap(),
    };

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(nozzl_metrics::Metrics::new(&registry).unwrap());

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(
        key.clone(),
        "zapstore-app-releases".into(),
        cfg,
        wal_root,
        store,
        rx,
        Some(metrics.clone()),
        test_permits(),
    ));

    tx.send(WriterMessage::Event {
        event: Box::new(make_addressable("myapp@1.0")),
        ingested_at: 1_700_000_000,
    })
    .await
    .unwrap();
    tx.send(WriterMessage::ForceFlush).await.unwrap();
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let count = metrics
        .chunks_published
        .with_label_values(&["addressable", "30063", "zapstore-app-releases", "a"])
        .get();
    assert_eq!(count, 1);
}

/// Successful flush sets `chunks_last_published_timestamp_seconds` to
/// roughly `now()` for the same `(class, kind, slug, shard)` labels as the
/// counter. The gauge is what lets a dashboard show a stalled writer - see
/// the rationale in `nozzl_metrics::Metrics::chunks_last_published_timestamp_seconds`.
#[tokio::test]
async fn flush_sets_last_published_timestamp_gauge() {
    let (store, wal_root, _chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig::default();
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(nozzl_metrics::Metrics::new(&registry).unwrap());
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(
        key.clone(),
        "profile-metadata".into(),
        cfg,
        wal_root,
        store,
        rx,
        Some(metrics.clone()),
        test_permits(),
    ));

    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "anything")),
        ingested_at: 1_700_000_000,
    })
    .await
    .unwrap();
    tx.send(WriterMessage::ForceFlush).await.unwrap();
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let ts = metrics
        .chunks_last_published_timestamp_seconds
        .with_label_values(&["replaceable", "0", "profile-metadata", "0"])
        .get();
    assert!(
        ts >= before && ts <= after,
        "gauge timestamp {ts} not within bounds [{before}, {after}]"
    );

    // A sibling label set was never written → gauge value is 0.0 there,
    // confirming the labels come from the writer (class from the key, slug from
    // the spawn arg) rather than being hard-coded.
    let other = metrics
        .chunks_last_published_timestamp_seconds
        .with_label_values(&["addressable", "0", "profile-metadata", "0"])
        .get();
    // Untouched gauge sits at exactly 0.0 (default), so exact compare is fine.
    #[allow(clippy::float_cmp)]
    {
        assert_eq!(other, 0.0);
    }
}

fn walkdir_recurse(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walkdir_recurse(&p, out);
            } else {
                out.push(p);
            }
        }
    }
}

fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    walkdir_recurse(root, &mut out);
    out
}

// ------------------------------------------------------------------
// AdhocFlush (operator-triggered, confirmed) message semantics.
// ------------------------------------------------------------------

fn k0_key() -> WriterKey {
    WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    }
}

/// A non-empty buffer flushes, publishes a chunk, and acks `published=true`
/// with the row count — and the per-chunk path actually lands on disk.
#[tokio::test]
async fn adhoc_flush_publishes_and_acks_outcome() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(
        k0_key(),
        "profile-metadata".into(),
        ChunkWriterConfig::default(),
        wal_root,
        store,
        rx,
        None,
        test_permits(),
    ));

    for _ in 0..5 {
        tx.send(WriterMessage::Event {
            event: Box::new(make_event(0, "hi")),
            ingested_at: 1_700_000_000,
        })
        .await
        .unwrap();
    }
    let (ack_tx, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::AdhocFlush { ack: ack_tx })
        .await
        .unwrap();
    let outcome = timeout(Duration::from_secs(5), ack_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(outcome.published);
    assert_eq!(outcome.rows, 5);
    assert!(outcome.error.is_none());

    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 5);
}

/// An empty buffer is a no-op: ack reports `published=false`, `rows=0`, and no
/// chunk (not even an empty one) is written.
#[tokio::test]
async fn adhoc_flush_on_empty_buffer_is_noop() {
    let (store, wal_root, chunks_root, _dir) = rig();
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(
        k0_key(),
        "profile-metadata".into(),
        ChunkWriterConfig::default(),
        wal_root,
        store,
        rx,
        None,
        test_permits(),
    ));

    let (ack_tx, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::AdhocFlush { ack: ack_tx })
        .await
        .unwrap();
    let outcome = timeout(Duration::from_secs(5), ack_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(!outcome.published);
    assert_eq!(outcome.rows, 0);
    assert!(outcome.error.is_none());

    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 0);
}

/// A failed PUT reports the error on the ack and leaves rows durable: a second
/// ad-hoc flush (store now healthy) publishes the same rows. Proves an ad-hoc
/// flush failure never loses buffered data.
#[tokio::test]
async fn adhoc_flush_failure_reports_error_then_recovers() {
    let dir = tempdir().unwrap();
    let chunks_root = dir.path().to_path_buf();
    let inner: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&chunks_root).unwrap());
    let store: Arc<dyn ObjectStore> = Arc::new(FaultyStore::new(inner, 1));
    let wal_root = dir.path().to_path_buf();
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(
        k0_key(),
        "profile-metadata".into(),
        ChunkWriterConfig::default(),
        wal_root,
        store,
        rx,
        None,
        test_permits(),
    ));

    for _ in 0..3 {
        tx.send(WriterMessage::Event {
            event: Box::new(make_event(0, "x")),
            ingested_at: 1_700_000_000,
        })
        .await
        .unwrap();
    }

    // First ad-hoc flush hits the injected PUT failure.
    let (a1, r1) = oneshot::channel();
    tx.send(WriterMessage::AdhocFlush { ack: a1 }).await.unwrap();
    let o1 = timeout(Duration::from_secs(5), r1).await.unwrap().unwrap();
    assert!(!o1.published);
    assert!(o1.error.is_some(), "first flush should report the injected failure");

    // Rows were preserved; a second ad-hoc flush (store now healthy) publishes them.
    let (a2, r2) = oneshot::channel();
    tx.send(WriterMessage::AdhocFlush { ack: a2 }).await.unwrap();
    let o2 = timeout(Duration::from_secs(5), r2).await.unwrap().unwrap();
    assert!(o2.published);
    assert_eq!(o2.rows, 3);

    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 3);
}

// ---------------------------------------------------------------------------
// Buffer-age jitter: #57 (Restart-synchronized buffer-age flush burst). The
// herd these tests describe is not
// hypothetical: a restart sets every writer's age clock within the same second,
// so before jitter 443 writers rolled inside one second on dev and 178 of their
// PUTs failed. The properties below are what keeps that from recurring.
// ---------------------------------------------------------------------------

/// The whole configured fleet, as writer keys: every archived class x kind x
/// all 16 shards, sized like a busy deployment (~1,000 writers). Shared by the
/// spread tests so they measure the real population rather than a toy one.
fn fleet(kinds: u16) -> Vec<WriterKey> {
    let mut out = Vec::new();
    for kind in 0..kinds {
        let class = match kind % 3 {
            0 => EventClass::Regular,
            1 => EventClass::Replaceable,
            _ => EventClass::Addressable,
        };
        for shard_n in 0u8..16 {
            let shard = PubkeyShard::from_pubkey_hex(&format!("{shard_n:x}")).unwrap();
            out.push(WriterKey { class, kind, shard });
        }
    }
    out
}

/// `jitter = 0.0` must give back exactly `chunk_max_buffer_seconds`: the
/// behaviour before #57 (Restart-synchronized buffer-age flush burst), so an
/// operator can turn the feature off and get the old semantics to the
/// millisecond.
#[test]
fn jitter_zero_yields_the_full_budget() {
    for key in fleet(8) {
        assert_eq!(
            jittered_buffer_budget(&key, 43_200, 0.0),
            Duration::from_secs(43_200),
            "jitter 0.0 must not move the deadline for {key:?}"
        );
    }
}

/// Deterministic: the same writer gets the same deadline on every boot. This is
/// the property that keeps a later restart from re-rolling the fleet into a new
/// herd, and it is why the hash is hand-rolled rather than `DefaultHasher`
/// (whose output `std` is free to change between releases).
#[test]
fn jittered_budget_is_deterministic_per_writer() {
    for key in fleet(8) {
        let first = jittered_buffer_budget(&key, 43_200, 0.10);
        for _ in 0..4 {
            assert_eq!(
                jittered_buffer_budget(&key, 43_200, 0.10),
                first,
                "deadline for {key:?} is not stable across calls"
            );
        }
    }
}

/// The deadline only ever moves *earlier*, and never by more than the jitter
/// fraction. `chunk_max_buffer_seconds` stays an upper bound on the age of
/// unflushed rows, which is what the config promises and what bounds WAL replay
/// cost.
#[test]
fn jittered_budget_never_exceeds_the_configured_budget() {
    let budget = Duration::from_secs(43_200);
    let floor = Duration::from_secs_f64(43_200.0 * 0.90);
    for key in fleet(66) {
        let d = jittered_buffer_budget(&key, 43_200, 0.10);
        assert!(
            d <= budget && d >= floor,
            "deadline {d:?} for {key:?} outside [{floor:?}, {budget:?}]"
        );
    }
}

/// The point of the exercise: the fleet's deadlines must land on many different
/// seconds, so the encode-and-PUT work arrives spread out instead of as a herd.
///
/// Asserts the two numbers that matter operationally, against the measured
/// 2026-10-15 burst: the *peak* per second (443 flushes in one second then; at
/// most a handful now) and the share of the window actually used. The bound of
/// 8 is ~5x the expected Poisson maximum for ~1,000 writers over 4,320 one-second
/// buckets, so it is a real regression guard rather than a tautology - a hash
/// that collapsed to a few distinct values would blow straight through it.
///
/// What it catches and what it does not, measured rather than assumed: dropping
/// the shard byte from `writer_key_hash` takes the peak to 16 and fails this,
/// on both this synthetic fleet and a real deployment's key space. Dropping the kind's
/// HIGH byte does **not** fail it - the peak stays at 3 - because these
/// synthetic kinds are all under 256 and the real configured set has only a
/// handful of low-byte collisions. So do not read a pass here as proof that
/// every byte of the key reaches the hash; the mixing of the high byte matters
/// only once kinds above 255 share a low byte, which
/// `jittered_budget_is_deterministic_per_writer` pins by value instead.
#[test]
fn jittered_budget_spreads_the_fleet_across_the_window() {
    let keys = fleet(66);
    let mut per_second: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for key in &keys {
        let d = jittered_buffer_budget(key, 43_200, 0.10);
        *per_second.entry(d.as_secs()).or_default() += 1;
    }
    let peak = *per_second.values().max().unwrap();
    assert!(
        peak <= 8,
        "jitter left {peak} of {} writers sharing one second; before \
         #57 (Restart-synchronized buffer-age flush burst) the number was 443",
        keys.len()
    );
    // 4,320 one-second buckets available; ~1,000 writers can occupy at most
    // that many, and a healthy hash should use a large fraction of them.
    assert!(
        per_second.len() >= keys.len() / 2,
        "only {} distinct deadlines for {} writers - the offsets are clustering",
        per_second.len(),
        keys.len()
    );
}

/// Out-of-range and nonsense jitter values are clamped rather than propagated.
/// `0.5` is the cap because beyond it "age ~ `chunk_max_buffer_seconds`" stops
/// being a useful description of the knob.
#[test]
fn jitter_fraction_is_clamped() {
    let key = fleet(1)[0].clone();
    let half = jittered_buffer_budget(&key, 43_200, 0.5);
    assert_eq!(
        jittered_buffer_budget(&key, 43_200, 5.0),
        half,
        "a jitter above 0.5 must behave as 0.5"
    );
    let full = Duration::from_secs(43_200);
    assert_eq!(
        jittered_buffer_budget(&key, 43_200, -1.0),
        full,
        "a negative jitter must behave as 0.0"
    );
    assert_eq!(
        jittered_buffer_budget(&key, 43_200, f64::NAN),
        full,
        "a non-finite jitter must behave as 0.0"
    );
    assert_eq!(
        jittered_buffer_budget(&key, 43_200, f64::INFINITY),
        full,
        "a non-finite jitter must behave as 0.0"
    );
}

/// A zero or sub-millisecond window degrades to the plain budget instead of
/// dividing by zero. `seconds = 0` is also pinned to 1 s here, matching the
/// `max(1)` the writer loop has always applied.
#[test]
fn degenerate_budgets_do_not_panic() {
    let key = fleet(1)[0].clone();
    // `seconds = 0` is pinned to 1 s (the `max(1)` the writer loop has always
    // applied), and the jitter then applies to that 1 s as to any other budget.
    let pinned = jittered_buffer_budget(&key, 0, 0.10);
    assert!(
        pinned <= Duration::from_secs(1) && pinned >= Duration::from_millis(900),
        "a zero budget must be pinned to 1 s and jittered within it, got {pinned:?}"
    );
    assert_eq!(
        jittered_buffer_budget(&key, 0, 0.0),
        Duration::from_secs(1),
        "with jitter off, a zero budget is exactly the 1 s floor"
    );
    // 1 s budget x 0.001 = 1 ms window, the smallest that still jitters.
    let tiny = jittered_buffer_budget(&key, 1, 0.001);
    assert!(tiny <= Duration::from_secs(1) && tiny >= Duration::from_millis(999));
    // 1 s budget x 0.0001 rounds to a 0 ms window: no jitter at all.
    assert_eq!(
        jittered_buffer_budget(&key, 1, 0.0001),
        Duration::from_secs(1)
    );
}

/// The `trigger` label values the writer emits must all exist in the list
/// `nozzl-metrics` preallocates, or a flush failure would increment a series no
/// alert is watching. The crates cannot share the enum (the dependency runs one
/// way), so this is the drift guard.
#[test]
fn roll_reason_labels_are_all_preallocated_triggers() {
    let reasons = [
        RollReason::Size,
        RollReason::Rows,
        RollReason::BufferAge,
        RollReason::Force,
        RollReason::Adhoc,
        RollReason::Shutdown,
        RollReason::Replay,
    ];

    // Exhaustiveness, enforced by the compiler rather than by this array
    // staying current. An eighth `RollReason` makes this match fail to build,
    // so whoever adds one lands in this test instead of shipping a trigger
    // that `preallocate_known_series` never emits. Without it the array could
    // simply go stale and the length assertion below would still hold at
    // 7 == 7.
    for r in reasons {
        match r {
            RollReason::Size
            | RollReason::Rows
            | RollReason::BufferAge
            | RollReason::Force
            | RollReason::Adhoc
            | RollReason::Shutdown
            | RollReason::Replay => {}
        }
        assert!(
            nozzl_metrics::CHUNK_FLUSH_TRIGGERS.contains(&r.as_str()),
            "RollReason::{r:?} labels flushes {:?}, which nozzl-metrics does not preallocate",
            r.as_str()
        );
    }

    // And the other direction: a preallocated label with no `RollReason` that
    // can produce it is a series that reads 0 forever, which is worse than
    // absent because it looks healthy.
    for trigger in nozzl_metrics::CHUNK_FLUSH_TRIGGERS {
        assert!(
            reasons.iter().any(|r| r.as_str() == trigger),
            "nozzl-metrics preallocates trigger {trigger:?}, which no RollReason produces"
        );
    }

    assert_eq!(
        reasons.len(),
        nozzl_metrics::CHUNK_FLUSH_TRIGGERS.len(),
        "a trigger was added or removed on one side only"
    );
}

/// A failed flush increments `chunk_flush_failures_total` under the trigger that
/// rolled the chunk, and a later successful retry does not decrement or
/// relabel it. Before this counter existed, the 178-failure burst of
/// 2026-10-15 was invisible to every alert.
#[tokio::test]
async fn failed_flush_increments_the_failure_counter_with_its_trigger() {
    let dir = tempdir().unwrap();
    let chunks_root = dir.path().to_path_buf();
    let inner: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&chunks_root).unwrap());
    // Fail the first PUT only: the first (rows-triggered) flush fails, the
    // retry on the next roll succeeds.
    let store: Arc<dyn ObjectStore> = Arc::new(FaultyStore::new(inner, 1));
    let cfg = ChunkWriterConfig {
        chunk_max_rows: 5,
        chunk_max_size_bytes: usize::MAX,
        chunk_max_buffer_seconds: 3600,
        ..ChunkWriterConfig::default()
    };
    let key = WriterKey {
        class: EventClass::Replaceable,
        kind: 0,
        shard: PubkeyShard::from_pubkey_hex("0").unwrap(),
    };
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(nozzl_metrics::Metrics::new(&registry).unwrap());
    let failures = |trigger: &str| {
        metrics
            .chunk_flush_failures
            .with_label_values(&["replaceable", "0", "profile-metadata", trigger])
            .get()
    };

    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(run_writer(
        key.clone(),
        "profile-metadata".into(),
        cfg,
        dir.path().to_path_buf(),
        store,
        rx,
        Some(metrics.clone()),
        test_permits(),
    ));
    for i in 0..5 {
        tx.send(WriterMessage::Event {
            event: Box::new(make_event(0, &format!("e{i}"))),
            ingested_at: 1_700_000_000 + i,
        })
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        failures("rows"),
        1,
        "the injected rows-triggered PUT failure must be counted"
    );

    // The retry lands on a force flush, which must succeed and must not touch
    // the `rows` series.
    tx.send(WriterMessage::ForceFlush).await.unwrap();
    drop(tx);
    timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
    assert_eq!(failures("rows"), 1, "a successful retry must not re-count");
    assert_eq!(failures("force"), 0, "the retry succeeded; nothing to count");
    assert_eq!(
        metrics
            .chunks_published
            .with_label_values(&["replaceable", "0", "profile-metadata", "0"])
            .get(),
        1,
        "the retry must have published exactly one chunk"
    );
    // No rows lost despite the failure: all 5 are in the archive.
    assert_eq!(reader::read_all_events(&chunks_root).unwrap().len(), 5);
}

/// The jitter has to be WIRED IN, not merely correct on its own.
///
/// Every other test in the jitter section calls `jittered_buffer_budget`
/// directly, and every buffer-age roll test uses `chunk_max_buffer_seconds: 1`,
/// whose 100 ms jitter window the 1 s age tick swallows. So replacing
/// `run_writer`'s deadline with a plain
/// `Duration::from_secs(cfg.chunk_max_buffer_seconds)` passes all of them: the
/// pure function stays right while the writer stops using it. This test is the
/// one that fails, and it is the reason the knob cannot silently become inert.
///
/// It runs a 10 s budget at jitter 0.5 (a 5 s window) on the writer the hash
/// jitters hardest, whose deadline is therefore near 5 s against an unjittered
/// 10 s, and asserts the chunk lands inside 8 s. Both margins are about 2 s
/// wide on purpose: a tight timing bound on a loaded CI runner is exactly how
/// the acquirer's flaky tests happened (see
/// `docs/nostr-relay-pool-limitations.md` entry #11), and the only thing this
/// test needs to distinguish is 5 s from 10 s.
#[tokio::test]
async fn buffer_age_roll_uses_the_jittered_deadline() {
    const BUDGET_SECS: u64 = 10;
    const JITTER: f64 = 0.5;

    // The most-jittered of kind-0 (profile-metadata)'s 16 shards. Deterministic,
    // so this picks the same writer on every run and every rebuild.
    let (key, deadline) = (0u8..16)
        .map(|shard_n| {
            let key = WriterKey {
                class: EventClass::Replaceable,
                kind: 0,
                shard: PubkeyShard::from_pubkey_hex(&format!("{shard_n:x}")).unwrap(),
            };
            let d = jittered_buffer_budget(&key, BUDGET_SECS, JITTER);
            (key, d)
        })
        .min_by_key(|(_, d)| *d)
        .unwrap();
    assert!(
        deadline <= Duration::from_secs(6),
        "premise failed: the most-jittered shard's deadline is {deadline:?}, \
         too close to the unjittered {BUDGET_SECS} s for this test to discriminate"
    );

    let (store, wal_root, chunks_root, _dir) = rig();
    let cfg = ChunkWriterConfig {
        chunk_max_buffer_seconds: BUDGET_SECS,
        chunk_buffer_age_jitter: JITTER,
        chunk_max_rows: usize::MAX,
        chunk_max_size_bytes: usize::MAX,
        ..ChunkWriterConfig::default()
    };

    let (tx, rx) = mpsc::channel(8);
    let task = tokio::spawn(run_writer(
        key,
        "profile-metadata".into(),
        cfg,
        wal_root,
        store,
        rx,
        None,
        test_permits(),
    ));

    // One event, then hold the channel open: the ONLY thing that can publish
    // this chunk is the buffer-age deadline. Dropping `tx` would flush it as a
    // shutdown roll and prove nothing.
    tx.send(WriterMessage::Event {
        event: Box::new(make_event(0, "jitter-wiring")),
        ingested_at: 1_700_000_000,
    })
    .await
    .unwrap();

    // Lower bound first. The deadline is ~5.2 s and the age tick is 1 s, so
    // nothing can publish before 5 s however loaded the runner is - a late
    // tick only delays a roll. This half fails if the jitter is applied too
    // aggressively (added, say, or scaled wrong).
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        count_chunks(&chunks_root),
        0,
        "a chunk landed before the jittered deadline {deadline:?}; the offset is \
         larger than it should be"
    );

    // Upper bound. 8 s is comfortably past the jittered deadline and
    // comfortably short of the unjittered one, so this half fails if the
    // jitter is not applied at all.
    let published = wait_for_chunks(&chunks_root, 1, Duration::from_secs(3)).await;
    assert_eq!(
        published, 1,
        "no chunk by 8 s: the writer's deadline is the unjittered \
         {BUDGET_SECS} s, not the jittered {deadline:?}"
    );

    drop(tx);
    timeout(Duration::from_secs(10), task)
        .await
        .expect("writer did not exit")
        .expect("writer task panicked");
}
