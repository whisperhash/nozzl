//! Integration tests for the chunked archive.
//!
//! Verifies the writer task model end-to-end: events arrive via
//! `Archive::put_event`, get demuxed to per-(class, kind, shard) writer tasks,
//! buffered into Arrow batches, and serialized to Parquet chunks under the
//! `replaceable/`, `addressable/`, and `regular/` roots. Each test reads chunks
//! back via `nozzl_archive::reader` to verify round-trip fidelity.

use std::collections::HashSet;
use std::sync::Arc;

use nostr::{Event, EventBuilder, Keys, Kind, Tag, Timestamp};
use nozzl_archive::demux::ChunkedArchive;
use nozzl_archive::{
    Archive, ChunkWriterConfig, LocalConfig, PutOutcome, StorageBackend, StorageConfig,
    build_archive, build_object_store, reader,
};
use object_store::ObjectStore;
use tempfile::TempDir;

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

fn signed_event(keys: &Keys, kind: u16, tags: Vec<Tag>, ts: u64, content: &str) -> Event {
    EventBuilder::new(Kind::from_u16(kind), content)
        .tags(tags)
        .custom_created_at(Timestamp::from_secs(ts))
        .sign_with_keys(keys)
        .unwrap()
}

fn random_event(kind: u16, ts: u64, content: &str) -> Event {
    signed_event(&Keys::generate(), kind, vec![], ts, content)
}

fn random_addressable(d: &str, ts: u64) -> Event {
    signed_event(
        &Keys::generate(),
        30063,
        vec![Tag::identifier(d)],
        ts,
        "release",
    )
}

fn rig() -> (Arc<ChunkedArchive>, std::path::PathBuf, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StorageConfig {
        backend: StorageBackend::Local,
        local: Some(LocalConfig {
            root: dir.path().to_path_buf(),
        }),
        s3: None,
    };
    let archive = build_archive(
        &cfg,
        dir.path().to_path_buf(),
        ChunkWriterConfig::default(),
        None,
    )
    .unwrap();
    let chunks_root = dir.path().to_path_buf();
    (archive, chunks_root, dir)
}

// -----------------------------------------------------------------------------
// tests
// -----------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_events_round_trip_through_chunks() {
    let (archive, chunks_root, _dir) = rig();

    // Use a small fixed set of pubkeys to ensure many events land in the
    // same shards and produce deterministic-ish chunk structure.
    let keys_pool: Vec<Keys> = (0..4).map(|_| Keys::generate()).collect();

    let n = 1_000usize;
    let mut sent_ids: HashSet<_> = HashSet::with_capacity(n);

    for i in 0..n {
        let keys = &keys_pool[i % keys_pool.len()];
        // Rotate across all three archived classes: addressable (30063),
        // regular (9735 zap receipts), replaceable (0).
        let kind = match i % 5 {
            0 => 30063,
            1 => 9735,
            _ => 0,
        };
        let tags = if kind == 30063 {
            vec![Tag::identifier(format!("app-{}", i % 10))]
        } else {
            vec![]
        };
        // Spread timestamps over two distinct UTC days so we exercise the
        // dt-partitioning even though the ingested_at is what actually drives
        // chunk partitioning. The day boundary is at 1700006400 (one day
        // after 1700000000).
        let ts = if i % 2 == 0 {
            1_700_000_000 + i as u64
        } else {
            1_700_086_400 + i as u64
        };
        let e = signed_event(keys, kind, tags, ts, &format!("e{i}"));
        sent_ids.insert(e.id);
        let slug = match kind {
            0 => "profile-metadata",
            9735 => "zap-receipts",
            _ => "zapstore-app-releases",
        };
        let outcome = archive.put_event(&e, slug).await.unwrap();
        assert!(matches!(outcome, PutOutcome::Written));
    }

    archive.force_flush_all().await;
    archive.shutdown().await;

    let readback = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(readback.len(), n);

    let recovered_ids: HashSet<_> = readback.iter().map(|r| r.event.id).collect();
    assert_eq!(recovered_ids, sent_ids);

    // Chunks live under the Hive class dirs c=replaceable/, c=addressable/, c=regular/.
    let mut top_level_dirs: HashSet<String> = HashSet::new();
    for entry in std::fs::read_dir(&chunks_root).unwrap().flatten() {
        if entry.file_type().unwrap().is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                top_level_dirs.insert(name.to_string());
            }
        }
    }
    assert!(top_level_dirs.contains("c=replaceable"));
    assert!(top_level_dirs.contains("c=addressable"));
    assert!(top_level_dirs.contains("c=regular"));
    assert!(top_level_dirs.contains("_wal"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replaceable_version_history_is_preserved() {
    let (archive, chunks_root, _dir) = rig();
    // Same author + kind, multiple `created_at`s — under the chunked archive
    // every version is preserved at write time. Read-side dedup logic
    // (QUALIFY row_number...) is what selects the latest at query time.
    let keys = Keys::generate();
    let pubkey = keys.public_key();
    let mut version_ids = Vec::new();
    for i in 0..5 {
        let e = signed_event(&keys, 0, vec![], 1_700_000_000 + i * 3600, &format!("v{i}"));
        version_ids.push(e.id);
        archive.put_event(&e, "profile-metadata").await.unwrap();
    }
    archive.force_flush_all().await;
    archive.shutdown().await;

    let readback = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(readback.len(), 5, "all 5 versions must be preserved");
    let recovered: HashSet<_> = readback.iter().map(|r| r.event.id).collect();
    for id in &version_ids {
        assert!(recovered.contains(id));
    }
    // All versions share the same pubkey.
    let pubkey_hex = pubkey.to_hex();
    for r in &readback {
        assert_eq!(r.event.pubkey.to_hex(), pubkey_hex);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn addressable_d_tag_is_projected() {
    let (archive, chunks_root, _dir) = rig();

    let e1 = random_addressable("myapp@1.0.0", 1_700_000_000);
    let e2 = random_addressable("myapp@1.1.0", 1_700_000_001);
    let e3 = random_event(0, 1_700_000_002, "non-addressable kind 0");

    archive
        .put_event(&e1, "zapstore-app-releases")
        .await
        .unwrap();
    archive
        .put_event(&e2, "zapstore-app-releases")
        .await
        .unwrap();
    archive.put_event(&e3, "profile-metadata").await.unwrap();

    archive.force_flush_all().await;
    archive.shutdown().await;

    let readback = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(readback.len(), 3);

    let by_id: std::collections::HashMap<_, _> =
        readback.iter().map(|r| (r.event.id, r.clone())).collect();
    assert_eq!(by_id[&e1.id].d_tag.as_deref(), Some("myapp@1.0.0"));
    assert_eq!(by_id[&e2.id].d_tag.as_deref(), Some("myapp@1.1.0"));
    assert!(by_id[&e3.id].d_tag.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_event_archives_regular_kinds() {
    // Regular kinds (e.g. kind-9735 zap receipts) are now archived under the
    // c=regular/ tree.
    let (archive, chunks_root, _dir) = rig();
    let e = random_event(9735, 1_700_000_000, "zap receipt");
    let outcome = archive.put_event(&e, "zap-receipts").await.unwrap();
    assert!(matches!(outcome, PutOutcome::Written));
    archive.force_flush_all().await;
    archive.shutdown().await;

    let readback = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(readback.len(), 1);
    assert_eq!(readback[0].event.id, e.id);
    assert!(readback[0].d_tag.is_none());
    assert!(chunks_root.join("c=regular").is_dir());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_event_skips_ephemeral_kinds() {
    let (archive, _chunks_root, _dir) = rig();
    let e = random_event(22500, 1_700_000_000, "ephemeral");
    let outcome = archive.put_event(&e, "ephemeral").await.unwrap();
    assert!(matches!(outcome, PutOutcome::Skipped { .. }));
    archive.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_events_within_a_batch_are_deduped() {
    let (archive, chunks_root, _dir) = rig();
    let e = random_event(0, 1_700_000_000, "dedup-me");
    for _ in 0..3 {
        archive.put_event(&e, "profile-metadata").await.unwrap();
    }
    archive.force_flush_all().await;
    archive.shutdown().await;

    let readback = reader::read_all_events(&chunks_root).unwrap();
    assert_eq!(
        readback.len(),
        1,
        "writer-task in-batch dedup must collapse repeats"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_object_store_local_creates_filesystem_backend() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StorageConfig {
        backend: StorageBackend::Local,
        local: Some(LocalConfig {
            root: dir.path().to_path_buf(),
        }),
        s3: None,
    };
    let store = build_object_store(&cfg).unwrap();
    // Smoke test the underlying store: a put + list round-trip.
    let path = object_store::path::Path::from("smoke/test");
    let payload = object_store::PutPayload::from(b"hello".to_vec());
    store
        .put_opts(
            &path,
            payload,
            object_store::PutOptions::from(object_store::PutMode::Create),
        )
        .await
        .unwrap();
    // The file should exist on disk under dir.path().
    assert!(dir.path().join("smoke/test").exists());
}

// Compile-time check that ChunkWriterConfig is constructible with all-defaults.
fn _compile_check_chunk_writer_config() {
    let _: ChunkWriterConfig = ChunkWriterConfig::default();
}
