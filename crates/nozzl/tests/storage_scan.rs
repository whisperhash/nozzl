//! End-to-end: drive `nozzl::run` against a `nozzl-test-relay`, wait for an
//! event to roll into a Parquet chunk, then scrape `/metrics` and verify the
//! `storage_resident_bytes` / `storage_resident_files` gauges are emitted for
//! the archived kind with plausible (>0) values and the right labels.
//!
//! Validates the full producer chain wired in `spawn_storage_scan_loop`:
//! `ChunkedArchive::scan_resident_usage()` → gauge `set()` → HTTP `/metrics`.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{NozzlHandle, pick_free_metrics_addr, write_config_full};
use nostr::{EventBuilder, Keys, Kind};
use nozzl_test_relay::TestRelay;
use tempfile::TempDir;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread")]
async fn storage_resident_bytes_gauge_is_emitted_after_chunk_lands() {
    let keys = Keys::generate();
    let created_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    // Kind 0 (profile-metadata) is replaceable → archived.
    let event = EventBuilder::new(Kind::from_u16(0), "hello")
        .custom_created_at(nostr::Timestamp::from_secs(created_at_unix))
        .sign_with_keys(&keys)
        .unwrap();

    let relay = TestRelay::builder().preload_events(vec![event]).start().await;

    let metrics_addr = pick_free_metrics_addr();
    let cfg_dir = TempDir::new().unwrap();
    let archive_dir = TempDir::new().unwrap();
    let cfg_path = write_config_full(
        &cfg_dir,
        archive_dir.path(),
        &[relay.url().as_str()],
        &[0],
        None,
        &metrics_addr.to_string(),
    );

    let nozzl = NozzlHandle::start(cfg_path, archive_dir.path().to_path_buf());

    // Chain: event archived → buffer-age roll (chunk_max_buffer_seconds = 2 in
    // the test config) publishes a chunk → the 1 s storage scan picks it up and
    // sets the gauge. Generous slack for the relay handshake + roll + scan on
    // slow CI.
    let metrics_url = format!("http://{metrics_addr}/metrics");
    let body = timeout(Duration::from_secs(20), async {
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let Ok(resp) = reqwest::get(&metrics_url).await else {
                continue;
            };
            let Ok(text) = resp.text().await else {
                continue;
            };
            if text.contains("storage_resident_bytes{") {
                return text;
            }
        }
    })
    .await
    .expect("storage_resident_bytes series should appear within 20s");

    // Format: `storage_resident_bytes{class="replaceable",kind="0",slug="k0-test"} <number>`
    let line = body
        .lines()
        .find(|l| l.starts_with("storage_resident_bytes{"))
        .unwrap_or_else(|| panic!("expected storage line; body:\n{body}"));

    // Correct labels: replaceable class, kind 0, the configured slug.
    assert!(
        line.contains(r#"class="replaceable""#),
        "missing/incorrect class label: `{line}`"
    );
    assert!(line.contains(r#"kind="0""#), "missing kind label: `{line}`");
    assert!(
        line.contains(r#"slug="k0-test""#),
        "missing/incorrect slug label: `{line}`"
    );

    // Bytes must be a real, positive count (a published Parquet chunk exists).
    let bytes: f64 = line
        .rsplit(' ')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("could not parse bytes from line `{line}`"));
    assert!(
        bytes > 0.0,
        "expected resident bytes > 0 once a chunk has landed; line: `{line}`"
    );

    // The companion file-count gauge is set from the same scan: with exactly
    // one published chunk it must report >= 1 resident `.parquet` for the kind,
    // carrying the same labels.
    let files_line = body
        .lines()
        .find(|l| l.starts_with("storage_resident_files{"))
        .unwrap_or_else(|| panic!("expected storage_resident_files line; body:\n{body}"));
    assert!(
        files_line.contains(r#"class="replaceable""#)
            && files_line.contains(r#"kind="0""#)
            && files_line.contains(r#"slug="k0-test""#),
        "missing/incorrect labels on file-count gauge: `{files_line}`"
    );
    let files: f64 = files_line
        .rsplit(' ')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("could not parse file count from line `{files_line}`"));
    assert!(
        files >= 1.0,
        "expected resident files >= 1 once a chunk has landed; line: `{files_line}`"
    );

    nozzl.kill();
}
