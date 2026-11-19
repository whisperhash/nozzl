//! End-to-end: drive `nozzl::run` with the admin endpoint enabled, then
//! `POST /admin/flush/{kind}` and verify the buffered events become queryable
//! parquet on disk — the operator's "fresh data on demand" workflow.
//!
//! The config pins `chunk_max_buffer_seconds` and `chunk_max_rows` high enough
//! that neither the buffer-age timer nor the size/rows thresholds can publish
//! during the test window. So the ONLY thing that turns buffered events into a
//! chunk (short of shutdown) is the ad-hoc flush — making the assertion that
//! the endpoint published them deterministic rather than racing a timer.

mod common;

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use common::{NozzlHandle, count_event_files, pick_free_metrics_addr, write_kinds, write_relays};
use nostr::{EventBuilder, Keys, Kind};
use nozzl_test_relay::TestRelay;
use tempfile::TempDir;
use tokio::time::timeout;

/// Write a `nozzl.toml` with the admin endpoint enabled and chunk-roll
/// thresholds set high so only an ad-hoc flush (or shutdown) publishes.
fn write_admin_config(
    cfg_dir: &TempDir,
    archive_root: &Path,
    relay_urls: &[&str],
    kinds: &[u16],
    metrics_addr: SocketAddr,
    admin_addr: SocketAddr,
) -> PathBuf {
    let nozzl_path = cfg_dir.path().join("nozzl.toml");
    let archive_root_disp = archive_root.display();
    let nozzl_toml = format!(
        r#"
[storage]
backend = "local"
root = "{archive_root_disp}"

[acquirer]
backfill_safety_margin_seconds = 60
backfill_default_limit = 500
backfill_empty_pages_to_complete = 8

[checkpoint]
path = "{archive_root_disp}/_state/checkpoints.json"
flush_interval_seconds = 1

[runtime]
channel_capacity = 256
# High thresholds: neither size/rows nor the buffer-age timer fire during the
# test, so the ad-hoc flush is the only publisher (deterministic assertion).
chunk_max_size_bytes = 67108864
chunk_max_rows = 100000
chunk_max_buffer_seconds = 3600

[observability]
metrics_addr = "{metrics_addr}"
admin_addr = "{admin_addr}"
log_format = "pretty"
log_level = "info"

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#
    );
    fs::write(&nozzl_path, nozzl_toml).unwrap();
    write_relays(cfg_dir, relay_urls);
    write_kinds(cfg_dir, kinds);
    fs::create_dir_all(archive_root).unwrap();
    nozzl_path
}

#[tokio::test(flavor = "multi_thread")]
async fn adhoc_flush_publishes_buffered_kind_on_demand() {
    // Four distinct authors' kind-0 (replaceable) events → four distinct ids.
    let events: Vec<_> = (0..4)
        .map(|i| {
            EventBuilder::new(Kind::from_u16(0), format!("profile {i}"))
                .custom_created_at(nostr::Timestamp::from_secs(1_700_000_000 + i))
                .sign_with_keys(&Keys::generate())
                .unwrap()
        })
        .collect();

    let relay = TestRelay::builder().preload_events(events).start().await;

    let metrics_addr = pick_free_metrics_addr();
    let admin_addr = pick_free_metrics_addr();
    let cfg_dir = TempDir::new().unwrap();
    let archive_dir = TempDir::new().unwrap();
    let cfg_path = write_admin_config(
        &cfg_dir,
        archive_dir.path(),
        &[relay.url().as_str()],
        &[0],
        metrics_addr,
        admin_addr,
    );

    let nozzl = NozzlHandle::start(cfg_path, archive_dir.path().to_path_buf());

    // Nothing publishes on its own (high thresholds), so before any flush the
    // archive should hold zero chunked events even after the relay handshake.
    // Wait until the acquirer has observed the events (checkpoint advances),
    // then drive the flush. We detect "observed" indirectly: repeatedly flush
    // and watch for the events to land. The first flush after the writers have
    // buffered them publishes; subsequent flushes are no-ops.
    let flush_url = format!("http://{admin_addr}/admin/flush/0");
    let client = reqwest::Client::new();

    let landed = timeout(Duration::from_secs(20), async {
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let Ok(resp) = client.post(&flush_url).send().await else {
                continue;
            };
            // Endpoint must always answer 200 for a configured kind.
            assert_eq!(resp.status(), 200, "configured-kind flush must return 200");
            let _body: serde_json::Value = resp.json().await.unwrap();
            if count_event_files(archive_dir.path()) >= 4 {
                return true;
            }
        }
    })
    .await
    .expect("ad-hoc flush should make all 4 events queryable within 20s");
    assert!(landed);

    // A final flush returns a well-formed summary for kind 0.
    let summary: serde_json::Value = client
        .post(&flush_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(summary["kind"], 0);
    assert!(summary["errors"].as_array().unwrap().is_empty());
    assert_eq!(summary["timed_out"], 0);

    // An unconfigured kind is rejected with 404 (catches operator typos).
    let resp = client
        .post(format!("http://{admin_addr}/admin/flush/12345"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "unconfigured kind");

    // A non-numeric kind is rejected by the path extractor with 400.
    let resp = client
        .post(format!("http://{admin_addr}/admin/flush/notakind"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // The metrics endpoint is unaffected by the separate admin listener.
    let metrics_body = reqwest::get(format!("http://{metrics_addr}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics_body.contains("chunks_published_total"), "metrics body missing counter");

    nozzl.kill();
}
