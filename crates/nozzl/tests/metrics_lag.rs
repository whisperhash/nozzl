//! End-to-end: drive `nozzl::run` against a `nozzl-test-relay`, wait for
//! events to land in the checkpoint, then scrape `/metrics` and verify
//! `checkpoint_lag_seconds` series are emitted with sensible values.
//!
//! Validates the full producer chain wired in `spawn_gauge_loop`:
//! `CheckpointStore::since_snapshot()` → `update_checkpoint_lag()` → HTTP.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{NozzlHandle, pick_free_metrics_addr, write_config_full};
use nostr::{EventBuilder, Keys, Kind};
use nozzl_test_relay::TestRelay;
use tempfile::TempDir;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_lag_gauge_is_emitted_after_observation() {
    let keys = Keys::generate();

    // Pin the event's `created_at` to "now" so the lag we read back is small
    // and bounded. If we used a fixed past timestamp, the lag would grow with
    // wall-clock time and be hard to assert against.
    let created_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let event = EventBuilder::new(Kind::from_u16(0), "hello")
        .custom_created_at(nostr::Timestamp::from_secs(created_at_unix))
        .sign_with_keys(&keys)
        .unwrap();

    let relay = TestRelay::builder()
        .preload_events(vec![event])
        .start()
        .await;

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

    // The gauge loop ticks every 1s after observing events. Allow generous
    // slack for the test relay handshake + initial sub + first tick on slow
    // CI runners.
    let metrics_url = format!("http://{metrics_addr}/metrics");
    let body = timeout(Duration::from_secs(15), async {
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let Ok(resp) = reqwest::get(&metrics_url).await else {
                continue;
            };
            let Ok(text) = resp.text().await else {
                continue;
            };
            if text.contains("checkpoint_lag_seconds{") {
                return text;
            }
        }
    })
    .await
    .expect("checkpoint_lag_seconds series should appear within 15s");

    // Pull the single emitted lag value out of the prometheus text body.
    // Format: `checkpoint_lag_seconds{kind="0",relay="wss://...",slug="..."} <number>`
    let lag_line = body
        .lines()
        .find(|l| l.starts_with("checkpoint_lag_seconds{"))
        .unwrap_or_else(|| panic!("expected lag line; body:\n{body}"));
    let lag_value: f64 = lag_line
        .rsplit(' ')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("could not parse lag from line `{lag_line}`"));

    // Lag should be small (event was created near "now") but >= 0 in normal
    // conditions. We allow a generous upper bound to absorb sub-second jitter
    // between the event timestamp and the gauge tick on slow CI.
    assert!(
        (-2.0..=30.0).contains(&lag_value),
        "lag {lag_value}s is outside the expected near-zero range; line: `{lag_line}`",
    );

    // Confirm the relay label matches the test relay (paranoia: avoids the
    // failure mode where we register a series under the wrong label and the
    // operator scrapes a phantom).
    let expected_relay = relay.url().to_string();
    assert!(
        lag_line.contains(&format!(r#"relay="{expected_relay}""#)),
        "lag line `{lag_line}` does not carry expected relay label `{expected_relay}`",
    );

    nozzl.kill();
}
