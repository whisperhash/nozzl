//! Integration tests for the hot-reload file watcher.
//!
//! These tests use `tempfile::TempDir` and exercise the real filesystem
//! watcher (`FSEvents` on macOS, inotify on Linux). They are intentionally
//! lenient on timing — file events are inherently asynchronous and
//! cross-platform timing varies — but each test waits with a generous
//! upper bound and fails fast if the expected event never arrives.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use nozzl_config::{Config, ConfigWatcher, watch};
use tempfile::TempDir;
use tokio::time::timeout;

const NOZZL_TOML: &str = r#"
[storage]
backend = "local"
root = "/data/events"

[checkpoint]
path = "/data/events/_state/checkpoints.json"

[runtime]
channel_capacity = 1024

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#;

const RELAYS_TOML: &str = r#"
[[relay]]
url = "wss://relay.damus.io"
"#;

const RELAYS_TOML_TWO: &str = r#"
[[relay]]
url = "wss://relay.damus.io"

[[relay]]
url = "wss://nos.lol"
"#;

const KINDS_TOML: &str = r#"
[[kind]]
k = 0
slug = "profile-metadata"

[[kind]]
k = 30063
slug = "zapstore-app-releases"
"#;

fn write_fixture(dir: &TempDir) -> PathBuf {
    let nozzl = dir.path().join("nozzl.toml");
    fs::write(&nozzl, NOZZL_TOML).unwrap();
    fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
    fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
    nozzl
}

/// Wait for the next snapshot up to `dur`, panicking if none arrives.
async fn next_snapshot(watcher: &mut ConfigWatcher, dur: Duration) -> std::sync::Arc<Config> {
    timeout(dur, watcher.next())
        .await
        .expect("expected a snapshot within timeout")
        .expect("watcher channel closed unexpectedly")
}

/// Try to receive a snapshot but expect timeout (no emission).
async fn expect_no_snapshot(watcher: &mut ConfigWatcher, dur: Duration) {
    if let Ok(Some(cfg)) = timeout(dur, watcher.next()).await {
        panic!("unexpected snapshot: relays={:?}", cfg.relays);
    }
}

/// Write a file via temp + rename to avoid the partial-write window that
/// some editors and `fs::write` implementations show. Useful when we
/// want a single coherent edit.
fn atomic_write(path: &Path, contents: &str) {
    let tmp = path.with_extension("tmp.write");
    fs::write(&tmp, contents).unwrap();
    fs::rename(&tmp, path).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_returns_initial_config() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (initial, _watcher) = watch(&nozzl).expect("initial load");
    assert_eq!(initial.relays.len(), 1);
    assert_eq!(initial.kinds.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn detects_relay_file_change() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (initial, mut watcher) = watch(&nozzl).expect("initial load");
    assert_eq!(initial.relays.len(), 1);

    // Give the watcher a moment to install OS subscriptions.
    tokio::time::sleep(Duration::from_millis(100)).await;

    fs::write(dir.path().join("relays.toml"), RELAYS_TOML_TWO).unwrap();

    let cfg = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(cfg.relays.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn detects_kinds_file_change() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (initial, mut watcher) = watch(&nozzl).expect("initial load");
    assert_eq!(initial.kinds.len(), 2);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let new_kinds = "[[kind]]\nk = 0\nslug = \"profile-metadata\"\n";
    fs::write(dir.path().join("kinds.toml"), new_kinds).unwrap();

    let cfg = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(cfg.kinds.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn detects_main_file_change() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (initial, mut watcher) = watch(&nozzl).expect("initial load");
    assert_eq!(initial.runtime.channel_capacity, 1024);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let updated = NOZZL_TOML.replace("channel_capacity = 1024", "channel_capacity = 2048");
    fs::write(&nozzl, &updated).unwrap();

    let cfg = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(cfg.runtime.channel_capacity, 2048);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_reload_keeps_previous_config() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (initial, mut watcher) = watch(&nozzl).expect("initial load");
    assert_eq!(initial.relays.len(), 1);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write garbage TOML to relays.toml; reload must fail and the
    // watcher must not emit anything.
    fs::write(dir.path().join("relays.toml"), "this is = = bad").unwrap();

    expect_no_snapshot(&mut watcher, Duration::from_millis(800)).await;

    // Repair the file; we should now see a snapshot.
    fs::write(dir.path().join("relays.toml"), RELAYS_TOML_TWO).unwrap();
    let cfg = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(cfg.relays.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_reload_validation_failure_keeps_previous_config() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (_initial, mut watcher) = watch(&nozzl).expect("initial load");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Empty relay list: parses fine, but fails validation.
    fs::write(dir.path().join("relays.toml"), "").unwrap();

    expect_no_snapshot(&mut watcher, Duration::from_millis(800)).await;

    // Fix it.
    fs::write(dir.path().join("relays.toml"), RELAYS_TOML_TWO).unwrap();
    let cfg = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(cfg.relays.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn atomic_rename_is_picked_up() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (_initial, mut watcher) = watch(&nozzl).expect("initial load");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Editors that "save atomically" do this: write to a sibling temp
    // file then rename over the target. The watcher must not lose its
    // subscription when the original inode is replaced.
    atomic_write(&dir.path().join("relays.toml"), RELAYS_TOML_TWO);

    let cfg = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(cfg.relays.len(), 2);

    // Now do it again to prove the watcher is still alive after the
    // rename swap.
    atomic_write(&dir.path().join("relays.toml"), RELAYS_TOML);
    let cfg = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(cfg.relays.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_then_recreate_is_picked_up() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (_initial, mut watcher) = watch(&nozzl).expect("initial load");

    tokio::time::sleep(Duration::from_millis(100)).await;

    fs::remove_file(dir.path().join("relays.toml")).unwrap();
    // Reload during the deleted window will error (no relays.toml);
    // that error is logged and the previous config kept. Now recreate.
    tokio::time::sleep(Duration::from_millis(400)).await;
    fs::write(dir.path().join("relays.toml"), RELAYS_TOML_TWO).unwrap();

    let cfg = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(cfg.relays.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn rapid_writes_coalesce_to_single_reload() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (_initial, mut watcher) = watch(&nozzl).expect("initial load");

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Burst-write the same final state several times within the
    // 250 ms debounce window. We expect exactly one snapshot.
    for _ in 0..5 {
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML_TWO).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let first = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(first.relays.len(), 2);

    // No further snapshot should arrive: the bursts collapsed into one.
    expect_no_snapshot(&mut watcher, Duration::from_millis(600)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cross_file_burst_coalesces() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_fixture(&dir);
    let (_initial, mut watcher) = watch(&nozzl).expect("initial load");

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Touch each of the three files in rapid succession.
    fs::write(dir.path().join("relays.toml"), RELAYS_TOML_TWO).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    fs::write(
        dir.path().join("kinds.toml"),
        "[[kind]]\nk = 0\nslug = \"profile-metadata\"\n",
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let updated = NOZZL_TOML.replace("channel_capacity = 1024", "channel_capacity = 2048");
    fs::write(&nozzl, &updated).unwrap();

    let cfg = next_snapshot(&mut watcher, Duration::from_secs(5)).await;
    assert_eq!(cfg.relays.len(), 2);
    assert_eq!(cfg.kinds.len(), 1);
    assert_eq!(cfg.runtime.channel_capacity, 2048);

    expect_no_snapshot(&mut watcher, Duration::from_millis(600)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_initial_config_returns_error() {
    let dir = TempDir::new().unwrap();
    let nozzl = dir.path().join("nozzl.toml");
    fs::write(&nozzl, "garbage = =").unwrap();
    let result = watch(&nozzl);
    assert!(result.is_err());
}
