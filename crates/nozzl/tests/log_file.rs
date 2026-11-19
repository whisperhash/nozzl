//! End-to-end test for the rolling-file log appender.
//!
//! Runs the full service against a `nozzl-test-relay` with `[observability]
//! log_file` configured. Asserts that:
//!   1) Events still land on disk (the file appender doesn't disrupt the
//!      archive hot path).
//!   2) The configured log file is created and contains the startup banner
//!      and at least one info-level line emitted during the run.
//!
//! Rotation itself is provided by `tracing-appender`'s `RollingFileAppender`
//! and is upstream-tested. The risk here is *integration*: that we wired a
//! non-blocking writer correctly, that the worker guard outlives the run,
//! and that file-side failures cannot stall the service. This test exercises
//! the wiring end-to-end without relying on a time-based rotation.

mod common;

use std::time::Duration;

use common::{NozzlHandle, write_config_with_log_file};
use nostr::{EventBuilder, Keys, Kind};
use nozzl_test_relay::TestRelay;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn log_file_receives_writes_alongside_event_archive() {
    let keys = Keys::generate();
    let n_events = 5usize;
    let events: Vec<_> = (0..n_events)
        .map(|i| {
            EventBuilder::new(Kind::from_u16(0), format!("e{i}"))
                .custom_created_at(nostr::Timestamp::from_secs(1_700_000_000 + i as u64))
                .sign_with_keys(&keys)
                .unwrap()
        })
        .collect();

    let relay = TestRelay::builder()
        .preload_events(events.clone())
        .start()
        .await;

    let cfg_dir = TempDir::new().unwrap();
    let archive_dir = TempDir::new().unwrap();
    let log_dir = TempDir::new().unwrap();
    // Path the appender will use as its filename prefix. The actual files
    // on disk are `nozzl.log.YYYY-MM-DD` (the rotating appender appends a
    // date suffix to the prefix).
    let log_file_prefix = log_dir.path().join("nozzl.log");

    let cfg_path = write_config_with_log_file(
        &cfg_dir,
        archive_dir.path(),
        &[relay.url().as_str()],
        &[0],
        Some(&log_file_prefix),
    );

    let archive_root = archive_dir.path().to_path_buf();
    let nozzl = NozzlHandle::start(cfg_path, archive_root);

    // Wait until events land. If the file appender were blocking on a slow
    // sink, this would never finish — the proof that the hot path is
    // decoupled from file I/O.
    let (landed, count) = nozzl.wait_for_events(n_events, Duration::from_secs(30)).await;
    assert!(landed, "expected {n_events} events archived, got {count}");

    // Shut down so the WorkerGuard drops and the appender flushes.
    nozzl.kill();
    relay.shutdown().await;

    // After shutdown, the worker guard has been dropped and the file is
    // flushed. Find any file in the log dir matching our prefix and assert
    // it has content. We glob rather than hard-coding the date suffix to
    // avoid clock-skew flakes around UTC midnight.
    let log_dir_path = log_dir.path();
    let mut matched: Vec<_> = std::fs::read_dir(log_dir_path)
        .expect("log dir should exist")
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("nozzl.log"))
        })
        .collect();
    matched.sort_by_key(std::fs::DirEntry::file_name);
    assert!(
        !matched.is_empty(),
        "expected at least one nozzl.log* file in {}, found {:?}",
        log_dir_path.display(),
        std::fs::read_dir(log_dir_path)
            .unwrap()
            .flatten()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );

    let combined: String = matched
        .iter()
        .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
        .collect();
    assert!(
        !combined.is_empty(),
        "log file(s) exist but are empty: {:?}",
        matched.iter().map(|e| e.file_name()).collect::<Vec<_>>()
    );
    assert!(
        combined.contains("nozzl starting"),
        "log file should contain the startup banner; got:\n{combined}"
    );
    assert!(
        combined.contains("effective config (startup snapshot)"),
        "log file should contain the full effective-config snapshot line; got:\n{combined}"
    );
    assert!(
        combined.contains("startup complete"),
        "log file should contain the post-init line; got:\n{combined}"
    );
}
