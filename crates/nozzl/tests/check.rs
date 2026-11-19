//! `--check` mode tests for `nozzl::run`.

use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

const NOZZL_TOML: &str = r#"
[storage]
backend = "local"
root = "/tmp/nozzl-test-archive"

[checkpoint]
path = "/tmp/nozzl-test-archive/_state/checkpoints.json"

[observability]
metrics_addr = "127.0.0.1:0"
log_format = "pretty"

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#;

const RELAYS_TOML: &str = r#"
[[relay]]
url = "wss://relay.damus.io"
"#;

const KINDS_TOML: &str = r#"
[[kind]]
k = 0
slug = "kind-0-profile-metadata"
"#;

fn write_good_config(dir: &TempDir) -> PathBuf {
    let nozzl = dir.path().join("nozzl.toml");
    fs::write(&nozzl, NOZZL_TOML).unwrap();
    fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
    fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
    nozzl
}

#[tokio::test]
async fn check_against_good_config_returns_ok() {
    let dir = TempDir::new().unwrap();
    let nozzl = write_good_config(&dir);
    nozzl::run(nozzl, true)
        .await
        .expect("good config should pass --check");
}

#[tokio::test]
async fn check_against_missing_file_returns_err() {
    let dir = TempDir::new().unwrap();
    let nozzl = dir.path().join("does-not-exist.toml");
    let err = nozzl::run(nozzl, true)
        .await
        .expect_err("missing file should fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("does-not-exist.toml") || msg.contains("No such file"),
        "got {msg}"
    );
}

#[tokio::test]
async fn check_against_invalid_toml_returns_err() {
    let dir = TempDir::new().unwrap();
    let nozzl = dir.path().join("nozzl.toml");
    fs::write(&nozzl, "this is not = = = valid toml").unwrap();
    fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
    fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
    assert!(nozzl::run(nozzl, true).await.is_err());
}

#[tokio::test]
async fn check_rejects_ephemeral_kind() {
    // Ephemeral kinds (20000-29999) are the only class refused for archival;
    // regular kinds (e.g. kind 1, 9735) are now accepted.
    let dir = TempDir::new().unwrap();
    let nozzl = write_good_config(&dir);
    fs::write(
        dir.path().join("kinds.toml"),
        "[[kind]]\nk = 22500\nslug = \"ephemeral-thing\"\n",
    )
    .unwrap();
    assert!(nozzl::run(nozzl, true).await.is_err());
}

#[tokio::test]
async fn check_accepts_regular_kind() {
    // A regular kind (kind-9735 zap receipts) validates cleanly.
    let dir = TempDir::new().unwrap();
    let nozzl = write_good_config(&dir);
    fs::write(
        dir.path().join("kinds.toml"),
        "[[kind]]\nk = 9735\nslug = \"zap-receipts\"\n",
    )
    .unwrap();
    assert!(nozzl::run(nozzl, true).await.is_ok());
}

#[tokio::test]
async fn check_rejects_a_relay_url_the_acquirer_cannot_use() {
    // The validator parses a relay URL as a URL. The acquirer parses it again,
    // as nozzl's RelayUrl, which also refuses credentials and a second "://".
    // `--check` must refuse what startup and a reload would: a deployment that
    // checks a file before installing it would otherwise install one the
    // running binary cannot use, and the next start would fail on it.
    for url in [
        "wss://user:pass@relay.example",
        "wss://relay.example/?via=wss://other.example",
    ] {
        let dir = TempDir::new().unwrap();
        let nozzl = write_good_config(&dir);
        fs::write(
            dir.path().join("relays.toml"),
            format!("[[relay]]\nurl = \"{url}\"\n"),
        )
        .unwrap();
        let err = nozzl::run(nozzl, true)
            .await
            .expect_err("a relay URL the acquirer refuses should fail --check");
        let msg = format!("{err:#}");
        assert!(msg.contains("relay URL"), "{url}: got {msg}");
    }
}
