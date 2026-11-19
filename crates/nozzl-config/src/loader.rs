//! Synchronous loader for the three TOML files.
//!
//! Hot path is initial startup; the watcher invokes this from a blocking
//! thread when files change.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::ConfigError;
use crate::schema::{Config, KindsFile, RelaysFile};

/// Parse and validate the three files starting from the path of `nozzl.toml`.
///
/// `sources.relays_path` and `sources.kinds_path` are resolved relative to
/// the directory containing `nozzl_path` if they are not absolute.
///
/// # Errors
///
/// Returns [`ConfigError::Io`] if any of the three files cannot be read,
/// [`ConfigError::Parse`] if any cannot be parsed as TOML, and
/// [`ConfigError::Validation`] if the parsed contents violate a structural
/// invariant (empty list, missing required field for the chosen backend,
/// kind out of range, etc.).
pub fn load(nozzl_path: &Path) -> Result<Config, ConfigError> {
    let nozzl_text = fs::read_to_string(nozzl_path).map_err(|e| ConfigError::io(nozzl_path, e))?;
    let mut config: Config =
        toml::from_str(&nozzl_text).map_err(|e| ConfigError::parse(nozzl_path, e))?;

    let base = nozzl_path.parent().unwrap_or_else(|| Path::new("."));
    let relays_path = resolve(base, &config.sources.relays_path);
    let kinds_path = resolve(base, &config.sources.kinds_path);

    let relays_text =
        fs::read_to_string(&relays_path).map_err(|e| ConfigError::io(&relays_path, e))?;
    let relays: RelaysFile =
        toml::from_str(&relays_text).map_err(|e| ConfigError::parse(&relays_path, e))?;

    let kinds_text =
        fs::read_to_string(&kinds_path).map_err(|e| ConfigError::io(&kinds_path, e))?;
    let kinds: KindsFile =
        toml::from_str(&kinds_text).map_err(|e| ConfigError::parse(&kinds_path, e))?;

    config.relays = relays.relay;
    config.kinds = kinds.kind;
    config.sources.relays_path = relays_path;
    config.sources.kinds_path = kinds_path;

    config.validate()?;
    Ok(config)
}

/// Join `base` with `path`, leaving absolute paths untouched.
pub(crate) fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Sample valid config sketched in design.md §5.D9, with paths
    /// rewritten to be relative for the temp dir.
    const NOZZL_TOML: &str = r#"
[storage]
backend = "local"
root = "/data/events"

[acquirer]
backfill_safety_margin_seconds = 60
backfill_default_limit = 500
backfill_empty_pages_to_complete = 8

[checkpoint]
path = "/data/events/_state/checkpoints.json"
flush_interval_seconds = 30

[runtime]
channel_capacity = 1024

[observability]
metrics_addr = "0.0.0.0:9090"
log_format = "json"
log_level = "info"

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#;

    const RELAYS_TOML: &str = r#"
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

    fn write_valid(dir: &TempDir) -> PathBuf {
        let nozzl = dir.path().join("nozzl.toml");
        fs::write(&nozzl, NOZZL_TOML).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        nozzl
    }

    #[test]
    fn loads_valid_config() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        let cfg = load(&nozzl).expect("valid config");
        assert_eq!(cfg.relays.len(), 2);
        assert_eq!(cfg.kinds.len(), 2);
        assert_eq!(cfg.relays[0].url, "wss://relay.damus.io");
        assert_eq!(cfg.kinds[0].k, 0);
        assert_eq!(cfg.kinds[0].slug, "profile-metadata");
        // Source paths are normalized to absolute after resolution.
        assert!(cfg.sources.relays_path.is_absolute());
        assert!(cfg.sources.kinds_path.is_absolute());
    }

    #[test]
    fn storage_metrics_defaults_when_section_omitted() {
        // NOZZL_TOML has no [storage_metrics] section.
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        let cfg = load(&nozzl).expect("valid config");
        assert!(cfg.storage_metrics.enabled);
        assert_eq!(cfg.storage_metrics.scan_interval_seconds, 300);
        assert_eq!(cfg.storage_metrics.scan_concurrency, 4);
    }

    #[test]
    fn storage_metrics_section_overrides_defaults() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let with_section = format!(
            "{NOZZL_TOML}\n[storage_metrics]\nenabled = true\nscan_interval_seconds = 600\nscan_concurrency = 2\n"
        );
        fs::write(&nozzl, with_section).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let cfg = load(&nozzl).expect("valid config");
        assert_eq!(cfg.storage_metrics.scan_interval_seconds, 600);
        assert_eq!(cfg.storage_metrics.scan_concurrency, 2);
    }

    #[test]
    fn rejects_zero_storage_scan_interval() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let with_section = format!("{NOZZL_TOML}\n[storage_metrics]\nscan_interval_seconds = 0\n");
        fs::write(&nozzl, with_section).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("scan_interval_seconds"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_out_of_range_chunk_buffer_age_jitter() {
        // A jitter above the 0.5 cap is a typo, not an intent; validation must
        // say so at parse time rather than let the archive clamp it silently.
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let injected = NOZZL_TOML.replace(
            "channel_capacity = 1024\n",
            "channel_capacity = 1024\nchunk_buffer_age_jitter = 0.6\n",
        );
        assert_ne!(injected, NOZZL_TOML, "the fixture's [runtime] anchor moved");
        fs::write(&nozzl, injected).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        match load(&nozzl).unwrap_err() {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("chunk_buffer_age_jitter"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// A negative jitter is the other half of the range check. TOML cannot
    /// express NaN or an infinity, so those branches are only reachable
    /// programmatically and are covered by `schema`'s own unit tests.
    #[test]
    fn rejects_negative_chunk_buffer_age_jitter() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let injected = NOZZL_TOML.replace(
            "channel_capacity = 1024\n",
            "channel_capacity = 1024\nchunk_buffer_age_jitter = -0.1\n",
        );
        assert_ne!(injected, NOZZL_TOML, "the fixture's [runtime] anchor moved");
        fs::write(&nozzl, injected).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        match load(&nozzl).unwrap_err() {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("chunk_buffer_age_jitter"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// Zero permits would park every flush forever.
    #[test]
    fn rejects_zero_chunk_flush_concurrency() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let injected = NOZZL_TOML.replace(
            "channel_capacity = 1024\n",
            "channel_capacity = 1024\nchunk_flush_concurrency = 0\n",
        );
        assert_ne!(injected, NOZZL_TOML, "the fixture's [runtime] anchor moved");
        fs::write(&nozzl, injected).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        match load(&nozzl).unwrap_err() {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("chunk_flush_concurrency"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// The upper bound. Without it a huge value either panics inside
    /// `Semaphore::new` at boot or, more likely, silently stops bounding
    /// anything - the failure this knob exists to prevent.
    #[test]
    fn rejects_oversized_chunk_flush_concurrency() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let injected = NOZZL_TOML.replace(
            "channel_capacity = 1024\n",
            "channel_capacity = 1024\nchunk_flush_concurrency = 100000\n",
        );
        assert_ne!(injected, NOZZL_TOML, "the fixture's [runtime] anchor moved");
        fs::write(&nozzl, injected).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        match load(&nozzl).unwrap_err() {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("chunk_flush_concurrency"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_zero_log_file_max_files() {
        // `log_file_max_files = 0` has no clean meaning for the rolling
        // appender (max_log_files(0) underflows its prune logic); validation
        // must reject it and point at the indefinite-retention escape hatch.
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let injected = NOZZL_TOML.replace(
            "log_level = \"info\"\n",
            "log_level = \"info\"\nlog_file_max_files = 0\n",
        );
        fs::write(&nozzl, injected).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("log_file_max_files"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn absent_log_file_max_files_is_indefinite() {
        // The default fixture omits the key entirely — that's the
        // "keep logs forever" setting and must deserialize to None.
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        let cfg = load(&nozzl).expect("absent log_file_max_files is valid");
        assert_eq!(cfg.observability.log_file_max_files, None);
    }

    #[test]
    fn disabled_storage_metrics_skips_zero_checks() {
        // With the scan disabled, a zero interval is irrelevant and must not
        // fail validation.
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let with_section = format!(
            "{NOZZL_TOML}\n[storage_metrics]\nenabled = false\nscan_interval_seconds = 0\nscan_concurrency = 0\n"
        );
        fs::write(&nozzl, with_section).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let cfg = load(&nozzl).expect("disabled scan tolerates zero knobs");
        assert!(!cfg.storage_metrics.enabled);
    }

    #[test]
    fn missing_file_yields_io_error() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let err = load(&nozzl).unwrap_err();
        assert!(matches!(err, ConfigError::Io { .. }), "got {err:?}");
    }

    #[test]
    fn missing_relays_file_yields_io_error() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        fs::write(&nozzl, NOZZL_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Io { path, .. } => {
                assert!(path.ends_with("relays.toml"), "got {path:?}");
            }
            other => panic!("expected Io, got {other:?}"),
        }
    }

    #[test]
    fn parse_error_in_main_file() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        fs::write(&nozzl, "this is not = = valid toml").unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let err = load(&nozzl).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }), "got {err:?}");
    }

    #[test]
    fn parse_error_in_relays_file() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(dir.path().join("relays.toml"), "[[relay]]\nbroken == ").unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Parse { path, .. } => {
                assert!(path.ends_with("relays.toml"), "got {path:?}");
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_relay_list() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(dir.path().join("relays.toml"), "").unwrap();
        let err = load(&nozzl).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("relay"), "got {msg}");
    }

    #[test]
    fn rejects_empty_kind_list() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(dir.path().join("kinds.toml"), "").unwrap();
        let err = load(&nozzl).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("kind"), "got {msg}");
    }

    #[test]
    fn rejects_ephemeral_kind() {
        // Ephemeral kinds (20000-29999) are the only class refused for
        // archival. Regular kinds (e.g. kind 1, 9735) are now accepted.
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 22500\nslug = \"ephemeral-thing\"\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("kind 22500"), "got {msg}");
                assert!(msg.contains("ephemeral"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_relay_with_bad_scheme() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("relays.toml"),
            "[[relay]]\nurl = \"https://example.com\"\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("scheme"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn local_backend_requires_root() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let toml = NOZZL_TOML.replace(r#"root = "/data/events""#, "");
        fs::write(&nozzl, &toml).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("storage.root"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn s3_backend_requires_bucket_prefix_region() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let toml = r#"
[storage]
backend = "s3"

[checkpoint]
path = "/data/events/_state/checkpoints.json"

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#;
        fs::write(&nozzl, toml).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("bucket"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }

        // Add bucket only — should now complain about prefix.
        let toml = r#"
[storage]
backend = "s3"
bucket = "my-bucket"

[checkpoint]
path = "/data/events/_state/checkpoints.json"

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#;
        fs::write(&nozzl, toml).unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("prefix"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }

        // Add prefix only — should now complain about region.
        let toml = r#"
[storage]
backend = "s3"
bucket = "my-bucket"
prefix = "events/"

[checkpoint]
path = "/data/events/_state/checkpoints.json"

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#;
        fs::write(&nozzl, toml).unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("region"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }

        // Full s3 config — passes.
        let toml = r#"
[storage]
backend = "s3"
bucket = "my-bucket"
prefix = "events/"
region = "us-east-1"

[checkpoint]
path = "/data/events/_state/checkpoints.json"

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#;
        fs::write(&nozzl, toml).unwrap();
        load(&nozzl).expect("full s3 config should parse");
    }

    #[test]
    fn defaults_applied_when_sections_omitted() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let toml = r#"
[storage]
backend = "local"
root = "/data/events"

[checkpoint]
path = "/data/events/_state/checkpoints.json"

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#;
        fs::write(&nozzl, toml).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let cfg = load(&nozzl).expect("defaulted config should parse");

        assert_eq!(cfg.acquirer.backfill_safety_margin_seconds, 60);
        assert_eq!(cfg.acquirer.backfill_default_limit, 500);
        assert_eq!(cfg.acquirer.backfill_empty_pages_to_complete, 8);
        // Pin the schema default so it stays in sync with the value
        // documented in docs/design.md (the dispatch-liveness probe).
        assert_eq!(cfg.acquirer.forward_silence_threshold_seconds, 60);
        assert_eq!(cfg.checkpoint.flush_interval_seconds, 30);
        assert_eq!(cfg.runtime.channel_capacity, 1024);
        assert_eq!(
            cfg.observability.metrics_addr,
            "0.0.0.0:9090".parse().unwrap()
        );
        // Admin endpoint is opt-in: absent from config ⇒ disabled.
        assert_eq!(cfg.observability.admin_addr, None);
        assert_eq!(cfg.observability.log_level, "info");
    }

    #[test]
    fn admin_addr_parsed_when_present() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let toml = r#"
[storage]
backend = "local"
root = "/data/events"

[checkpoint]
path = "/data/events/_state/checkpoints.json"

[observability]
admin_addr = "127.0.0.1:9091"

[sources]
relays_path = "relays.toml"
kinds_path = "kinds.toml"
"#;
        fs::write(&nozzl, toml).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let cfg = load(&nozzl).expect("config with admin_addr should parse");
        assert_eq!(
            cfg.observability.admin_addr,
            Some("127.0.0.1:9091".parse().unwrap())
        );
    }

    #[test]
    fn absolute_source_paths_not_rebased() {
        let dir = TempDir::new().unwrap();
        let other_dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let relays_abs = other_dir.path().join("relays.toml");
        let kinds_abs = other_dir.path().join("kinds.toml");
        fs::write(&relays_abs, RELAYS_TOML).unwrap();
        fs::write(&kinds_abs, KINDS_TOML).unwrap();

        let toml = format!(
            r#"
[storage]
backend = "local"
root = "/data/events"

[checkpoint]
path = "/data/events/_state/checkpoints.json"

[sources]
relays_path = "{}"
kinds_path = "{}"
"#,
            relays_abs.display(),
            kinds_abs.display(),
        );
        fs::write(&nozzl, &toml).unwrap();

        let cfg = load(&nozzl).expect("absolute paths");
        assert_eq!(cfg.sources.relays_path, relays_abs);
        assert_eq!(cfg.sources.kinds_path, kinds_abs);
    }

    #[test]
    fn rejects_zero_backfill_empty_pages_to_complete() {
        let dir = TempDir::new().unwrap();
        let nozzl = dir.path().join("nozzl.toml");
        let toml = NOZZL_TOML.replace(
            "backfill_empty_pages_to_complete = 8",
            "backfill_empty_pages_to_complete = 0",
        );
        fs::write(&nozzl, &toml).unwrap();
        fs::write(dir.path().join("relays.toml"), RELAYS_TOML).unwrap();
        fs::write(dir.path().join("kinds.toml"), KINDS_TOML).unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(
                    msg.contains("backfill_empty_pages_to_complete"),
                    "got {msg}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_kind_without_slug() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(dir.path().join("kinds.toml"), "[[kind]]\nk = 0\n").unwrap();
        let err = load(&nozzl).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }), "got {err:?}");
    }

    #[test]
    fn rejects_kind_with_invalid_slug() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"Profile Metadata\"\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("invalid slug"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_duplicate_kind() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"a\"\n[[kind]]\nk = 0\nslug = \"b\"\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => assert!(msg.contains("duplicate kind"), "got {msg}"),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_duplicate_slug() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"x\"\n[[kind]]\nk = 3\nslug = \"x\"\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => assert!(msg.contains("duplicate slug"), "got {msg}"),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    // ---- per-kind `only_relays` (the inverse of a relay's `only_kinds`) ----

    #[test]
    fn accepts_kind_with_only_relays_subset() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        // RELAYS_TOML configures damus + nos.lol; pin kind 30063 to damus only.
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"profile-metadata\"\n\
             [[kind]]\nk = 30063\nslug = \"zapstore-app-releases\"\n\
             only_relays = [\"wss://relay.damus.io\"]\n",
        )
        .unwrap();
        let cfg = load(&nozzl).expect("only_relays naming a configured relay is valid");
        assert_eq!(cfg.kinds[0].only_relays, None);
        assert_eq!(
            cfg.kinds[1].only_relays,
            Some(vec!["wss://relay.damus.io".to_string()])
        );
    }

    #[test]
    fn accepts_kind_without_only_relays() {
        // Absence means "all configured relays" - the legacy default every
        // existing kinds.toml relies on. Pins that backward compatibility.
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        let cfg = load(&nozzl).expect("kinds without only_relays remain valid");
        assert!(
            cfg.kinds.iter().all(|k| k.only_relays.is_none()),
            "fixture kinds should have no only_relays"
        );
    }

    #[test]
    fn rejects_only_relays_referencing_unconfigured_relay() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"profile-metadata\"\n\
             [[kind]]\nk = 30063\nslug = \"zapstore-app-releases\"\n\
             only_relays = [\"wss://relay.notconfigured.example\"]\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("relay.notconfigured.example"), "got {msg}");
                assert!(msg.contains("not a configured relay"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_only_relays() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"profile-metadata\"\n\
             [[kind]]\nk = 30063\nslug = \"zapstore-app-releases\"\nonly_relays = []\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => assert!(msg.contains("empty only_relays"), "got {msg}"),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn accepts_only_relays_differing_only_by_trailing_slash() {
        // `wss://host` and `wss://host/` are the same relay. Comparing raw
        // strings would hard-fail the load - and a config error at boot takes
        // the whole service down, so this must not be a footgun.
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"profile-metadata\"\n\
             [[kind]]\nk = 30063\nslug = \"zapstore-app-releases\"\n\
             only_relays = [\"wss://relay.damus.io/\"]\n",
        )
        .unwrap();
        let cfg = load(&nozzl).expect("trailing-slash-only difference must still resolve");
        assert_eq!(
            cfg.kinds[1].only_relays,
            Some(vec!["wss://relay.damus.io/".to_string()])
        );
    }

    #[test]
    fn rejects_unknown_field_in_kind_entry() {
        // deny_unknown_fields is what stops an OLDER binary from silently
        // ignoring a NEWER scoping field during a deploy's build window (and a
        // typo like `only_relay` from silently meaning "no scoping at all").
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"profile-metadata\"\nonly_relay = [\"wss://relay.damus.io\"]\n",
        )
        .unwrap();
        assert!(
            load(&nozzl).is_err(),
            "an unknown kind field must fail the load, not be silently dropped"
        );
    }

    #[test]
    fn rejects_unknown_field_in_relay_entry() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("relays.toml"),
            "[[relay]]\nurl = \"wss://relay.damus.io\"\nonly_kind = [0]\n",
        )
        .unwrap();
        assert!(
            load(&nozzl).is_err(),
            "an unknown relay field must fail the load, not be silently dropped"
        );
    }

    #[test]
    fn rejects_only_relays_with_duplicates() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"profile-metadata\"\n\
             [[kind]]\nk = 30063\nslug = \"zapstore-app-releases\"\n\
             only_relays = [\"wss://relay.damus.io\", \"wss://relay.damus.io\"]\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("duplicate relay"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_kind_that_reaches_no_relay_via_only_relays_and_only_kinds() {
        // The composed guard: kind 30063 is pinned to damus, but damus is
        // scoped to kind 0 only. Nothing would ever request 30063 - a silent
        // no-data configuration that must fail loudly at load time.
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("relays.toml"),
            "[[relay]]\nurl = \"wss://relay.damus.io\"\nonly_kinds = [0]\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"profile-metadata\"\n\
             [[kind]]\nk = 30063\nslug = \"zapstore-app-releases\"\n\
             only_relays = [\"wss://relay.damus.io\"]\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("30063"), "got {msg}");
                assert!(msg.contains("reaches no relay"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn accepts_kind_scoped_to_one_relay_while_others_are_unscoped() {
        // The pinned-kind shape: a kind pinned to a dedicated relay that
        // itself carries an only_kinds allowlist admitting that kind, while the
        // general relays stay unscoped and simply never see the pinned kind.
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("relays.toml"),
            "[[relay]]\nurl = \"wss://relay.damus.io\"\n\
             [[relay]]\nurl = \"wss://relay.plebeian.market\"\nonly_kinds = [30063]\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("kinds.toml"),
            "[[kind]]\nk = 0\nslug = \"profile-metadata\"\n\
             [[kind]]\nk = 30063\nslug = \"zapstore-app-releases\"\n\
             only_relays = [\"wss://relay.plebeian.market\"]\n",
        )
        .unwrap();
        let cfg = load(&nozzl).expect("kind pinned to a relay whose only_kinds admits it is valid");
        assert_eq!(cfg.relays.len(), 2);
        assert_eq!(
            cfg.kinds[1].only_relays,
            Some(vec!["wss://relay.plebeian.market".to_string()])
        );
    }

    #[test]
    fn accepts_relay_with_only_kinds_subset() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        // The scoped relay is paired with an unscoped one so kind 0 still
        // reaches a relay. A lone `only_kinds = [30063]` relay would leave the
        // fixture's kind 0 unrequestable, which the "reaches no relay" guard
        // now rejects (see rejects_kind_that_reaches_no_relay_*). The subject
        // of this test is only_kinds parsing, so keep the config coherent.
        fs::write(
            dir.path().join("relays.toml"),
            "[[relay]]\nurl = \"wss://relay.damus.io\"\n\
             [[relay]]\nurl = \"wss://relay.zapstore.dev\"\nonly_kinds = [30063]\n",
        )
        .unwrap();
        let cfg = load(&nozzl).expect("only_kinds subset of configured kinds is valid");
        assert_eq!(cfg.relays.len(), 2);
        assert_eq!(cfg.relays[0].only_kinds, None);
        assert_eq!(cfg.relays[1].only_kinds, Some(vec![30063]));
    }

    #[test]
    fn accepts_relay_without_only_kinds() {
        // Absence of only_kinds means "all configured kinds" — the legacy
        // default that all existing relays.toml files rely on. This test
        // pins that backward compatibility.
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        let cfg = load(&nozzl).expect("relays without only_kinds remain valid");
        assert!(
            cfg.relays.iter().all(|r| r.only_kinds.is_none()),
            "fixture relays should have no only_kinds"
        );
    }

    #[test]
    fn rejects_only_kinds_referencing_unconfigured_kind() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        // kinds.toml has 0 and 30063; only_kinds references 10002 which isn't
        // in the global list.
        fs::write(
            dir.path().join("relays.toml"),
            "[[relay]]\nurl = \"wss://relay.zapstore.dev\"\nonly_kinds = [30063, 10002]\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("10002"), "got {msg}");
                assert!(msg.contains("not in kinds.toml"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_only_kinds() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("relays.toml"),
            "[[relay]]\nurl = \"wss://relay.zapstore.dev\"\nonly_kinds = []\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("empty only_kinds"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_only_kinds_with_duplicates() {
        let dir = TempDir::new().unwrap();
        let nozzl = write_valid(&dir);
        fs::write(
            dir.path().join("relays.toml"),
            "[[relay]]\nurl = \"wss://relay.zapstore.dev\"\nonly_kinds = [30063, 30063]\n",
        )
        .unwrap();
        let err = load(&nozzl).unwrap_err();
        match err {
            ConfigError::Validation(msg) => {
                assert!(msg.contains("duplicate kind 30063"), "got {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }
}
