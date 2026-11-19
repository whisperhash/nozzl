//! Pure-function mappings from `nozzl_config::Config` to per-subsystem
//! configs. Tested in isolation in `tests::mappings`.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use nozzl_acquirer::AcquirerConfig;
use nozzl_archive::{LocalConfig, S3Config, StorageConfig};
use nozzl_config::{Config, StorageBackend as ConfigBackend};
use nozzl_types::RelayUrl;

/// Build the archive's `StorageConfig` from a parsed `Config`.
///
/// Returns an error if the storage section is internally inconsistent —
/// e.g. backend = local but `root` is missing. The config validator already
/// rejects most of these, but we re-check here so this function is usable
/// from tests that build a `Config` programmatically and skip validation.
pub fn storage_config_from(cfg: &Config) -> Result<StorageConfig> {
    let archive_backend = match cfg.storage.backend {
        ConfigBackend::Local => nozzl_archive::StorageBackend::Local,
        ConfigBackend::S3 => nozzl_archive::StorageBackend::S3,
    };

    let local = match cfg.storage.backend {
        ConfigBackend::Local => Some(LocalConfig {
            root: cfg
                .storage
                .root
                .clone()
                .ok_or_else(|| anyhow!("storage.backend = local requires storage.root"))?,
        }),
        ConfigBackend::S3 => None,
    };

    let s3 = match cfg.storage.backend {
        ConfigBackend::S3 => Some(S3Config {
            bucket: cfg
                .storage
                .bucket
                .clone()
                .ok_or_else(|| anyhow!("storage.backend = s3 requires storage.bucket"))?,
            prefix: cfg.storage.prefix.clone().unwrap_or_default(),
            region: cfg
                .storage
                .region
                .clone()
                .ok_or_else(|| anyhow!("storage.backend = s3 requires storage.region"))?,
        }),
        ConfigBackend::Local => None,
    };

    Ok(StorageConfig {
        backend: archive_backend,
        local,
        s3,
    })
}

/// Build the acquirer's `AcquirerConfig` from a parsed `Config`.
///
/// Returns an error if any relay URL fails to parse as a `RelayUrl`. The
/// config validator screens this out at startup; we re-validate here so the
/// mapping is also safe to call from tests.
pub fn acquirer_config_from(cfg: &Config) -> Result<AcquirerConfig> {
    let mut relays = Vec::with_capacity(cfg.relays.len());
    let mut relay_only_kinds: std::collections::HashMap<RelayUrl, Vec<u16>> =
        std::collections::HashMap::new();
    for entry in &cfg.relays {
        let url = RelayUrl::parse(&entry.url)
            .with_context(|| format!("relay URL {:?}", entry.url))?;
        if let Some(only) = &entry.only_kinds {
            relay_only_kinds.insert(url.clone(), only.clone());
        }
        relays.push(url);
    }
    // Per-kind relay allowlist. Parsed through `RelayUrl` exactly like the
    // relay list above so both sides of the later `contains(url)` check are
    // normalized identically - the config file writes `wss://nos.lol`, the
    // parsed form is `wss://nos.lol/`, and comparing raw strings here would
    // silently never match. Config validation has already guaranteed every URL
    // names a configured relay, so a parse failure is the same bug the relay
    // loop above reports.
    let mut kind_only_relays: std::collections::HashMap<u16, std::collections::HashSet<RelayUrl>> =
        std::collections::HashMap::new();
    for entry in &cfg.kinds {
        if let Some(only) = &entry.only_relays {
            let mut set = std::collections::HashSet::with_capacity(only.len());
            for raw in only {
                let url = RelayUrl::parse(raw)
                    .with_context(|| format!("kind {} only_relays entry {:?}", entry.k, raw))?;
                set.insert(url);
            }
            kind_only_relays.insert(entry.k, set);
        }
    }
    let kinds: Vec<u16> = cfg.kinds.iter().map(|k| k.k).collect();
    let kind_slugs: std::collections::HashMap<u16, String> = cfg
        .kinds
        .iter()
        .map(|k| (k.k, k.slug.clone()))
        .collect();
    Ok(AcquirerConfig {
        relays,
        kinds,
        kind_slugs,
        relay_only_kinds,
        kind_only_relays,
        backfill_safety_margin: Duration::from_secs(cfg.acquirer.backfill_safety_margin_seconds),
        backfill_default_limit: cfg.acquirer.backfill_default_limit,
        backfill_empty_pages_to_complete: cfg.acquirer.backfill_empty_pages_to_complete,
        pool_dedup_max_events: cfg.runtime.pool_dedup_max_events,
        forward_silence_threshold: Duration::from_secs(
            cfg.acquirer.forward_silence_threshold_seconds,
        ),
        probe_timeout: Duration::from_secs(cfg.acquirer.probe_timeout_seconds),
        probe_min_interval: Duration::from_secs(cfg.acquirer.probe_min_interval_seconds),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nozzl_config::{
        AcquirerConfig as CfgAcquirer, CheckpointConfig, KindEntry, ObservabilityConfig,
        RelayEntry, RuntimeConfig, SourcesConfig, StorageConfig as CfgStorage,
        StorageMetricsConfig,
    };
    use std::path::PathBuf;

    fn base_cfg() -> Config {
        Config {
            storage: CfgStorage {
                backend: ConfigBackend::Local,
                root: Some(PathBuf::from("/data/events")),
                bucket: None,
                prefix: None,
                region: None,
            },
            acquirer: CfgAcquirer {
                backfill_safety_margin_seconds: 90,
                backfill_default_limit: 250,
                backfill_empty_pages_to_complete: 12,
                forward_silence_threshold_seconds: 60,
                probe_timeout_seconds: 5,
                probe_min_interval_seconds: 60,
            },
            checkpoint: CheckpointConfig {
                path: PathBuf::from("/data/events/_state/checkpoints.json"),
                flush_interval_seconds: 30,
            },
            runtime: RuntimeConfig::default(),
            observability: ObservabilityConfig::default(),
            storage_metrics: StorageMetricsConfig::default(),
            sources: SourcesConfig {
                relays_path: PathBuf::from("/etc/nozzl/relays.toml"),
                kinds_path: PathBuf::from("/etc/nozzl/kinds.toml"),
            },
            relays: vec![
                RelayEntry {
                    url: "wss://relay.damus.io".into(),
                    only_kinds: None,
                },
                RelayEntry {
                    url: "wss://nos.lol".into(),
                    only_kinds: None,
                },
            ],
            kinds: vec![
                KindEntry {
                    k: 0,
                    slug: "profile-metadata".into(),
                    only_relays: None,
                },
                KindEntry {
                    k: 30063,
                    slug: "zapstore-app-releases".into(),
                    only_relays: None,
                },
            ],
        }
    }

    #[test]
    fn storage_local_round_trip() {
        let cfg = base_cfg();
        let s = storage_config_from(&cfg).unwrap();
        assert_eq!(s.backend, nozzl_archive::StorageBackend::Local);
        assert!(s.s3.is_none());
        assert_eq!(
            s.local.unwrap().root,
            PathBuf::from("/data/events"),
        );
    }

    #[test]
    fn storage_local_missing_root_errors() {
        let mut cfg = base_cfg();
        cfg.storage.root = None;
        let err = storage_config_from(&cfg).unwrap_err();
        assert!(err.to_string().contains("storage.root"), "got {err}");
    }

    #[test]
    fn storage_s3_full() {
        let mut cfg = base_cfg();
        cfg.storage.backend = ConfigBackend::S3;
        cfg.storage.root = None;
        cfg.storage.bucket = Some("nozzl".into());
        cfg.storage.prefix = Some("events/".into());
        cfg.storage.region = Some("us-east-1".into());
        let s = storage_config_from(&cfg).unwrap();
        assert_eq!(s.backend, nozzl_archive::StorageBackend::S3);
        assert!(s.local.is_none());
        let s3 = s.s3.unwrap();
        assert_eq!(s3.bucket, "nozzl");
        assert_eq!(s3.prefix, "events/");
        assert_eq!(s3.region, "us-east-1");
    }

    #[test]
    fn storage_s3_default_prefix_is_empty_string() {
        let mut cfg = base_cfg();
        cfg.storage.backend = ConfigBackend::S3;
        cfg.storage.root = None;
        cfg.storage.bucket = Some("nozzl".into());
        cfg.storage.prefix = None;
        cfg.storage.region = Some("us-east-1".into());
        let s = storage_config_from(&cfg).unwrap();
        assert_eq!(s.s3.unwrap().prefix, "");
    }

    #[test]
    fn storage_s3_missing_bucket_errors() {
        let mut cfg = base_cfg();
        cfg.storage.backend = ConfigBackend::S3;
        cfg.storage.root = None;
        cfg.storage.region = Some("us-east-1".into());
        let err = storage_config_from(&cfg).unwrap_err();
        assert!(err.to_string().contains("bucket"), "got {err}");
    }

    #[test]
    fn storage_s3_missing_region_errors() {
        let mut cfg = base_cfg();
        cfg.storage.backend = ConfigBackend::S3;
        cfg.storage.root = None;
        cfg.storage.bucket = Some("nozzl".into());
        let err = storage_config_from(&cfg).unwrap_err();
        assert!(err.to_string().contains("region"), "got {err}");
    }

    #[test]
    fn acquirer_round_trip() {
        let cfg = base_cfg();
        let a = acquirer_config_from(&cfg).unwrap();
        assert_eq!(a.relays.len(), 2);
        assert_eq!(a.relays[0].as_str(), "wss://relay.damus.io/");
        assert_eq!(a.relays[1].as_str(), "wss://nos.lol/");
        assert_eq!(a.kinds, vec![0u16, 30063]);
        assert_eq!(a.backfill_safety_margin, Duration::from_secs(90));
        assert_eq!(a.backfill_default_limit, 250);
        assert_eq!(a.backfill_empty_pages_to_complete, 12);
    }

    #[test]
    fn acquirer_rejects_bad_relay_url() {
        let mut cfg = base_cfg();
        cfg.relays.push(RelayEntry {
            url: "not a url".into(),
            only_kinds: None,
        });
        assert!(acquirer_config_from(&cfg).is_err());
    }

    #[test]
    fn acquirer_relay_only_kinds_propagated() {
        let mut cfg = base_cfg();
        // Restrict the second relay (nos.lol) to a single kind.
        cfg.relays[1].only_kinds = Some(vec![30063]);
        let a = acquirer_config_from(&cfg).unwrap();
        let damus = RelayUrl::parse("wss://relay.damus.io").unwrap();
        let nos = RelayUrl::parse("wss://nos.lol").unwrap();
        // damus has no restriction → effective_kinds equals the global list.
        assert_eq!(a.effective_kinds_for(&damus), vec![0u16, 30063]);
        // nos.lol restricted to 30063 only.
        assert_eq!(a.effective_kinds_for(&nos), vec![30063]);
        // The raw map only contains entries for relays with a restriction.
        assert!(!a.relay_only_kinds.contains_key(&damus));
        assert_eq!(a.relay_only_kinds.get(&nos), Some(&vec![30063]));
    }

    #[test]
    fn acquirer_kind_only_relays_propagated_and_url_normalized() {
        // Pin kind 30063 to damus only. The config file writes the URL WITHOUT
        // a trailing slash; `RelayUrl::parse` normalizes it WITH one. Both
        // sides of the membership check must go through the same parse, or the
        // pin silently matches nothing and the kind is dropped everywhere.
        let mut cfg = base_cfg();
        cfg.kinds[1].only_relays = Some(vec!["wss://relay.damus.io".into()]);
        let a = acquirer_config_from(&cfg).unwrap();
        let damus = RelayUrl::parse("wss://relay.damus.io").unwrap();
        let nos = RelayUrl::parse("wss://nos.lol").unwrap();
        assert_eq!(
            damus.as_str(),
            "wss://relay.damus.io/",
            "precondition: parse normalizes a trailing slash onto the config string"
        );
        assert_eq!(a.effective_kinds_for(&damus), vec![0u16, 30063]);
        assert_eq!(a.effective_kinds_for(&nos), vec![0u16]);
        // The raw map only carries entries for kinds with a restriction.
        assert!(!a.kind_only_relays.contains_key(&0));
        assert!(a.kind_only_relays.get(&30063).unwrap().contains(&damus));
    }

    #[test]
    fn acquirer_kind_only_relays_composes_with_relay_only_kinds() {
        // Both scoping directions through the real mapping function, in the
        // pinned-kind shape: nos.lol acts as the dedicated relay (scoped to
        // kind-30063 (zapstore-app-releases)) and that kind is pinned to it, so
        // damus keeps only kind-0 (profile-metadata).
        let mut cfg = base_cfg();
        cfg.relays[1].only_kinds = Some(vec![30063]);
        cfg.kinds[1].only_relays = Some(vec!["wss://nos.lol".into()]);
        let a = acquirer_config_from(&cfg).unwrap();
        let damus = RelayUrl::parse("wss://relay.damus.io").unwrap();
        let nos = RelayUrl::parse("wss://nos.lol").unwrap();
        assert_eq!(a.effective_kinds_for(&damus), vec![0u16]);
        assert_eq!(a.effective_kinds_for(&nos), vec![30063u16]);
    }

    /// Regression: `relay_connection_state` (and every other per-relay
    /// metric) is registered by the acquirer using `RelayUrl::as_str()`,
    /// which `url::Url` normalizes (e.g. trailing-slash insertion). The
    /// binary's `gauge_loop` must therefore *also* read those metrics with
    /// the normalized form. A previous version of the binary read the
    /// gauge using the raw config string, which silently registered a
    /// new zero-valued time series and made `/health` falsely report
    /// unhealthy. This test pins the normalization gap that motivates
    /// the `gauge_loop` wiring.
    #[test]
    fn acquirer_relays_are_normalized_not_raw_config_strings() {
        let cfg = base_cfg();
        // Raw config strings have no trailing slash.
        assert_eq!(cfg.relays[0].url, "wss://relay.damus.io");
        assert_eq!(cfg.relays[1].url, "wss://nos.lol");

        let a = acquirer_config_from(&cfg).unwrap();
        // url::Url canonicalizes — trailing slash is added.
        assert_eq!(a.relays[0].as_str(), "wss://relay.damus.io/");
        assert_eq!(a.relays[1].as_str(), "wss://nos.lol/");
        // Therefore raw strings DIFFER from normalized: anything reading
        // metrics with the raw form would address a different time series.
        for (raw, parsed) in cfg.relays.iter().zip(a.relays.iter()) {
            assert_ne!(
                raw.url, parsed.as_str(),
                "if these become equal, this regression test no longer guards \
                 the gauge-label parity invariant — revisit gauge_loop wiring"
            );
        }
    }
}
