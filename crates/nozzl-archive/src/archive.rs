//! `Archive` trait and the `build_archive` helper.
//!
//! The default implementation is [`crate::demux::ChunkedArchive`], which
//! buffers events into Apache Parquet chunks behind a per-(class, kind, shard)
//! writer task model with a write-ahead log on local disk for crash
//! durability. See `docs/design.md` for the layout and the project's storage
//! plan for the rationale.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nostr::Event;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;

use crate::config::{ChunkWriterConfig, StorageBackend, StorageConfig};
use crate::error::{ArchiveError, PutOutcome};

/// Write-only archive interface. Reading from the trait is out of scope by
/// design; production analytics consume Parquet chunks via `DuckDB` / Athena /
/// polars over the directory tree, and tests use the small helper in
/// [`crate::reader`].
#[async_trait]
pub trait Archive: Send + Sync {
    /// Hand off an event to the archive.
    ///
    /// `slug` is the kind's slug from `kinds.toml`. It is **not** part of the
    /// on-disk path (the layout is slugless: `c={class}/k={kind}/...`); it is
    /// carried only as the writer task's metric label. Caller is responsible for
    /// ensuring the slug matches the validation rules in `nozzl-config`.
    ///
    /// Under chunked storage, returning [`PutOutcome::Written`] means the
    /// event has been accepted by the per-shard writer task's mpsc channel.
    /// The WAL fsync and Parquet roll happen asynchronously after this
    /// returns; see [`PutOutcome::Written`] for the durability caveats.
    /// Returns [`PutOutcome::Skipped`] for kinds outside the archive scope
    /// (regular / ephemeral).
    async fn put_event(&self, event: &Event, slug: &str) -> Result<PutOutcome, ArchiveError>;
}

/// Retry-window cap for the S3 backend (`RetryConfig::retry_timeout`).
///
/// `object_store`'s default is 180s, longer than the process shutdown budget
/// (`SHUTDOWN_BUDGET` = 120s in `nozzl::run`), so a *retriable* PUT that keeps
/// failing during shutdown could spin the retry loop past the budget. Capping it
/// keeps a degraded-S3 shutdown from over-running on retries. Deliberately honest
/// about what this is NOT:
/// - It does **nothing** for the `error sending request` failures of 2026-07-01
///   or 2026-10-15, because the retry loop never engages for them at all. Those
///   are connect timeouts, which `object_store` classifies
///   `HttpErrorKind::Timeout` and retries only for *idempotent* requests -
///   and its S3 client does not mark a `PutMode::Create` PUT idempotent, unlike
///   `PutMode::Overwrite` (verified against `object_store` 0.13.2: the same
///   blackholed PUT fails in 5.00s with no retry under `Create` and retries 10
///   times under `Overwrite`). An earlier version of this comment blamed
///   `HttpErrorKind::Unknown`, which was inferred from the error text; the
///   2026-10-16 comment on #30 (Shutdown flush starves the tokio runtime)
///   corrects it. Those failures were fixed by bounding the flush fan-out
///   ([`crate::demux::ChunkedArchive`]'s flush permits), spreading the
///   buffer-age deadline, and raising [`S3_CONNECT_TIMEOUT`] - not by retry
///   configuration.
/// - It does **not** make the shutdown budget airtight: `retry_timeout` is
///   checked *between* attempts, so a final attempt can still run its 30s request
///   timeout on top, and `SHUTDOWN_BUDGET` wraps the whole shutdown sequence.
/// - It is a **coarse global lever** - it applies to every S3 op (steady-state
///   ingest PUTs + `scan_resident_usage` LISTs), shrinking their brownout
///   ride-out window too, for no steady-state benefit. A finer, shutdown-scoped
///   per-PUT `tokio::time::timeout` was considered as a replacement but declined:
///   no shutdown has ever exceeded the budget, so both levers are held pending an
///   actual budget-overrun signal: `elapsed_ms` on the shutdown-summary log,
///   the archive-flush portion of the budget, recorded in #26 (Unbounded
///   shutdown-flush fan-out fails many S3 PUTs at once).
///
/// A capped-out PUT is safe regardless: its rows stay in the WAL and replay on
/// the next boot. The per-request timeout keeps its `object_store` default
/// (30s) - tightening it would risk a legitimate <=64MB chunk PUT on the hot
/// path; the connect timeout does not (see [`S3_CONNECT_TIMEOUT`]).
const S3_RETRY_TIMEOUT: Duration = Duration::from_secs(60);

/// Connect timeout for the S3 backend, raised from `object_store`'s 5s default.
///
/// **Why.** The 5s default is what turns a CPU stall into a hard flush failure.
/// Every one of the 556 failed chunk PUTs in dev's logging history (2026-06-23
/// to 2026-10-15) reported an elapsed of at least 5.03s, and `RetryError`'s
/// `Display` shows none of them retried: a connect timeout carries
/// `io::ErrorKind::TimedOut`, so `object_store` classifies it
/// `HttpErrorKind::Timeout`, and its retry loop retries that kind only for
/// *idempotent* requests. Chunks are written with `PutMode::Create`, which
/// `object_store`'s S3 client does not mark idempotent (unlike
/// `PutMode::Overwrite`), so the retry loop never engages. The failures were
/// self-inflicted: a herd of simultaneous flushes saturating the host's CPUs with
/// inline ZSTD-9 while ~600 TLS handshakes competed for the same CPU, which is
/// #30 (Shutdown flush starves the tokio runtime) and #57 (Restart-synchronized
/// buffer-age flush burst). A handshake that loses that race for 5s is not a
/// sick connection.
///
/// **Why 15s and not more.** A healthy connect to S3 from a host in the bucket's region is ~20ms,
/// so this costs nothing when the host is not saturated, and it sits under the
/// 30s per-request timeout, so it cannot be the longest wait a flush can take.
/// It is deliberately *insurance*, not the fix: the bounded flush fan-out and
/// the jittered buffer-age deadline are what stop the herd from forming. Making
/// S3 failures retriable by switching to `PutMode::Overwrite` was considered
/// and declined on #57 (Restart-synchronized buffer-age flush burst): S3's own
/// failures already retry, because status-code retries ignore idempotency, and
/// in four months exactly one hard flush failure was externally caused.
const S3_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Apply [`S3_CONNECT_TIMEOUT`] to an `AmazonS3Builder` without disturbing any
/// other client option.
///
/// The obvious spelling,
/// `.with_client_options(ClientOptions::new().with_connect_timeout(..))`, is
/// **wrong here**: `with_client_options` is `self.client_options = options`
/// (`object_store` 0.13.2 `src/aws/builder.rs:999`), so it discards whatever
/// `from_env()` resolved into `client_options`. That call does route
/// `AWS_*` vars there - an explicit `aws_allow_http` arm plus a catch-all that
/// strips the `aws_` prefix and parses the rest as a `ClientConfigKey` - so
/// `AWS_PROXY_URL`, `AWS_ALLOW_HTTP`, `AWS_TIMEOUT` and
/// `AWS_POOL_IDLE_TIMEOUT` would be read and then silently thrown away. A
/// deployment using the standard credential chain sets none of them, so this
/// is latent rather than live - but the next person who points
/// a deployment at a `MinIO` or `localstack` endpoint with `AWS_ALLOW_HTTP=true`
/// would get an unexplained failure.
///
/// Setting the single key leaves every sibling option intact.
/// `connect_timeout` is stored as a deferred string and parsed by `humantime`,
/// hence the `{secs}s` formatting rather than a typed `Duration`.
fn with_s3_connect_timeout(
    builder: object_store::aws::AmazonS3Builder,
) -> object_store::aws::AmazonS3Builder {
    builder.with_config(
        object_store::aws::AmazonS3ConfigKey::Client(
            object_store::ClientConfigKey::ConnectTimeout,
        ),
        format!("{}s", S3_CONNECT_TIMEOUT.as_secs()),
    )
}

/// Build the underlying `ObjectStore` for a `StorageConfig`. Used by
/// [`build_archive`] (and exposed for callers that want to construct a
/// `ChunkedArchive` directly with non-default chunk-writer config).
pub fn build_object_store(cfg: &StorageConfig) -> Result<Arc<dyn ObjectStore>, ArchiveError> {
    let store: Arc<dyn ObjectStore> = match cfg.backend {
        StorageBackend::Local => {
            let local = cfg.local.as_ref().ok_or_else(|| {
                ArchiveError::Config("backend=local but [storage.local] missing".into())
            })?;
            // `LocalFileSystem::new_with_prefix` canonicalizes the path, which
            // fails if the directory doesn't exist yet. Bootstrapping a fresh
            // deployment (or running after a `data/` wipe) is a supported
            // operation, so create the prefix on demand. Idempotent: a no-op
            // when the directory is already there.
            std::fs::create_dir_all(&local.root).map_err(|e| {
                ArchiveError::Other(format!(
                    "creating storage root {}: {e}",
                    local.root.display()
                ))
            })?;
            let fs = object_store::local::LocalFileSystem::new_with_prefix(&local.root)
                .map_err(ArchiveError::from)?;
            Arc::new(fs)
        }
        StorageBackend::S3 => {
            let s3cfg = cfg.s3.as_ref().ok_or_else(|| {
                ArchiveError::Config("backend=s3 but [storage.s3] missing".into())
            })?;
            // Credentials are intentionally NOT taken from `s3cfg` - the
            // standard AWS chain (env / IAM / `~/.aws/credentials`) is
            // consulted via `from_env()`. `retry_timeout` is capped below the
            // shutdown budget (see `S3_RETRY_TIMEOUT`) and the connect timeout
            // is raised off its 5s default (see `S3_CONNECT_TIMEOUT`); every
            // other client option keeps whatever `from_env()` resolved, which
            // is why the connect timeout is set through `with_connect_timeout`
            // below rather than `with_client_options`.
            let s3 = with_s3_connect_timeout(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(&s3cfg.bucket)
                    .with_region(&s3cfg.region)
                    .with_retry(object_store::RetryConfig {
                        retry_timeout: S3_RETRY_TIMEOUT,
                        ..Default::default()
                    }),
            )
            .build()
            .map_err(ArchiveError::from)?;
            if s3cfg.prefix.is_empty() {
                Arc::new(s3)
            } else {
                let prefix = ObjectPath::from(s3cfg.prefix.as_str());
                Arc::new(object_store::prefix::PrefixStore::new(s3, prefix))
            }
        }
    };
    Ok(store)
}

/// Build a chunked archive from a `StorageConfig`.
///
/// `wal_root` is the local-disk directory for WAL files (independent of the
/// backend; even with S3 storage, WAL stays local). For local-FS backends a
/// natural choice is the same directory as `cfg.local.root`.
pub fn build_archive(
    cfg: &StorageConfig,
    wal_root: std::path::PathBuf,
    chunk_cfg: ChunkWriterConfig,
    metrics: Option<Arc<nozzl_metrics::Metrics>>,
) -> Result<Arc<crate::demux::ChunkedArchive>, ArchiveError> {
    let store = build_object_store(cfg)?;
    Ok(Arc::new(crate::demux::ChunkedArchive::new(
        store, wal_root, chunk_cfg, metrics,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LocalConfig;
    use std::path::PathBuf;

    /// D5 regression: applying the connect timeout must not discard the other
    /// client options `from_env()` resolved. The `ProxyUrl` below stands in for
    /// an `AWS_PROXY_URL`; under the
    /// `with_client_options(ClientOptions::new()...)` spelling it comes back
    /// `None` and this test fails, which is the whole point of the helper.
    #[test]
    fn s3_connect_timeout_preserves_other_client_options() {
        use object_store::ClientConfigKey;
        use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};

        const PROXY: &str = "http://proxy.invalid:3128";
        let builder = with_s3_connect_timeout(AmazonS3Builder::new().with_config(
            AmazonS3ConfigKey::Client(ClientConfigKey::ProxyUrl),
            PROXY,
        ));

        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Client(
                ClientConfigKey::ConnectTimeout
            )),
            Some("15s".to_string()),
            "the connect timeout must be applied, and must match S3_CONNECT_TIMEOUT"
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Client(ClientConfigKey::ProxyUrl)),
            Some(PROXY.to_string()),
            "applying the connect timeout discarded a sibling client option"
        );
    }

    /// The formatted string has to agree with the constant, and has to be a
    /// shape `humantime` accepts. `object_store` stores `connect_timeout` as a
    /// deferred string parsed at *use*, not at build, so a malformed value
    /// would otherwise surface as a runtime S3 error rather than a test
    /// failure. `{secs}s` is within humantime's grammar; this pins the
    /// whole-seconds assumption that makes it so.
    #[test]
    fn s3_connect_timeout_string_matches_the_constant() {
        assert_eq!(S3_CONNECT_TIMEOUT, Duration::from_secs(15));
        assert_eq!(
            S3_CONNECT_TIMEOUT.subsec_nanos(),
            0,
            "a sub-second connect timeout would format as a truncated `{{secs}}s`"
        );
        assert_eq!(format!("{}s", S3_CONNECT_TIMEOUT.as_secs()), "15s");
    }

    #[test]
    fn build_archive_local_missing_section_errors() {
        let cfg = StorageConfig {
            backend: StorageBackend::Local,
            local: None,
            s3: None,
        };
        match build_archive(&cfg, PathBuf::from("/tmp"), ChunkWriterConfig::default(), None) {
            Err(ArchiveError::Config(_)) => {}
            Ok(_) => panic!("expected config error, got Ok"),
            Err(e) => panic!("expected Config error, got {e:?}"),
        }
    }

    #[test]
    fn build_archive_s3_missing_section_errors() {
        let cfg = StorageConfig {
            backend: StorageBackend::S3,
            local: None,
            s3: None,
        };
        match build_archive(&cfg, PathBuf::from("/tmp"), ChunkWriterConfig::default(), None) {
            Err(ArchiveError::Config(_)) => {}
            Ok(_) => panic!("expected config error, got Ok"),
            Err(e) => panic!("expected Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn build_archive_local_succeeds() {
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
        .expect("should build local archive");
        archive.shutdown().await;
    }

    /// Regression: a fresh deployment (or a `data/` wipe) leaves the
    /// configured storage root absent. `build_archive` must create it on
    /// demand instead of returning a `canonicalize` error from `LocalFileSystem`.
    #[tokio::test]
    async fn build_archive_local_creates_missing_root() {
        let dir = tempfile::tempdir().unwrap();
        let missing_root = dir.path().join("nested/does/not/exist/yet");
        assert!(!missing_root.exists(), "precondition: root should be absent");
        let cfg = StorageConfig {
            backend: StorageBackend::Local,
            local: Some(LocalConfig {
                root: missing_root.clone(),
            }),
            s3: None,
        };
        let archive = build_archive(
            &cfg,
            missing_root.clone(),
            ChunkWriterConfig::default(),
            None,
        )
        .expect("should build local archive even when root is missing");
        assert!(missing_root.is_dir(), "root should be created on demand");
        archive.shutdown().await;
    }
}
