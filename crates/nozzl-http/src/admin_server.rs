//! The admin/control + extraction listener:
//! - `POST /admin/flush/{kind}` — publish a kind's buffered events on demand
//!   (drives the archive's [`FlushControl`] contract) so freshly-archived parquet
//!   is queryable without waiting for the buffer-age roll.
//! - `GET /export/{kind}` — stream a kind's archived events as newline-delimited
//!   NIP-01 JSON via the duckdb CLI (read-only; reads committed parquet only).
//!
//! Opt-in (started only when `observability.admin_addr` is set). Both routes are
//! non-destructive; see the invariant comment on [`build_admin_router`] before
//! adding anything here, since this listener may be reachable beyond loopback and
//! is unauthenticated.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use nozzl_archive::{FlushControl, FlushSummary};
use nozzl_export::{ExportError, ExportMode, ExportParams, spawn_export};
use prometheus::IntCounterVec;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::{HttpError, bind_listener};

/// Shared state for the admin router.
#[derive(Clone)]
struct AdminState {
    flush: Arc<dyn FlushControl>,
    /// Hot-reload-tracked configured kind→slug map (reused from the binary, so
    /// a kind added/removed via `kinds.toml` reload validates correctly).
    configured_kinds: Arc<RwLock<HashMap<u16, String>>>,
    /// Per-request flush timeout (a constant from the binary; short in tests).
    flush_timeout: Duration,
    /// Local Parquet archive root for `GET /export/{kind}`. `None` when the
    /// service has no local archive (e.g. an S3-only deploy) — export then 503s,
    /// but flush stays available.
    export_root: Option<PathBuf>,
    /// Path to the `duckdb` CLI the export handler shells out to.
    duckdb_bin: PathBuf,
    /// Global single-flight gate: at most one export runs at a time. A second
    /// concurrent request gets 429. The permit is held for the whole stream.
    export_sem: Arc<Semaphore>,
    /// `export_requests_total{kind,outcome}` — incremented per export outcome.
    export_metric: IntCounterVec,
}

/// Spin up the admin HTTP endpoint on `addr`, serving `POST /admin/flush/{kind}`.
///
/// Binds synchronously before returning. Returns a [`JoinHandle`] for the
/// background server task.
///
/// # Errors
/// - [`HttpError::Bind`] if the listener cannot bind to `addr`.
// `configured_kinds` is the binary's existing hot-reload mirror (a concrete
// `HashMap<u16, String>` with the default hasher); generalizing over the hasher
// would force the caller to name a type parameter for no benefit.
#[allow(clippy::implicit_hasher)]
pub fn install_admin(
    addr: SocketAddr,
    flush: Arc<dyn FlushControl>,
    configured_kinds: Arc<RwLock<HashMap<u16, String>>>,
    flush_timeout: Duration,
    export_root: Option<PathBuf>,
    duckdb_bin: PathBuf,
    export_metric: IntCounterVec,
) -> Result<JoinHandle<()>, HttpError> {
    let listener = bind_listener(addr)?;
    let bound_addr = listener.local_addr().unwrap_or(addr);
    let app = build_admin_router(AdminState {
        flush,
        configured_kinds,
        flush_timeout,
        export_root,
        duckdb_bin,
        export_sem: Arc::new(Semaphore::new(1)),
        export_metric,
    });

    // `into_make_service_with_connect_info` so the export handler can log the
    // requesting peer. Harmless for the flush route.
    let handle = tokio::spawn(async move {
        tracing::info!(addr = %bound_addr, "admin server listening");
        let svc = app.into_make_service_with_connect_info::<SocketAddr>();
        if let Err(err) = axum::serve(listener, svc).await {
            tracing::error!(error = %err, "admin server exited with error");
        }
    });

    Ok(handle)
}

fn build_admin_router(state: AdminState) -> Router {
    // INVARIANT — this listener may be reachable beyond loopback and
    // is UNAUTHENTICATED. Every route mounted here MUST be:
    //   * non-destructive  — never deletes/mutates archived event data. (flush only
    //     publishes already-buffered events; export is read-only.)
    //   * resource-bounded — e.g. export's global single-flight semaphore caps
    //     concurrent duckdb processes at one.
    //   * strongly typed at the edge — `Path<u16>`, enum query params; no free-form
    //     string reaches a query/shell.
    // Any future *destructive* operation (purge, config writes, deleting compaction)
    // must NOT be added here — put it on a separate loopback-only listener.
    Router::new()
        .route("/admin/flush/{kind}", post(flush_handler))
        .route("/export/{kind}", get(export_handler))
        .with_state(state)
}

/// Query string for `GET /export/{kind}`.
#[derive(Deserialize)]
struct ExportQuery {
    /// `latest` (default) or `history`. Unknown values are a 400.
    mode: Option<String>,
}

/// `GET /export/{kind}?mode=latest|history` — stream a kind's archived events as
/// newline-delimited bare NIP-01 JSON (`application/x-ndjson`) via the duckdb CLI.
/// Reads committed Parquet only; it does **not** flush (freshness is the operator's
/// separate call). Canonical contract: `docs/design.md` §5.D10 (Export endpoint).
///
/// Status mapping (all resolved before the 200 body starts):
/// - `400` — non-numeric `{kind}` (path extractor) or unknown `mode`.
/// - `404` — no parquet on disk for `kind` (unconfigured, ephemeral, or only
///   WAL-buffered so far). Note: a kind that exists on disk but matches zero rows
///   streams `200` with an **empty body**, not 404.
/// - `429` — another export is already in flight (`Retry-After` set).
/// - `500` — duckdb failed to spawn.
/// - `503` — this deploy has no local archive root (export unsupported here).
/// - `200` — streaming. A mid-stream duckdb failure truncates the body (status is
///   already committed); the cause is logged by the stderr drainer.
async fn export_handler(
    State(state): State<AdminState>,
    Path(kind): Path<u16>,
    Query(q): Query<ExportQuery>,
    // Populated in prod by `into_make_service_with_connect_info`; the oneshot tests
    // inject it into request extensions (see the `get` test helper).
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
) -> Response {
    let kind_label = kind.to_string();
    let record = |outcome: &str| {
        state
            .export_metric
            .with_label_values(&[kind_label.as_str(), outcome])
            .inc();
    };

    let mode = match q.mode.as_deref() {
        None => ExportMode::Latest,
        Some(s) => match s.parse::<ExportMode>() {
            Ok(m) => m,
            Err(()) => {
                // A client typo (400); not one of the counted outcomes.
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "invalid mode", "mode": s })),
                )
                    .into_response();
            }
        },
    };

    // Acquire the global single-flight permit before anything expensive (incl. the
    // filesystem scan), so at most one export ever touches the disk/duckdb at once.
    let Ok(permit) = Arc::clone(&state.export_sem).try_acquire_owned() else {
        tracing::warn!(kind, ?mode, %peer, "export rejected: another export already in flight");
        record("rejected_429");
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, "30")],
            Json(json!({ "error": "export already in progress" })),
        )
            .into_response();
    };

    let Some(export_root) = state.export_root.clone() else {
        tracing::warn!(kind, "export unavailable: no local archive root configured");
        // Distinct from a duckdb fault: this deploy structurally can't export
        // (e.g. S3-only, no local parquet). Don't let it look like duckdb broke.
        record("unsupported");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "export unavailable: no local archive" })),
        )
            .into_response();
    };

    let params = ExportParams {
        archive_root: export_root,
        kind,
        mode,
        duckdb_bin: state.duckdb_bin.clone(),
    };

    // `spawn_export` takes the permit by value: it moves into the stream only on
    // success, so any early error drops it and frees the single-flight slot.
    match spawn_export(params, Some(permit)) {
        Ok(stream) => {
            tracing::info!(kind, ?mode, %peer, "export stream started");
            record("started");
            (
                [(header::CONTENT_TYPE, "application/x-ndjson")],
                Body::from_stream(stream),
            )
                .into_response()
        }
        Err(ExportError::NotFound { .. } | ExportError::Ephemeral { .. }) => {
            tracing::info!(kind, %peer, "export: no archive on disk for kind");
            record("not_found");
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "no archive for kind", "kind": kind })),
            )
                .into_response()
        }
        Err(ExportError::Spawn { .. }) => {
            tracing::error!(kind, "export failed: could not spawn duckdb");
            record("error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "export engine unavailable" })),
            )
                .into_response()
        }
    }
}

/// `POST /admin/flush/{kind}` — publish `kind`'s buffered events now.
///
/// Status mapping:
/// - `400` — non-numeric `{kind}` (rejected by the path extractor).
/// - `404` — `kind` is not currently configured for acquisition.
/// - `200` — flush serviced; every signaled shard confirmed (incl. a
///   configured-but-idle kind with `writers_signaled: 0`).
/// - `502` — at least one shard's publish failed (rows stay durable in WAL).
/// - `504` — at least one shard did not confirm within the timeout (also still
///   durable in WAL; the publish completes shortly after).
///
/// `502` takes precedence over `504` when both occur: a definite failure is the
/// more actionable signal.
async fn flush_handler(State(state): State<AdminState>, Path(kind): Path<u16>) -> Response {
    let configured = state
        .configured_kinds
        .read()
        .is_ok_and(|g| g.contains_key(&kind));
    if !configured {
        tracing::warn!(kind, "ad-hoc flush rejected: kind not configured for acquisition");
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unconfigured kind", "kind": kind })),
        )
            .into_response();
    }

    let summary = state.flush.flush_kind(kind, state.flush_timeout).await;
    let status = if summary.errors.is_empty() {
        if summary.timed_out > 0 {
            StatusCode::GATEWAY_TIMEOUT
        } else {
            StatusCode::OK
        }
    } else {
        StatusCode::BAD_GATEWAY
    };
    (status, Json(summary_json(&summary))).into_response()
}

fn summary_json(summary: &FlushSummary) -> serde_json::Value {
    json!({
        "kind": summary.kind,
        "writers_signaled": summary.writers_signaled,
        "shards_published": summary.shards_published,
        "rows": summary.rows,
        "timed_out": summary.timed_out,
        "errors": summary
            .errors
            .iter()
            .map(|e| json!({
                "shard": e.shard.to_string(),
                "message": e.message,
            }))
            .collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use nozzl_archive::ShardError;

    /// Mock `FlushControl` returning a canned summary, so handler status
    /// mapping is tested without a real archive.
    struct MockFlush(FlushSummary);

    #[async_trait]
    impl FlushControl for MockFlush {
        async fn flush_kind(&self, _kind: u16, _timeout: Duration) -> FlushSummary {
            self.0.clone()
        }
    }

    fn summary(
        kind: u16,
        writers: usize,
        published: usize,
        rows: u64,
        timed_out: usize,
        errors: Vec<ShardError>,
    ) -> FlushSummary {
        FlushSummary {
            kind,
            writers_signaled: writers,
            shards_published: published,
            rows,
            timed_out,
            errors,
        }
    }

    fn kinds(set: &[(u16, &str)]) -> Arc<RwLock<HashMap<u16, String>>> {
        Arc::new(RwLock::new(
            set.iter().map(|(k, s)| (*k, (*s).to_string())).collect(),
        ))
    }

    fn test_export_metric() -> IntCounterVec {
        IntCounterVec::new(
            prometheus::Opts::new("export_requests_total", "test"),
            &["kind", "outcome"],
        )
        .unwrap()
    }

    fn router(flush: FlushSummary, configured: Arc<RwLock<HashMap<u16, String>>>) -> Router {
        build_admin_router(AdminState {
            flush: Arc::new(MockFlush(flush)),
            configured_kinds: configured,
            flush_timeout: Duration::from_secs(1),
            export_root: None,
            duckdb_bin: PathBuf::from("duckdb"),
            export_sem: Arc::new(Semaphore::new(1)),
            export_metric: test_export_metric(),
        })
    }

    /// Build an admin router wired for export tests, with an explicit archive root
    /// and single-flight semaphore (so a test can pre-acquire the permit).
    fn export_router(export_root: Option<PathBuf>, export_sem: Arc<Semaphore>) -> Router {
        build_admin_router(AdminState {
            flush: Arc::new(MockFlush(summary(0, 0, 0, 0, 0, vec![]))),
            configured_kinds: kinds(&[]),
            flush_timeout: Duration::from_secs(1),
            export_root,
            duckdb_bin: PathBuf::from("duckdb"),
            export_sem,
            export_metric: test_export_metric(),
        })
    }

    /// Issue a GET and return (status, headers, parsed-json-body-or-Null).
    /// Injects a `ConnectInfo` extension since `oneshot` bypasses the connect-info
    /// make-service the real server uses; without it the handler's extractor 500s.
    async fn get(app: Router, uri: &str) -> (StatusCode, axum::http::HeaderMap, serde_json::Value) {
        let mut request = axum::http::Request::builder()
            .method("GET")
            .uri(uri)
            .body(axum::body::Body::empty())
            .unwrap();
        request.extensions_mut().insert(ConnectInfo(
            "127.0.0.1:50000".parse::<SocketAddr>().unwrap(),
        ));
        let response = <Router as tower::ServiceExt<_>>::oneshot(app, request)
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, headers, body)
    }

    async fn post(app: Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(uri)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = <Router as tower::ServiceExt<_>>::oneshot(app, request)
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, body)
    }

    #[tokio::test]
    async fn configured_kind_published_returns_200_with_summary() {
        let app = router(
            summary(0, 16, 3, 42, 0, vec![]),
            kinds(&[(0, "profile-metadata")]),
        );
        let (status, body) = post(app, "/admin/flush/0").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["rows"], 42);
        assert_eq!(body["shards_published"], 3);
        assert_eq!(body["writers_signaled"], 16);
    }

    #[tokio::test]
    async fn configured_but_idle_kind_returns_200_with_zero_writers() {
        let app = router(summary(0, 0, 0, 0, 0, vec![]), kinds(&[(0, "profile-metadata")]));
        let (status, body) = post(app, "/admin/flush/0").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["writers_signaled"], 0);
        assert_eq!(body["rows"], 0);
    }

    #[tokio::test]
    async fn unconfigured_kind_returns_404() {
        let app = router(summary(7, 0, 0, 0, 0, vec![]), kinds(&[(0, "profile-metadata")]));
        let (status, body) = post(app, "/admin/flush/7").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unconfigured kind");
    }

    #[tokio::test]
    async fn non_numeric_kind_returns_400() {
        let app = router(summary(0, 0, 0, 0, 0, vec![]), kinds(&[(0, "profile-metadata")]));
        let (status, _body) = post(app, "/admin/flush/notakind").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn shard_error_returns_502() {
        let app = router(
            summary(
                0,
                16,
                15,
                100,
                0,
                vec![ShardError {
                    shard: 'a',
                    message: "object store unavailable".to_string(),
                }],
            ),
            kinds(&[(0, "profile-metadata")]),
        );
        let (status, body) = post(app, "/admin/flush/0").await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["errors"][0]["shard"], "a");
        assert_eq!(body["errors"][0]["message"], "object store unavailable");
    }

    #[tokio::test]
    async fn timeout_returns_504() {
        let app = router(summary(0, 16, 10, 50, 6, vec![]), kinds(&[(0, "profile-metadata")]));
        let (status, body) = post(app, "/admin/flush/0").await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(body["timed_out"], 6);
    }

    #[tokio::test]
    async fn error_takes_precedence_over_timeout() {
        let app = router(
            summary(
                0,
                16,
                14,
                90,
                1,
                vec![ShardError {
                    shard: 'b',
                    message: "put failed".to_string(),
                }],
            ),
            kinds(&[(0, "profile-metadata")]),
        );
        let (status, _body) = post(app, "/admin/flush/0").await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn validation_tracks_hot_reload_of_configured_kinds() {
        let configured = kinds(&[(0, "profile-metadata")]);

        // Kind 30023 not configured yet → 404.
        let (status, _) = post(
            router(summary(30023, 0, 0, 0, 0, vec![]), configured.clone()),
            "/admin/flush/30023",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Simulate a kinds.toml hot-reload that adds 30023 and removes 0.
        {
            let mut g = configured.write().unwrap();
            g.clear();
            g.insert(30023, "long-form-content".to_string());
        }

        // Now 30023 validates, and 0 has become unconfigured → 404.
        let (status, _) = post(
            router(summary(30023, 0, 0, 0, 0, vec![]), configured.clone()),
            "/admin/flush/30023",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = post(
            router(summary(0, 0, 0, 0, 0, vec![]), configured.clone()),
            "/admin/flush/0",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // ----- GET /export/{kind} status mapping (no duckdb needed) -----

    #[tokio::test]
    async fn export_invalid_mode_returns_400() {
        // mode is parsed before anything else, so export_root/duckdb don't matter.
        let app = export_router(None, Arc::new(Semaphore::new(1)));
        let (status, _h, body) = get(app, "/export/10002?mode=bogus").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid mode");
    }

    #[tokio::test]
    async fn export_non_numeric_kind_returns_400() {
        let app = export_router(None, Arc::new(Semaphore::new(1)));
        let (status, _h, _b) = get(app, "/export/notakind").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn export_in_flight_returns_429_with_retry_after() {
        // Pre-acquire the single permit so the request can't get one.
        let sem = Arc::new(Semaphore::new(1));
        let _held = Arc::clone(&sem).try_acquire_owned().unwrap();
        let app = export_router(None, sem);
        let (status, headers, body) = get(app, "/export/10002?mode=latest").await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(headers.get(header::RETRY_AFTER).unwrap(), "30");
        assert_eq!(body["error"], "export already in progress");
    }

    #[tokio::test]
    async fn export_no_local_archive_returns_503() {
        // Valid request, permit available, but no archive root configured.
        let app = export_router(None, Arc::new(Semaphore::new(1)));
        let (status, _h, body) = get(app, "/export/10002").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "export unavailable: no local archive");
    }

    #[tokio::test]
    async fn export_kind_absent_on_disk_returns_404() {
        // Empty archive root → the filesystem pre-check fails before any duckdb spawn.
        let dir = tempfile::tempdir().unwrap();
        let app = export_router(Some(dir.path().to_path_buf()), Arc::new(Semaphore::new(1)));
        let (status, _h, body) = get(app, "/export/10002").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "no archive for kind");
    }

    #[tokio::test]
    async fn export_ephemeral_kind_returns_404() {
        let dir = tempfile::tempdir().unwrap();
        let app = export_router(Some(dir.path().to_path_buf()), Arc::new(Semaphore::new(1)));
        let (status, _h, _b) = get(app, "/export/22500").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn export_permit_released_after_404_allows_next_request() {
        // A 404 must drop the permit so a subsequent export isn't wedged at 429.
        let dir = tempfile::tempdir().unwrap();
        let sem = Arc::new(Semaphore::new(1));
        let app = export_router(Some(dir.path().to_path_buf()), Arc::clone(&sem));
        let (status, _h, _b) = get(app, "/export/10002").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        // Permit is back: a fresh try_acquire succeeds.
        assert!(sem.try_acquire_owned().is_ok(), "permit leaked after early-error");
    }
}
