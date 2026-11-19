//! The metrics/health listener: `GET /metrics` (Prometheus exposition) and
//! `GET /health`.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use nozzl_metrics::Metrics;
use prometheus::{Encoder, Registry, TextEncoder};
use std::net::SocketAddr;
use tokio::task::JoinHandle;

use crate::{HealthSource, HttpError, bind_listener};

/// Spin up the HTTP endpoint on `addr`, serving `/metrics` and `/health`.
///
/// Returns the registered [`Metrics`] (callers record observations through it),
/// the underlying [`Registry`] (so the binary can register additional metrics
/// if needed), and a [`JoinHandle`] for the background server task.
///
/// The function binds synchronously before returning — once it returns
/// successfully, the listener is ready and the server task is running.
///
/// # Errors
/// - [`HttpError::Metrics`] if metric registration fails.
/// - [`HttpError::Bind`] if the listener cannot bind to `addr`.
//
// The triple return shape is mandated by the `install` contract in
// `docs/crates/nozzl-metrics.md`; pulling it into a type alias would obscure
// the public API more than help.
#[allow(clippy::type_complexity)]
pub fn install_metrics(
    addr: SocketAddr,
    health: Arc<dyn HealthSource>,
) -> Result<(Arc<Metrics>, Arc<Registry>, JoinHandle<()>), HttpError> {
    let registry = Arc::new(Registry::new());
    let metrics = Arc::new(Metrics::new(&registry)?);

    let listener = bind_listener(addr)?;
    let app = build_metrics_router(registry.clone(), health);
    let bound_addr = listener.local_addr().unwrap_or(addr);

    let handle = tokio::spawn(async move {
        tracing::info!(addr = %bound_addr, "metrics server listening");
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!(error = %err, "metrics server exited with error");
        }
    });

    Ok((metrics, registry, handle))
}

#[derive(Clone)]
struct AppState {
    registry: Arc<Registry>,
    health: Arc<dyn HealthSource>,
}

fn build_metrics_router(registry: Arc<Registry>, health: Arc<dyn HealthSource>) -> Router {
    let state = AppState { registry, health };
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(state)
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    let encoder = TextEncoder::new();
    let metric_families = state.registry.gather();
    let mut buffer = Vec::with_capacity(4096);
    if let Err(err) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(error = %err, "failed to encode prometheus metrics");
        return (StatusCode::INTERNAL_SERVER_ERROR, "encode error").into_response();
    }

    let mut response = (StatusCode::OK, buffer).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        // Static, valid header value — `from_static` cannot fail at runtime
        // for the prometheus text exposition content type.
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    response
}

async fn health_handler(State(state): State<AppState>) -> Response {
    if state.health.is_healthy().await {
        json_response(StatusCode::OK, r#"{"status":"ok"}"#)
    } else {
        json_response(StatusCode::SERVICE_UNAVAILABLE, r#"{"status":"unhealthy"}"#)
    }
}

fn json_response(status: StatusCode, body: &'static str) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal `HealthSource` for unit tests.
    struct FixedHealth(bool);

    #[async_trait::async_trait]
    impl HealthSource for FixedHealth {
        async fn is_healthy(&self) -> bool {
            self.0
        }
    }

    #[tokio::test]
    async fn router_metrics_endpoint_emits_prometheus_text() {
        let registry = Arc::new(Registry::new());
        let metrics = Metrics::new(&registry).unwrap();
        metrics
            .events_received
            .with_label_values(&["wss://r", "0", "profile-metadata"])
            .inc();

        let app = build_metrics_router(registry, Arc::new(FixedHealth(true)));

        // Build a Request without spinning up a TCP listener.
        let request = axum::http::Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = <Router as tower::ServiceExt<_>>::oneshot(app, request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; version=0.0.4",
        );

        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        assert!(body.contains("events_received_total"), "body: {body}");
        assert!(body.contains(r#"relay="wss://r""#), "body: {body}");
    }

    #[tokio::test]
    async fn metrics_endpoint_exposes_checkpoint_lag_after_update() {
        let registry = Arc::new(Registry::new());
        let metrics = Metrics::new(&registry).unwrap();
        nozzl_metrics::update_checkpoint_lag(
            &metrics,
            [(
                "wss://relay.damus.io/".to_string(),
                0_u16,
                "profile-metadata".to_string(),
                1_999_999_900_i64,
            )],
            2_000_000_000,
        );

        let app = build_metrics_router(registry, Arc::new(FixedHealth(true)));
        let request = axum::http::Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = <Router as tower::ServiceExt<_>>::oneshot(app, request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        assert!(
            body.contains(
                r#"checkpoint_lag_seconds{kind="0",relay="wss://relay.damus.io/",slug="profile-metadata"} 100"#
            ),
            "expected lag=100 series in /metrics output, body was:\n{body}"
        );
    }

    #[tokio::test]
    async fn router_health_flips_on_source() {
        let registry = Arc::new(Registry::new());
        let _metrics = Metrics::new(&registry).unwrap();

        for (healthy, expected_status, expected_body) in [
            (true, StatusCode::OK, r#"{"status":"ok"}"#),
            (
                false,
                StatusCode::SERVICE_UNAVAILABLE,
                r#"{"status":"unhealthy"}"#,
            ),
        ] {
            let app = build_metrics_router(registry.clone(), Arc::new(FixedHealth(healthy)));
            let request = axum::http::Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap();
            let response = <Router as tower::ServiceExt<_>>::oneshot(app, request)
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status);
            let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&bytes).unwrap(), expected_body);
        }
    }
}
