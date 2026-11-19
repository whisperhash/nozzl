//! nozzl's HTTP surface — every route the service exposes, in one crate.
//!
//! # Route manifest (the whole API surface)
//!
//! Two independent listeners, bound to two different addresses, so the
//! mutating control plane stays off the Prometheus scrape port:
//!
//! | Listener | Bind (config)                     | Method + path          | Handler |
//! |----------|-----------------------------------|------------------------|---------|
//! | metrics  | `observability.metrics_addr`      | `GET  /metrics`        | [`metrics_server::install_metrics`] |
//! | metrics  | `observability.metrics_addr`      | `GET  /health`         | [`metrics_server::install_metrics`] |
//! | admin    | `observability.admin_addr` (opt)  | `POST /admin/flush/{kind}` | [`admin_server::install_admin`] |
//! | admin    | `observability.admin_addr` (opt)  | `GET  /export/{kind}`  | [`admin_server::install_admin`] |
//!
//! The admin listener is **opt-in**: absent `admin_addr` ⇒ no admin server is
//! started. It is unauthenticated, and anything on a deployment's network can
//! reach it, so every route on it must stay
//! non-destructive, resource-bounded, and strongly typed — see the invariant
//! comment on [`admin_server::build_admin_router`].
//!
//! # Why this crate exists
//!
//! Keeping both routers here (rather than scattered across `nozzl-metrics` and
//! the binary) means the full API surface is inspectable in one place. The
//! crate depends *downward* only: on `nozzl-metrics` for the metric registry
//! and on `nozzl-archive` for the [`nozzl_archive::FlushControl`] contract the
//! admin endpoint drives.

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use std::net::SocketAddr;

pub mod admin_server;
pub mod metrics_server;

pub use admin_server::install_admin;
pub use metrics_server::install_metrics;

/// Probe used by `GET /health`. Implemented by the binary; this crate does not
/// know what "healthy" means in business terms.
#[async_trait::async_trait]
pub trait HealthSource: Send + Sync {
    /// Return `true` for HTTP 200, `false` for HTTP 503.
    async fn is_healthy(&self) -> bool;
}

/// Errors surfaced while standing up an HTTP listener.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// A metric failed to register against the registry (metrics server only).
    #[error(transparent)]
    Metrics(#[from] nozzl_metrics::MetricsError),

    /// The HTTP server could not bind to the requested socket address.
    #[error("failed to bind HTTP server to {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}

/// Bind `addr` synchronously via std (so a bind failure surfaces before we
/// return), set non-blocking, and hand the listener to tokio for async accept.
/// Shared by both listeners.
fn bind_listener(addr: SocketAddr) -> Result<tokio::net::TcpListener, HttpError> {
    let std_listener =
        std::net::TcpListener::bind(addr).map_err(|source| HttpError::Bind { addr, source })?;
    std_listener
        .set_nonblocking(true)
        .map_err(|source| HttpError::Bind { addr, source })?;
    tokio::net::TcpListener::from_std(std_listener).map_err(|source| HttpError::Bind { addr, source })
}
