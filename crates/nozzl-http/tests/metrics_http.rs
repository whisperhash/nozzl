//! Integration tests: drive `install_metrics` against a real bound socket and
//! hit the live HTTP endpoints with `reqwest`.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nozzl_http::{HealthSource, HttpError, install_metrics};

/// Mutable health probe — flips between healthy and unhealthy under test
/// control to exercise the `/health` 200/503 split.
struct ToggleHealth(AtomicBool);

impl ToggleHealth {
    fn new(initial: bool) -> Self {
        Self(AtomicBool::new(initial))
    }

    fn set(&self, healthy: bool) {
        self.0.store(healthy, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl HealthSource for ToggleHealth {
    async fn is_healthy(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

fn loopback_zero() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

/// Bind to an ephemeral port, ask the OS what it actually got, and reuse
/// that. Avoids racing two test processes against a hard-coded port.
fn pick_free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind(loopback_zero()).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_text() {
    let health = Arc::new(ToggleHealth::new(true));
    let addr = pick_free_addr();
    let (metrics, _registry, handle) = install_metrics(addr, health).expect("install");

    // Increment something so the body is non-trivial.
    metrics
        .events_received
        .with_label_values(&["wss://relay.example", "0", "profile-metadata"])
        .inc_by(3);
    metrics.channel_depth.set(7);

    let url = format!("http://{addr}/metrics");
    let response = reqwest::get(&url).await.expect("request");
    assert_eq!(response.status(), 200);
    let ct = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(ct, "text/plain; version=0.0.4");

    let body = response.text().await.unwrap();
    assert!(body.contains("events_received_total"), "body: {body}");
    assert!(
        body.contains(r#"relay="wss://relay.example""#),
        "body: {body}",
    );
    assert!(body.contains("channel_depth 7"), "body: {body}");

    handle.abort();
}

#[tokio::test]
async fn health_endpoint_flips() {
    let health = Arc::new(ToggleHealth::new(true));
    let addr = pick_free_addr();
    let (_metrics, _registry, handle) = install_metrics(addr, health.clone()).expect("install");

    let url = format!("http://{addr}/health");

    let response = reqwest::get(&url).await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), r#"{"status":"ok"}"#);

    health.set(false);

    let response = reqwest::get(&url).await.unwrap();
    assert_eq!(response.status(), 503);
    assert_eq!(response.text().await.unwrap(), r#"{"status":"unhealthy"}"#);

    handle.abort();
}

#[tokio::test]
async fn install_returns_bind_error_when_port_taken() {
    // Hold a listener on a port; install on the same address should fail.
    let std_listener = std::net::TcpListener::bind(loopback_zero()).unwrap();
    let addr = std_listener.local_addr().unwrap();
    let health = Arc::new(ToggleHealth::new(true));

    let err = install_metrics(addr, health).expect_err("bind must fail");
    assert!(matches!(err, HttpError::Bind { .. }), "got {err:?}");
}

#[tokio::test]
async fn registry_is_shared_so_callers_can_register_extra_metrics() {
    let health = Arc::new(ToggleHealth::new(true));
    let addr = pick_free_addr();
    let (_metrics, registry, handle) = install_metrics(addr, health).expect("install");

    // Register an additional counter through the returned registry handle.
    let extra = prometheus::IntCounter::new("extra_test_counter", "test").unwrap();
    registry.register(Box::new(extra.clone())).unwrap();
    extra.inc_by(42);

    let body = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("extra_test_counter 42"), "body: {body}");

    handle.abort();
}

#[tokio::test]
async fn concurrent_increments_produce_correct_total() {
    let health = Arc::new(ToggleHealth::new(true));
    let addr = pick_free_addr();
    let (metrics, _registry, handle) = install_metrics(addr, health).expect("install");

    const TASKS: usize = 64;
    const PER_TASK: u64 = 250;

    let mut joins = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let m = metrics.clone();
        joins.push(tokio::spawn(async move {
            let counter = m
                .events_received
                .with_label_values(&["wss://relay.example", "0", "profile-metadata"]);
            for _ in 0..PER_TASK {
                counter.inc();
            }
        }));
    }
    for j in joins {
        j.await.unwrap();
    }

    let body = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let expected = TASKS as u64 * PER_TASK;
    let needle = format!(
        r#"events_received_total{{kind="0",relay="wss://relay.example",slug="profile-metadata"}} {expected}"#,
    );
    assert!(body.contains(&needle), "missing `{needle}`; body:\n{body}");

    handle.abort();
}
