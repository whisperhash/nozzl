//! End-to-end tests for `nozzl-test-relay`. Most use `tokio-tungstenite`
//! directly so we can assert the exact JSON wire frames the relay produces.
//! One test connects via `nostr-relay-pool` to confirm the relay is honest
//! enough for downstream integration tests.

#![warn(clippy::pedantic)]

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use nostr::{EventBuilder, Filter, Keys, Kind, RelayMessage};
use nostr_relay_pool::{RelayOptions, RelayPool, RelayPoolNotification, SubscribeOptions};
use nozzl_test_relay::TestRelay;
use serde_json::{Value, json};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

/// Deadline for *positive* waits — reads where a frame (EVENT/EOSE/NOTICE/
/// CLOSED/Close) is expected to arrive. In the happy path the frame lands in
/// well under a millisecond, so this bound is only ever consumed when
/// something is actually broken; setting it generously turns a hang into a
/// clean failure without slowing passing runs. It is deliberately large
/// because CI runs the suite only under `cargo llvm-cov` instrumentation (the
/// `test + coverage` job), where a tight 2s bound flaked (CI, 2026-06-03).
/// NOTE: this does *not* apply to the negative-assertion reads
/// below (the `timeout(200ms, …); assert!(res.is_err())` checks that prove a
/// message is absent) — those must stay short by design.
const RECV_TIMEOUT: Duration = Duration::from_secs(30);

/// Helper: build and sign a kind-1 event with the given content. The
/// `created_at` is the supplied unix-seconds value (or "now" if `None`).
fn make_event(keys: &Keys, content: &str, kind: u16, created_at: Option<u64>) -> nostr::Event {
    let mut b = EventBuilder::new(Kind::from(kind), content);
    if let Some(ts) = created_at {
        b = b.custom_created_at(nostr::Timestamp::from(ts));
    }
    b.sign_with_keys(keys).expect("sign failed")
}

/// Helper: connect to the relay over raw tungstenite.
async fn connect_ws(
    url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("ws connect failed");
    ws
}

/// Helper: receive one text frame, decoded as JSON. Times out after
/// `RECV_TIMEOUT`.
async fn recv_json(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Value {
    let msg = timeout(RECV_TIMEOUT, ws.next())
        .await
        .expect("timed out waiting for ws frame")
        .expect("ws closed unexpectedly")
        .expect("ws error");
    match msg {
        Message::Text(t) => serde_json::from_str(t.as_str()).expect("not json"),
        other => panic!("expected text frame, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn req_returns_preloaded_events_in_desc_order_then_eose() {
    let keys = Keys::generate();
    let e_old = make_event(&keys, "old", 1, Some(1_000));
    let e_mid = make_event(&keys, "mid", 1, Some(2_000));
    let e_new = make_event(&keys, "new", 1, Some(3_000));

    let relay = TestRelay::builder()
        .preload_events([e_old.clone(), e_mid.clone(), e_new.clone()])
        .start()
        .await;

    let mut ws = connect_ws(relay.url().as_str()).await;
    let req = json!(["REQ", "s1", { "kinds": [1] }]).to_string();
    ws.send(Message::Text(req.into())).await.unwrap();

    let m1 = recv_json(&mut ws).await;
    let m2 = recv_json(&mut ws).await;
    let m3 = recv_json(&mut ws).await;
    let m4 = recv_json(&mut ws).await;

    // Expect EVENTs in created_at-desc order: new, mid, old.
    assert_eq!(m1[0], "EVENT");
    assert_eq!(m1[1], "s1");
    assert_eq!(m1[2]["content"], "new");
    assert_eq!(m2[2]["content"], "mid");
    assert_eq!(m3[2]["content"], "old");
    assert_eq!(m4[0], "EOSE");
    assert_eq!(m4[1], "s1");

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn req_filter_kinds_excludes_non_matching() {
    let keys = Keys::generate();
    let e_kind1 = make_event(&keys, "k1", 1, Some(1_000));
    let e_kind7 = make_event(&keys, "k7", 7, Some(2_000));

    let relay = TestRelay::builder()
        .preload_events([e_kind1.clone(), e_kind7.clone()])
        .start()
        .await;

    let mut ws = connect_ws(relay.url().as_str()).await;
    let req = json!(["REQ", "k", { "kinds": [7] }]).to_string();
    ws.send(Message::Text(req.into())).await.unwrap();

    let event = recv_json(&mut ws).await;
    assert_eq!(event[0], "EVENT");
    assert_eq!(event[2]["content"], "k7");
    let eose = recv_json(&mut ws).await;
    assert_eq!(eose[0], "EOSE");

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn req_limit_caps_returned_events() {
    let keys = Keys::generate();
    let e1 = make_event(&keys, "1", 1, Some(1_000));
    let e2 = make_event(&keys, "2", 1, Some(2_000));
    let e3 = make_event(&keys, "3", 1, Some(3_000));

    let relay = TestRelay::builder()
        .preload_events([e1, e2, e3])
        .start()
        .await;

    let mut ws = connect_ws(relay.url().as_str()).await;
    let req = json!(["REQ", "lim", { "kinds": [1], "limit": 2 }]).to_string();
    ws.send(Message::Text(req.into())).await.unwrap();

    let a = recv_json(&mut ws).await;
    let b = recv_json(&mut ws).await;
    let c = recv_json(&mut ws).await;
    assert_eq!(a[0], "EVENT");
    assert_eq!(b[0], "EVENT");
    assert_eq!(c[0], "EOSE");
    // Newest two: created_at 3000 and 2000.
    assert_eq!(a[2]["content"], "3");
    assert_eq!(b[2]["content"], "2");

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn req_filter_since_until_authors_ids() {
    let keys_a = Keys::generate();
    let keys_b = Keys::generate();
    let ea1 = make_event(&keys_a, "a1", 1, Some(1_000));
    let ea2 = make_event(&keys_a, "a2", 1, Some(2_000));
    let eb1 = make_event(&keys_b, "b1", 1, Some(1_500));

    let relay = TestRelay::builder()
        .preload_events([ea1.clone(), ea2.clone(), eb1.clone()])
        .start()
        .await;

    // authors filter
    let mut ws = connect_ws(relay.url().as_str()).await;
    let req = json!(["REQ", "auth", { "authors": [keys_b.public_key().to_hex()] }]).to_string();
    ws.send(Message::Text(req.into())).await.unwrap();
    let m1 = recv_json(&mut ws).await;
    assert_eq!(m1[2]["content"], "b1");
    let m2 = recv_json(&mut ws).await;
    assert_eq!(m2[0], "EOSE");

    // since/until
    let req = json!(["REQ", "win", { "since": 1_200, "until": 1_800 }]).to_string();
    ws.send(Message::Text(req.into())).await.unwrap();
    let m1 = recv_json(&mut ws).await;
    assert_eq!(m1[2]["content"], "b1");
    let m2 = recv_json(&mut ws).await;
    assert_eq!(m2[0], "EOSE");

    // ids
    let req = json!(["REQ", "ids", { "ids": [ea2.id.to_hex()] }]).to_string();
    ws.send(Message::Text(req.into())).await.unwrap();
    let m1 = recv_json(&mut ws).await;
    assert_eq!(m1[2]["content"], "a2");
    let m2 = recv_json(&mut ws).await;
    assert_eq!(m2[0], "EOSE");

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn close_stops_forward_delivery() {
    let relay = TestRelay::builder().start().await;

    let mut ws = connect_ws(relay.url().as_str()).await;
    ws.send(Message::Text(
        json!(["REQ", "s", { "kinds": [1] }]).to_string().into(),
    ))
    .await
    .unwrap();
    // Drain EOSE.
    let eose = recv_json(&mut ws).await;
    assert_eq!(eose[0], "EOSE");

    // Now close the sub.
    ws.send(Message::Text(json!(["CLOSE", "s"]).to_string().into()))
        .await
        .unwrap();

    // Give the relay a moment to process CLOSE before publish.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish — the now-closed sub should not receive it.
    let keys = Keys::generate();
    let e = make_event(&keys, "post-close", 1, Some(5_000));
    relay.publish(e).await;

    // Expect the next read to time out.
    let res = timeout(Duration::from_millis(200), ws.next()).await;
    assert!(res.is_err(), "expected no further messages after CLOSE");

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_broadcasts_to_matching_open_sub() {
    let relay = TestRelay::builder().start().await;

    let mut ws = connect_ws(relay.url().as_str()).await;
    ws.send(Message::Text(
        json!(["REQ", "fwd", { "kinds": [1] }]).to_string().into(),
    ))
    .await
    .unwrap();
    let eose = recv_json(&mut ws).await;
    assert_eq!(eose[0], "EOSE");

    let keys = Keys::generate();
    let e = make_event(&keys, "live", 1, Some(9_000));
    relay.publish(e.clone()).await;

    let m = recv_json(&mut ws).await;
    assert_eq!(m[0], "EVENT");
    assert_eq!(m[1], "fwd");
    assert_eq!(m[2]["id"], e.id.to_hex());

    // A non-matching kind should not be delivered.
    let e2 = make_event(&keys, "other", 7, Some(9_500));
    relay.publish(e2).await;
    let res = timeout(Duration::from_millis(200), ws.next()).await;
    assert!(res.is_err(), "non-matching event should not be forwarded");

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn send_notice_reaches_clients() {
    let relay = TestRelay::builder().start().await;
    let mut ws = connect_ws(relay.url().as_str()).await;

    // Roundtrip a REQ → EOSE before broadcasting so the relay's server-side
    // connection task is guaranteed to have registered this client in
    // `state.connections`. `send_notice` snapshots that map and broadcasts to
    // whoever is in it; without the roundtrip it races the registration and
    // the NOTICE goes to nobody, hanging the read until RECV_TIMEOUT. Same
    // race (and same fix) as `disconnect_all_drops_clients`. Flaked in CI on
    // 2026-06-07.
    let req = json!(["REQ", "ping", { "kinds": [9999] }]).to_string();
    ws.send(Message::Text(req.into())).await.unwrap();
    let eose = recv_json(&mut ws).await;
    assert_eq!(eose[0], "EOSE");
    assert_eq!(eose[1], "ping");

    relay.send_notice("be advised").await;

    let m = recv_json(&mut ws).await;
    assert_eq!(m[0], "NOTICE");
    assert_eq!(m[1], "be advised");

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn close_subscription_emits_closed_frame() {
    let relay = TestRelay::builder().start().await;
    let mut ws = connect_ws(relay.url().as_str()).await;

    ws.send(Message::Text(
        json!(["REQ", "s", { "kinds": [1] }]).to_string().into(),
    ))
    .await
    .unwrap();
    let eose = recv_json(&mut ws).await;
    assert_eq!(eose[0], "EOSE");

    relay.close_subscription("s", "for testing").await;

    let m = recv_json(&mut ws).await;
    assert_eq!(m[0], "CLOSED");
    assert_eq!(m[1], "s");
    assert_eq!(m[2], "for testing");

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_all_drops_clients() {
    let relay = TestRelay::builder().start().await;
    let mut ws = connect_ws(relay.url().as_str()).await;

    // Roundtrip a REQ → EOSE before issuing disconnect_all so the relay's
    // server-side connection task is guaranteed to have registered this
    // client. Otherwise disconnect_all races the registration and the
    // disconnect signal can miss us. Without the roundtrip this test is
    // flaky on busy CI machines.
    let req = json!(["REQ", "ping", { "kinds": [9999] }]).to_string();
    ws.send(Message::Text(req.into())).await.unwrap();
    let eose = recv_json(&mut ws).await;
    assert_eq!(eose[0], "EOSE");
    assert_eq!(eose[1], "ping");

    relay.disconnect_all().await;

    // Read until we see a None or Close — that's the disconnect.
    let saw_close = timeout(RECV_TIMEOUT, async {
        loop {
            match ws.next().await {
                None | Some(Ok(Message::Close(_)) | Err(_)) => return true,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(saw_close.unwrap_or(false), "client was not disconnected");

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nip11_returns_configured_doc() {
    let custom = serde_json::json!({"name": "custom-test", "supported_nips": [1, 11, 42]});
    let relay = TestRelay::builder().nip11_doc(custom.clone()).start().await;

    let client = reqwest_minimal::Client::new();
    let resp = client
        .get(format!("{}/", relay.http_url()))
        .header("Accept", "application/nostr+json")
        .send()
        .await
        .expect("nip11 fetch failed");
    assert_eq!(resp.status, 200);
    assert!(
        resp.content_type
            .as_deref()
            .is_some_and(|s| s.contains("application/nostr+json"))
    );
    let body: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body, custom);

    // Without the right Accept header we get the fallback (200, plain text).
    let resp2 = client
        .get(format!("{}/", relay.http_url()))
        .send()
        .await
        .expect("plain GET failed");
    assert_eq!(resp2.status, 200);
    assert!(
        resp2
            .content_type
            .as_deref()
            .is_none_or(|s| !s.contains("application/nostr+json"))
    );

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_nip11_returns_404() {
    let relay = TestRelay::builder().no_nip11().start().await;
    let client = reqwest_minimal::Client::new();

    let resp = client
        .get(format!("{}/", relay.http_url()))
        .header("Accept", "application/nostr+json")
        .send()
        .await
        .expect("nip11 fetch failed");
    assert_eq!(resp.status, 404);

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nip11_status_override_propagates() {
    let relay = TestRelay::builder()
        .nip11_status(500)
        .nip11_doc(serde_json::json!({"error": "synthetic"}))
        .start()
        .await;
    let client = reqwest_minimal::Client::new();
    let resp = client
        .get(format!("{}/", relay.http_url()))
        .header("Accept", "application/nostr+json")
        .send()
        .await
        .expect("fetch failed");
    assert_eq!(resp.status, 500);

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn open_subscriptions_snapshot_reflects_state() {
    let relay = TestRelay::builder().start().await;
    let mut ws = connect_ws(relay.url().as_str()).await;

    ws.send(Message::Text(
        json!(["REQ", "alpha", { "kinds": [1] }]).to_string().into(),
    ))
    .await
    .unwrap();
    let _ = recv_json(&mut ws).await; // EOSE

    // Tiny sleep so the server has registered the sub.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let subs = relay.open_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].sub_id, "alpha");

    ws.send(Message::Text(json!(["CLOSE", "alpha"]).to_string().into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(relay.open_subscriptions().len(), 0);

    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nostr_relay_pool_can_subscribe_and_receive() {
    // End-to-end: drives the relay through the same library the production
    // acquirer uses. Validates filter wire format + EVENT/EOSE handling.
    let keys = Keys::generate();
    let e = make_event(&keys, "via-pool", 1, Some(123_456));

    let relay = TestRelay::builder().preload_event(e.clone()).start().await;

    let pool = RelayPool::new();
    pool.add_relay(relay.url().as_str(), RelayOptions::default())
        .await
        .unwrap();
    pool.connect().await;

    let mut notifications = pool.notifications();

    let filter = Filter::new().kind(Kind::TextNote);
    pool.subscribe(vec![filter], SubscribeOptions::default())
        .await
        .unwrap();

    let mut got_event = false;
    let mut got_eose = false;
    let _ = timeout(RECV_TIMEOUT, async {
        while !(got_event && got_eose) {
            match notifications.recv().await {
                Ok(RelayPoolNotification::Event { event, .. }) => {
                    if event.id == e.id {
                        got_event = true;
                    }
                }
                Ok(RelayPoolNotification::Message { message, .. }) => {
                    if matches!(message, RelayMessage::EndOfStoredEvents(_)) {
                        got_eose = true;
                    }
                }
                Ok(RelayPoolNotification::Shutdown) | Err(_) => break,
            }
        }
    })
    .await;

    assert!(got_event, "did not receive expected event via relay pool");
    assert!(got_eose, "did not receive EOSE via relay pool");

    pool.shutdown().await;
    relay.shutdown().await;
}

/// A relay built with `require_user_agent()` refuses an upgrade with no
/// `User-Agent` (tungstenite's own request carries none) or an empty one with
/// `403 Forbidden`, as some relays do, and accepts the same upgrade with one. The
/// default relay accepts both. Every upgrade's header is recorded.
#[tokio::test(flavor = "multi_thread")]
async fn require_user_agent_refuses_an_upgrade_without_one() {
    use tokio_tungstenite::tungstenite::Error as WsError;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let relay = TestRelay::builder().require_user_agent().start().await;
    let url = relay.url().to_string();

    match tokio_tungstenite::connect_async(url.as_str()).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status(), 403),
        Err(other) => panic!("expected an HTTP 403, got {other:?}"),
        Ok(_) => panic!("an upgrade without a User-Agent was accepted"),
    }

    // An empty User-Agent is no User-Agent.
    let mut empty = url.as_str().into_client_request().unwrap();
    empty
        .headers_mut()
        .insert("user-agent", "".parse().unwrap());
    match tokio_tungstenite::connect_async(empty).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status(), 403),
        Err(other) => panic!("expected an HTTP 403, got {other:?}"),
        Ok(_) => panic!("an upgrade with an empty User-Agent was accepted"),
    }

    let mut request = url.as_str().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("user-agent", "test-agent/1".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("an upgrade with a User-Agent must be accepted");
    ws.send(Message::Text(
        json!(["REQ", "s", {"kinds": [1]}]).to_string().into(),
    ))
    .await
    .unwrap();
    assert_eq!(recv_json(&mut ws).await, json!(["EOSE", "s"]));

    assert_eq!(
        relay.upgrade_user_agents(),
        vec![None, Some(String::new()), Some("test-agent/1".to_string())]
    );

    // The default relay takes an upgrade without one, and records that.
    let open = TestRelay::builder().start().await;
    let _ws = connect_ws(&open.url().to_string()).await;
    assert_eq!(open.upgrade_user_agents(), vec![None]);
}

// --- Tiny in-test HTTP client to avoid pulling reqwest as a dev-dep. -------

mod reqwest_minimal {
    use std::collections::HashMap;

    use std::fmt::Write as _;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    pub struct Client;

    impl Client {
        pub fn new() -> Self {
            Self
        }

        #[allow(clippy::unused_self)]
        pub fn get(&self, url: String) -> RequestBuilder {
            RequestBuilder {
                url,
                headers: HashMap::new(),
            }
        }
    }

    pub struct RequestBuilder {
        url: String,
        headers: HashMap<String, String>,
    }

    impl RequestBuilder {
        pub fn header(mut self, k: &str, v: &str) -> Self {
            self.headers.insert(k.to_string(), v.to_string());
            self
        }

        pub async fn send(self) -> Result<Response, std::io::Error> {
            let url = url::Url::parse(&self.url)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            let host = url.host_str().unwrap();
            let port = url.port().unwrap_or(80);
            let path = if url.path().is_empty() {
                "/"
            } else {
                url.path()
            };

            let mut stream = TcpStream::connect((host, port)).await?;
            let mut req =
                format!("GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n");
            for (k, v) in &self.headers {
                let _ = writeln!(req, "{k}: {v}\r");
            }
            req.push_str("\r\n");
            stream.write_all(req.as_bytes()).await?;

            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await?;

            let split = buf
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .ok_or_else(|| std::io::Error::other("no header terminator"))?;
            let header_str = std::str::from_utf8(&buf[..split])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let body = buf[split + 4..].to_vec();

            let mut lines = header_str.lines();
            let status_line = lines.next().unwrap_or("");
            let status: u16 = status_line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let mut content_type = None;
            for line in lines {
                if let Some((k, v)) = line.split_once(':') {
                    if k.trim().eq_ignore_ascii_case("content-type") {
                        content_type = Some(v.trim().to_string());
                    }
                }
            }

            Ok(Response {
                status,
                content_type,
                body,
            })
        }
    }

    pub struct Response {
        pub status: u16,
        pub content_type: Option<String>,
        pub body: Vec<u8>,
    }
}
