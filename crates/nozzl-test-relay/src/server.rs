//! Server-side: axum router serving HTTP/NIP-11 and WebSocket on the same port,
//! plus per-connection message handling.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequestParts, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use nostr::filter::MatchEventOptions;
use nostr::{Event, Filter};
use serde_json::Value;
use std::sync::Mutex;
use tokio::net::TcpListener;

use tokio::sync::{mpsc, oneshot};

use crate::state::{ConnectionEntry, OutboundMessage, RelayState, SubscriptionEntry};

/// State the axum router clones per request.
#[derive(Clone)]
struct AppState {
    inner: Arc<Mutex<RelayState>>,
}

/// Drive the relay until the shutdown channel fires. Returns when the axum
/// server's graceful-shutdown future completes.
pub(crate) async fn run(
    listener: TcpListener,
    state: Arc<Mutex<RelayState>>,
    shutdown: oneshot::Receiver<()>,
) {
    let app_state = AppState { inner: state };
    let router = Router::new()
        .route("/", get(handle_root))
        .with_state(app_state);

    let serve = axum::serve(listener, router).with_graceful_shutdown(async move {
        let _ = shutdown.await;
    });

    if let Err(e) = serve.await {
        tracing::error!(error = %e, "test relay: server exited with error");
    }
}

/// Routes `GET /`: NIP-11 if the right Accept is set or a WebSocket upgrade
/// is requested; otherwise the plain-HTTP fallback configured on the builder.
async fn handle_root(State(state): State<AppState>, request: Request) -> Response {
    let (mut parts, _) = request.into_parts();

    // WebSocket upgrade detection: standard handshake sends `Connection:
    // upgrade` and `Upgrade: websocket`. Use axum's extractor only if so —
    // attempting the extractor on a non-upgrade request produces an error
    // response instead of falling through to the HTTP path.
    if is_ws_upgrade(&parts.headers) {
        let user_agent = parts
            .headers
            .get(axum::http::header::USER_AGENT)
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned());
        let refuse = {
            let mut s = state.inner.lock().expect("relay state poisoned");
            s.upgrade_user_agents.push(user_agent.clone());
            s.require_user_agent && user_agent.as_deref().is_none_or(str::is_empty)
        };
        if refuse {
            return (StatusCode::FORBIDDEN, "forbidden").into_response();
        }
        match WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
            Ok(ws) => {
                let inner = state.inner.clone();
                return ws.on_upgrade(move |socket| connection_task(socket, inner));
            }
            Err(rej) => return rej.into_response(),
        }
    }

    let wants_nip11 = parts
        .headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.contains("application/nostr+json"));

    let cfg = {
        let s = state.inner.lock().expect("relay state poisoned");
        s.nip11.clone()
    };

    if wants_nip11 {
        let status = StatusCode::from_u16(cfg.status).unwrap_or(StatusCode::OK);
        let body = cfg.body.unwrap_or(Value::Null);
        return (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/nostr+json")],
            body.to_string(),
        )
            .into_response();
    }

    // Without the NIP-11 Accept header. If `no_nip11()` was set the configured
    // status is 404; honor that. Otherwise serve a small 200 page.
    if cfg.body.is_none() {
        let status = StatusCode::from_u16(cfg.status).unwrap_or(StatusCode::NOT_FOUND);
        return (status, "not found").into_response();
    }
    (StatusCode::OK, "nozzl-test-relay").into_response()
}

fn is_ws_upgrade(headers: &HeaderMap) -> bool {
    let connection_upgrade = headers
        .get(axum::http::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| {
            s.split(',')
                .any(|p| p.trim().eq_ignore_ascii_case("upgrade"))
        });
    let upgrade_websocket = headers
        .get(axum::http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.eq_ignore_ascii_case("websocket"));
    connection_upgrade && upgrade_websocket
}

/// Per-connection task. Owns the WebSocket; reads client messages and forwards
/// outbound messages from the global state.
async fn connection_task(socket: WebSocket, state: Arc<Mutex<RelayState>>) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Outbound channel: anything written here gets pushed onto the WS by the
    // writer half of this task. Capacity is small but generous for tests.
    let (out_tx, mut out_rx) = mpsc::channel::<OutboundMessage>(64);

    // Subscribe to the disconnect-broadcast and register this connection.
    let (conn_id, mut disconnect_rx) = {
        let mut s = state.lock().expect("relay state poisoned");
        let id = s.next_conn_id;
        s.next_conn_id += 1;
        s.connections.insert(
            id,
            ConnectionEntry {
                sender: out_tx.clone(),
            },
        );
        (id, s.disconnect_tx.subscribe())
    };

    tracing::debug!(conn_id, "test relay: connection opened");

    // Writer half: pull from the outbound channel into the WS sink.
    let writer = tokio::spawn(async move {
        while let Some(OutboundMessage::Text(s)) = out_rx.recv().await {
            if ws_tx.send(Message::Text(s.into())).await.is_err() {
                break;
            }
        }
        // Best-effort close; ignore errors.
        let _ = ws_tx.close().await;
    });

    // Reader half: process incoming client messages until the socket closes
    // or a disconnect is signaled.
    loop {
        tokio::select! {
            biased;
            _ = disconnect_rx.recv() => {
                tracing::debug!(conn_id, "test relay: forced disconnect");
                break;
            }
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_client_text(&state, conn_id, &out_tx, text.as_str()).await;
                    }
                    // Ignore non-Text frames: NIP-01 is JSON over text frames,
                    // and pings/pongs are handled transparently by axum.
                    Some(Ok(
                        Message::Binary(_) | Message::Ping(_) | Message::Pong(_),
                    )) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        tracing::debug!(conn_id, error = %e, "test relay: ws read error");
                        break;
                    }
                }
            }
        }
    }

    // Cleanup: drop our entry from the global maps and tear down the writer.
    {
        let mut s = state.lock().expect("relay state poisoned");
        s.connections.remove(&conn_id);
        s.subscriptions.retain(|(cid, _), _| *cid != conn_id);
    }
    drop(out_tx);
    let _ = writer.await;

    tracing::debug!(conn_id, "test relay: connection closed");
}

/// Handle one decoded client text frame.
async fn handle_client_text(
    state: &Arc<Mutex<RelayState>>,
    conn_id: u64,
    out_tx: &mpsc::Sender<OutboundMessage>,
    text: &str,
) {
    let v: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(conn_id, error = %e, raw = text, "test relay: bad json");
            send_notice(out_tx, "invalid json").await;
            return;
        }
    };

    let Some(arr) = v.as_array() else {
        send_notice(out_tx, "expected JSON array").await;
        return;
    };
    let Some(tag) = arr.first().and_then(Value::as_str) else {
        send_notice(out_tx, "missing message tag").await;
        return;
    };

    match tag {
        "REQ" => handle_req(state, conn_id, out_tx, arr).await,
        "CLOSE" => handle_close(state, conn_id, out_tx, arr).await,
        // EVENT publishes from clients are dropped (acquirer doesn't publish
        // in v1). Other messages (AUTH, COUNT, NEG-*) are also silently ignored.
        _ => {}
    }
}

/// Parse a `["REQ", <sub_id>, <filter>, ...]` and dispatch matching events.
async fn handle_req(
    state: &Arc<Mutex<RelayState>>,
    conn_id: u64,
    out_tx: &mpsc::Sender<OutboundMessage>,
    arr: &[Value],
) {
    if arr.len() < 3 {
        send_notice(out_tx, "REQ requires sub_id and at least one filter").await;
        return;
    }
    let Some(sub_id) = arr.get(1).and_then(Value::as_str) else {
        send_notice(out_tx, "REQ sub_id must be a string").await;
        return;
    };
    let sub_id = sub_id.to_string();

    let mut filters: Vec<Filter> = Vec::with_capacity(arr.len() - 2);
    for raw in &arr[2..] {
        match serde_json::from_value::<Filter>(raw.clone()) {
            Ok(f) => filters.push(f),
            Err(e) => {
                tracing::debug!(conn_id, %sub_id, error = %e, "test relay: bad filter");
                send_notice(out_tx, "invalid filter").await;
                return;
            }
        }
    }

    // Check the rejection predicate (if any) before doing anything else.
    // If it returns Some(reason), reply with CLOSED and stop — no events,
    // no EOSE, no subscription registration. Tests use this to exercise
    // the acquirer's auto-split logic deterministically.
    let reject_reason = {
        let s = state.lock().expect("relay state poisoned");
        s.req_reject.as_ref().and_then(|f| f(&filters))
    };
    if let Some(reason) = reject_reason {
        let payload = serde_json::json!(["CLOSED", sub_id, reason]).to_string();
        let _ = out_tx.send(OutboundMessage::Text(payload)).await;
        return;
    }

    // Check the ignore predicate (if any): if it matches, register the
    // sub (so `open_subscriptions()` observes it) but send nothing — no
    // events, no EOSE. Simulates an application-layer-dead relay for the
    // acquirer's dispatch-liveness probe tests.
    let ignore_match = {
        let s = state.lock().expect("relay state poisoned");
        s.req_ignore.as_ref().is_some_and(|f| f(&filters))
    };
    if ignore_match {
        let mut s = state.lock().expect("relay state poisoned");
        s.subscriptions.insert(
            (conn_id, sub_id.clone()),
            SubscriptionEntry {
                sub_id: sub_id.clone(),
                filters: filters.clone(),
                sender: out_tx.clone(),
            },
        );
        return;
    }

    // Snapshot preloaded events under the lock; do the work outside.
    let (preloaded, truncate_by, delay_eose_by, page_clamp) = {
        let mut s = state.lock().expect("relay state poisoned");
        s.subscriptions.insert(
            (conn_id, sub_id.clone()),
            SubscriptionEntry {
                sub_id: sub_id.clone(),
                filters: filters.clone(),
                sender: out_tx.clone(),
            },
        );
        (
            s.preloaded.clone(),
            s.truncate_by_per_page,
            s.delay_eose_by,
            s.page_clamp,
        )
    };

    let mut matches = matching_events(&preloaded, &filters);
    // Builder-configured page clamp: serve at most this many events whatever
    // `limit` the client asked for, the way a relay with a silent clamp does.
    if let Some(n) = page_clamp {
        matches.truncate(n);
    }
    // Builder-configured page truncation: simulate relays that return
    // fewer events than asked even with more history available. Only
    // applies when the page has > truncate_by events to drop, otherwise
    // we'd return zero and end up exercising the actually-empty path.
    if truncate_by > 0 && matches.len() > truncate_by {
        matches.truncate(matches.len() - truncate_by);
    }
    for ev in matches {
        let payload = serde_json::json!(["EVENT", sub_id, ev]).to_string();
        if out_tx.send(OutboundMessage::Text(payload)).await.is_err() {
            return;
        }
    }
    // Builder-configured EOSE delay: simulate relays that pace their EOSE
    // frame. Used to exercise borderline timeout paths in liveness-probe
    // tests.
    if delay_eose_by > Duration::ZERO {
        tokio::time::sleep(delay_eose_by).await;
    }
    let eose = serde_json::json!(["EOSE", sub_id]).to_string();
    let _ = out_tx.send(OutboundMessage::Text(eose)).await;
}

/// Apply the union of filters, sort `created_at` DESC (NIP-01 ordering for
/// stored events), then apply each filter's `limit` AFTER sort. The smallest
/// `limit` across all filters governs the cap — that's a generous reading of
/// NIP-01 (which says "limit is per-filter") but matches what tests need:
/// a single filter with a single limit.
fn matching_events(preloaded: &[Event], filters: &[Filter]) -> Vec<Event> {
    let opts = MatchEventOptions::new();
    let mut matched: Vec<Event> = preloaded
        .iter()
        .filter(|e| filters.iter().any(|f| f.match_event(e, opts)))
        .cloned()
        .collect();

    matched.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });

    if let Some(limit) = filters.iter().filter_map(|f| f.limit).min() {
        matched.truncate(limit);
    }

    matched
}

async fn handle_close(
    state: &Arc<Mutex<RelayState>>,
    conn_id: u64,
    out_tx: &mpsc::Sender<OutboundMessage>,
    arr: &[Value],
) {
    let Some(sub_id) = arr.get(1).and_then(Value::as_str) else {
        return;
    };
    let echo = {
        let mut s = state.lock().expect("relay state poisoned");
        s.subscriptions.remove(&(conn_id, sub_id.to_string()));
        // Track CLOSE frames per sub_id so tests can pin re-issue paths
        // (e.g. the silence watchdog closing-and-reopening "forward").
        *s.close_counts_by_sub_id
            .entry(sub_id.to_string())
            .or_insert(0) += 1;
        s.echo_closed_on_close
    };
    if echo {
        let payload = serde_json::json!(["CLOSED", sub_id, ""]).to_string();
        let _ = out_tx.send(OutboundMessage::Text(payload)).await;
    }
}

async fn send_notice(out_tx: &mpsc::Sender<OutboundMessage>, msg: &str) {
    let payload = serde_json::json!(["NOTICE", msg]).to_string();
    let _ = out_tx.send(OutboundMessage::Text(payload)).await;
}
