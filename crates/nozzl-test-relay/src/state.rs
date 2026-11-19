//! Shared mutable state for the running relay: preloaded events, NIP-11
//! config, and the registry of currently-open subscriptions across all
//! connected clients.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nostr::{Event, Filter};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

/// One server-side broadcast message destined for a client connection.
///
/// Connection tasks own a `mpsc::Receiver<OutboundMessage>`; this is how
/// `publish()`, `send_notice()`, and `close_subscription()` push data into
/// in-flight `WebSocket`s without holding any lock across an await on the
/// `WebSocket` sink.
#[derive(Clone, Debug)]
pub(crate) enum OutboundMessage {
    /// A pre-serialized JSON line to be written verbatim as a Text frame.
    Text(String),
}

/// One open `REQ` subscription tracked on the server. The connection task
/// owns the channel; we keep a clone of the sender so global broadcasts
/// (`publish`, `send_notice`, targeted `close_subscription`) can reach it.
#[derive(Clone)]
pub(crate) struct SubscriptionEntry {
    pub sub_id: String,
    pub filters: Vec<Filter>,
    pub sender: mpsc::Sender<OutboundMessage>,
}

/// Per-connection registration: the outbound channel sender (so we can push
/// NOTICEs / forward events) and a clone of the disconnect broadcast (so we
/// could individually disconnect later, if ever needed).
#[derive(Clone)]
pub(crate) struct ConnectionEntry {
    pub sender: mpsc::Sender<OutboundMessage>,
}

/// NIP-11 configuration. The builder supplies the body; the server hands it
/// back per request.
#[derive(Clone)]
pub(crate) struct Nip11Config {
    pub status: u16,
    pub body: Option<Value>,
}

impl Nip11Config {
    pub fn default_doc() -> Value {
        serde_json::json!({
            "name": "nozzl-test-relay",
            "description": "in-process test relay",
            "supported_nips": [1, 11],
            "software": "nozzl-test-relay",
            "version": env!("CARGO_PKG_VERSION"),
        })
    }
}

/// Predicate evaluated on every incoming `REQ` — if it returns `Some(reason)`,
/// the relay sends `["CLOSED", sub_id, reason]` *immediately* and processes
/// no events / EOSE for that subscription. Lets tests reject a REQ before
/// the natural EOSE flow advances acquirer state, which is required to
/// exercise auto-split paths deterministically (otherwise EOSE fires first
/// and the original `sub_id` is gone before the test can close it).
pub(crate) type ReqRejectPredicate =
    Arc<dyn Fn(&[Filter]) -> Option<String> + Send + Sync + 'static>;

/// Predicate evaluated on every incoming `REQ` — if it returns `true`, the
/// relay registers the subscription (so [`crate::TestRelayHandle::open_subscriptions`]
/// observes it) but then says nothing: no `EVENT` frames, no `EOSE`. Used
/// to simulate a relay whose application-layer dispatch is dead while the
/// WebSocket connection is otherwise healthy — the acquirer's dispatch-
/// liveness probe should time out and force-reconnect against such a relay.
pub(crate) type ReqIgnorePredicate = Arc<dyn Fn(&[Filter]) -> bool + Send + Sync + 'static>;

/// Top-level shared state. Wrapped in an `Arc<Mutex<_>>`; locks are only ever
/// held for short, non-async sections.
pub(crate) struct RelayState {
    pub preloaded: Vec<Event>,
    pub nip11: Nip11Config,
    pub connections: HashMap<u64, ConnectionEntry>,
    /// Subscriptions keyed by `(conn_id, sub_id)`.
    pub subscriptions: HashMap<(u64, String), SubscriptionEntry>,
    /// Monotonic id allocator for new connections.
    pub next_conn_id: u64,
    /// Broadcast channel that signals connection tasks to drop. A new sender
    /// replaces the old one each time `disconnect_all` is called, so that
    /// signal is one-shot per generation.
    pub disconnect_tx: broadcast::Sender<()>,
    /// Optional REQ-rejection predicate. See [`ReqRejectPredicate`].
    pub req_reject: Option<ReqRejectPredicate>,
    /// Optional REQ-silencing predicate. See [`ReqIgnorePredicate`]. The
    /// subscription is still registered (so tests can observe it via
    /// `open_subscriptions()`), but no `EVENT` / `EOSE` frames are sent
    /// for matching REQs.
    pub req_ignore: Option<ReqIgnorePredicate>,
    /// Fixed delay inserted between the last matching `EVENT` and the
    /// `EOSE` frame for every REQ. `Duration::ZERO` disables the delay.
    /// Used to exercise borderline timeout paths in liveness-probe tests.
    pub delay_eose_by: Duration,
    /// Drop this many events from the tail of every paginated REQ response.
    /// Simulates production relays (e.g. damus.io) that return slightly
    /// fewer events than the requested `limit` due to timestamp-tie
    /// pagination quirks even when more history remains.
    pub truncate_by_per_page: usize,
    /// Serve at most this many events per REQ, whatever `limit` the client
    /// asked for (a silent page clamp, as on relay.conduit.market). `None`
    /// serves up to the filter's own `limit`.
    pub page_clamp: Option<usize>,
    /// Answer every client `CLOSE` with `["CLOSED", sub_id, ""]`, as some
    /// production relays do (relay.conduit.market, on khatru). Off by default.
    pub echo_closed_on_close: bool,
    /// Count of `["CLOSE", sub_id]` frames received over the relay's
    /// lifetime, keyed by `sub_id`. Used by tests to detect the
    /// silence-watchdog re-issue path (which closes the existing forward
    /// sub before re-opening it). Per-sub-id rather than aggregate so a
    /// test can pin "was the *forward* sub re-issued" without false
    /// positives from the natural backfill close traffic.
    pub close_counts_by_sub_id: HashMap<String, u64>,
    /// Answer a WebSocket upgrade that carries no `User-Agent` (or an empty
    /// one) with `403 Forbidden`, as some relays do. Off by default.
    pub require_user_agent: bool,
    /// The `User-Agent` of every WebSocket upgrade request, in arrival order,
    /// `None` where the request had none; refused upgrades included.
    pub upgrade_user_agents: Vec<Option<String>>,
}

impl RelayState {
    pub fn new(preloaded: Vec<Event>, nip11: Nip11Config) -> Arc<Mutex<Self>> {
        let (disconnect_tx, _) = broadcast::channel(8);
        Arc::new(Mutex::new(Self {
            preloaded,
            nip11,
            connections: HashMap::new(),
            subscriptions: HashMap::new(),
            next_conn_id: 0,
            disconnect_tx,
            req_reject: None,
            req_ignore: None,
            delay_eose_by: Duration::ZERO,
            truncate_by_per_page: 0,
            page_clamp: None,
            echo_closed_on_close: false,
            close_counts_by_sub_id: HashMap::new(),
            require_user_agent: false,
            upgrade_user_agents: Vec::new(),
        }))
    }
}
