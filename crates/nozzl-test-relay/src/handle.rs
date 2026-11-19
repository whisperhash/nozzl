//! `TestRelayHandle` — runtime control surface returned from `start()`.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use nostr::{Event, Filter};
use nozzl_types::RelayUrl;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::state::{OutboundMessage, RelayState};

/// A snapshot of one open `REQ` subscription on the relay. Returned by
/// [`TestRelayHandle::open_subscriptions`].
#[derive(Clone, Debug)]
pub struct OpenSub {
    pub sub_id: String,
    pub filters: Vec<Filter>,
}

/// Live handle to a running [`crate::TestRelay`]. Drop without calling
/// [`shutdown`] is allowed (the server task ends when the handle's shutdown
/// channel sender is dropped), but `shutdown` is the tested path.
pub struct TestRelayHandle {
    bound: SocketAddr,
    state: Arc<Mutex<RelayState>>,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl TestRelayHandle {
    pub(crate) fn new(
        bound: SocketAddr,
        state: Arc<Mutex<RelayState>>,
        shutdown: oneshot::Sender<()>,
        join: JoinHandle<()>,
    ) -> Self {
        Self {
            bound,
            state,
            shutdown: Some(shutdown),
            join: Some(join),
        }
    }

    /// `ws://127.0.0.1:N` URL for the relay. Use this to connect a Nostr client.
    ///
    /// # Panics
    /// Panics if the bound address cannot be formatted as a `ws://` URL —
    /// shouldn't happen for `127.0.0.1`.
    #[must_use]
    pub fn url(&self) -> RelayUrl {
        let s = format!("ws://{}", self.bound);
        RelayUrl::parse(&s).expect("test relay: bound address must form a ws:// url")
    }

    /// `http://127.0.0.1:N` URL — for direct NIP-11 fetches in tests.
    #[must_use]
    pub fn http_url(&self) -> String {
        format!("http://{}", self.bound)
    }

    /// Inject an event the relay should immediately broadcast to all matching
    /// open subscriptions.
    pub async fn publish(&self, event: Event) {
        // Snapshot matching subscriptions under the lock, then send outside
        // the lock so a slow client's full mpsc can't stall other connections.
        let targets: Vec<_> = {
            let state = self.state.lock().expect("relay state poisoned");
            state
                .subscriptions
                .values()
                .filter(|s| {
                    s.filters
                        .iter()
                        .any(|f| f.match_event(&event, nostr::filter::MatchEventOptions::new()))
                })
                .cloned()
                .collect()
        };

        for sub in targets {
            let payload = serde_json::json!(["EVENT", sub.sub_id, event]).to_string();
            // Best-effort: a closed receiver means the connection is gone.
            let _ = sub.sender.send(OutboundMessage::Text(payload)).await;
        }
    }

    /// Send a NOTICE message to all currently connected clients.
    pub async fn send_notice(&self, msg: &str) {
        let payload = serde_json::json!(["NOTICE", msg]).to_string();
        let senders: Vec<_> = {
            let state = self.state.lock().expect("relay state poisoned");
            state
                .connections
                .values()
                .map(|c| c.sender.clone())
                .collect()
        };
        for s in senders {
            let _ = s.send(OutboundMessage::Text(payload.clone())).await;
        }
    }

    /// Drop a subscription from the relay's state **without sending CLOSED**
    /// to the client. Simulates a relay that silently garbage-collects
    /// subscriptions under load — observed live against nos.lol on
    /// 2026-05-02 17:01 UTC, which dropped our forward sub during a
    /// concurrency-pressure storm without notification, leaving the client
    /// with an open handle to a dead sub. After this call, events published
    /// via [`Self::publish`] will not match the dropped sub even though the
    /// client believes it's still subscribed.
    ///
    /// If no subscription with `sub_id` is currently open, this is a no-op.
    // Async signature is part of the public contract (matches the rest of
    // the handle's mutators) even though the body is non-async today.
    #[allow(clippy::unused_async)]
    pub async fn silently_drop_subscription(&self, sub_id: &str) {
        let mut state = self.state.lock().expect("relay state poisoned");
        let keys: Vec<(u64, String)> = state
            .subscriptions
            .keys()
            .filter(|(_, sid)| sid == sub_id)
            .cloned()
            .collect();
        for k in keys {
            state.subscriptions.remove(&k);
        }
    }

    /// Send a CLOSED to a specific subscription with the given reason. If no
    /// such subscription is currently open, this is a no-op.
    pub async fn close_subscription(&self, sub_id: &str, reason: &str) {
        let payload = serde_json::json!(["CLOSED", sub_id, reason]).to_string();
        let targets: Vec<_> = {
            let mut state = self.state.lock().expect("relay state poisoned");
            let keys: Vec<(u64, String)> = state
                .subscriptions
                .keys()
                .filter(|(_, sid)| sid == sub_id)
                .cloned()
                .collect();
            keys.into_iter()
                .filter_map(|k| state.subscriptions.remove(&k))
                .collect()
        };
        for sub in targets {
            let _ = sub
                .sender
                .send(OutboundMessage::Text(payload.clone()))
                .await;
        }
    }

    /// Disconnect every currently connected client. New connections after
    /// this call are unaffected.
    // Async signature is part of the public contract even though no current
    // code path awaits — keeps room for future I/O without a breaking change.
    #[allow(clippy::unused_async)]
    pub async fn disconnect_all(&self) {
        // Replace the broadcast sender with a fresh one before signaling, so
        // future connections don't latch the old "you should die" message.
        let old_tx = {
            let mut state = self.state.lock().expect("relay state poisoned");
            let (new_tx, _) = tokio::sync::broadcast::channel(8);
            std::mem::replace(&mut state.disconnect_tx, new_tx)
        };
        let _ = old_tx.send(());
    }

    /// Number of `["CLOSE", sub_id]` frames received for the given
    /// `sub_id` over the relay's lifetime. Tests use this to detect the
    /// silence-watchdog's re-issue path, which closes the existing forward
    /// sub before re-opening it (so "watchdog fired" = `>= 1`).
    ///
    /// # Panics
    /// Panics only if the internal mutex is poisoned.
    #[must_use]
    pub fn close_count(&self, sub_id: &str) -> u64 {
        let state = self.state.lock().expect("relay state poisoned");
        state
            .close_counts_by_sub_id
            .get(sub_id)
            .copied()
            .unwrap_or(0)
    }

    /// Number of `["CLOSE", sub_id]` frames received for any `sub_id`
    /// starting with `"probe-"` — i.e. the dispatch-liveness probe
    /// `sub_id`s. Tests use this to assert the acquirer politely CLOSEs
    /// probes after settlement.
    ///
    /// # Panics
    /// Panics only if the internal mutex is poisoned.
    #[must_use]
    pub fn probe_close_count(&self) -> u64 {
        let state = self.state.lock().expect("relay state poisoned");
        state
            .close_counts_by_sub_id
            .iter()
            .filter_map(|(sid, n)| sid.starts_with("probe-").then_some(*n))
            .sum()
    }

    /// Number of `["CLOSE", sub_id]` frames received for any `sub_id`
    /// starting with `"backfill-"` (the acquirer's `BACKFILL_SUB_BASE`
    /// prefix) — i.e. the cumulative count of backfill pages the acquirer has
    /// closed over the relay's lifetime. Because each page is closed on EOSE
    /// before the next opens, this is a polling-cadence-independent counter of
    /// completed backfill pages (unlike [`Self::open_subscriptions`], which
    /// only ever shows the single currently-open page).
    ///
    /// # Panics
    /// Panics only if the internal mutex is poisoned.
    #[must_use]
    pub fn backfill_close_count(&self) -> u64 {
        let state = self.state.lock().expect("relay state poisoned");
        state
            .close_counts_by_sub_id
            .iter()
            .filter_map(|(sid, n)| sid.starts_with("backfill-").then_some(*n))
            .sum()
    }

    /// The `User-Agent` of every WebSocket upgrade request so far, in arrival
    /// order, `None` where the request carried none. Refused upgrades
    /// (see [`crate::TestRelayBuilder::require_user_agent`]) are included.
    ///
    /// # Panics
    /// Panics only if the internal mutex is poisoned.
    #[must_use]
    pub fn upgrade_user_agents(&self) -> Vec<Option<String>> {
        let state = self.state.lock().expect("relay state poisoned");
        state.upgrade_user_agents.clone()
    }

    /// Snapshot of REQs currently open against this relay.
    ///
    /// # Panics
    /// Panics only if the internal mutex is poisoned (a panic happened while
    /// another thread held the lock).
    #[must_use]
    pub fn open_subscriptions(&self) -> Vec<OpenSub> {
        let state = self.state.lock().expect("relay state poisoned");
        state
            .subscriptions
            .values()
            .map(|s| OpenSub {
                sub_id: s.sub_id.clone(),
                filters: s.filters.clone(),
            })
            .collect()
    }

    /// Stop the relay. Idempotent — calling twice (or dropping after) is fine.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

impl Drop for TestRelayHandle {
    fn drop(&mut self) {
        // Still signal shutdown so that the background task exits even if a
        // test forgot to await `shutdown()`. The JoinHandle is dropped (not
        // awaited) — fine for tests; the runtime will cancel it on teardown.
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}
