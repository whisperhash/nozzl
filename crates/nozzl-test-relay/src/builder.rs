//! `TestRelay` and `TestRelayBuilder` — public construction API.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use nostr::{Event, Filter};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::handle::TestRelayHandle;
use crate::server;
use crate::state::{Nip11Config, RelayState, ReqIgnorePredicate, ReqRejectPredicate};

/// Marker / namespace type. Construction always goes through [`TestRelay::builder`].
pub struct TestRelay;

impl TestRelay {
    /// Start a builder for an in-process relay. See [`TestRelayBuilder`] for
    /// available knobs.
    #[must_use]
    pub fn builder() -> TestRelayBuilder {
        TestRelayBuilder::default()
    }
}

/// Builder for a [`TestRelay`]. Defaults: ephemeral port, default NIP-11 doc,
/// no preloaded events.
pub struct TestRelayBuilder {
    port: u16,
    nip11_status: u16,
    nip11_body: Option<Value>,
    nip11_disabled: bool,
    preloaded: Vec<Event>,
    req_reject: Option<ReqRejectPredicate>,
    req_ignore: Option<ReqIgnorePredicate>,
    delay_eose_by: Duration,
    truncate_by_per_page: usize,
    page_clamp: Option<usize>,
    echo_closed_on_close: bool,
    require_user_agent: bool,
}

impl Default for TestRelayBuilder {
    fn default() -> Self {
        Self {
            port: 0,
            nip11_status: 200,
            nip11_body: None,
            nip11_disabled: false,
            preloaded: Vec::new(),
            req_reject: None,
            req_ignore: None,
            delay_eose_by: Duration::ZERO,
            truncate_by_per_page: 0,
            page_clamp: None,
            echo_closed_on_close: false,
            require_user_agent: false,
        }
    }
}

impl TestRelayBuilder {
    /// Bind to a specific port. `0` (the default) requests an ephemeral one.
    #[must_use]
    pub fn port(mut self, p: u16) -> Self {
        self.port = p;
        self
    }

    /// Override the JSON body returned for NIP-11 requests. Defaults to a
    /// minimal `nozzl-test-relay` doc advertising NIPs 1 and 11.
    #[must_use]
    pub fn nip11_doc(mut self, body: Value) -> Self {
        self.nip11_body = Some(body);
        self.nip11_disabled = false;
        self
    }

    /// Override the HTTP status returned for the NIP-11 endpoint. Useful for
    /// driving error-path tests in the acquirer.
    #[must_use]
    pub fn nip11_status(mut self, code: u16) -> Self {
        self.nip11_status = code;
        self
    }

    /// Disable NIP-11: respond `404` to `GET /` regardless of `Accept`.
    #[must_use]
    pub fn no_nip11(mut self) -> Self {
        self.nip11_disabled = true;
        self
    }

    /// Preload one event. Matched against `REQ` filters when subscriptions open.
    #[must_use]
    pub fn preload_event(mut self, e: Event) -> Self {
        self.preloaded.push(e);
        self
    }

    /// Preload a batch of events.
    #[must_use]
    pub fn preload_events<I>(mut self, es: I) -> Self
    where
        I: IntoIterator<Item = Event>,
    {
        self.preloaded.extend(es);
        self
    }

    /// Drop the last `n` events from every paginated REQ response.
    /// Simulates relays (observed live: relay.damus.io) that return
    /// slightly under-cap pages due to timestamp-tie pagination quirks
    /// even when more history exists. Used to exercise the
    /// empty-page-only backfill termination heuristic — without that, an
    /// `n=2` truncation can prematurely complete a backfill against a
    /// relay that has plenty more events to give.
    #[must_use]
    pub fn truncate_each_page_by(mut self, n: usize) -> Self {
        self.truncate_by_per_page = n;
        self
    }

    /// Serve at most `n` events per REQ, whatever `limit` the client asked
    /// for, as a relay with a silent page clamp does: relay.conduit.market
    /// serves 100 when asked for 500 and advertises no limit, which is
    /// #32 (Backfill silently drops boundary-tie events when a relay serves
    /// fewer events per REQ than `backfill_default_limit`). Applied after the
    /// filter's own `limit` and before [`Self::truncate_each_page_by`].
    #[must_use]
    pub fn clamp_each_page_to(mut self, n: usize) -> Self {
        self.page_clamp = Some(n);
        self
    }

    /// Install a per-REQ rejection predicate. The predicate is called with
    /// the parsed filter list before any events / EOSE are sent. If it
    /// returns `Some(reason)`, the relay immediately replies with
    /// `["CLOSED", sub_id, reason]` and processes nothing else for that
    /// subscription. Used by acquirer tests to exercise auto-split paths
    /// deterministically.
    #[must_use]
    pub fn reject_req_if<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&[Filter]) -> Option<String> + Send + Sync + 'static,
    {
        self.req_reject = Some(Arc::new(predicate));
        self
    }

    /// Like [`Self::reject_req_if`], but instead of sending `CLOSED`, the
    /// relay registers the subscription and then **says nothing** — no
    /// events, no `EOSE`. Used to simulate a relay whose application-layer
    /// dispatch is dead while the WebSocket connection is otherwise healthy.
    /// The acquirer's dispatch-liveness probe should time out against such
    /// a relay and force-reconnect.
    #[must_use]
    pub fn ignore_req_if<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&[Filter]) -> bool + Send + Sync + 'static,
    {
        self.req_ignore = Some(Arc::new(predicate));
        self
    }

    /// Answer every client `CLOSE` with `["CLOSED", sub_id, ""]`, as
    /// relay.conduit.market does in production. A REQ that follows the CLOSE
    /// on the same socket is handled after it, so the echo refers to the sub
    /// the CLOSE ended, not to one re-opened under the same id.
    #[must_use]
    pub fn echo_closed_on_close(mut self) -> Self {
        self.echo_closed_on_close = true;
        self
    }

    /// Refuse a WebSocket upgrade that carries no `User-Agent` with
    /// `403 Forbidden`, as some relays do (seen 2026-10-14). The NIP-11 and plain
    /// HTTP paths are unaffected.
    #[must_use]
    pub fn require_user_agent(mut self) -> Self {
        self.require_user_agent = true;
        self
    }

    /// Apply a fixed delay between sending matching events and the `EOSE`
    /// frame for every REQ. Used to exercise borderline timeout paths in
    /// liveness-probe tests.
    #[must_use]
    pub fn delay_eose_by(mut self, d: Duration) -> Self {
        self.delay_eose_by = d;
        self
    }

    /// Start the relay. Returns once the listener is bound; the accept loop
    /// runs on a background task. Panics if the listener can't be bound — the
    /// builder is intended for tests, where bind failure is fatal.
    ///
    /// # Panics
    /// Panics if the configured port cannot be bound on `127.0.0.1`.
    pub async fn start(self) -> TestRelayHandle {
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, self.port));
        let listener = TcpListener::bind(addr)
            .await
            .expect("test relay: bind failed");
        let bound = listener
            .local_addr()
            .expect("test relay: local_addr failed");

        let nip11 = if self.nip11_disabled {
            Nip11Config {
                status: 404,
                body: None,
            }
        } else {
            Nip11Config {
                status: self.nip11_status,
                body: Some(self.nip11_body.unwrap_or_else(Nip11Config::default_doc)),
            }
        };

        let state = RelayState::new(self.preloaded, nip11);
        {
            let mut s = state.lock().expect("relay state poisoned");
            if let Some(pred) = self.req_reject {
                s.req_reject = Some(pred);
            }
            if let Some(pred) = self.req_ignore {
                s.req_ignore = Some(pred);
            }
            s.delay_eose_by = self.delay_eose_by;
            s.truncate_by_per_page = self.truncate_by_per_page;
            s.page_clamp = self.page_clamp;
            s.echo_closed_on_close = self.echo_closed_on_close;
            s.require_user_agent = self.require_user_agent;
        }
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let join = tokio::spawn(server::run(listener, state.clone(), shutdown_rx));

        TestRelayHandle::new(bound, state, shutdown_tx, join)
    }
}
