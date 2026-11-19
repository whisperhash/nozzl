# nozzl-test-relay - crate brief

## Purpose

A tiny in-process WebSocket relay used by integration tests where we need exact control of relay output — forcing a NOTICE, returning a CLOSED with a specific reason, simulating an oddly-shaped NIP-11 doc, returning EOSE before all events are sent, etc.

In v1 this fixture is the sole integration-test target. Spawning `nostr-rs-relay` as an additional subprocess test was scoped out — coverage is provided by this fixture plus a live smoke run against real seed relays. The fixture is the primary tool for protocol-edge-case tests where a real relay is too uncontrollable.

The integration-testing strategy is in [../design.md §8](../design.md#8-testing-strategy).

## Dependencies

crates.io:
- `tokio`
- `tokio-tungstenite` (WebSocket server)
- `serde_json`
- `axum` or `hyper` (for the HTTP / NIP-11 side; `axum` is easiest)
- `nostr` (for `Event`, parsing of incoming REQ filters)
- `tracing`

sister crates:
- `nozzl-types` (for `RelayUrl` to express the URL the relay binds to).

## Public API contract

```rust
/// Builder for an in-process relay.
pub struct TestRelay {
    /* … */
}

impl TestRelay {
    pub fn builder() -> TestRelayBuilder;
}

pub struct TestRelayBuilder {
    /* port = 0 default, NIP-11 doc default, …  */
}

impl TestRelayBuilder {
    pub fn port(self, p: u16) -> Self;                          // 0 = ephemeral
    pub fn nip11_doc(self, body: serde_json::Value) -> Self;    // raw JSON returned for the NIP-11 GET
    pub fn nip11_status(self, code: u16) -> Self;               // for testing weird responses
    pub fn no_nip11(self) -> Self;                              // 404 the NIP-11 endpoint
    pub fn preload_event(self, e: nostr::Event) -> Self;
    pub fn preload_events<I: IntoIterator<Item = nostr::Event>>(self, es: I) -> Self;

    /// Start the relay; returns a handle that the test holds for the duration.
    pub async fn start(self) -> TestRelayHandle;
}

pub struct TestRelayHandle {
    /* … */
}

impl TestRelayHandle {
    pub fn url(&self) -> RelayUrl;          // wss://127.0.0.1:N
    pub fn http_url(&self) -> String;       // http://127.0.0.1:N — for NIP-11 fetch tests

    /// Inject an event the relay should immediately broadcast to all matching
    /// open subscriptions. Useful for triggering a forward subscription mid-test.
    pub async fn publish(&self, e: nostr::Event);

    /// Send a NOTICE message to all connected clients.
    pub async fn send_notice(&self, msg: &str);

    /// Send a CLOSED to a specific subscription with a reason.
    pub async fn close_subscription(&self, sub_id: &str, reason: &str);

    /// Disconnect all clients (for reconnect tests).
    pub async fn disconnect_all(&self);

    /// Returns a snapshot of REQs currently open against this relay.
    pub fn open_subscriptions(&self) -> Vec<OpenSub>;

    /// Number of `["CLOSE", sub_id]` frames received over the relay's
    /// lifetime for the given sub_id. Tests use this to detect the
    /// silence-watchdog re-issue path (which closes the existing forward
    /// sub before re-opening it).
    pub fn close_count(&self, sub_id: &str) -> u64;

    /// Stop the relay. Idempotent.
    pub async fn shutdown(self);
}

pub struct OpenSub {
    pub sub_id: String,
    pub filters: Vec<nostr::Filter>,
}
```

## Behaviors / acceptance criteria

The relay must implement enough of NIP-01 to be useful for testing the acquirer. **Minimum viable subset**:

1. **Connection**: accept WebSocket upgrade on `ws://127.0.0.1:N`.
2. **REQ**: parse `["REQ", <sub_id>, <filter1>, <filter2>, …]` per NIP-01. Match preloaded events against the filters; emit `["EVENT", <sub_id>, <event>]` for each match in `created_at`-descending order; then `["EOSE", <sub_id>]`.
3. **CLOSE**: parse `["CLOSE", <sub_id>]`; stop sending events for that sub.
4. **EVENT** from client: ignore (we never publish from acquirer in v1).
5. **Filter matching** must support: `kinds`, `authors`, `since`, `until`, `limit`, `ids`. Other fields can be ignored (and tests will avoid them unless we add support later).
6. **NIP-11**: respond to `GET /` with `Accept: application/nostr+json` by returning the configured doc. Without `Accept`, return 200 with a minimal HTML body or 404 (configurable).
7. **Forward subscription**: when `publish()` is called, broadcast the event to every open sub whose filter matches.

The relay does not need to:
- Validate event signatures (rust-nostr does this on the consumer side).
- Persist events.
- Handle AUTH (we explicitly opt out in v1).

## Testing

This crate is itself a test utility. Its tests verify the relay behaves correctly enough to be trusted by other crates' integration tests.

Tests in `tests/`:
- Connect with `nostr-relay-pool`, send a REQ for a kind, verify expected events are returned and EOSE arrives.
- Inject a notice; verify the client sees it.
- Open a sub, then `publish()`; verify the client receives the event.
- NIP-11: GET with the right Accept header returns the configured doc.

## Out of scope

- High performance. This is a test fixture; correctness > throughput.
- Persistence.
- Real-world quirks of `damus.io` etc. (e.g. trailing-slash sensitivity, weird CLOSED messages). Each such quirk gets a targeted test using the configurable hooks.
