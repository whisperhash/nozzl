# nozzl-types - crate brief

## Purpose

Foundational types shared by every other crate in the workspace. Pure, no I/O, no async, no heavyweight deps. The single source of truth for Nostr event classification (replaceable / addressable / regular / ephemeral) and address-coordinate construction.

## Why this exists as its own crate

Other crates need these types but don't need each other. Putting them in `nozzl-archive` or `nozzl-acquirer` would create false cross-dependencies. A tiny shared types crate is the standard Rust workspace pattern.

## Dependencies

crates.io:
- `nostr` (rust-nostr core types — re-exports `Event`, `Kind`, `PublicKey`, `EventId`, `Tag`)
- `serde` (with `derive`) — for any types that flow through config or checkpoints
- `thiserror` — for the crate's error type
- `url` — to wrap relay URLs as a validated newtype

sister crates: none.

## Public API contract

```rust
// Re-exports for downstream convenience.
pub use nostr::{Event, EventId, Kind, PublicKey};

/// A relay URL, validated at construction.
#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RelayUrl(/* private — wrap url::Url with wss/ws scheme check */);

impl RelayUrl {
    pub fn parse(s: &str) -> Result<Self, RelayUrlError>;
    pub fn as_str(&self) -> &str;
}

/// An event observed from a specific relay. The unit of work flowing through
/// the bounded mpsc from acquirer to writer pool. `kind_slug` is the
/// human-readable kind slug from `kinds.toml`, attached at acquirer emit
/// time so a hot-reload of the slug map can't retroactively rewrite paths
/// for events already in flight.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EventEnvelope {
    pub relay: RelayUrl,
    pub event: nostr::Event,
    pub kind_slug: String,
}

/// NIP-01 event-class taxonomy. Pure function of `kind`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventClass {
    Regular,
    Replaceable,
    Addressable,
    Ephemeral,
}

impl EventClass {
    pub fn classify(kind: u16) -> Self;
}

/// The address coordinate that uniquely identifies a replaceable or addressable
/// event slot. None for regular/ephemeral events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddressCoord {
    Replaceable { kind: u16, pubkey: PublicKey },
    Addressable { kind: u16, pubkey: PublicKey, d: String },
}

impl AddressCoord {
    /// Returns Some(...) for replaceable or addressable kinds, None otherwise.
    /// For addressable kinds without a `d` tag, treats `d` as empty string.
    pub fn from_event(e: &nostr::Event) -> Option<Self>;
}

#[derive(Debug, thiserror::Error)]
pub enum RelayUrlError {
    #[error("invalid URL: {0}")]
    Invalid(#[from] url::ParseError),
    #[error("scheme must be ws or wss, got {0}")]
    BadScheme(String),
}
```

## Acceptance criteria

1. `EventClass::classify` returns the correct class for **every** boundary value in [../design.md §2](../design.md#replaceable-vs-addressable-detection-nip-01) — kind 0, 3, 4, 999, 1000, 9999, 10000, 19999, 20000, 29999, 30000, 39999, 40000, u16::MAX, etc.
2. `AddressCoord::from_event`:
   - returns `Replaceable` for kinds 0, 3, and `[10000, 19999]`
   - returns `Addressable` for kinds `[30000, 39999]`
   - returns `None` for kinds in `[1000, 9999]` and `[20000, 29999]` and other regular ranges
   - for an addressable event with no `d` tag, returns `Addressable { …, d: String::new() }`
   - for an addressable event with multiple `d` tags, takes the first one (matches rust-nostr behavior)
3. `RelayUrl::parse` accepts `wss://…` and `ws://…`; rejects `https://`, empty, malformed.
4. `RelayUrl` round-trips through `serde_json` and `toml` cleanly.
5. `EventEnvelope` is `Clone + Send + Sync`.

## Testing

Unit tests in `src/`:
- Boundary table for `EventClass::classify` covering every range edge.
- `AddressCoord::from_event` against fabricated `Event` values for all four taxonomy outcomes plus the no-d-tag and multiple-d-tag corner cases.
- `RelayUrl::parse` accept/reject table.
- Property test (`proptest`) for `RelayUrl` round-trip serialization.

Integration tests: not required at this layer; downstream crates exercise these types in their own integration tests.

## Out of scope

- Any I/O, async, or trait that has an `async fn`.
- Custom `Event` validation logic beyond what `nostr` already provides (signature checking is the lib's responsibility).
- Any storage / serialization formats specific to a downstream crate (those live in the consuming crate).

## Notes

`PublicKey` and `EventId` are re-exported from `nostr` rather than wrapped — they're already cryptographically-validated newtypes upstream and re-wrapping adds friction without value.
