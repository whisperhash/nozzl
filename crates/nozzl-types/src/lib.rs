//! Foundational types shared across the nozzl workspace.
//!
//! See `docs/crates/nozzl-types.md` for the contract.

mod event_class;
mod relay_url;

pub use event_class::{AddressCoord, EventClass};
pub use nostr::{Event, EventId, Kind, PublicKey};
pub use relay_url::{RelayUrl, RelayUrlError};

use serde::{Deserialize, Serialize};

/// An event observed from a specific relay. The unit of work flowing through
/// the bounded mpsc from acquirer to writer pool.
///
/// `kind_slug` carries the human-readable slug for the event's kind as
/// configured in `kinds.toml` at the moment the event was received. It's
/// part of the envelope (rather than looked up at write time) so that
/// hot-reloads of the slug map can't retroactively rewrite paths for
/// in-flight events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub relay: RelayUrl,
    pub event: Event,
    pub kind_slug: String,
}

/// Unix-seconds timestamp (signed, per NIP-01).
pub type Timestamp = i64;
