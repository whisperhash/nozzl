//! In-memory and on-disk state shapes.
//!
//! The on-disk schema is documented in `docs/design.md` §5.D8 and the work
//! package brief. This module is the single source of truth for the wire
//! format. Any change to the JSON shape must bump [`SCHEMA_VERSION`].

use std::collections::BTreeMap;

use nozzl_types::{EventId, RelayUrl, Timestamp};
use serde::{Deserialize, Serialize};

/// Current on-disk schema version. Files declaring any other value are rejected
/// with [`CheckpointError::UnsupportedSchema`](crate::CheckpointError::UnsupportedSchema).
pub const SCHEMA_VERSION: u32 = 1;

/// Per-(relay, kind) backfill progress.
///
/// Serializes with a `state` discriminator field, e.g. `{"state":"complete"}`,
/// `{"state":"pending"}`, or `{"state":"in_progress","until_cursor":1700000000}`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BackfillState {
    /// Onboarding backfill not yet started.
    #[default]
    Pending,
    /// Onboarding backfill running. `until_cursor` is the next paginated REQ's
    /// `until` value — i.e. we have walked back to `until_cursor` and the next
    /// page should request events older than this.
    InProgress { until_cursor: Timestamp },
    /// Onboarding backfill walked to the relay's earliest visible history.
    Complete,
}

/// State for a single (relay, kind) — the leaf entry in the on-disk file.
///
/// `BTreeMap` is used at the parent level so the serialized JSON is
/// deterministically ordered, simplifying byte-level format tests.
#[derive(Clone, Debug, Serialize, Deserialize, Default, Eq, PartialEq)]
pub(crate) struct KindState {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub since: Option<Timestamp>,

    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_event_id: Option<EventId>,

    pub backfill: BackfillState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default, Eq, PartialEq)]
pub(crate) struct RelayState {
    /// Map keyed by the kind as a decimal string — matches the JSON schema and
    /// keeps the file readable. `BTreeMap` for stable ordering.
    pub kinds: BTreeMap<String, KindState>,
}

/// The full on-disk file format. Versioned via `schema_version`.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct FileFormat {
    pub schema_version: u32,
    pub updated_at: String,
    /// Keyed by the relay URL string. `BTreeMap` for stable ordering.
    pub relays: BTreeMap<String, RelayState>,
}

/// Build the relay-keyed map for serialization, given an iterator over the
/// in-memory `(RelayUrl, kind, KindState)` triples.
pub(crate) fn build_relay_map<I>(entries: I) -> BTreeMap<String, RelayState>
where
    I: IntoIterator<Item = (RelayUrl, u16, KindState)>,
{
    let mut out: BTreeMap<String, RelayState> = BTreeMap::new();
    for (relay, kind, state) in entries {
        out.entry(relay.to_string())
            .or_default()
            .kinds
            .insert(kind.to_string(), state);
    }
    out
}

/// Inverse of [`build_relay_map`]: hydrate `(RelayUrl, kind) → KindState` from
/// the on-disk shape. Bad relay URLs or non-numeric kind keys are skipped with
/// a warning rather than aborting load — the file is operator-edited and we'd
/// rather start with partial state than refuse to boot.
pub(crate) fn flatten_relay_map(
    relays: BTreeMap<String, RelayState>,
) -> Vec<((RelayUrl, u16), KindState)> {
    let mut out = Vec::new();
    for (relay_str, relay_state) in relays {
        let Ok(relay) = RelayUrl::parse(&relay_str) else {
            tracing::warn!(relay = %relay_str, "skipping checkpoint entry with invalid relay URL");
            continue;
        };
        for (kind_str, kind_state) in relay_state.kinds {
            let Ok(kind) = kind_str.parse::<u16>() else {
                tracing::warn!(relay = %relay_str, kind = %kind_str, "skipping checkpoint entry with non-numeric kind");
                continue;
            };
            out.push(((relay.clone(), kind), kind_state));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_pending_serde_shape() {
        let v = serde_json::to_value(BackfillState::Pending).unwrap();
        assert_eq!(v, serde_json::json!({ "state": "pending" }));
    }

    #[test]
    fn backfill_complete_serde_shape() {
        let v = serde_json::to_value(BackfillState::Complete).unwrap();
        assert_eq!(v, serde_json::json!({ "state": "complete" }));
    }

    #[test]
    fn backfill_in_progress_serde_shape() {
        let v = serde_json::to_value(BackfillState::InProgress {
            until_cursor: 1_700_000_000,
        })
        .unwrap();
        assert_eq!(
            v,
            serde_json::json!({ "state": "in_progress", "until_cursor": 1_700_000_000 })
        );
    }

    #[test]
    fn backfill_deserializes_from_documented_shapes() {
        let pending: BackfillState =
            serde_json::from_value(serde_json::json!({ "state": "pending" })).unwrap();
        assert_eq!(pending, BackfillState::Pending);

        let complete: BackfillState =
            serde_json::from_value(serde_json::json!({ "state": "complete" })).unwrap();
        assert_eq!(complete, BackfillState::Complete);

        let in_progress: BackfillState = serde_json::from_value(
            serde_json::json!({ "state": "in_progress", "until_cursor": 42_i64 }),
        )
        .unwrap();
        assert_eq!(in_progress, BackfillState::InProgress { until_cursor: 42 });
    }
}
