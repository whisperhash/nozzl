//! Read archived nostr events for a single kind back out of the chunked Parquet
//! archive as newline-delimited bare NIP-01 JSON, by shelling out to the `duckdb`
//! CLI. The output is byte-interchangeable with strfry `scan`. The canonical
//! endpoint contract is `docs/design.md` §5.D10 (Export endpoint).
//!
//! Two consumers share one core ([`spawn_export`]):
//! - the `GET /export/{kind}` HTTP handler in `nozzl-http` (streams to the client),
//! - the `nozzl-export` CLI bin (streams to a file / stdout over any archive root,
//!   e.g. a read-only mount or a local dev tree).
//!
//! The query logic ([`sql`]) is pure and unit-tested; [`runner`] does the
//! filesystem pre-check, spawns duckdb, and owns the child + single-flight permit
//! for the lifetime of the stream.

#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    // Panics in this crate are internal-invariant violations (piped stdio always
    // present after a successful spawn). Matches nozzl-archive's stance.
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

pub mod runner;
mod sql;

pub use runner::{ExportError, ExportParams, ExportStream, spawn_export};

/// Which versions to export.
///
/// `Latest` (the default) collapses replaceable/addressable events to one row per
/// coordinate (replaceable → `(kind, pubkey)`; addressable → `(kind, pubkey, d)`).
/// `History` keeps every version. **Regular kinds ignore the mode** — they have no
/// coordinate, so every (id-deduped) event is returned in either mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum ExportMode {
    /// Newest event per coordinate (replaceable/addressable); all events (regular).
    #[default]
    Latest,
    /// Every version of every event.
    History,
}

impl std::str::FromStr for ExportMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "latest" => Ok(Self::Latest),
            "history" => Ok(Self::History),
            _ => Err(()),
        }
    }
}
