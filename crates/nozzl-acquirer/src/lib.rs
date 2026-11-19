//! Relay-pool management: per-relay tasks, forward subscriptions, onboarding
//! backfills, NIP-11 awareness, REQ-split-on-rejection (filter-shape) with a
//! distinct concurrency-pressure back-off path, stuck-REQ recovery, and
//! hot-reload of config.
//!
//! See `docs/crates/nozzl-acquirer.md` for the contract and
//! `docs/design.md` §5.D6 / §5.D9 / §5.D10 for the canonical behaviour.

#![warn(clippy::all, clippy::pedantic)]
#![allow(
    // Rationale: this crate's public types are intentionally re-exported under
    // the same name as the module they live in (e.g. `AcquirerConfig` from
    // `config`). The public-API contract in `docs/crates/nozzl-acquirer.md`
    // calls for those exact names - accepting the lint here is the right
    // trade-off.
    clippy::module_name_repetitions,
    // Many short helper closures inside `select!`, `let _ =`, etc., make the
    // pedantic version of `if_not_else` overly noisy. Localized.
    clippy::if_not_else,
    // Internal control-flow only.
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    // We accept logical complexity in the relay-task state machine; refactoring
    // for cyclomatic-complexity targets would obscure the protocol shape.
    clippy::too_many_lines,
    // The relay-task spawn helper takes 10 args because every collaborator
    // (pool, sink, ckpt, metrics, channels, scalars) has to be plumbed through.
    // A builder struct would obscure the call site without simplifying anything.
    clippy::too_many_arguments,
    // The acquirer/relay-task code threads `Arc` and channel handles through
    // many functions; pass-by-value of `Arc` is intentional so callers retain
    // the option to clone before the call.
    clippy::needless_pass_by_value,
)]

mod acquirer;
mod config;
mod filter;
mod nip11;
mod rejection;
mod relay_task;
mod transport;

pub use acquirer::Acquirer;
pub use config::{AcquirerConfig, ActionPlan};
// Exposed for integration tests that need to recognize the dispatch-liveness
// probe's REQ shape (e.g. `ignore_req_if(probe-shape)` predicates). Not part
// of the binary's hot path — `relay_task` uses `filter::probe_filter()`
// directly via its own internal `use`.
pub use filter::PROBE_KIND;
// Exposed for integration tests that drive the exact issue-#8 reconnect
// sequence against a raw `RelayPool` (`barrier_prevents_connect_relay_race_wedge`,
// `fork_accessor_tracks_connection_task_lifecycle`). Production code calls
// `teardown_barrier_and_rearm` only from the probe-timeout recovery path in
// `relay_task`.
pub use relay_task::{TeardownBarrierOutcome, teardown_barrier_and_rearm};
// The `User-Agent` every relay handshake carries, and the transport that sends it: exposed
// for the integration test that connects to a relay refusing upgrades without one.
pub use transport::{USER_AGENT, UserAgentTransport};

// Re-exports useful to the binary crate that wires this together. Keeping
// them here means downstream code doesn't need a direct `nozzl-types` dep
// just to spell out the channel item.
pub use nozzl_types::EventEnvelope;
