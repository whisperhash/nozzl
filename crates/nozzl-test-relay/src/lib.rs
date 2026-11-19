//! In-process WebSocket test relay for integration tests.
//!
//! Provides a [`TestRelay`] that implements a minimum viable subset of NIP-01
//! plus NIP-11, so downstream crates' integration tests can drive a relay with
//! exact, deterministic behavior (force a NOTICE, force a CLOSED, etc.).
//!
//! See `docs/crates/nozzl-test-relay.md` for the contract.

#![warn(clippy::pedantic)]
// Mutex poison panics are documented at the type level; per-method `# Panics`
// sections would be noise.
#![allow(clippy::missing_panics_doc)]

mod builder;
mod handle;
mod server;
mod state;

pub use builder::{TestRelay, TestRelayBuilder};
pub use handle::{OpenSub, TestRelayHandle};

/// Errors surfaced by the test relay. Currently used internally; tests use the
/// fallible `start` path implicitly by panicking on failure.
#[derive(Debug, thiserror::Error)]
pub enum TestRelayError {
    /// Failed to bind the TCP listener.
    #[error("failed to bind listener: {0}")]
    Bind(#[from] std::io::Error),
}
