//! `HealthSource` implementation.
//!
//! v1 simplification of the rule in `docs/crates/nozzl-bin.md`, "Health source":
//! healthy = at least one configured relay's `relay_connection_state` gauge
//! shows 1 (Connected). The orchestration layer polls this once per second
//! from a dedicated gauge loop and stores the boolean in [`SharedHealth`].
//! `/health` reads it via the [`HealthSource`] trait.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use nozzl_http::HealthSource;

/// Atomic boolean shared between the gauge loop and the `/health` handler.
/// `Arc<Self>` is the canonical handle; cloning it is cheap.
#[derive(Debug, Default)]
pub struct SharedHealth {
    connected: AtomicBool,
}

impl SharedHealth {
    /// Construct a fresh, "unhealthy" snapshot. The gauge loop will flip
    /// it once relays come up.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Update the snapshot. Called by the gauge loop each second.
    pub fn set_connected(&self, value: bool) {
        self.connected.store(value, Ordering::Release);
    }

    /// Read the current snapshot. Used by tests; production reads go
    /// through the `HealthSource` trait impl.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }
}

#[async_trait]
impl HealthSource for SharedHealth {
    async fn is_healthy(&self) -> bool {
        self.is_connected()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn defaults_to_unhealthy() {
        let h = SharedHealth::new();
        assert!(!h.is_connected());
        assert!(!h.is_healthy().await);
    }

    #[tokio::test]
    async fn flips_on_set() {
        let h = SharedHealth::new();
        h.set_connected(true);
        assert!(h.is_connected());
        assert!(h.is_healthy().await);
        h.set_connected(false);
        assert!(!h.is_healthy().await);
    }
}
