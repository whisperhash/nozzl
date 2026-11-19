//! Config schema, TOML loader, and file watcher.
//!
//! See `docs/crates/nozzl-config.md` for the contract and
//! `docs/design.md §5.D9` for the on-disk schema.

mod error;
mod loader;
mod schema;
mod watcher;

pub use error::ConfigError;
pub use loader::load;
pub use schema::{
    AcquirerConfig, CheckpointConfig, Config, KindEntry, LogFormat, ObservabilityConfig,
    RelayEntry, RuntimeConfig, SourcesConfig, StorageBackend, StorageConfig, StorageMetricsConfig,
};
pub use watcher::{ConfigWatcher, watch};
