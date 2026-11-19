//! `ConfigError`: every failure mode of loading or watching the three
//! config files.

use std::io;
use std::path::PathBuf;

/// Errors that can occur while loading or watching the configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Reading a config file from disk failed.
    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// A file's contents could not be parsed as TOML, or didn't match the
    /// expected schema.
    #[error("failed to parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    /// The parsed contents violated a structural invariant (empty list,
    /// missing required field for the chosen backend, kind out of range,
    /// etc.).
    #[error("invalid configuration: {0}")]
    Validation(String),

    /// The filesystem watcher could not be initialized or its target
    /// directory could not be subscribed to.
    #[error("filesystem watcher error: {0}")]
    Watch(#[from] notify::Error),
}

impl ConfigError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    pub(crate) fn parse(path: impl Into<PathBuf>, source: toml::de::Error) -> Self {
        Self::Parse {
            path: path.into(),
            source,
        }
    }

    pub(crate) fn validation(msg: impl Into<String>) -> Self {
        Self::Validation(msg.into())
    }
}
