//! Error and outcome types for the archive write path.

/// Outcome of a single `put_event` call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutOutcome {
    /// The event was accepted into the per-(class, kind, shard) writer
    /// task's bounded mpsc channel.
    ///
    /// Durability note: this is an *acceptance* outcome, not a durable
    /// commit. The writer task fsyncs each event to the WAL before pulling
    /// the next message off the channel, but that happens asynchronously
    /// after `put_event` returns. In normal operation the gap is sub-second
    /// (channel is bounded at 1024 messages so a saturated shard
    /// backpressures `put_event`). A WAL-append failure inside the writer
    /// task is logged and drops the event silently — it's already been
    /// counted by `events_written_total`.
    Written,
    /// An object with the same path already existed; nothing was overwritten.
    /// Reserved for backends that perform conditional writes; the chunked
    /// archive never emits this variant.
    AlreadyExists,
    /// The event was not eligible for archival.
    Skipped { reason: SkipReason },
}

/// Why an event was skipped instead of written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkipReason {
    /// The event's kind is neither replaceable nor addressable per NIP-01.
    NotReplaceableNorAddressable,
    /// The event's kind is outside the configured archive scope (e.g. a
    /// regular or ephemeral kind that the chunked archive will not store).
    UnsupportedKind { kind: u16 },
}

/// Errors produced by the archive write path.
#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    /// A non-`AlreadyExists` failure from the underlying object store.
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    /// JSON serialization of the event failed.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    /// A storage configuration was invalid or could not be initialized.
    #[error("invalid storage config: {0}")]
    Config(String),
    /// Generic structural error not covered by the categories above.
    #[error("archive error: {0}")]
    Other(String),
}
