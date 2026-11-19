//! Write-ahead log for in-flight events buffered by a chunk writer task.
//!
//! Each writer task owns one WAL file at a stable path. The WAL is the durability
//! boundary: an event is "received" only after its record has been appended to
//! the WAL and `fsync`ed to disk. Once the writer publishes a chunk derived from
//! the buffered events, the WAL is truncated to length 0 and reused for the next
//! batch.
//!
//! # Record format
//!
//! Each record is laid out as:
//!
//! ```text
//! [u32 little-endian payload_length]
//! [u64 little-endian ingested_at_secs]
//! [N bytes compact-JSON nostr::Event] (where N = payload_length - 8)
//! ```
//!
//! The JSON payload is exactly what `serde_json::to_vec(&event)` produces. We
//! store `ingested_at` outside the JSON so replay can faithfully restore the
//! original ingest wall-clock without mutating the event itself, and so the
//! reader does not have to parse JSON to know how big the record is.
//!
//! # Crash semantics
//!
//! A torn append (process killed mid-write) can leave a partial record at the
//! tail — either fewer than 4 length-prefix bytes, or a full prefix followed by
//! fewer than `payload_length` payload bytes. Replay detects both, stops
//! cleanly, and logs a warning. The torn record is discarded on the next
//! truncate (because the writer task starts the next chunk from the file's
//! current length, then truncates after publish).
//!
//! Per-record JSON-parse errors do not abort replay: the corrupt record is
//! reported as `ReplayItem::Corrupt` and iteration continues. A counter metric
//! lets operators alarm on this — corruption mid-file is unexpected.

use std::path::{Path, PathBuf};

use nostr::Event;
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

const LENGTH_PREFIX_BYTES: usize = 4;
const INGESTED_AT_BYTES: usize = 8;

/// WAL-specific errors. IO and serialization failures bubble up via `Io` /
/// `Serde`; structural problems detected during replay surface as
/// `ReplayItem::Corrupt` rather than errors so iteration can continue.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("wal io error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("wal serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

impl WalError {
    fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

/// One item produced by [`Wal::replay`].
#[derive(Debug)]
pub enum ReplayItem {
    /// A successfully decoded record.
    Event {
        ingested_at: u64,
        event: Box<Event>,
    },
    /// A record whose length prefix was valid but whose payload failed to
    /// decode as a Nostr event JSON. The reader has already advanced past it.
    Corrupt {
        offset: u64,
        len: u32,
        reason: String,
    },
}

/// Append-only write-ahead log handle. Owns an open `tokio::fs::File`.
///
/// The handle is `!Sync` (one writer task per WAL). Constructing two handles to
/// the same path concurrently is a programming error and has undefined
/// behavior — only one writer task should exist per `(class, kind, shard)`.
pub struct Wal {
    path: PathBuf,
    file: File,
}

impl Wal {
    /// Open (or create) a WAL file. The file is opened for append and read; any
    /// existing content is preserved. Call [`Wal::replay`] separately to
    /// recover its contents before appending new records.
    ///
    /// `path`'s parent must already exist.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self, WalError> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| WalError::io(&path, e))?;
        Ok(Self { path, file })
    }

    /// Append a record and fsync the file. After this returns, the record is
    /// durable.
    pub async fn append(&mut self, ingested_at: u64, event: &Event) -> Result<(), WalError> {
        self.append_no_sync(ingested_at, event).await?;
        self.file
            .sync_data()
            .await
            .map_err(|e| WalError::io(&self.path, e))?;
        Ok(())
    }

    /// Append a record but do NOT fsync. Caller is responsible for calling
    /// [`Wal::flush`] before treating the record as durable. Intended for the
    /// `wal_fsync_batch_ms` mode where many appends are batched into a single
    /// fsync.
    pub async fn append_no_sync(
        &mut self,
        ingested_at: u64,
        event: &Event,
    ) -> Result<(), WalError> {
        let event_json = serde_json::to_vec(event)?;
        let payload_len: u32 = u32::try_from(INGESTED_AT_BYTES + event_json.len())
            .expect("event JSON length must fit in u32 (would be a >4 GiB single event)");

        let mut buf = Vec::with_capacity(LENGTH_PREFIX_BYTES + payload_len as usize);
        buf.extend_from_slice(&payload_len.to_le_bytes());
        buf.extend_from_slice(&ingested_at.to_le_bytes());
        buf.extend_from_slice(&event_json);

        self.file
            .write_all(&buf)
            .await
            .map_err(|e| WalError::io(&self.path, e))?;
        Ok(())
    }

    /// Fsync the underlying file. Use after a sequence of `append_no_sync`
    /// calls to make those records durable.
    pub async fn flush(&mut self) -> Result<(), WalError> {
        self.file
            .sync_data()
            .await
            .map_err(|e| WalError::io(&self.path, e))?;
        Ok(())
    }

    /// Truncate the file to length 0, fsync, and seek to position 0. Called
    /// after a chunk has been durably published — the records covered by that
    /// chunk are no longer needed.
    pub async fn truncate(&mut self) -> Result<(), WalError> {
        self.file
            .set_len(0)
            .await
            .map_err(|e| WalError::io(&self.path, e))?;
        self.file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|e| WalError::io(&self.path, e))?;
        self.file
            .sync_all()
            .await
            .map_err(|e| WalError::io(&self.path, e))?;
        Ok(())
    }

    /// Returns the file path of this WAL.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Replay a WAL file, yielding records in append order.
///
/// The returned iterator produces:
/// - `ReplayItem::Event` for each successfully-decoded record;
/// - `ReplayItem::Corrupt` for records whose length prefix was valid but whose
///   JSON payload failed to parse (the reader skips past these and continues);
/// - terminates cleanly when the file ends, or when a torn write is detected
///   at the tail (logged via `tracing::warn!`).
///
/// This function does **not** mutate the WAL file. Truncation is the writer's
/// responsibility and only happens after a successful chunk publish.
pub async fn replay(path: impl AsRef<Path>) -> Result<Vec<ReplayItem>, WalError> {
    let path = path.as_ref();
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(WalError::io(path, e)),
    };

    let metadata = file
        .metadata()
        .await
        .map_err(|e| WalError::io(path, e))?;
    let total_len = metadata.len();

    let mut reader = tokio::io::BufReader::new(file);
    let mut out = Vec::new();
    let mut offset: u64 = 0;

    loop {
        // Length prefix
        if total_len.saturating_sub(offset) < LENGTH_PREFIX_BYTES as u64 {
            if total_len > offset {
                tracing::warn!(
                    path = %path.display(),
                    offset,
                    remaining = total_len - offset,
                    "wal tail has partial length prefix; treating as torn write"
                );
            }
            break;
        }
        let mut len_buf = [0u8; LENGTH_PREFIX_BYTES];
        reader
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| WalError::io(path, e))?;
        offset += LENGTH_PREFIX_BYTES as u64;
        let payload_len = u32::from_le_bytes(len_buf);

        // Sanity: a single record bigger than ~64 MiB strongly suggests
        // corruption. Cap at 64 MiB; larger values abort replay.
        if payload_len > 64 * 1024 * 1024 {
            tracing::error!(
                path = %path.display(),
                offset,
                payload_len,
                "wal record claims absurd size; aborting replay"
            );
            break;
        }

        // Payload
        if total_len.saturating_sub(offset) < u64::from(payload_len) {
            tracing::warn!(
                path = %path.display(),
                offset,
                claimed_len = payload_len,
                actual_remaining = total_len - offset,
                "wal tail has truncated payload; treating as torn write"
            );
            break;
        }
        let mut payload = vec![0u8; payload_len as usize];
        reader
            .read_exact(&mut payload)
            .await
            .map_err(|e| WalError::io(path, e))?;
        let record_offset = offset - LENGTH_PREFIX_BYTES as u64;
        offset += u64::from(payload_len);

        if payload.len() < INGESTED_AT_BYTES {
            out.push(ReplayItem::Corrupt {
                offset: record_offset,
                len: payload_len,
                reason: "payload shorter than 8-byte ingested_at preamble".to_string(),
            });
            continue;
        }
        let mut ingested_buf = [0u8; INGESTED_AT_BYTES];
        ingested_buf.copy_from_slice(&payload[..INGESTED_AT_BYTES]);
        let ingested_at = u64::from_le_bytes(ingested_buf);
        let event_json = &payload[INGESTED_AT_BYTES..];

        match serde_json::from_slice::<Event>(event_json) {
            Ok(event) => {
                out.push(ReplayItem::Event {
                    ingested_at,
                    event: Box::new(event),
                });
            }
            Err(e) => {
                out.push(ReplayItem::Corrupt {
                    offset: record_offset,
                    len: payload_len,
                    reason: format!("nostr::Event JSON parse failed: {e}"),
                });
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind};
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    fn make_event(content: &str) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(Kind::from_u16(1), content)
            .sign_with_keys(&keys)
            .unwrap()
    }

    #[tokio::test]
    async fn empty_wal_replays_as_no_op() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.wal");
        let mut wal = Wal::open(&path).await.unwrap();
        wal.flush().await.unwrap();

        let items = replay(&path).await.unwrap();
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn nonexistent_path_replays_as_no_op() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does-not-exist.wal");
        let items = replay(&path).await.unwrap();
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn append_and_replay_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("rt.wal");
        let mut wal = Wal::open(&path).await.unwrap();

        let e1 = make_event("hello");
        let e2 = make_event("world");
        wal.append(1_700_000_001, &e1).await.unwrap();
        wal.append(1_700_000_002, &e2).await.unwrap();
        // Drop the wal handle to ensure no in-flight buffering hides bugs.
        drop(wal);

        let items = replay(&path).await.unwrap();
        assert_eq!(items.len(), 2);
        match &items[0] {
            ReplayItem::Event { ingested_at, event } => {
                assert_eq!(*ingested_at, 1_700_000_001);
                assert_eq!(event.id, e1.id);
            }
            ReplayItem::Corrupt { .. } => panic!("expected Event"),
        }
        match &items[1] {
            ReplayItem::Event { ingested_at, event } => {
                assert_eq!(*ingested_at, 1_700_000_002);
                assert_eq!(event.id, e2.id);
            }
            ReplayItem::Corrupt { .. } => panic!("expected Event"),
        }
    }

    #[tokio::test]
    async fn truncate_yields_empty_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("trunc.wal");
        let mut wal = Wal::open(&path).await.unwrap();
        let e = make_event("about to be truncated");
        wal.append(42, &e).await.unwrap();
        wal.truncate().await.unwrap();

        // After truncate, file exists at length 0.
        let meta = tokio::fs::metadata(&path).await.unwrap();
        assert_eq!(meta.len(), 0);

        let items = replay(&path).await.unwrap();
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn append_after_truncate_works() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("post-trunc.wal");
        let mut wal = Wal::open(&path).await.unwrap();
        wal.append(1, &make_event("first batch")).await.unwrap();
        wal.truncate().await.unwrap();

        let e = make_event("second batch");
        wal.append(2, &e).await.unwrap();
        drop(wal);

        let items = replay(&path).await.unwrap();
        assert_eq!(items.len(), 1);
        if let ReplayItem::Event { ingested_at, event } = &items[0] {
            assert_eq!(*ingested_at, 2);
            assert_eq!(event.id, e.id);
        } else {
            panic!("expected Event");
        }
    }

    #[tokio::test]
    async fn batched_fsync_works() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("batched.wal");
        let mut wal = Wal::open(&path).await.unwrap();
        for i in 0..10u64 {
            wal.append_no_sync(1_700_000_000 + i, &make_event(&format!("e{i}")))
                .await
                .unwrap();
        }
        wal.flush().await.unwrap();
        drop(wal);

        let items = replay(&path).await.unwrap();
        assert_eq!(items.len(), 10);
        for (i, it) in items.iter().enumerate() {
            if let ReplayItem::Event { ingested_at, .. } = it {
                assert_eq!(*ingested_at, 1_700_000_000 + i as u64);
            } else {
                panic!("expected Event");
            }
        }
    }

    /// Simulate a torn write where the process died after writing the length
    /// prefix but before flushing the payload. The truncated tail must be
    /// detected, the previous records returned, and replay must terminate
    /// cleanly without error.
    #[tokio::test]
    async fn torn_write_truncated_payload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("torn-payload.wal");
        let mut wal = Wal::open(&path).await.unwrap();
        wal.append(1, &make_event("complete")).await.unwrap();

        // Append a malformed tail: length prefix claims 100 bytes, but only 10
        // follow. We bypass the `Wal::append` API by writing raw bytes via the
        // file handle.
        wal.file
            .write_all(&100u32.to_le_bytes())
            .await
            .unwrap();
        wal.file.write_all(&[0u8; 10]).await.unwrap();
        wal.flush().await.unwrap();
        drop(wal);

        let items = replay(&path).await.unwrap();
        assert_eq!(items.len(), 1, "torn tail must not produce a record");
        assert!(matches!(items[0], ReplayItem::Event { .. }));
    }

    /// Simulate a torn write where the process died mid-length-prefix.
    #[tokio::test]
    async fn torn_write_partial_length_prefix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("torn-prefix.wal");
        let mut wal = Wal::open(&path).await.unwrap();
        wal.append(1, &make_event("complete")).await.unwrap();
        // Write 2 of the 4 length-prefix bytes, then nothing.
        wal.file.write_all(&[0xab, 0xcd]).await.unwrap();
        wal.flush().await.unwrap();
        drop(wal);

        let items = replay(&path).await.unwrap();
        assert_eq!(items.len(), 1);
        assert!(matches!(items[0], ReplayItem::Event { .. }));
    }

    /// A complete record whose payload bytes are corrupt (not valid JSON for
    /// `nostr::Event`) must surface as `Corrupt` and not stop iteration.
    #[tokio::test]
    async fn corrupt_record_is_reported_and_skipped() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.wal");
        let mut wal = Wal::open(&path).await.unwrap();
        wal.append(1, &make_event("first")).await.unwrap();

        // Manually write a record with valid length prefix + ingested_at
        // preamble, but garbage JSON bytes.
        let garbage = b"this is not a nostr event";
        let payload_len = u32::try_from(INGESTED_AT_BYTES + garbage.len()).unwrap();
        wal.file.write_all(&payload_len.to_le_bytes()).await.unwrap();
        wal.file.write_all(&7u64.to_le_bytes()).await.unwrap();
        wal.file.write_all(garbage).await.unwrap();
        wal.flush().await.unwrap();

        let e3 = make_event("third");
        wal.append(99, &e3).await.unwrap();
        drop(wal);

        let items = replay(&path).await.unwrap();
        assert_eq!(items.len(), 3);
        assert!(matches!(items[0], ReplayItem::Event { .. }));
        match &items[1] {
            ReplayItem::Corrupt { reason, .. } => {
                assert!(reason.contains("nostr::Event JSON parse failed"));
            }
            ReplayItem::Event { .. } => panic!("expected Corrupt"),
        }
        match &items[2] {
            ReplayItem::Event { event, ingested_at } => {
                assert_eq!(*ingested_at, 99);
                assert_eq!(event.id, e3.id);
            }
            ReplayItem::Corrupt { .. } => panic!("expected Event"),
        }
    }

    /// Replaying the same file twice yields the same items both times. Used
    /// by the writer to recover from a crash mid-replay.
    #[tokio::test]
    async fn replay_is_idempotent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("idempotent.wal");
        let mut wal = Wal::open(&path).await.unwrap();
        for i in 0..5u64 {
            wal.append(i, &make_event(&format!("e{i}"))).await.unwrap();
        }
        drop(wal);

        let first = replay(&path).await.unwrap();
        let second = replay(&path).await.unwrap();
        assert_eq!(first.len(), 5);
        assert_eq!(second.len(), 5);
        for (a, b) in first.iter().zip(second.iter()) {
            match (a, b) {
                (
                    ReplayItem::Event {
                        ingested_at: ia,
                        event: ea,
                    },
                    ReplayItem::Event {
                        ingested_at: ib,
                        event: eb,
                    },
                ) => {
                    assert_eq!(ia, ib);
                    assert_eq!(ea.id, eb.id);
                }
                _ => panic!("mismatched ReplayItem kinds across replays"),
            }
        }
    }
}
