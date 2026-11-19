//! Test helper: read events back from Parquet chunks.
//!
//! Production read paths go through `DuckDB` / Athena / polars (which speak
//! Parquet natively over local FS or S3). This module exists purely to give
//! Rust tests a way to verify the writer-side round trip without shelling out
//! to an external tool.
//!
//! Public surface is intentionally small: a single function that walks a
//! directory tree, reads every `*.parquet` file under it, decodes each row
//! into a `nostr::Event`, and returns them as `Vec<EventReadback>` (Event +
//! `ingested_at`, since `ingested_at` lives in a column outside the canonical
//! `nostr::Event` shape).

use std::fs::File;
use std::path::Path;

use arrow::array::{Array, Int32Array, Int64Array, ListArray, StringArray};
use nostr::Event;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("io error reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parquet error in {path}: {source}")]
    Parquet {
        path: String,
        #[source]
        source: parquet::errors::ParquetError,
    },
    #[error("arrow error in {path}: {source}")]
    Arrow {
        path: String,
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("unexpected schema in {path}: {reason}")]
    Schema { path: String, reason: String },
    #[error("event JSON reconstruction in {path} row {row}: {source}")]
    EventDecode {
        path: String,
        row: usize,
        #[source]
        source: serde_json::Error,
    },
}

/// One row read back from a Parquet chunk.
#[derive(Debug, Clone)]
pub struct EventReadback {
    pub event: Event,
    pub ingested_at: u64,
    pub d_tag: Option<String>,
}

/// Read all events from every `*.parquet` file under `root`, recursively.
/// Returns events in file-then-row order; callers wanting deterministic
/// ordering should sort by `event.id` (or whatever they need) themselves.
pub fn read_all_events(root: &Path) -> Result<Vec<EventReadback>, ReadError> {
    let mut paths = Vec::new();
    collect_parquet_files(root, &mut paths)?;
    paths.sort();

    let mut out = Vec::new();
    for p in paths {
        out.extend(read_chunk(&p)?);
    }
    Ok(out)
}

/// Read a single Parquet chunk file.
pub fn read_chunk(path: &Path) -> Result<Vec<EventReadback>, ReadError> {
    let path_str = path.display().to_string();
    let file = File::open(path).map_err(|e| ReadError::Io {
        path: path_str.clone(),
        source: e,
    })?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| ReadError::Parquet {
            path: path_str.clone(),
            source: e,
        })?;
    let reader = builder.build().map_err(|e| ReadError::Parquet {
        path: path_str.clone(),
        source: e,
    })?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| ReadError::Arrow {
            path: path_str.clone(),
            source: e,
        })?;
        decode_batch(&batch, &path_str, &mut out)?;
    }
    Ok(out)
}

fn decode_batch(
    batch: &arrow::array::RecordBatch,
    path: &str,
    out: &mut Vec<EventReadback>,
) -> Result<(), ReadError> {
    let id = downcast::<StringArray>(batch, "id", path)?;
    let pubkey = downcast::<StringArray>(batch, "pubkey", path)?;
    let kind = downcast::<Int32Array>(batch, "kind", path)?;
    let created_at = downcast::<Int64Array>(batch, "created_at", path)?;
    let content = downcast::<StringArray>(batch, "content", path)?;
    let sig = downcast::<StringArray>(batch, "sig", path)?;
    let d_tag = downcast::<StringArray>(batch, "d_tag", path)?;
    let ingested_at = downcast::<Int64Array>(batch, "ingested_at", path)?;
    let tags_col = batch
        .column_by_name("tags")
        .ok_or_else(|| ReadError::Schema {
            path: path.to_string(),
            reason: "missing column `tags`".to_string(),
        })?;
    let tags = tags_col
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| ReadError::Schema {
            path: path.to_string(),
            reason: "column `tags` is not a ListArray".to_string(),
        })?;

    for i in 0..batch.num_rows() {
        let tags_value = tags.value(i);
        let inner_list =
            tags_value
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| ReadError::Schema {
                    path: path.to_string(),
                    reason: "inner tags layer is not a ListArray".to_string(),
                })?;

        let mut tag_arrays: Vec<Vec<String>> = Vec::with_capacity(inner_list.len());
        for j in 0..inner_list.len() {
            let elem_value = inner_list.value(j);
            let strs =
                elem_value
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ReadError::Schema {
                        path: path.to_string(),
                        reason: "innermost tags element is not a StringArray".to_string(),
                    })?;
            let mut row = Vec::with_capacity(strs.len());
            for k in 0..strs.len() {
                row.push(strs.value(k).to_string());
            }
            tag_arrays.push(row);
        }

        let event_json = serde_json::json!({
            "id": id.value(i),
            "pubkey": pubkey.value(i),
            "kind": kind.value(i),
            "created_at": created_at.value(i),
            "content": content.value(i),
            "tags": tag_arrays,
            "sig": sig.value(i),
        });
        let event: Event =
            serde_json::from_value(event_json).map_err(|e| ReadError::EventDecode {
                path: path.to_string(),
                row: i,
                source: e,
            })?;

        out.push(EventReadback {
            event,
            ingested_at: u64::try_from(ingested_at.value(i)).unwrap_or(0),
            d_tag: if d_tag.is_null(i) {
                None
            } else {
                Some(d_tag.value(i).to_string())
            },
        });
    }
    Ok(())
}

fn downcast<'a, T: 'static>(
    batch: &'a arrow::array::RecordBatch,
    name: &str,
    path: &str,
) -> Result<&'a T, ReadError> {
    let col = batch.column_by_name(name).ok_or_else(|| ReadError::Schema {
        path: path.to_string(),
        reason: format!("missing column `{name}`"),
    })?;
    col.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| ReadError::Schema {
            path: path.to_string(),
            reason: format!("column `{name}` is the wrong array type"),
        })
}

fn collect_parquet_files(root: &Path, out: &mut Vec<std::path::PathBuf>) -> Result<(), ReadError> {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(ReadError::Io {
                path: root.display().to_string(),
                source: e,
            });
        }
    };

    for entry in entries {
        let entry = entry.map_err(|e| ReadError::Io {
            path: root.display().to_string(),
            source: e,
        })?;
        let p = entry.path();
        if p.is_dir() {
            collect_parquet_files(&p, out)?;
        } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
            out.push(p);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_writer::ChunkBuilder;
    use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};
    use tempfile::tempdir;

    fn make_event(kind: u16, content: &str, tags: Vec<Tag>) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(Kind::from_u16(kind), content)
            .tags(tags)
            .custom_created_at(Timestamp::from_secs(1_700_000_000))
            .sign_with_keys(&keys)
            .unwrap()
    }

    #[test]
    fn roundtrip_chunk_writer_to_reader() {
        let mut b = ChunkBuilder::new();
        let e1 = make_event(0, r#"{"name":"alice"}"#, vec![]);
        let e2 = make_event(
            30063,
            "release notes",
            vec![Tag::identifier("myapp@1.2.3")],
        );
        b.push(&e1, 100);
        b.push(&e2, 200);
        let bytes = b.serialize(9).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("c.parquet");
        std::fs::write(&path, &bytes).unwrap();

        let readback = read_chunk(&path).unwrap();
        assert_eq!(readback.len(), 2);

        // Find each event in the readback (order is per-sort, not arrival).
        let r1 = readback.iter().find(|r| r.event.id == e1.id).unwrap();
        let r2 = readback.iter().find(|r| r.event.id == e2.id).unwrap();

        assert_eq!(r1.event.id, e1.id);
        assert_eq!(r1.event.pubkey, e1.pubkey);
        assert_eq!(r1.event.content, e1.content);
        assert_eq!(r1.ingested_at, 100);
        assert!(r1.d_tag.is_none());

        assert_eq!(r2.event.id, e2.id);
        assert_eq!(r2.event.content, e2.content);
        assert_eq!(r2.ingested_at, 200);
        assert_eq!(r2.d_tag.as_deref(), Some("myapp@1.2.3"));
    }

    #[test]
    fn read_all_events_walks_subdirectories() {
        let dir = tempdir().unwrap();
        // Create a nested layout that mirrors the real chunk path scheme.
        let p1 = dir.path().join("c=replaceable/k=0/p=8/dt=2026-05-02");
        let p2 = dir.path().join("c=addressable/k=30063/p=a/dt=2026-05-02");
        std::fs::create_dir_all(&p1).unwrap();
        std::fs::create_dir_all(&p2).unwrap();

        let mut b1 = ChunkBuilder::new();
        b1.push(&make_event(0, "alice", vec![]), 1);
        std::fs::write(p1.join("c.parquet"), b1.serialize(9).unwrap()).unwrap();

        let mut b2 = ChunkBuilder::new();
        b2.push(
            &make_event(30063, "release", vec![Tag::identifier("x")]),
            2,
        );
        std::fs::write(p2.join("c.parquet"), b2.serialize(9).unwrap()).unwrap();

        let all = read_all_events(dir.path()).unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn nonexistent_root_returns_empty() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        assert!(read_all_events(&missing).unwrap().is_empty());
    }

    #[test]
    fn tags_preserved_through_roundtrip() {
        let tags = vec![
            Tag::custom(nostr::TagKind::Custom("p".into()), ["abc", "wss://relay"]),
            Tag::custom(nostr::TagKind::Custom("e".into()), ["def"]),
        ];
        let e = make_event(1, "hello", tags);

        let mut b = ChunkBuilder::new();
        b.push(&e, 0);
        let bytes = b.serialize(9).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("c.parquet");
        std::fs::write(&path, &bytes).unwrap();

        let readback = read_chunk(&path).unwrap();
        assert_eq!(readback.len(), 1);
        let got = &readback[0].event;
        assert_eq!(got.id, e.id);
        assert_eq!(got.tags.len(), e.tags.len());
        // Tag ordering must be preserved (NIP-01 cares about it).
        for (orig, recovered) in e.tags.iter().zip(got.tags.iter()) {
            assert_eq!(orig.as_slice(), recovered.as_slice());
        }
    }
}
