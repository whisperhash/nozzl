//! Arrow batch builder + Parquet serializer for chunked event storage.
//!
//! Event flow inside a writer task:
//!
//! ```text
//! incoming nostr::Event ─► ChunkBuilder::push() ─► (held in Arrow column builders)
//!                                                                  │
//!                                              roll trigger fires  ▼
//!                                                ChunkBuilder::serialize()
//!                                                  │
//!                                                  ├─ sort by (pubkey ASC, created_at DESC)
//!                                                  ├─ build RecordBatch
//!                                                  ├─ encode via parquet::arrow::ArrowWriter
//!                                                  │   (zstd, bloom on id+pubkey, sorting cols)
//!                                                  └─► Vec<u8> Parquet bytes
//! ```
//!
//! Schema is universal across event kinds — `d_tag` projection is NULL for any
//! event whose first `d` tag is absent. See `docs/design.md` and the storage
//! plan in the project memory for rationale.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, StringBuilder,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::datatypes::{DataType, Field, Schema};
use nostr::Event;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::SortingColumn;
use parquet::schema::types::ColumnPath;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChunkSerializeError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] ParquetError),
}

/// Builder that accumulates events into per-column buffers, ready to be
/// serialized to a Parquet chunk on roll.
///
/// Memory layout:
/// - String columns are append-only `String`s (cheap to grow, single allocation
///   reused).
/// - Tag column is held as a flat `Vec<Vec<Vec<String>>>` until serialize time.
///   The doubly-nested `ListArray` builder is awkward; assembling at the end
///   from a Vec is simpler and runs once per chunk.
///
/// `ingested_at` is taken at receipt and stored alongside the event so replays
/// from WAL preserve the original observation time, decoupled from
/// `created_at` (author-claimed).
pub struct ChunkBuilder {
    ids: Vec<String>,
    pubkeys: Vec<String>,
    kinds: Vec<i32>,
    created_at: Vec<i64>,
    contents: Vec<String>,
    tags: Vec<Vec<Vec<String>>>,
    sigs: Vec<String>,
    d_tags: Vec<Option<String>>,
    ingested_at: Vec<i64>,
    estimated_bytes: usize,
}

impl Default for ChunkBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            ids: Vec::new(),
            pubkeys: Vec::new(),
            kinds: Vec::new(),
            created_at: Vec::new(),
            contents: Vec::new(),
            tags: Vec::new(),
            sigs: Vec::new(),
            d_tags: Vec::new(),
            ingested_at: Vec::new(),
            estimated_bytes: 0,
        }
    }

    /// Push one event. `ingested_at` is the writer's wall-clock at receipt.
    pub fn push(&mut self, event: &Event, ingested_at: u64) {
        let id = event.id.to_hex();
        let pubkey = event.pubkey.to_hex();
        let content = event.content.clone();
        let sig = event.sig.to_string();
        let d_tag = event.tags.identifier().map(str::to_owned);

        // Pre-tally an approximate byte count for the chunk-roll-size check.
        // Underestimates compression — that's fine, the threshold is a soft
        // upper bound, not an SLA.
        let mut size = id.len() + pubkey.len() + content.len() + sig.len() + 24;
        if let Some(ref d) = d_tag {
            size += d.len();
        }

        let tag_rows: Vec<Vec<String>> = event
            .tags
            .iter()
            .map(|t| {
                let row = t.as_slice().to_vec();
                size += row.iter().map(String::len).sum::<usize>() + row.len() * 2;
                row
            })
            .collect();

        self.ids.push(id);
        self.pubkeys.push(pubkey);
        self.kinds.push(i32::from(event.kind.as_u16()));
        self.created_at
            .push(i64::try_from(event.created_at.as_secs()).unwrap_or(i64::MAX));
        self.contents.push(content);
        self.tags.push(tag_rows);
        self.sigs.push(sig);
        self.d_tags.push(d_tag);
        self.ingested_at
            .push(i64::try_from(ingested_at).unwrap_or(i64::MAX));
        self.estimated_bytes = self.estimated_bytes.saturating_add(size);
    }

    #[must_use]
    pub fn row_count(&self) -> usize {
        self.ids.len()
    }

    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Sort the buffered rows by `(pubkey ASC, created_at DESC)`, encode to
    /// Parquet, and return the bytes. Borrows the builder so a failed
    /// serialize/PUT in the writer task can retry against the same in-memory
    /// rows on the next roll trigger.
    pub fn serialize(&self, zstd_level: i32) -> Result<Vec<u8>, ChunkSerializeError> {
        let batch = self.to_record_batch()?;
        let sorted = sort_batch(&batch)?;
        encode_parquet(&sorted, zstd_level)
    }

    /// Build a `RecordBatch` from the buffered rows in arrival order. Public
    /// for tests; production path goes through `serialize()`. Borrows the
    /// builder; the Arrow constructors allocate their own contiguous buffers
    /// and copy bytes in, so the source `Vec`s remain owned and reusable.
    pub fn to_record_batch(&self) -> Result<RecordBatch, ChunkSerializeError> {
        let id = StringArray::from_iter_values(self.ids.iter());
        let pubkey = StringArray::from_iter_values(self.pubkeys.iter());
        let kind = Int32Array::from_iter_values(self.kinds.iter().copied());
        let created_at = Int64Array::from_iter_values(self.created_at.iter().copied());
        let content = StringArray::from_iter_values(self.contents.iter());
        let sig = StringArray::from_iter_values(self.sigs.iter());
        let d_tag: StringArray = self.d_tags.iter().map(|o| o.as_deref()).collect();
        let ingested_at = Int64Array::from_iter_values(self.ingested_at.iter().copied());
        let tags = build_tags_array(&self.tags);

        let schema = nozzl_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id),
                Arc::new(pubkey),
                Arc::new(kind),
                Arc::new(created_at),
                Arc::new(content),
                Arc::new(tags),
                Arc::new(sig),
                Arc::new(d_tag),
                Arc::new(ingested_at),
            ],
        )?;
        Ok(batch)
    }
}

/// The Arrow schema used for every Parquet chunk. One schema across all event
/// kinds; kind-specific structure lives inside `content` and `tags`.
#[must_use]
pub fn nozzl_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("pubkey", DataType::Utf8, false),
        Field::new("kind", DataType::Int32, false),
        Field::new("created_at", DataType::Int64, false),
        Field::new("content", DataType::Utf8, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ))),
            false,
        ),
        Field::new("sig", DataType::Utf8, false),
        Field::new("d_tag", DataType::Utf8, true),
        Field::new("ingested_at", DataType::Int64, false),
    ]))
}

/// Construct a `ListArray<ListArray<String>>` from a `Vec<Vec<Vec<String>>>`.
///
/// We hand-build offsets rather than going through `ListBuilder<ListBuilder<…>>`
/// because (a) the API is fiddly with three layers of nullability and (b) we
/// know up front there are no nulls in the tag structure (every event has a
/// `tags` array; every tag has at least its name element).
fn build_tags_array(tags: &[Vec<Vec<String>>]) -> ListArray {
    // Inner level: flat list of all tag *element* strings, with offsets that
    // delimit each tag's slice.
    let total_inner_strings: usize = tags
        .iter()
        .flat_map(|row| row.iter().map(Vec::len))
        .sum();
    let mut inner_string_builder = StringBuilder::with_capacity(total_inner_strings, 0);
    let mut inner_offsets: Vec<i32> = Vec::with_capacity(
        tags.iter().map(Vec::len).sum::<usize>().saturating_add(1),
    );
    let mut outer_offsets: Vec<i32> = Vec::with_capacity(tags.len().saturating_add(1));

    inner_offsets.push(0);
    outer_offsets.push(0);
    let mut inner_count: i32 = 0;
    let mut tag_count_total: i32 = 0;

    for row in tags {
        for tag in row {
            for s in tag {
                inner_string_builder.append_value(s);
            }
            inner_count = inner_count.saturating_add(i32::try_from(tag.len()).unwrap_or(i32::MAX));
            inner_offsets.push(inner_count);
        }
        tag_count_total =
            tag_count_total.saturating_add(i32::try_from(row.len()).unwrap_or(i32::MAX));
        outer_offsets.push(tag_count_total);
    }

    let inner_string_array = inner_string_builder.finish();

    let inner_field = Arc::new(Field::new("item", DataType::Utf8, false));
    let inner_list = ListArray::new(
        inner_field,
        OffsetBuffer::new(inner_offsets.into()),
        Arc::new(inner_string_array),
        None,
    );

    let outer_field = Arc::new(Field::new(
        "item",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        false,
    ));
    ListArray::new(
        outer_field,
        OffsetBuffer::new(outer_offsets.into()),
        Arc::new(inner_list),
        None,
    )
}

/// Sort a `RecordBatch` by `(pubkey ASC, created_at DESC)`. Stable sort keeps
/// equal-key rows in arrival order.
fn sort_batch(batch: &RecordBatch) -> Result<RecordBatch, ChunkSerializeError> {
    let pubkey = batch
        .column_by_name("pubkey")
        .expect("schema includes pubkey")
        .clone();
    let created_at = batch
        .column_by_name("created_at")
        .expect("schema includes created_at")
        .clone();

    let columns = vec![
        SortColumn {
            values: pubkey,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        },
        SortColumn {
            values: created_at,
            options: Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
        },
    ];
    let indices = lexsort_to_indices(&columns, None)?;

    let sorted_cols: Result<Vec<ArrayRef>, _> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect();
    let sorted_cols = sorted_cols?;
    Ok(RecordBatch::try_new(batch.schema(), sorted_cols)?)
}

/// Encode a `RecordBatch` to Parquet bytes with our standard properties at the
/// given ZSTD level. The level is validated to `1..=22` at config load
/// (`nozzl-config`); `try_new` falls back to the crate default only as defense
/// in depth if an out-of-range value ever reaches here. The level is lossless —
/// it changes on-disk size and encode CPU only, never the decoded rows.
fn encode_parquet(batch: &RecordBatch, zstd_level: i32) -> Result<Vec<u8>, ChunkSerializeError> {
    let compression =
        Compression::ZSTD(ZstdLevel::try_new(zstd_level).unwrap_or_else(|_| ZstdLevel::default()));
    let props = writer_props(batch.num_rows() as u64, compression, None);
    encode_with_props(batch, props)
}

/// Encode a `RecordBatch` to Parquet bytes with the given writer properties.
/// Split out from [`encode_parquet`] so the zstd-level / page-size bench can
/// drive the real writer with swept properties without duplicating the
/// schema/write/close plumbing.
fn encode_with_props(
    batch: &RecordBatch,
    props: WriterProperties,
) -> Result<Vec<u8>, ChunkSerializeError> {
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(buf)
}

/// Build the standard chunk [`WriterProperties`], parameterized on the
/// compression codec and (optional) data-page size limit so the bench can
/// sweep them while reusing the exact production bloom / sort / statistics
/// setup. The bloom / sort / page-statistics configuration here is unchanged
/// from the previous inline construction; only the compression codec and the
/// optional data-page-size limit are now parameters. Production passes the
/// configured `parquet_zstd_level` (default 9) and `None` (parquet-rs default
/// 1 MiB data page) — so on-disk *structure* matches the old code, but the
/// compressed bytes differ because the level moved off the parquet-rs default
/// of 1.
fn writer_props(
    row_count: u64,
    compression: Compression,
    data_page_size_limit: Option<usize>,
) -> WriterProperties {
    // pubkey is column index 1, created_at is column index 3 in our schema.
    // Sorting columns metadata is informational; readers (DuckDB, etc) can
    // use it for further pruning.
    let sorting_columns = vec![
        SortingColumn::new(1, false, false),
        SortingColumn::new(3, true, false),
    ];

    // Bloom filter NDV must be set explicitly. The parquet-rs default is
    // 1,000,000 — at FPP=0.01 that produces a ~16 MiB Split Block Bloom
    // Filter per column per row group regardless of actual row count, which
    // utterly dominates file size for chunks that finish below the
    // configured row cap (e.g. on graceful shutdown). Size NDV from the
    // batch itself with a small floor; this gives blooms tight enough to
    // skip empty/tiny chunks cheaply while still covering full chunks.
    let ndv = row_count.max(BLOOM_NDV_FLOOR);

    // Bloom filters: enabled only on `id` and `pubkey` (the high-cardinality
    // columns that drive query pruning). We do NOT call
    // `set_bloom_filter_enabled(true)` because that would default-enable blooms
    // on every column — wasted bytes for low-cardinality columns like `kind`.
    let id_path = ColumnPath::from("id");
    let pubkey_path = ColumnPath::from("pubkey");
    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_bloom_filter_enabled(id_path.clone(), true)
        .set_column_bloom_filter_enabled(pubkey_path.clone(), true)
        .set_column_bloom_filter_fpp(id_path.clone(), BLOOM_FPP)
        .set_column_bloom_filter_fpp(pubkey_path.clone(), BLOOM_FPP)
        .set_column_bloom_filter_ndv(id_path, ndv)
        .set_column_bloom_filter_ndv(pubkey_path, ndv)
        .set_sorting_columns(Some(sorting_columns));
    if let Some(limit) = data_page_size_limit {
        builder = builder.set_data_page_size_limit(limit);
    }
    builder.build()
}

/// Bloom filter false-positive rate. 1% gives effective pruning on
/// high-cardinality lookups (id, pubkey) without dramatically inflating
/// filter size compared to the parquet-rs default of 5%.
const BLOOM_FPP: f64 = 0.01;

/// Lower bound on bloom filter NDV. Even a chunk of 1 row gets a bloom
/// sized for at least this many distinct values, which keeps the SBBF
/// useful and avoids degenerate single-block filters. At FPP=0.01,
/// NDV=1024 → ~1.5 KiB per column.
const BLOOM_NDV_FLOOR: u64 = 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;
    use tempfile::tempdir;

    fn make_addressable(d: &str) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(Kind::from_u16(30063), "release notes")
            .tags(vec![
                Tag::identifier(d),
                Tag::custom(nostr::TagKind::Custom("title".into()), ["MyApp"]),
            ])
            .custom_created_at(Timestamp::from_secs(1_700_000_000))
            .sign_with_keys(&keys)
            .unwrap()
    }

    fn make_replaceable(content: &str, created_at: u64) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(Kind::from_u16(0), content)
            .custom_created_at(Timestamp::from_secs(created_at))
            .sign_with_keys(&keys)
            .unwrap()
    }

    /// Faithful ZSTD-level / data-page-size bench against *real* on-disk chunks.
    ///
    /// Drives the actual production encoder (`writer_props` + `encode_with_props`)
    /// so the numbers reflect `parquet-rs`, not a different encoder (`duckdb`'s
    /// numbers don't capture our bloom filters or the 1 MiB default page split).
    /// Each candidate re-encodes every file 1:1 (same rows, same per-file bloom
    /// and footer overhead as production), so the sums are directly comparable
    /// to what the writer would have produced under the swept settings.
    ///
    /// Ignored by default (needs a real chunk dir). Run with:
    ///
    /// ```sh
    /// NOZZL_ZSTD_BENCH_DIR=/path/to/k=38422-routstr-analytics-snapshots \
    ///   cargo test -p nozzl-archive --release bench_zstd_levels -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "needs a real chunk directory via NOZZL_ZSTD_BENCH_DIR"]
    #[allow(clippy::cast_precision_loss)] // ratio display only; precision loss irrelevant
    fn bench_zstd_levels() {
        use arrow::compute::concat_batches;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // Recursively collect *.parquet under `dir`. Declared before any
        // statement to satisfy clippy::items_after_statements.
        fn collect(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap().flatten() {
                let p = entry.path();
                if p.is_dir() {
                    collect(&p, out);
                } else if p.extension().is_some_and(|e| e == "parquet") {
                    out.push(p);
                }
            }
        }
        const MIB: usize = 1024 * 1024;

        let dir = std::env::var("NOZZL_ZSTD_BENCH_DIR")
            .expect("set NOZZL_ZSTD_BENCH_DIR to a directory of real chunk .parquet files");
        let mut files = Vec::new();
        collect(std::path::Path::new(&dir), &mut files);
        files.sort();
        assert!(!files.is_empty(), "no .parquet files under {dir}");

        // Read each real file into one concatenated RecordBatch (one chunk =
        // one file = one row group in production), keeping its exact rows.
        let mut batches: Vec<RecordBatch> = Vec::with_capacity(files.len());
        let mut orig_on_disk: u64 = 0;
        let mut total_rows: usize = 0;
        for f in &files {
            orig_on_disk += std::fs::metadata(f).unwrap().len();
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(f).unwrap())
                .unwrap()
                .build()
                .unwrap();
            let parts: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
            let schema = parts[0].schema();
            let batch = concat_batches(&schema, &parts).unwrap();
            total_rows += batch.num_rows();
            batches.push(batch);
        }

        // (label, compression, data_page_size_limit). `None` == parquet-rs
        // default 1 MiB page — i.e. exactly what production writes today.
        let candidates: &[(&str, Compression, Option<usize>)] = &[
            (
                "zstd=1   page=1MiB (PRODUCTION)",
                Compression::ZSTD(ZstdLevel::try_new(1).unwrap()),
                None,
            ),
            (
                "zstd=3   page=1MiB",
                Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
                None,
            ),
            (
                "zstd=9   page=1MiB",
                Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
                None,
            ),
            (
                "zstd=19  page=1MiB",
                Compression::ZSTD(ZstdLevel::try_new(19).unwrap()),
                None,
            ),
            (
                "zstd=3   page=64MiB",
                Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
                Some(64 * MIB),
            ),
            (
                "zstd=9   page=64MiB",
                Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
                Some(64 * MIB),
            ),
            (
                "zstd=19  page=64MiB",
                Compression::ZSTD(ZstdLevel::try_new(19).unwrap()),
                Some(64 * MIB),
            ),
        ];

        println!(
            "\nfiles={} rows={} orig_on_disk={} bytes",
            files.len(),
            total_rows,
            orig_on_disk
        );
        println!(
            "{:<34} {:>14} {:>10}",
            "candidate", "encoded_bytes", "vs_orig"
        );
        for (label, compression, page) in candidates {
            let mut sum: u64 = 0;
            for batch in &batches {
                let props = writer_props(batch.num_rows() as u64, *compression, *page);
                sum += encode_with_props(batch, props).unwrap().len() as u64;
            }
            println!(
                "{label:<34} {sum:>14} {:>9.2}x",
                orig_on_disk as f64 / sum as f64
            );
        }
    }

    #[test]
    fn schema_has_expected_columns() {
        let s = nozzl_schema();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "id",
                "pubkey",
                "kind",
                "created_at",
                "content",
                "tags",
                "sig",
                "d_tag",
                "ingested_at"
            ]
        );
        // d_tag is the only nullable column.
        for (i, f) in s.fields().iter().enumerate() {
            let expected_nullable = f.name() == "d_tag";
            assert_eq!(
                f.is_nullable(),
                expected_nullable,
                "field {} at index {i} nullability",
                f.name()
            );
        }
    }

    #[test]
    fn roundtrip_record_batch_preserves_columns() {
        let mut b = ChunkBuilder::new();
        let e1 = make_replaceable("alice", 100);
        let e2 = make_addressable("myapp@1.0.0");
        b.push(&e1, 1_700_000_005);
        b.push(&e2, 1_700_000_006);

        let batch = b.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 9);

        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pubkey_col = batch
            .column_by_name("pubkey")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let d_tag_col = batch
            .column_by_name("d_tag")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(id_col.value(0), e1.id.to_hex());
        assert_eq!(id_col.value(1), e2.id.to_hex());
        assert_eq!(pubkey_col.value(0), e1.pubkey.to_hex());
        assert!(d_tag_col.is_null(0), "kind 0 has no d-tag");
        assert!(!d_tag_col.is_null(1));
        assert_eq!(d_tag_col.value(1), "myapp@1.0.0");
    }

    #[test]
    fn sort_orders_by_pubkey_asc_then_created_at_desc() {
        // We can't easily control pubkey ordering without crafting keys, so
        // we'll just verify the order is consistent with the rule.
        let mut b = ChunkBuilder::new();
        let mut events = Vec::new();
        for i in 0..5 {
            let e = make_replaceable(&format!("e{i}"), 1_000_000 + i * 100);
            events.push(e.clone());
            b.push(&e, 0);
        }
        let batch = b.to_record_batch().unwrap();
        let sorted = sort_batch(&batch).unwrap();

        let pubkey_col = sorted
            .column_by_name("pubkey")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let created_col = sorted
            .column_by_name("created_at")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Pubkeys must be non-decreasing.
        let mut prev: Option<&str> = None;
        for i in 0..sorted.num_rows() {
            let cur = pubkey_col.value(i);
            if let Some(p) = prev {
                assert!(p <= cur, "pubkeys not sorted: {p} vs {cur}");
            }
            prev = Some(cur);
        }

        // Within equal pubkeys (none here in practice with random keys, but
        // assert no obvious break): created_at must be non-increasing.
        for i in 1..sorted.num_rows() {
            if pubkey_col.value(i) == pubkey_col.value(i - 1) {
                assert!(
                    created_col.value(i) <= created_col.value(i - 1),
                    "created_at not desc within equal pubkey"
                );
            }
        }
    }

    #[test]
    fn parquet_roundtrip_via_file() {
        let mut b = ChunkBuilder::new();
        for i in 0..50 {
            b.push(&make_replaceable(&format!("e{i}"), 1_000_000 + i), 1_700_000_000 + i);
        }
        let bytes = b.serialize(9).unwrap();
        assert!(!bytes.is_empty());

        // Write to a temp file and read back.
        let dir = tempdir().unwrap();
        let path = dir.path().join("c.parquet");
        std::fs::write(&path, &bytes).unwrap();

        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let meta = reader.metadata();
        assert_eq!(meta.file_metadata().num_rows(), 50);
        assert_eq!(meta.num_row_groups(), 1);

        // Parquet schema should match what we declared.
        let parquet_schema = meta.file_metadata().schema_descr();
        let column_names: Vec<&str> = parquet_schema
            .columns()
            .iter()
            .map(|c| c.name())
            .collect();
        assert!(column_names.contains(&"id"));
        assert!(column_names.contains(&"pubkey"));
        assert!(column_names.contains(&"d_tag"));
    }

    #[test]
    fn bloom_filters_are_present_for_id_and_pubkey() {
        let mut b = ChunkBuilder::new();
        for i in 0..20 {
            b.push(&make_replaceable(&format!("e{i}"), 1_000_000 + i), i);
        }
        let bytes = b.serialize(9).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("bloom.parquet");
        std::fs::write(&path, &bytes).unwrap();

        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let row_group = reader.get_row_group(0).unwrap();

        // For the columns we enabled, bloom filter offsets must be set in the
        // column chunk metadata. This is the per-row-group footer info.
        let parquet_schema = reader.metadata().file_metadata().schema_descr();
        let id_idx = parquet_schema
            .columns()
            .iter()
            .position(|c| c.name() == "id")
            .unwrap();
        let pubkey_idx = parquet_schema
            .columns()
            .iter()
            .position(|c| c.name() == "pubkey")
            .unwrap();

        let id_meta = row_group.metadata().column(id_idx);
        let pubkey_meta = row_group.metadata().column(pubkey_idx);

        assert!(
            id_meta.bloom_filter_offset().is_some(),
            "id column must have a bloom filter"
        );
        assert!(
            pubkey_meta.bloom_filter_offset().is_some(),
            "pubkey column must have a bloom filter"
        );
    }

    // ---- Compression-level correctness ------------------------------------
    //
    // A published chunk must decode back to exactly the rows that went in —
    // byte-for-byte, at every ZSTD level. ZSTD is lossless by construction, so
    // these tests are really a guard against the level/page wiring (or any
    // future encode change) ever corrupting, truncating, or dropping data, and
    // a standing proof that raising `parquet_zstd_level` changes only on-disk
    // size, never fidelity. We verify three ways per row: exact per-column
    // equality vs. the input, identical decoded data across all levels, and
    // semantic integrity (the decoded NIP-01 fields re-hash to the same `id`
    // and the signature still verifies).

    #[derive(Clone, PartialEq, Eq, Debug)]
    struct DecodedRow {
        id: String,
        pubkey: String,
        kind: i32,
        created_at: i64,
        content: String,
        tags: Vec<Vec<String>>,
        sig: String,
        d_tag: Option<String>,
        ingested_at: i64,
    }

    fn col_str<'a>(b: &'a RecordBatch, name: &str) -> &'a StringArray {
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    /// Decode one row's `LIST<LIST<STRING>>` tags cell back to `Vec<Vec<String>>`.
    fn decode_tags_row(col: &ListArray, i: usize) -> Vec<Vec<String>> {
        let inner_any = col.value(i);
        let inner = inner_any.as_any().downcast_ref::<ListArray>().unwrap();
        (0..inner.len())
            .map(|j| {
                let strs = inner.value(j);
                let sa = strs.as_any().downcast_ref::<StringArray>().unwrap();
                (0..sa.len()).map(|k| sa.value(k).to_string()).collect()
            })
            .collect()
    }

    /// Decode every row of a serialized Parquet chunk through the real
    /// parquet→arrow reader (the same path DuckDB/Athena/polars use).
    fn decode_chunk(data: &[u8]) -> Vec<DecodedRow> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(data))
            .unwrap()
            .build()
            .unwrap();
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let id = col_str(&batch, "id");
            let pubkey = col_str(&batch, "pubkey");
            let content = col_str(&batch, "content");
            let sig = col_str(&batch, "sig");
            let d_tag = col_str(&batch, "d_tag");
            let kind = batch
                .column_by_name("kind")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let created = batch
                .column_by_name("created_at")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let ingested = batch
                .column_by_name("ingested_at")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let tags = batch
                .column_by_name("tags")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                out.push(DecodedRow {
                    id: id.value(i).to_string(),
                    pubkey: pubkey.value(i).to_string(),
                    kind: kind.value(i),
                    created_at: created.value(i),
                    content: content.value(i).to_string(),
                    tags: decode_tags_row(tags, i),
                    sig: sig.value(i).to_string(),
                    d_tag: (!d_tag.is_null(i)).then(|| d_tag.value(i).to_string()),
                    ingested_at: ingested.value(i),
                });
            }
        }
        out
    }

    /// The row we expect on disk for an event, mirroring `ChunkBuilder::push`.
    fn expected_row(ev: &Event, ingested_at: u64) -> DecodedRow {
        DecodedRow {
            id: ev.id.to_hex(),
            pubkey: ev.pubkey.to_hex(),
            kind: i32::from(ev.kind.as_u16()),
            created_at: i64::try_from(ev.created_at.as_secs()).unwrap_or(i64::MAX),
            content: ev.content.clone(),
            tags: ev.tags.iter().map(|t| t.as_slice().to_vec()).collect(),
            sig: ev.sig.to_string(),
            d_tag: ev.tags.identifier().map(str::to_owned),
            ingested_at: i64::try_from(ingested_at).unwrap_or(i64::MAX),
        }
    }

    /// Order-independent: chunks are sorted by `(pubkey, created_at DESC)` at
    /// write time, so compare as a multiset keyed by `(id, ingested_at)`.
    fn sort_rows(rows: &mut [DecodedRow]) {
        rows.sort_by(|a, b| {
            (&a.id, a.ingested_at, &a.content).cmp(&(&b.id, b.ingested_at, &b.content))
        });
    }

    /// Reconstruct a NIP-01 event from the decoded columns and confirm its
    /// `id` re-hashes and signature verifies — i.e. not one byte of the
    /// integrity-bearing fields (`pubkey` / `created_at` / `kind` / `tags` /
    /// `content`) drifted.
    fn reconstruct_and_verify(row: &DecodedRow) {
        use nostr::JsonUtil;
        let json = serde_json::json!({
            "id": row.id,
            "pubkey": row.pubkey,
            "created_at": row.created_at,
            "kind": row.kind,
            "tags": row.tags,
            "content": row.content,
            "sig": row.sig,
        });
        let ev =
            Event::from_json(json.to_string()).expect("decoded row must parse as a nostr event");
        ev.verify()
            .expect("decoded row must preserve id + signature integrity (no degradation)");
    }

    fn signed_with(keys: &Keys, kind: u16, content: &str, tags: Vec<Tag>, created: u64) -> Event {
        EventBuilder::new(Kind::from_u16(kind), content)
            .tags(tags)
            .custom_created_at(Timestamp::from_secs(created))
            .sign_with_keys(keys)
            .unwrap()
    }

    fn signed(kind: u16, content: &str, tags: Vec<Tag>, created: u64) -> Event {
        signed_with(&Keys::generate(), kind, content, tags, created)
    }

    /// A deliberately nasty cross-section: empty / unicode / emoji / embedded
    /// NUL & control-char content, large (200 KB) near-identical payloads under
    /// one pubkey (the `k=38422` redundancy + equal-pubkey sort path), absent
    /// vs. empty-string d-tags, and tags with empty values, multi-element
    /// e-tags, single-element markers, and emoji.
    fn corner_case_events() -> Vec<(Event, u64)> {
        const HEX: &str = "0000000000000000000000000000000000000000000000000000000000000001";
        let big = "A".repeat(200_000);
        let big_a = format!("{big}0");
        let big_b = format!("{big}1");
        let provider = Keys::generate();
        vec![
            (signed(0, "", vec![], 1_700_000_000), 10),
            (
                signed(
                    1,
                    "Ünîçödé 日本語 🤙🔥 \u{1F600} café",
                    vec![],
                    1_700_000_001,
                ),
                11,
            ),
            (
                signed(1, "a\nb\tc\r\"q\"\\z\u{0}end", vec![], 1_700_000_002),
                12,
            ),
            (signed(7, "🤙", vec![], 1_700_000_003), 13),
            (signed(1, "epoch", vec![], 0), 14),
            (signed(1, "far-future", vec![], 4_102_444_800), 15),
            (
                signed(
                    30063,
                    "release",
                    vec![
                        Tag::identifier("myapp@1.0.0"),
                        Tag::parse(["title", "My \"App\"\nv2"]).unwrap(),
                    ],
                    1_700_000_004,
                ),
                16,
            ),
            (
                signed(30063, "empty-d", vec![Tag::identifier("")], 1_700_000_005),
                17,
            ),
            (
                signed(
                    1,
                    "tagged",
                    vec![
                        Tag::parse(["e", HEX, "wss://relay.example", "root"]).unwrap(),
                        Tag::parse(["p", HEX]).unwrap(),
                        Tag::parse(["t", ""]).unwrap(),
                        Tag::parse(["-"]).unwrap(),
                        Tag::parse(["alt", "🤙 inside a tag"]).unwrap(),
                    ],
                    1_700_000_006,
                ),
                18,
            ),
            // Two versions of one addressable coordinate: same pubkey + d-tag,
            // ~99% identical 200 KB payloads, distinct created_at (and thus id).
            (
                signed_with(
                    &provider,
                    38422,
                    &big_a,
                    vec![Tag::identifier("p-x")],
                    1_700_000_100,
                ),
                19,
            ),
            (
                signed_with(
                    &provider,
                    38422,
                    &big_b,
                    vec![Tag::identifier("p-x")],
                    1_700_000_200,
                ),
                20,
            ),
        ]
    }

    #[test]
    fn compression_levels_are_lossless_over_corner_cases() {
        let events = corner_case_events();
        let mut expected: Vec<DecodedRow> =
            events.iter().map(|(e, i)| expected_row(e, *i)).collect();
        sort_rows(&mut expected);

        let mut prev: Option<Vec<DecodedRow>> = None;
        for level in [1, 3, 9, 19, 22] {
            let mut b = ChunkBuilder::new();
            for (e, ing) in &events {
                b.push(e, *ing);
            }
            let bytes = b.serialize(level).unwrap();
            let mut decoded = decode_chunk(&bytes);
            assert_eq!(
                decoded.len(),
                events.len(),
                "row count changed at zstd level {level}"
            );
            sort_rows(&mut decoded);
            assert_eq!(
                decoded, expected,
                "decoded rows differ from input at zstd level {level}"
            );
            for row in &decoded {
                reconstruct_and_verify(row);
            }
            if let Some(prev) = &prev {
                assert_eq!(
                    &decoded, prev,
                    "decoded data must be identical across zstd levels"
                );
            }
            prev = Some(decoded);
        }
    }

    #[test]
    fn duplicate_ids_both_survive_roundtrip() {
        // Re-archived event (issue #9): the same id arrives twice with distinct
        // ingested_at. The writer must keep both rows — dedup is a read-time
        // QUALIFY concern, not the writer's. Verify at a high level too.
        let e = make_replaceable("dup", 1_000);
        let mut b = ChunkBuilder::new();
        b.push(&e, 100);
        b.push(&e, 200);
        let decoded = decode_chunk(&b.serialize(19).unwrap());
        assert_eq!(decoded.len(), 2, "both re-archived rows must persist");
        assert!(decoded.iter().all(|r| r.id == e.id.to_hex()));
        let ings: std::collections::BTreeSet<i64> = decoded.iter().map(|r| r.ingested_at).collect();
        assert_eq!(ings, [100, 200].into_iter().collect());
    }

    #[test]
    fn large_redundant_payload_bytes_survive_intact() {
        // The k=38422 shape: a big, highly compressible payload must come back
        // byte-identical, not just "close", even at max compression.
        let content = format!("{}{}", "snapshot-".repeat(20_000), "🤙");
        let e = make_replaceable(&content, 1_700_000_000);
        let decoded = decode_chunk(&{
            let mut b = ChunkBuilder::new();
            b.push(&e, 1);
            b.serialize(22).unwrap()
        });
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].content, content, "large payload must be exact");
        reconstruct_and_verify(&decoded[0]);
    }

    #[test]
    fn large_multi_page_chunk_is_lossless_across_levels() {
        // The corner-case tests all fit in a single small data page. This one
        // exercises the page-split regime a real backfill hits: a content
        // column whose uncompressed size far exceeds parquet-rs's 1 MiB default
        // data-page limit, so the writer emits MANY data pages within the row
        // group and ZSTD compresses each independently. A page-boundary encode
        // bug would surface here and nowhere else. ~12k rows × ~120 B content
        // ≈ 1.4 MiB content column. Keys are drawn from a small pool so pubkeys
        // repeat (equal-pubkey sort path) without paying 12k keygens.
        const N: usize = 12_000;
        let pool: Vec<Keys> = (0..8).map(|_| Keys::generate()).collect();
        let events: Vec<(Event, u64)> = (0..N)
            .map(|i| {
                let keys = &pool[i % pool.len()];
                let content = format!("event-{i:06}-{}", "padding-".repeat(12));
                let e = signed_with(&keys.clone(), 1, &content, vec![], 1_000_000 + i as u64);
                (e, 1_700_000_000 + i as u64)
            })
            .collect();
        let mut expected: Vec<DecodedRow> =
            events.iter().map(|(e, i)| expected_row(e, *i)).collect();
        sort_rows(&mut expected);

        let mut prev: Option<Vec<DecodedRow>> = None;
        for level in [1, 22] {
            let mut b = ChunkBuilder::new();
            for (e, ing) in &events {
                b.push(e, *ing);
            }
            let bytes = b.serialize(level).unwrap();
            let mut decoded = decode_chunk(&bytes);
            assert_eq!(decoded.len(), N, "row count changed at level {level}");
            sort_rows(&mut decoded);
            assert_eq!(decoded, expected, "page-split chunk lost data at level {level}");
            if let Some(prev) = &prev {
                assert_eq!(&decoded, prev, "page-split decode differs across levels");
            }
            prev = Some(decoded);
        }
        // Spot-check signature integrity on a sample (verifying all 12k Schnorr
        // sigs would dominate test time); exhaustive equality above already
        // proves every column round-tripped byte-for-byte.
        let sample = prev.unwrap();
        for row in sample.iter().step_by(N / 50) {
            reconstruct_and_verify(row);
        }
    }

    #[test]
    fn empty_builder_has_zero_rows() {
        let b = ChunkBuilder::new();
        assert!(b.is_empty());
        assert_eq!(b.row_count(), 0);
        assert_eq!(b.estimated_bytes(), 0);
    }

    #[test]
    fn estimated_bytes_grows_with_pushes() {
        let mut b = ChunkBuilder::new();
        let initial = b.estimated_bytes();
        b.push(&make_replaceable("hello", 1), 0);
        assert!(b.estimated_bytes() > initial);
    }

    /// Regression: with parquet-rs's default bloom-filter NDV (1,000,000)
    /// every chunk — even tiny ones — landed at ~16 MiB because each bloom
    /// is sized for that NDV regardless of row count. We size NDV from the
    /// batch instead, with a small floor. A 20-row chunk's serialized size
    /// must therefore stay well under 100 KiB. If this regresses we'll see
    /// it instantly: a fresh backfill goes from ~50 MiB on disk to ~1 GiB.
    #[test]
    fn small_chunk_does_not_balloon_from_oversized_bloom_filters() {
        let mut b = ChunkBuilder::new();
        for i in 0..20 {
            b.push(&make_replaceable(&format!("e{i}"), 1_000_000 + i), i);
        }
        let bytes = b.serialize(9).unwrap();
        assert!(
            bytes.len() < 100 * 1024,
            "20-row chunk serialized to {} bytes (expected < 100 KiB); \
             check that bloom filter NDV is sized from the batch, not the parquet-rs default",
            bytes.len()
        );
    }
}
