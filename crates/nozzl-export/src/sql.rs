//! Pure, unit-testable builders for the `DuckDB` query + Parquet glob. No I/O, no
//! free-form input: the only externally-derived bytes that reach the SQL are the
//! single-quote-escaped glob literal — `kind` (a `u16`) and `mode` (an enum) only
//! select static SQL fragments and shape the glob.

use std::path::Path;

use nozzl_types::EventClass;

use crate::ExportMode;

/// Escape a string for embedding as a single-quoted SQL literal (double each `'`).
pub(crate) fn sql_str_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The per-kind chunk-directory segment (`k={kind}`, exact — slug is no longer on
/// disk). Single source of truth for the kind-segment match rule, shared by
/// [`kind_glob`] and the filesystem pre-check so they cannot drift.
///
/// It must be matched **exactly** (not as a prefix): with the slug suffix gone,
/// a prefix match on `k=3` would also catch `k=30`, `k=300`, etc.
pub(crate) fn kind_dir_name(kind: u16) -> String {
    format!("k={kind}")
}

/// Build the `read_parquet` glob for `(archive_root, class, kind)`:
/// `{archive_root}/{class_dir}/k={kind}/**/*.parquet`. Returns `None` for the
/// non-archived (Ephemeral) class. Reuses `nozzl_archive::class_dir_segment` as the
/// single source of truth for the (Hive `c=…`) class directory name.
pub(crate) fn kind_glob(archive_root: &Path, class: EventClass, kind: u16) -> Option<String> {
    let class_dir = nozzl_archive::class_dir_segment(class)?;
    // Lossy is acceptable: archive roots are ASCII paths (config/mount/S3 prefix).
    // A non-UTF8 root would yield a glob with replacement chars that matches
    // nothing — surfacing as a clean 404, never silent wrong data.
    let root = archive_root.to_string_lossy();
    let root = root.trim_end_matches('/');
    let seg = kind_dir_name(kind);
    Some(format!("{root}/{class_dir}/{seg}/**/*.parquet"))
}

/// Build the full `COPY (...) TO '/dev/stdout' (FORMAT JSON, ARRAY false)`
/// statement that streams newline-delimited bare NIP-01 events.
///
/// - `ARRAY false` is **mandatory**: it makes `DuckDB` emit one JSON object per line
///   (ndjson) instead of a single enclosing JSON array.
/// - `hive_partitioning` is **deliberately not used**: the glob already pins one
///   kind directory and the schema carries no `k`/`p`/`dt` columns, so enabling it
///   would only add phantom partition columns and inference risk.
/// - id-level dedup runs first (`PARTITION BY id ORDER BY ingested_at`); in `Latest`
///   mode a second window keeps the newest event per coordinate. Regular kinds and
///   `History` mode stop after id-dedup.
/// - Only the 7 NIP-01 columns are projected; `d_tag`/`ingested_at` exist in the
///   CTEs purely to drive the windows.
pub(crate) fn copy_statement(glob: &str, class: EventClass, mode: ExportMode) -> String {
    let glob_lit = sql_str_lit(glob);
    let base = format!(
        "WITH src AS (\n  \
           SELECT id, pubkey, kind, created_at, content, tags, sig, d_tag, ingested_at\n  \
           FROM read_parquet({glob_lit})\n\
         ),\n\
         deduped AS (\n  \
           SELECT * FROM src\n  \
           QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingested_at) = 1\n\
         )"
    );

    // Pick the final relation. Latest mode adds a per-coordinate window for the two
    // replaceable classes; everything else (regular kinds, history mode) is just the
    // id-deduped set.
    let (final_rel, extra_cte) = match (class, mode) {
        (EventClass::Replaceable, ExportMode::Latest) => (
            "latest",
            Some(
                "latest AS (\n  \
                   SELECT * FROM deduped\n  \
                   QUALIFY row_number() OVER (PARTITION BY kind, pubkey ORDER BY created_at DESC) = 1\n\
                 )",
            ),
        ),
        (EventClass::Addressable, ExportMode::Latest) => (
            "latest",
            Some(
                "latest AS (\n  \
                   SELECT * FROM deduped\n  \
                   QUALIFY row_number() OVER (PARTITION BY kind, pubkey, d_tag ORDER BY created_at DESC) = 1\n\
                 )",
            ),
        ),
        _ => ("deduped", None),
    };

    let ctes = match extra_cte {
        Some(c) => format!("{base},\n{c}"),
        None => base,
    };

    format!(
        "COPY (\n{ctes}\n\
         SELECT id, pubkey, kind, created_at, content, tags, sig FROM {final_rel}\n\
         ) TO '/dev/stdout' (FORMAT JSON, ARRAY false)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn escapes_single_quotes() {
        assert_eq!(sql_str_lit("O'Brien"), "'O''Brien'");
        assert_eq!(sql_str_lit("plain"), "'plain'");
    }

    #[test]
    fn glob_per_class() {
        let root = Path::new("/data/events");
        assert_eq!(
            kind_glob(root, EventClass::Replaceable, 10002).unwrap(),
            "/data/events/c=replaceable/k=10002/**/*.parquet"
        );
        assert_eq!(
            kind_glob(root, EventClass::Addressable, 30023).unwrap(),
            "/data/events/c=addressable/k=30023/**/*.parquet"
        );
        assert_eq!(
            kind_glob(root, EventClass::Regular, 9735).unwrap(),
            "/data/events/c=regular/k=9735/**/*.parquet"
        );
        // Ephemeral is never archived.
        assert!(kind_glob(root, EventClass::Ephemeral, 22500).is_none());
    }

    #[test]
    fn trailing_slash_in_root_is_normalized() {
        assert_eq!(
            kind_glob(Path::new("/data/events/"), EventClass::Regular, 1).unwrap(),
            "/data/events/c=regular/k=1/**/*.parquet"
        );
    }

    /// Exact kind-segment match: `k=3` must not be a prefix of `k=30` now that the
    /// slug suffix (which used to anchor the boundary) is gone.
    #[test]
    fn kind_dir_name_is_exact_not_prefix() {
        assert_eq!(kind_dir_name(3), "k=3");
        assert_ne!(kind_dir_name(3), "k=30");
        assert!(!"k=30".starts_with(&format!("{}/", kind_dir_name(3))));
    }

    /// The ndjson + no-hive-partitioning contract must hold in CI without duckdb.
    #[test]
    fn ndjson_contract_locked() {
        let glob = kind_glob(Path::new("/d"), EventClass::Replaceable, 10002).unwrap();
        let sql = copy_statement(&glob, EventClass::Replaceable, ExportMode::Latest);
        assert!(
            sql.contains("FORMAT JSON, ARRAY false"),
            "must emit ndjson: {sql}"
        );
        assert!(
            !sql.contains("hive_partitioning"),
            "must not enable hive partitioning: {sql}"
        );
        // Only the 7 NIP-01 columns are projected in the final SELECT.
        let final_select = sql.rsplit_once("SELECT").unwrap().1;
        assert!(
            !final_select.contains("d_tag"),
            "d_tag must not be projected: {sql}"
        );
        assert!(
            !final_select.contains("ingested_at"),
            "ingested_at must not be projected: {sql}"
        );
    }

    #[test]
    fn replaceable_latest_has_both_windows() {
        let sql = copy_statement("g", EventClass::Replaceable, ExportMode::Latest);
        assert!(sql.contains("PARTITION BY id ORDER BY ingested_at"));
        assert!(sql.contains("PARTITION BY kind, pubkey ORDER BY created_at DESC"));
    }

    #[test]
    fn addressable_latest_partitions_by_d_tag() {
        let sql = copy_statement("g", EventClass::Addressable, ExportMode::Latest);
        assert!(sql.contains("PARTITION BY kind, pubkey, d_tag ORDER BY created_at DESC"));
    }

    #[test]
    fn regular_ignores_mode_id_dedup_only() {
        let latest = copy_statement("g", EventClass::Regular, ExportMode::Latest);
        let history = copy_statement("g", EventClass::Regular, ExportMode::History);
        // mode is ignored for regular kinds: identical SQL, id-dedup only.
        assert_eq!(latest, history);
        assert!(latest.contains("PARTITION BY id ORDER BY ingested_at"));
        assert!(
            !latest.contains("created_at DESC"),
            "no latest window for regular"
        );
    }

    #[test]
    fn history_keeps_all_versions_for_replaceable() {
        let sql = copy_statement("g", EventClass::Replaceable, ExportMode::History);
        assert!(sql.contains("PARTITION BY id ORDER BY ingested_at"));
        assert!(
            !sql.contains("created_at DESC"),
            "history keeps all versions"
        );
    }

    /// Kind/mode never inject raw SQL beyond the escaped glob: two different roots
    /// that yield the same glob produce byte-identical statements.
    #[test]
    fn only_glob_literal_varies() {
        let a = copy_statement("same-glob", EventClass::Replaceable, ExportMode::Latest);
        let b = copy_statement("same-glob", EventClass::Replaceable, ExportMode::Latest);
        assert_eq!(a, b);
    }
}
