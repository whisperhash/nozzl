//! Filesystem pre-check, duckdb spawn, and the streaming body that owns the child
//! process + single-flight permit for the stream's lifetime.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;

use bytes::Bytes;
use futures_util::stream::{Stream, StreamExt};
use nozzl_types::EventClass;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::io::ReaderStream;

use crate::ExportMode;
use crate::sql;

/// Boxed stream of ndjson chunks. Owns the duckdb child and the single-flight
/// permit, so both are released — and (via `kill_on_drop`) duckdb is killed — when
/// the stream completes OR is dropped (client disconnect / timeout).
pub type ExportStream = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send>>;

/// Inputs for one export. `mode` is ignored for regular kinds.
pub struct ExportParams {
    pub archive_root: PathBuf,
    pub kind: u16,
    pub mode: ExportMode,
    pub duckdb_bin: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    /// No parquet matched on disk for this kind (unconfigured, or only WAL-buffered
    /// so far). Maps to HTTP 404.
    #[error("no archived parquet on disk for kind {kind}")]
    NotFound { kind: u16 },
    /// Ephemeral kinds are never archived. Maps to HTTP 404 (with a clearer CLI msg).
    #[error("kind {kind} is ephemeral and is never archived")]
    Ephemeral { kind: u16 },
    /// duckdb could not be spawned (binary missing / not executable). Maps to 500.
    #[error("failed to spawn duckdb at {bin}: {source}")]
    Spawn {
        bin: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Build the export stream: classify the kind, reject ephemeral, confirm parquet
/// exists on disk, then spawn `duckdb -c <copy-statement>` and stream its stdout.
///
/// `permit` is the already-acquired single-flight permit (the HTTP caller passes
/// `Some`; the CLI passes `None`). It moves into the returned stream **only on the
/// success path** — any early `Err` drops the local permit, releasing single-flight
/// immediately.
pub fn spawn_export(
    params: ExportParams,
    permit: Option<OwnedSemaphorePermit>,
) -> Result<ExportStream, ExportError> {
    let ExportParams {
        archive_root,
        kind,
        mode,
        duckdb_bin,
    } = params;
    let class = EventClass::classify(kind);

    // Ephemeral classes have no archive directory at all.
    let glob = sql::kind_glob(&archive_root, class, kind).ok_or(ExportError::Ephemeral { kind })?;

    // Cheap filesystem pre-check so "no data for this kind" is a clean 404 returned
    // before any 200 stream starts. Uses the same class-dir + exact `k={kind}` rule
    // the SQL glob does (single source of truth in `sql`).
    glob_has_match(&archive_root, class, kind)?;

    let stmt = sql::copy_statement(&glob, class, mode);

    let mut cmd = Command::new(&duckdb_bin);
    cmd.arg("-c")
        .arg(&stmt)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().map_err(|source| ExportError::Spawn {
        bin: duckdb_bin,
        source,
    })?;

    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");

    // Drain stderr concurrently so a chatty duckdb can't fill the stderr pipe and
    // deadlock the stdout read. The task holds only the pipe, never the Child, so
    // `kill_on_drop` can still fire promptly on client disconnect.
    tokio::spawn(async move {
        let mut stderr = stderr;
        let mut buf = Vec::new();
        if stderr.read_to_end(&mut buf).await.is_ok() && !buf.is_empty() {
            tracing::warn!(kind, stderr = %String::from_utf8_lossy(&buf), "duckdb export stderr");
        }
    });

    // `unfold` owns (reader, child, permit). On EOF it reaps the child and logs a
    // nonzero exit (the body is already committed as 200, so a mid-stream duckdb
    // failure can only truncate). On drop, the state drops → SIGKILL + permit release.
    let reader = ReaderStream::new(stdout);
    let stream = futures_util::stream::unfold(
        (reader, child, permit),
        move |(mut reader, mut child, permit)| async move {
            if let Some(item) = reader.next().await {
                return Some((item, (reader, child, permit)));
            }
            // EOF: reap the child and surface a nonzero exit (the 200 body is already
            // committed, so a mid-stream duckdb failure can only truncate).
            match child.wait().await {
                Ok(status) if !status.success() => {
                    tracing::error!(
                        kind,
                        code = status.code(),
                        "duckdb export exited nonzero; stream may be truncated"
                    );
                }
                Err(e) => tracing::error!(kind, error = %e, "awaiting duckdb exit failed"),
                _ => {}
            }
            drop(permit);
            None
        },
    );

    Ok(Box::pin(stream))
}

/// Recursively confirm at least one `*.parquet` exists under an exact `k={kind}`
/// directory of the kind's class tree. Mirrors the SQL glob's
/// `k={kind}/**/*.parquet` rule so the pre-check and the query can't disagree.
fn glob_has_match(archive_root: &Path, class: EventClass, kind: u16) -> Result<(), ExportError> {
    let class_dir =
        nozzl_archive::class_dir_segment(class).ok_or(ExportError::Ephemeral { kind })?;
    let seg = sql::kind_dir_name(kind);
    let base = archive_root.join(class_dir);

    let Ok(entries) = std::fs::read_dir(&base) else {
        return Err(ExportError::NotFound { kind });
    };
    for entry in entries.flatten() {
        // Exact segment match: `k=3` must not match `k=30` now that the slug
        // suffix (which used to anchor the boundary) is gone.
        if entry.file_name().to_string_lossy() == seg
            && entry.path().is_dir()
            && dir_has_parquet(&entry.path())
        {
            return Ok(());
        }
    }
    Err(ExportError::NotFound { kind })
}

/// True if any `*.parquet` exists anywhere under `dir`.
fn dir_has_parquet(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if dir_has_parquet(&path) {
                return true;
            }
        } else if path.extension().is_some_and(|e| e == "parquet") {
            return true;
        }
    }
    false
}
