//! CLI front-end for `nozzl-export`: stream one kind's archived events as
//! newline-delimited NIP-01 JSON over any archive root (local tree, a read-only
//! mount, …). Shares the exact query core with the HTTP endpoint.
//!
//! Example:
//!   nozzl-export --archive-root ./data/events --kind 10002 --mode latest -o out.jsonl

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::StreamExt;
use tokio::io::{AsyncWriteExt, BufWriter};

use nozzl_export::{ExportError, ExportMode, ExportParams, spawn_export};

#[derive(Parser)]
#[command(
    about = "Stream archived nostr events for one kind as newline-delimited NIP-01 JSON via duckdb"
)]
struct Args {
    /// Archive root, e.g. ./data/events or a read-only mount.
    #[arg(long)]
    archive_root: PathBuf,
    /// Event kind.
    #[arg(long)]
    kind: u16,
    /// latest | history. Default latest. Ignored for regular kinds.
    #[arg(long, default_value = "latest")]
    mode: String,
    /// Output file; omit or "-" for stdout.
    #[arg(long, short = 'o')]
    output: Option<PathBuf>,
    /// Path to the duckdb binary (default: "duckdb" on PATH).
    #[arg(long, default_value = "duckdb")]
    duckdb_bin: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mode = args.mode.parse::<ExportMode>().map_err(|()| {
        anyhow::anyhow!(
            "invalid --mode {:?} (expected `latest` or `history`)",
            args.mode
        )
    })?;

    let params = ExportParams {
        archive_root: args.archive_root,
        kind: args.kind,
        mode,
        duckdb_bin: args.duckdb_bin,
    };

    let mut stream = match spawn_export(params, None) {
        Ok(s) => s,
        // "Nothing to export" is a clean, non-error exit-2 for scripting.
        Err(e @ (ExportError::NotFound { .. } | ExportError::Ephemeral { .. })) => {
            eprintln!("nozzl-export: nothing to export: {e}");
            std::process::exit(2);
        }
        Err(e) => return Err(anyhow::Error::new(e)).context("starting export"),
    };

    // Stream chunks straight to the sink — never buffer the whole result (memory-flat
    // over multi-GB kinds).
    let to_stdout = args.output.as_deref().is_none_or(|p| p.as_os_str() == "-");
    if to_stdout {
        let out = BufWriter::new(tokio::io::stdout());
        pump(&mut stream, out).await
    } else {
        let path = args.output.unwrap();
        let file = tokio::fs::File::create(&path)
            .await
            .with_context(|| format!("creating {}", path.display()))?;
        pump(&mut stream, BufWriter::new(file)).await
    }
}

/// Copy the export stream into `out`. A `BrokenPipe` while writing (the normal
/// case when a downstream `| head`/`| grep` closes early) is a clean exit, not an
/// error — standard Unix-filter behavior.
async fn pump<W: tokio::io::AsyncWrite + Unpin>(
    stream: &mut nozzl_export::ExportStream,
    mut out: BufWriter<W>,
) -> Result<()> {
    while let Some(chunk) = stream.next().await {
        if let Err(e) = out.write_all(&chunk?).await {
            if e.kind() == std::io::ErrorKind::BrokenPipe {
                return Ok(());
            }
            return Err(e.into());
        }
    }
    if let Err(e) = out.flush().await {
        if e.kind() != std::io::ErrorKind::BrokenPipe {
            return Err(e.into());
        }
    }
    Ok(())
}
