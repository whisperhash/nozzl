//! Thin clap-based wrapper around [`nozzl::run`].

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "nozzl",
    version,
    about = "Long-running archival service for replaceable, addressable, and regular Nostr events"
)]
struct Cli {
    /// Path to `nozzl.toml`. Other config files are resolved through it.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,

    /// Validate the config and exit. No service is started.
    #[arg(long)]
    check: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("nozzl: failed to start runtime: {e}");
            return ExitCode::from(2);
        }
    };

    match runtime.block_on(nozzl::run(cli.config, cli.check)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Tracing isn't initialized for `--check` failures, and may not
            // be initialized at all if config load failed; print to stderr
            // so the error is always visible.
            eprintln!("nozzl: {e:#}");
            ExitCode::FAILURE
        }
    }
}
