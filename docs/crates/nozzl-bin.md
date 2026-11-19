# nozzl (binary) - crate brief

## Purpose

The single binary that wires every other crate together: loads config, builds the chunked archive (which owns its own per-(class, kind, shard) writer tasks internally), recovers any pending WAL, opens the checkpoint store, installs the metrics endpoint, spawns a single consumer task that drains the event channel into `archive.put_event`, spawns the acquirer, and routes config-reload updates to the appropriate consumers.

This is also where `tracing-subscriber` is configured and the process-wide `anyhow` error context is added.

## Dependencies

crates.io:
- `tokio` (full + macros + rt-multi-thread)
- `tracing`
- `tracing-subscriber` (with `env-filter`, `fmt`, `json`)
- `anyhow` (binary-only convenience)
- `clap` (CLI args — at minimum `--config` flag pointing to `nozzl.toml`)

sister crates:
- `nozzl-types`
- `nozzl-config`
- `nozzl-archive`
- `nozzl-acquirer`
- `nozzl-checkpoint`
- `nozzl-metrics`
- `nozzl-http` (added 2026-06; the HTTP surface — `/metrics`, `/health`, `POST /admin/flush/{kind}`)

## CLI

```
nozzl --config /etc/nozzl/nozzl.toml
```

Flags:
- `--config PATH` (required): path to `nozzl.toml`. Other config files (`relays.toml`, `kinds.toml`) are resolved via `[sources]` inside `nozzl.toml`.
- `--check`: validate config and exit 0/1 without starting the service. Useful in CI and pre-restart smoke checks.

## Startup sequence

1. Parse CLI args.
2. Load config via `nozzl_config::load`. Fail fast on error.
3. If `--check`, print `"config OK"` and exit 0.
4. Initialize `tracing-subscriber` from the config's `log_format` and `log_level`. After this, all logging uses the subscriber.
5. Log the startup banner (version, git sha, env, kinds, relays).
6. Build the storage backend: `nozzl_archive::build_archive(&storage_config, &runtime_config)`. The returned `ChunkedArchive` has spawned its per-(class, kind, shard) writer tasks but is not yet receiving events.
7. **Recover any pending WAL: `archive.recover_from_wal(&configured_kinds).await`.** Replays every record under `{root}/_wal/` into a fresh chunk buffer so a previous crash's events land in Parquet before new traffic arrives. Must complete before step 11.
8. Open the checkpoint store: `FileCheckpointStore::open(path, flush_interval).await`.
9. Install metrics: `nozzl_metrics::install(metrics_addr, health_source)`.
10. Create the bounded mpsc channel: `tokio::sync::mpsc::channel::<EventEnvelope>(channel_capacity)`.
11. Spawn a **single consumer task** that loops `while let Some(envelope) = rx.recv().await { archive.put_event(&envelope.event, &envelope.kind_slug).await }`. The chunked archive demuxes internally to its per-shard writer tasks, so the binary does not own a writer pool. On `Written`, increment `events_written_total{relay, kind, slug}`. On `Skipped`, increment `events_rejected_total`. On error, log loudly and increment `events_dropped_total{reason="write-failed"}` — the per-shard writer task already retries WAL/Parquet I/O on its own, so a surfaced error is treated as terminal for that event.
12. Spawn the acquirer: `Acquirer::spawn(acquirer_config_from(&config), sink, ckpt, metrics)`.
13. Spawn the config-watch loop: `nozzl_config::watch(nozzl_path)`, and on each new `Arc<Config>`:
    - Diff against last-applied config.
    - Call `acquirer.apply_config(...)` if relays/kinds changed.
    - For changes that require restart (storage backend, chunk roll thresholds), log a warning and continue with the old value.
14. Install signal handlers: SIGTERM / SIGINT triggers graceful shutdown.

## Graceful shutdown

1. Stop accepting new events from the acquirer (`acquirer.shutdown().await`).
2. Close the event channel sender and `await` the consumer task so every queued envelope reaches `archive.put_event`.
3. `archive.shutdown().await` — drains every per-shard writer, flushes its current buffer to a final `events-{ulid}.parquet`, fsyncs, and truncates the WAL. Bounded by a **120-second budget**; on timeout, log the unflushed buffer state at error level and proceed (the WAL guarantees the events will replay on next start).
4. `ckpt.flush().await` one last time.
5. Cancel the metrics endpoint.
6. Exit 0.

The whole shutdown should complete in ≤ 150 s (acquirer drain + 120 s archive flush). Beyond that, exit anyway and rely on the orchestrator to restart us; WAL recovery on the next boot will finish the job.

## Health source

The binary implements `nozzl_metrics::HealthSource`. Healthy = at least one relay in the acquirer has `connection_state == Connected` for ≥ 5 s in the last minute. (Avoids flapping during reconnect storms.)

## Acceptance criteria

1. `nozzl --check --config <good>` → exit 0.
2. `nozzl --check --config <bad>` → exit non-zero, error printed to stderr.
3. Starting against the seed config (4 relays, 6 kinds) results in 4 connected relays within 30 s on a normal network.
4. SIGTERM during steady-state results in a clean shutdown within 150 s with the archive's per-shard buffers flushed to Parquet, all WAL files truncated, and the checkpoint flushed.
5. A killed process that didn't get a chance to flush still resumes correctly on next start: `recover_from_wal` replays in-flight events into Parquet, and the checkpoint resumes from `observe` updates within the past `flush_interval` (with `safety_margin` covering the gap).
6. Config reload (touching `relays.toml` or `kinds.toml`) takes effect within 1 s.

## Testing

Unit:
- The "config-to-AcquirerConfig" mapping function — pure transform, easy to test.
- The "relay added → diff" routing.

Integration / e2e (`tests/` in the binary crate):
- Full-stack smoke: spawn `nozzl-test-relay`, write a config pointed at it, start `nozzl::run` (extract a library entry point if useful), publish events, send SIGTERM, then scan the resulting `tempfile::TempDir`-backed archive (DuckDB or `reader.rs`) and assert every published event landed in the expected `(class, k=..., p=..., dt=...)` partition.
- Hot-reload: start with 1 kind, add a 2nd; verify auto-backfill triggers and the new kind's events arrive in their own partition tree.
- Crash & restart: kill `-9` `nozzl` mid-stream, restart, verify (a) `recover_from_wal` flushes the in-flight events into Parquet and (b) the checkpoint resumes so events from before the crash are not double-archived.

## Out of scope

- Building a separate `nozzl admin` subcommand (no `backfill --kind=K` etc. — auto-backfill on config change handles it).
- Daemonization / systemd integration (the orchestrator handles process supervision).
- A `--migrate` subcommand (no layout migrations in v1).

## Notes

Keep `main` thin. The actual orchestration logic should live in a `lib.rs` `run()` function so it's testable from the e2e suite without spawning a subprocess.
