# nozzl

nozzl is a long-running service that archives Nostr events. It reads a configured set of event kinds from a configured set of relays, walks back through what each relay has stored, stays subscribed for new events, and writes what it receives as Parquet files, on local disk or in S3.

It is the archiver behind [nostrolo.gy](https://nostrolo.gy): nostrology-data, the site's data pipeline, reads the archive.

The design, with the reasoning behind each decision, is in [docs/design.md](docs/design.md).

## What it does

- **Archives replaceable, addressable and regular kinds**, whichever the config lists. Ephemeral events (kind numbers 20000 to 29999), which relays do not store, are refused.
- **Backfills and follows.** When a relay or a kind is added, nozzl pages backward through what the relay holds until it runs out, while a long-lived subscription picks up new events. Progress is checkpointed per relay and kind, so both resume after a restart.
- **Copes with real relays.** It splits a multi-kind request a relay rejects, backs off when a relay signals too many subscriptions or a rate limit, and recovers a request the relay never finishes or a subscription it silently stops serving.
- **Applies relay and kind changes live.** Adding a relay or a kind to its file starts the backfill at once, without a restart.
- **Writes plain Parquet**, as a partitioned dataset that DuckDB, Athena, Spark, polars or pyarrow read in place, with no ingest step.
- **Survives crashes.** Each event goes to a write-ahead log on local disk before it is buffered, and the log is replayed at the next start.
- **Only reads from Nostr.** It never publishes or signs an event, stores encrypted content as it arrives, and does not authenticate to relays (NIP-42 (relay AUTH)), so it gets only what a relay serves to anyone.

## Quick start

You need Rust, through [rustup](https://rustup.rs), which uses the toolchain pinned in [rust-toolchain.toml](rust-toolchain.toml). From the repository root:

```sh
cargo run --release -p nozzl -- --config config/nozzl.toml --check   # validate the config
cargo run --release -p nozzl -- --config config/nozzl.toml
```

As shipped, it writes under `data/` in the current directory: the archive, its write-ahead log and its checkpoints in `data/events/`, and its logs in `data/logs/`. Metrics and health are on `127.0.0.1:9090`, and the flush route on `127.0.0.1:9091`. Ctrl-C or SIGTERM stops it cleanly: it publishes its buffered events first, and anything not published within 120 seconds stays in the write-ahead log for the next start.

The shipped lists are long, so a first backfill takes a while and a good deal of disk. Trim [config/relays.toml](config/relays.toml) and [config/kinds.toml](config/kinds.toml) for a smaller archive.

## Configuration

Three TOML files in [config/](config/), each documenting its own settings and their defaults:

| File | Holds | Takes effect |
|---|---|---|
| [nozzl.toml](config/nozzl.toml) | storage, buffering, listeners, logging, and where the other two files are | at startup |
| [relays.toml](config/relays.toml) | the relays, each optionally limited to some kinds (`only_kinds`) | on save |
| [kinds.toml](config/kinds.toml) | the kinds, each with a slug that names it in metrics and logs, and optionally limited to some relays (`only_relays`) | on save |

`--check` validates all three without starting anything. To archive to S3 instead of local disk, see the end of `nozzl.toml`. Credentials come from the standard AWS chain (environment, profile or instance role), never from a config file, and the write-ahead log and checkpoints stay on local disk either way.

## The archive

```
data/events/c={class}/k={kind}/p={shard}/dt={date}/events-{ulid}.parquet
```

- `class` is `replaceable`, `addressable` or `regular`, and `kind` is the event kind number.
- `shard` is the first hex digit of the author's pubkey, which divides every kind into 16 shards.
- `date` is the UTC date nozzl received the events, not their `created_at`.
- Each file holds many events and is never changed once written.

Each directory level is a `key=value` pair, the layout called Hive partitioning, which query engines read as columns. Every file has the same columns: the event's own `id`, `pubkey`, `kind`, `created_at`, `content`, `tags` and `sig`, plus `d_tag`, the value of its first `d` tag if it has one, and `ingested_at`, when nozzl received it, in Unix seconds. design.md has the layout in [D3](docs/design.md#d3--storage-layout-chunked-parquet-under-three-top-level-class-trees) and the schema in [D4](docs/design.md#d4--universal-parquet-schema-bloom-filters-and-dedup).

### Reading it

Two things to know before you query it:

- **The same event can be stored more than once**, for example when a relay sends it again after a restart. Deduplicate on `id`.
- **A replaceable or addressable kind can hold several events per address** (the author and the kind, plus the `d` tag for an addressable kind). For the current one, keep the newest per address.

With [DuckDB](https://duckdb.org):

```sql
-- kind-9735 (zap-receipts), each event once
SELECT *
FROM read_parquet('data/events/c=regular/k=9735/**/*.parquet')
QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingested_at) = 1;

-- kind-10002 (relay-list-metadata): each author's current relay list.
-- On a created_at tie the lowest id wins, as NIP-01 (protocol basics) says.
SELECT pubkey, created_at, tags
FROM read_parquet('data/events/c=replaceable/k=10002/**/*.parquet')
QUALIFY row_number() OVER (PARTITION BY pubkey ORDER BY created_at DESC, id) = 1;
```

For an addressable kind, partition by `pubkey, d_tag`. On S3 the same paths sit under your bucket and prefix.

## Running it

| Route | Listener | What it does |
|---|---|---|
| `GET /metrics` | `metrics_addr` | Prometheus metrics: events received and written per relay and kind, relay connections, backfill progress, chunk publishes and failures, the size of the write-ahead log and of the archive |
| `GET /health` | `metrics_addr` | `200 (ok)` while at least one relay is connected, `503 (service unavailable)` otherwise |
| `POST /admin/flush/{kind}` | `admin_addr` | publishes a kind's buffered events now, so a reader sees them without waiting for the buffers to fill or age |

The admin listener is off unless `admin_addr` is set, and it has no authentication: keep it on loopback. Logs go to stderr and, if `log_file` is set, to a daily file; `log_format = "json"` suits a log shipper. At startup nozzl logs its whole effective configuration on one line. Metrics and logs are described in [design.md, D10](docs/design.md#d10--observability).

### In a container

```sh
docker build -f deploy/Dockerfile -t nozzl .
```

The image runs nozzl as an unprivileged user and has no default command: pass `--config`. [design.md, section 10](docs/design.md#10-deployment) lists what a deployment provides: a volume for the archive, the config directory mounted read-only (as a directory, so live changes survive an editor that saves by renaming), and a stop grace period above the 120-second shutdown budget. Inside a container, a listener on `127.0.0.1` is reachable only from within it, so use `0.0.0.0` for one you scrape from outside.

## Documentation

- [docs/design.md](docs/design.md): the goals, the architecture, each decision and its reasoning, the archive layout, testing and deployment.
- [docs/req-semantics.md](docs/req-semantics.md): how a subscription request (REQ) behaves against a real relay, and how nozzl pages back through a relay's history without skipping events.
- [docs/relay-acquisition-limits.md](docs/relay-acquisition-limits.md): what the relays nozzl reads from enforce (page caps, subscription limits, throttling), read from their source code. Read it before walking a relay.
- [docs/nostr-relay-pool-limitations.md](docs/nostr-relay-pool-limitations.md): where the rust-nostr relay pool falls short for an archiver and what nozzl does about it, including why it builds from a fork.
- [docs/crates/](docs/crates/): a brief per crate, with its job, its API contract and its tests.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets --locked -- -D warnings
```

- The tests run against `nozzl-test-relay`, a relay inside the test process. Never publish a test event to a relay you do not control.
- The rust-nostr crates build from a fork that carries one additive patch: [the fork's section](docs/nostr-relay-pool-limitations.md#the-whisperhash-fork-active) of the pool limitations doc says why.
- [CLAUDE.md](CLAUDE.md) has the contributor conventions, such as formatting only the files you edit and naming each kind as `kind-NNNNN (slug)`.

| Crate | Role |
|---|---|
| `nozzl` | the binary, which wires the others together |
| `nozzl-acquirer` | relay connections, backfills and live subscriptions |
| `nozzl-archive` | the write-ahead log, the Parquet chunk writers and the storage backends |
| `nozzl-checkpoint` | progress per relay and kind |
| `nozzl-config` | loading, validating and watching the config files |
| `nozzl-http` | the HTTP routes |
| `nozzl-metrics` | the metric definitions |
| `nozzl-types` | the types the crates share |
| `nozzl-test-relay` | the in-process relay the tests use |

## License

Copyright (C) 2026 the nozzl authors.

nozzl is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

nozzl is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

The license's full text is in [LICENSE](LICENSE). Code that came from other projects keeps its own license: [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) lists it.
