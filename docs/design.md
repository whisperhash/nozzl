# nozzl — Design

A long-running service that acquires and durably preserves **every observable version** of replaceable and addressable Nostr events — and **every regular event** (e.g. kind-9735 zap receipts) — from a configurable set of relays.

This document is the canonical reference for implementation. It captures decisions, the reasoning behind them, the module boundaries the code is split along so specialized agents can implement parts in parallel against stable contracts, and the operational requirements (logging, testing, deployment) the system must meet.

---

## 1. Goal and non-goals

### Goal

For each Nostr event kind in our configured list, capture every distinct event (and, for replaceable/addressable kinds, every distinct version) that we observe across our configured relays, and persist each one as an immutable, content-addressed artifact in a configurable storage backend. The configured set may include replaceable, addressable, and **regular** kinds; only ephemeral kinds (which relays don't persist) are out of scope.

### Non-goals

- Acquiring **ephemeral events** (kinds 20000–29999). Relays don't persist them, so backfill is pointless and forward capture is lossy-by-design; the config validator and the acquirer both reject them.
- Decrypting encrypted-content kinds (e.g. kind-7376 (nutzap-redemptions), kind-1059 (gift wrap)). We archive envelopes verbatim.
- Republishing or signing events. The service is read-only with respect to the Nostr network.
- Recovering versions superseded before we onboarded a relay that no relay still holds. Per NIP-01, relays SHOULD delete superseded versions; this loss is unrecoverable.
- NIP-42 AUTH-required relays. Skip them in v1; revisit when a target relay requires it.
- Real-time push notifications to consumers. Consumers batch-scan on a daily-or-so cadence.
- Multi-instance deployment. Should work via conditional creates but not optimized.

---

## 2. Scope of kinds

### v1 starting set

v1 archived six replaceable and addressable kinds, among them:

| Kind  | Slug                      | Address shape  |
|------:|---------------------------|----------------|
| 0     | profile-metadata          | replaceable    |
| 10002 | relay-list-metadata       | replaceable    |
| 30063 | zapstore-app-releases     | addressable    |
| 30267 | zapstore-app-stacks       | addressable    |
| 32267 | zapstore-app-catalog      | addressable    |

These cover identity (0), infrastructure topology (10002) and the Zapstore replaceable/addressable kinds (30063, 30267, 32267). Three of the six are addressable, so the addressable layout is exercised in v1.

The kind list is **driven entirely by configuration**; adding a kind is a config change, not a code change. The project started from a list of 31 kinds; `config/kinds.toml` holds the kinds the shipped config archives.

### Class detection (NIP-01)

Pure function of `kind`:

| Range                       | Class        | Archived |
|-----------------------------|--------------|----------|
| `kind == 0` or `kind == 3`  | Replaceable  | yes      |
| `1000 ≤ kind < 10000`       | Regular      | yes      |
| `10000 ≤ kind < 20000`      | Replaceable  | yes      |
| `20000 ≤ kind < 30000`      | Ephemeral    | **no**   |
| `30000 ≤ kind < 40000`      | Addressable  | yes      |
| (everything else)           | Regular      | yes      |

Implemented in `nozzl-types::EventClass` so every crate sees the same source of truth. Replaceable, addressable, and regular are all archived; **ephemeral is the only non-archived class**. The acquirer rejects ephemeral events defensively at runtime (REQ filters already restrict, but we don't trust relays), and the config validator refuses to start with an ephemeral kind configured (`is_archivable_kind`, the complement of the ephemeral band).

### Regular events

Regular kinds (kind 1, 9735 zap receipts, …) have **no address coordinate and no supersession** — every event is independent and permanent, and relays keep all of them subject to retention. They reuse the full pipeline unchanged (schema, WAL, chunk writer, demux, checkpoint, backfill, forward sub) into an `events/regular/` tree (§3.D3); the only semantic differences are:

- **No version collapse on read.** There is no `(kind, pubkey[, d])` coordinate, so read-side dedup is **id-level only** (§6). `AddressCoord::from_event` returns `None` for regular kinds.
- **Firehose volume + walk-to-exhaustion backfill.** A regular kind is captured in full; backfill walks as far back as each relay's retention goes. There is no per-kind backfill depth bound — relay retention is the natural floor.
- **Shard skew for service-authored kinds (accepted).** Sharding is by `pubkey_hex[0..1]` as for every other kind (§3.D3). For service-authored kinds like 9735 (signed by a few LNURL-provider pubkeys) this skews the 16 shards, and `event.pubkey`-based pruning doesn't help the dominant "zaps to X" query (the recipient lives in a `p` tag). Recipient/tag queries are time-partition scans in DuckDB.
- **Adding a regular kind triggers the same bounded re-acquisition burst** as any new kind (§5.D6): the new kind's missing checkpoint nulls the shared forward `since`, so the re-issued forward REQ pulls each relay's `max_limit`-capped newest-N (≈500 on strfry) once, largely LRU-absorbed and read-side-collapsed. Acquirer logic is deliberately unchanged.

---

## 3. Environments

| Env     | Host                  | Storage backend          | Notes                                           |
|---------|-----------------------|--------------------------|-------------------------------------------------|
| local   | a workstation         | local filesystem         | Dev loop                                        |
| dev     | AWS                   | S3                       | First AWS env; seeded from an earlier local archive |
| prod    | AWS                   | S3                       | After dev pipeline is proven; IAM scoped to exact ARNs |
| stage   | AWS                   | S3                       | Possibly later, identical-to-prod sized smaller |

The storage backend abstraction (D2) hides the difference; the rest of the code path is identical across environments.

---

## 4. Architecture overview

```
                         ┌──────────────┐
                         │   config     │  ← TOML on disk, hot-reloaded
                         │   (file      │     nozzl.toml + relays.toml + kinds.toml
                         │   watcher)   │
                         └──────┬───────┘
                                │ desired state
                                ▼
   ┌──────────────────────────────────────────────────────┐
   │                    acquirer                          │
   │  ┌────────────┐  ┌────────────┐  ┌────────────┐      │
   │  │ relay task │  │ relay task │  │ relay task │ ...  │
   │  │  (damus)   │  │  (nos.lol) │  │  (primal)  │      │
   │  └─────┬──────┘  └──────┬─────┘  └─────┬──────┘      │
   │        │                │              │             │
   │        └────────────────┴──────────────┘             │
   │                         │                            │
   │                         ▼                            │
   │              bounded mpsc<EventEnvelope>             │
   └─────────────────────────┬────────────────────────────┘
                             │
                             ▼
                    ┌──────────────────────────┐
                    │   single consumer task   │
                    │   (calls put_event)      │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                  ┌──────────────────────────────┐
                  │  ChunkedArchive demux        │
                  │  (one task per kind×shard,   │
                  │   in-memory Arrow buffer +   │
                  │   per-task local-disk WAL)   │
                  └────────────┬─────────────────┘
                               │ PutMode::Create via object_store
                               ▼ (Parquet chunks on roll)
            ┌────────────────────────────────────┐
            │      ObjectStore (LocalFS | S3)    │
            └────────────────────────────────────┘

  side channels:
  ┌──────────────────────┐   ┌─────────────────────┐
  │ checkpoint state     │   │ /metrics (Prom)     │
  │ (per (relay, kind)   │   │ /health             │
  │  since timestamp)    │   │                     │
  └──────────────────────┘   └─────────────────────┘
```

Single binary, single process, Tokio runtime. One async task per relay (with reconnect loop); a small bounded `mpsc` to a single consumer task that hands events to the chunked archive's per-(kind, shard) writer-task demux.

---

## 5. Decisions

### D1 — Library: `nostr-relay-pool` (rust-nostr)

Use [`nostr-relay-pool`](https://docs.rs/nostr-relay-pool) from the rust-nostr workspace. It is the relay-pool component of `nostr-sdk` extracted; we don't need signing/publishing for a passive archival client.

**Why:** Mature, designed for many simultaneous relay connections, handles WebSocket lifecycle, reconnection, NIP-01 protocol parsing, NIP-11 fetch, signature verification by default. Comfortably handles our scale (6 kinds × 4 seed relays at v1, dozens of kinds × dozens-to-hundreds of relays eventually).

**Friction log / exit criteria:** running register of where this library fights nozzl's lossless-archiver needs (and when it might pay to replace it) in [nostr-relay-pool-limitations.md](nostr-relay-pool-limitations.md).

**FORK IN USE (2026-07-05):** we build against [`whisperhash/nostr`](https://github.com/whisperhash/nostr) (branch `nozzl-v0.44`, one additive accessor exposing connection-task liveness) via `[patch.crates-io]`, to fix the `disconnect_relay`/`connect_relay` teardown race (issue #8). Full what/why/maintenance/exit: [nostr-relay-pool-limitations.md → *The whisperhash fork*](nostr-relay-pool-limitations.md).

### D2 — Storage abstraction: `object_store` crate

Use [`object_store`](https://docs.rs/object_store) (Apache Arrow project). Unifies LocalFS, S3, GCS, Azure, and in-memory behind one trait. Battle-tested in DataFusion, delta-rs, the Apache Iceberg Rust impl.

**Why:** One code path across `local` (LocalFileSystem) and `dev`/`prod` (AmazonS3). Supports `PutMode::Create` on both LocalFS (POSIX `O_EXCL`) and S3 (If-None-Match, since the late-2024 conditional-writes feature).

### D3 — Storage layout: chunked Parquet under three top-level class trees

Layout, rooted at a configurable `archive_root`:

```
{root}/c=replaceable/k={kind}/p={shard}/dt={YYYY-MM-DD}/events-{ulid}.parquet
{root}/c=addressable/k={kind}/p={shard}/dt={YYYY-MM-DD}/events-{ulid}.parquet
{root}/c=regular/k={kind}/p={shard}/dt={YYYY-MM-DD}/events-{ulid}.parquet
{root}/_wal/c={class}-k={kind}-p={shard}.wal
{root}/_state/checkpoints.json
```

The path is fully **Hive-partitioned** — every directory level is `key=value` (`c=`/`k=`/`p=`/`dt=`) — so the parquet tree is a standard partitioned dataset consumable as-is by DuckDB (`hive_partitioning=true`), pyarrow datasets, Spark, Trino/Athena, etc.

There are three top-level class trees: `c=replaceable/`, `c=addressable/`, and `c=regular/`. The on-disk class segment is produced by `path_enc::class_segment` (Hive-prefixed, e.g. `c=addressable`); ephemeral kinds resolve to no segment and are never written. Note the deliberate split from the **metric** `class` label, which stays bare (`addressable`) via `EventClass::chunk_metric_label` — on-disk segment ≠ label value.

The per-kind **slug is not on disk.** It is functionally determined by `kind` (1:1 in config) and surfaces only in metrics/logs, sourced from the config `kind→slug` map — never parsed back out of a path. See §5.D9.

Where:

- `{kind}` — decimal integer.
- `{shard}` — single lowercase hex char equal to `pubkey_hex[0..1]`, giving 16 buckets. See **Pubkey sharding** below.
- `{ulid}` — 26-char Crockford base32 [ULID](https://github.com/ulid/spec). The 48-bit timestamp prefix makes filenames lex-sort by chunk-creation wall-clock; the random suffix avoids collisions across writers.

Example real path:

```
c=replaceable/k=0/p=8/dt=2026-05-02/events-01HXYZ08K5N2P3Q4R5S6T7V8W9.parquet
c=addressable/k=32267/p=a/dt=2026-05-02/events-01HXYZ09M2N4P5Q6R7S8T9V0WC.parquet
c=regular/k=9735/p=3/dt=2026-05-02/events-01HXYZ0AN3P5Q6R7S8T9V0WC1D.parquet
```

Each Parquet chunk holds many events from a single `(class, kind, shard)` over a single UTC date. The schema is universal across all event kinds (id / pubkey / kind / created_at / content / tags / sig / d_tag / ingested_at) — see §3.D4 below.

**Why chunked, not per-event.** A v1 backfill against four production relays produced 1.38M JSON files / ~6 GB; counting and tallying the tree took ~5 min on a workstation, and the layout would not have survived ingest of regular event kinds (kind 1 alone could push it into tens of millions of files). Chunked Parquet collapses this to hundreds of files for the same data and makes downstream analytics (DuckDB, Athena, polars) work directly over the directory tree with no separate ingest or metastore step.

#### Pubkey sharding (`p={shard}`)

A single hex-character shard equal to `pubkey_hex[0..1]`, giving 16 buckets per `(class, kind)`. This is a load-bearing layout decision — not cosmetic.

**Why a hex prefix works.** A Nostr pubkey is a 32-byte uniform random scalar (a secp256k1 public key x-coordinate). Its hex bytes are uniformly distributed by construction — we don't need to hash to get balanced shards; the leading hex char *is* a good shard key.

**Shard skew for service-authored regular kinds (accepted).** Balance assumes a diverse author population. That holds for replaceable/addressable kinds and for kind-1-style regular kinds, but **breaks for service-authored regular kinds like 9735**: a zap receipt is signed by the recipient's LNURL-provider pubkey, so a handful of providers author nearly all zaps and 2–3 of the 16 shards carry most of the volume. We keep `event.pubkey` sharding anyway (uniform reuse, no special case); the hot shards' writer mpsc backpressures `put_event` at firehose rates, and the single consumer task is the shared serialization point across all classes. Note also that pubkey sort-key / bloom pruning gives little benefit for the dominant "zaps **to** X" query, since the recipient is in a `p` tag rather than `event.pubkey` — those queries are time-partition scans.

**Why one level (16 buckets), not two (65,536).** With per-event JSON we sharded aggressively to keep any single directory under operational-tooling limits (file managers/`ls`/`tar`). Under chunked storage, file count grows sub-linearly with event volume — at 6 GB total we're producing ~hundreds of files, not millions — so over-sharding fragments chunks and starves them of events. 16 buckets gives:

- enough parallelism for writes (16 concurrent writer tasks per kind),
- enough Parquet pruning for pubkey lookups (DuckDB hive-prunes to 1/16 of a kind, then bloom-filters per-row-group within),
- chunks dense enough to compress well under zstd (3-5× vs JSON).

**Scale math.** 16 buckets × 6 v1 kinds = 96 writer tasks. At a future kind-1 firehose scale of ~600 GB/year, each bucket holds ~37 GB/year → ~100 MB/day → 1-2 chunks/day per bucket, well under any reasonable file-count concern. Re-shard via a compaction job if any bucket grows unbalanced.

**Navigating to a known coordinate.** Given a pubkey hex `H`, the chunk directory for kind `K` is `…/c={class}/k={K}/p={H[0..1]}/dt=*/`. Pure string slicing for the partition prefix; bloom filters narrow row groups within.

**S3 indifference.** S3 keys are a flat namespace; the shard segment adds no semantic cost there. Bucket dumps remain byte-identical to filesystem dumps for downstream tooling.

**Properties:**

- One Parquet chunk per `(class, kind, shard, dt, ulid)`, immutable once published. Many events per chunk (default roll: 64 MiB, 50,000 rows, or 1 h buffer age, whichever fires first).
- Chunks are written via `PutMode::Create` (POSIX `O_EXCL` on local; `If-None-Match: *` on S3). Same atomicity as the per-event scheme; collision-resistance comes from the random ULID component.
- DuckDB / Athena / polars read the tree directly with hive partitioning enabled — no ingest step. Per-row-group min/max stats and bloom filters on `id` and `pubkey` give effective pruning even with time-only directory partitioning.

**Empty d-tag** for an addressable event → `d_tag` column is the empty string. **Missing d-tag** in a 30000-range event → `d_tag` is NULL, matching the dominant client interpretation including `rust-nostr`. Both forms are projected into the dedicated `d_tag` column at write time and dictionary-encoded; the originating tag remains in the `tags` LIST<LIST<STRING>> column.

**Observation timestamp** is recorded as the `ingested_at` column (writer wall-clock at receipt), independent of `created_at` (author-claimed). Used for read-side dedup tie-breaking via `QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingested_at) = 1`.

### D4 — Universal Parquet schema, bloom filters, and dedup

**Schema** — one Arrow schema serves every kind, present and future. NIP-01 events all conform to the same wire shape; kind-specific structure lives inside `content` and `tags`.

```text
id           STRING            required   — bloom filter
pubkey       STRING            required   — bloom filter, sort key
kind         INT32             required
created_at   INT64             required   — secondary sort key (DESC within pubkey)
content      STRING            required   — may be ""
tags         LIST<LIST<STRING>>required   — may be []
sig          STRING            required
d_tag        STRING            nullable   — projected from first ["d", …] tag
ingested_at  INT64             required   — writer wall-clock at receipt
```

Within a chunk, rows are sorted by `(pubkey ASC, created_at DESC)` to maximize stats-based pruning on time-range and pubkey-range queries.

**Bloom filters** are enabled on the `id` and `pubkey` columns at the row-group level. Readers (DuckDB, Athena, polars) read the column-chunk metadata + bloom (small, KB scale) and skip entire row groups whose bloom says "no" for a given `WHERE pubkey = …` filter — effective ~99% pruning when the queried key is rare in the corpus.

**Three-layer dedup**:

1. **`nostr-relay-pool` `MemoryDatabase` LRU** (per-process, in-memory) — bounded LRU of seen event ids gates `RelayPoolNotification::Event` so each id fires that variant exactly once per pool lifetime. Default capacity 5,000,000 (~600 MB at ~120 bytes/entry); covers near-term duplicates from the same event arriving via multiple relays.
2. **Read-side dedup via `QUALIFY`** is the source of truth. Across process restarts the in-memory LRU loses state, so a re-observed event lands in a new chunk. Queries collapse duplicates with `QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingested_at) = 1` — `id` bloom filters keep this cheap.
3. **Optional periodic compaction** (out of scope for v1) — a maintenance job that reads small chunks per `(kind, shard, dt)`, dedups by `id`, and rewrites larger chunks. Revisit if duplicate amplification on disk becomes operationally costly.

**Crash durability via WAL**: the chunked archive is a buffered writer, so events not yet rolled into a chunk would otherwise be at risk. Each `(class, kind, shard)` writer task owns a local-disk WAL at `_wal/c={class}-k={kind}-p={shard}.wal`. Each event is written length-prefix + 8-byte ingested_at + canonical-JSON event, fsync'd, before being acknowledged into the in-memory Arrow buffer. After a successful chunk publish, the WAL is truncated. On startup, every existing WAL file is replayed into its writer task before the event channel opens. Crash window with default per-event fsync ≤ 1 event; a `wal_fsync_batch_ms > 0` knob trades durability for throughput. WAL files live on local disk regardless of storage backend (S3 has no append).

### D5 — Storage backend per env

Same code path; configuration switches the backend.

**Local.** `object_store::local::LocalFileSystem` rooted at a configured filesystem path. In a container: mount a host volume (e.g. host `~/nozzl/data` → container `/data`). Config sets `archive_root = "/data/events"`. Container's user must own the volume. Dedup uses POSIX `O_EXCL`.

**AWS.** `object_store::aws::AmazonS3` with bucket and prefix from config. The container/task IAM role needs `s3:PutObject`. `s3:GetObject` is rarely hit (only on LRU misses where we want to verify); `s3:DeleteObject` is **not granted** — archive is append-only. Object naming is byte-identical to local layout, so bucket dumps are interchangeable with FS dumps for downstream tooling.

**AWS credentials.** Standard AWS credential chain only — env vars, IAM role, or `~/.aws/credentials`. **No secrets in `nozzl.toml` or any other file committed to git.**

The acquirer/writer code receives an `Arc<dyn ObjectStore>` constructed at startup. Nothing downstream knows which backend it is.

### D6 — Acquisition model

For each (relay, kind-set) the relay-task runs two flows:

1. **Forward subscription** — one long-lived REQ filter listing the kind-set with `since = min(checkpoint_per_kind) - safety_margin_seconds` (default safety margin: 60 s). Re-issued when the kind list changes (config reload), when the dispatch-liveness probe finds the relay silent (section 5.D6, forward-sub silent-drop), after the relay CLOSEs it (with a back-off; see "Forward subs the relay CLOSEs" below) and when a floored kind is retried. A reconnect needs no re-issue: the library replays the subscriptions it holds. Every re-issue closes the old sub on that relay only, through the relay's own `unsubscribe`; until 2026-10-09 it went through `RelayPool::unsubscribe`, which CLOSEs the id on every relay, and since every relay task names its forward sub `forward`, one relay's re-issue tore down every relay's (#39 (Forward-sub teardown is pool-wide)).

2. **Onboarding backfill** - a separate, time-bounded REQ kicked off at relay-add OR kind-add (see §5.D9). Same kind-set, paginated by `until` working backward in time, `limit = backfill_default_limit` (500) for every relay: nozzl reads no relay's advertised `max_limit` today (#49 (NIP-11 (relay information) is never read)), so a relay that clamps lower pages short every time. Until 2026-10-12 a short page stepped the cursor past its oldest second and skipped the ties there, the loss of #32 (Backfill silently drops boundary-tie events); since then any non-empty page whose oldest moved holds the cursor inclusively, full or short ([req-semantics.md](req-semantics.md#short-pages)). `backfill_empty_pages_to_complete` consecutive EOSE-with-zero-events pages declare backfill complete (default 8); tear down. Counting empty (not "fewer than limit") pages avoids mis-completion on near-full pages from timestamp-tie pagination quirks - empirically observed against `relay.damus.io` returning 498/500 events repeatedly while years of older history remained. Runs concurrently with the forward subscription. Resumable across restarts via the `until_cursor` in the checkpoint file. **Each page's subscription is CLOSEd the moment it EOSEs - before the next page's REQ opens - so at most one backfill sub is open per relay at a time.** Per NIP-01 a finished historical REQ (its `until` is pinned in the past) has no live tail worth keeping; leaving pages open accumulates subscriptions against a relay's `max_subscriptions` cap and self-inflicts a "too many concurrent REQs" throttle - observed against nos.lol's strfry (cap 20), where ~30 leaked pages pinned backfill to the pressure-backoff ceiling (issue #12).

**Default: one big multi-kind filter per relay.** NIP-01 places no cap on kinds per filter; every configured relay handles multi-kind filters comfortably; one subscription minimizes connection overhead and config-reload churn. So the baseline filter for both flows looks like `{"kinds":[10002,30063,30267,32267], …}` — one REQ per relay, period.

#### Kind-list auto-split

Some relays reject a multi-kind REQ as too broad and ask us to narrow it. When that happens — and *only* when that happens — the relay-task halves the offending REQ's kind list and re-issues it as **two** narrower REQs. If a half is still rejected, halve again, recursing down to one kind per REQ. If a *single*-kind REQ is still rejected, that kind is suppressed on that relay (logged at error and counted as `events_rejected_total{reason="kind-rejected-at-floor"}` or `…="backfill-rejected-at-floor"`). A forward kind suppressed so is retried: after `probe_min_interval`, doubling per further floor to six hours, at once after a reconnect, and on any config change; until 2026-10-09 only a config change cleared it, so a relay whose only kind was floored stayed connected and silent (#48 (Forward-sub CLOSED handling)). A backfill page rejected at the floor is abandoned for that run, as before. Per-relay split fan-out is exposed as the `relay_req_split_count{relay}` gauge.

Worked example. Suppose a relay rejects a well-formed multi-kind backfill REQ as too broad:

```
["REQ", "backfill-…", {"kinds":[10002,30063,30267,32267], "since":0, "until":<ts>, "limit":500}]
→ ["CLOSED", "backfill-…", "filters are too vague"]
```

The relay-task halves the kind list and re-issues:

```
["REQ", "backfill-…A", {"kinds":[10002,30063], "since":0, "until":<ts>, "limit":500}]
["REQ", "backfill-…B", {"kinds":[30267,32267], "since":0, "until":<ts>, "limit":500}]
```

Both halves complete independently. The forward subscription for the same relay stays at 1 kind per REQ until its kind groups are recomputed: on a config change or a floor retry, not on a plain reconnect (the library replays the subscriptions it holds).

> **`relay.zapstore.dev` — a *specificity* floor, not a breadth one.** zapstore's `VagueFilters(3)` scores each filter (`kinds`+1, `since`+1, `until`+1, …) and rejects `< 3` regardless of kind count. Our backfill shape `{kinds, since:0, until, limit}` scores 3 → accepted, so a *well-formed* multi-kind page does **not** trip the split there. The one shape that historically did was the **fresh-onboarding first page**, which omitted `until` (score 2) and — onboarded alone — was abandoned at the single-kind floor, #25 (Single-kind backfill onboarding permanently abandons against filter-specificity relays). Fixed by seeding the first page's `until = now` (score 3); the split above remains the fallback for genuinely breadth-based rejections.

##### Forward subs the relay CLOSEs

A forward sub the relay CLOSEs for a reason that is not a filter rejection (an empty reason, an exhausted read budget, a rate limit, an upstream's CLOSED passed through) is CLOSEd back on that relay only, and re-opened after a back-off: 2 s, doubling per CLOSED to 30 s, back to 2 s after 120 s without one. Until 2026-10-09 it was re-opened at once, which turned a relay that keeps CLOSing into a REQ per round trip: up to 71 a minute from relay.damus.io's read budget, 10,883 a day from relay.conduit.market (#40 (Forward-sub CLOSED path re-issues with no backoff), #48 (Forward-sub CLOSED handling)). The re-issue replays from the checkpoint, so the dark window costs latency, not events, while the relay's replay covers it. The CLOSE back matters on a relay that keeps its own registration for a sub it CLOSEd (a relay we read does, for an upstream's CLOSED it passes through), where it would count against the per-connection cap. A filter rejection still splits the group, as above. One CLOSED is not the relay's doing: relay.conduit.market answers every client CLOSE with an empty CLOSED, which arrives after nozzl's REQ re-opening the same id but refers to the sub the CLOSE ended. An empty CLOSED within 5 s of nozzl's own CLOSE of that id is taken as that acknowledgement, counted as `action="echo"`, and the sub kept on probation: unless an EOSE or EVENT for the id arrives within 10 s, the CLOSED is treated as the relay's close after all, so a relay refusing REQs with an empty reason cannot hide behind the rule. Every forward CLOSED is counted in `forward_sub_closed_total{relay, action}`.

##### Filter-shape vs concurrency-pressure rejections

NOTICE / CLOSED text from real relays is unstructured English; we substring-match. The two semantically opposite classes — and they really are opposite — are:

- **Filter-shape rejection** ("filter", "vague", "broad", "limit", "invalid", "unsupported"): the REQ touches too many things at once. Response: **halve the kind list** as above.
- **Concurrency / rate-limit pressure** ("concurrent", "subscriptions", "rate", "too many requests", and since 2026-10-09 "bandwidth" and "budget", for relay.damus.io's `read bandwidth budget exhausted`): we already have too many subscriptions or are over the relay's rate quota. Response: **back off** (2 s → 30 s, doubling) and re-issue the *same* page; do **not** split — splitting doubles the open-sub count and amplifies the pressure. The back-off is honoured on every path that opens a backfill REQ (the EOSE-driven next-page open included). It resets on the next EOSE **only when no pressure NOTICE/CLOSED arrived during the page just completed** — pressure seen mid-page survives the EOSE that ends that page, so a sustained NOTICE storm doesn't get its throttle wiped on every cycle.

The classifier detects pressure first and short-circuits, so a string like "too many concurrent REQs" (which contains the substring `"too many"` that filter-shape would match) is correctly routed to the back-off path. See [`crates/nozzl-acquirer/src/rejection.rs`](../crates/nozzl-acquirer/src/rejection.rs) for the canonical fixture table.

##### NIP-11 awareness

On connect, nozzl tries to fetch the relay's NIP-11 (relay information) document, and in practice gets nothing, for three reasons recorded in #49 (NIP-11 (relay information) is never read): it fetches only over plain `ws://` (no TLS for this advisory GET), and every configured relay is `wss://` (dev's logs since 2026-06-23 hold no `NIP-11 doc fetched` line); its parser reads `limitations.{max_filters, max_subscriptions}`, while NIP-11 names the object `limitation` (singular in every revision since 2023-04-08) and dropped `max_filters` on 2025-04-13; and nothing reads the parsed result, which is stored and logged only. The auto-split path runs **reactively** on NOTICE/CLOSED rejections rather than pre-emptively from the advertised limits - the end state is the same, and the reactive path is the one continuously exercised in practice.

#### Stuck-REQ recovery

Real-world relays sometimes silently drop a backfill REQ — typically after a concurrency-pressure NOTICE — without sending CLOSED, leaving the page open forever. Each backfill REQ carries a 60 s timeout; if no EOSE arrives by then, the relay-task abandons the sub, advances `until` past any events already received (or retries the same cursor if the page was empty), and re-issues. This keeps the cursor moving even against misbehaving relays.

The same silent-drop failure mode applies to **forward** subs — observed live against nos.lol on 2026-05-02 17:01 UTC, where the relay garbage-collected our long-lived forward REQ during a concurrency-pressure storm while continuing to serve backfill REQs and other frames over the same socket. A **dispatch-liveness probe** handles this (modelled on NDK's [`core/src/relay/keepalive.ts`](https://github.com/nostr-dev-kit/ndk/blob/master/ndk-core/src/relay/keepalive.ts)): we track a relay-wide `last_activity_at`, bumped on any subscription-dispatch signal — an event on any of our subs (forward **or** backfill), an EOSE, or a CLOSED for one of our REQs (ambient NOTICE/OK/AUTH deliberately does **not** count). On a 1 s tick (`tick_forward_sub_probe` in [`crates/nozzl-acquirer/src/relay_task.rs`](../crates/nozzl-acquirer/src/relay_task.rs)), if at least one forward sub exists and the relay has been dispatch-**silent** for `forward_silence_threshold_seconds` (default 60) the relay-task issues a one-shot probe REQ — sub_id `probe-<nonce>`, filter `kinds=[u16::MAX], limit:0` (see `probe_filter()` in [`crates/nozzl-acquirer/src/filter.rs`](../crates/nozzl-acquirer/src/filter.rs)). Keying on relay-wide activity (not per-forward-sub `last_event_at`, which is now diagnostic-only) is what stops a long backfill from false-firing the probe: while the relay is serving backfill pages it is demonstrably dispatching to us, so even a deprioritized/silent forward sub does not trigger a reconnect — whose `resubscribe()` reflood would overflow the per-relay fan-out channel. Once the relay is genuinely silent (backfill finished **and** forward dead) the threshold elapses and the probe fires as before; Bug-2 detection is intact, just deferred until there is no other evidence of dispatch. The probe should `EOSE` almost instantly on a healthy relay; `EOSE`, `EVENT`, or `CLOSED` matching the probe sub_id within `probe_timeout` (default 5 s) settles the probe as **Alive** and we close+reopen all forward subs against the relay — the actual Bug-2 recovery, equivalent to what the deleted asymmetric watchdog did when its gate fired. No response within `probe_timeout` settles as **Timeout**: the relay-task bumps `relay_disconnects_total` inline, force-`disconnect_relay`s, then **explicitly re-arms** through `teardown_barrier_and_rearm` ([`crates/nozzl-acquirer/src/relay_task.rs`](../crates/nozzl-acquirer/src/relay_task.rs)). The explicit re-arm is load-bearing — `disconnect_relay` puts the relay into `RelayStatus::Terminated`, which the lib's connect-loop treats as terminal (the reconnect loop breaks on Terminated; `inner.rs:568-571` in 0.44.0 numbering). `RelayOptions::reconnect = true` covers only *network-level* failures that bubble up through `connect_and_run`; an explicit `disconnect_relay` is a permanent kill. And the re-arm must be **sequenced, not fire-and-forget**: a bare `connect_relay` immediately after `disconnect_relay` races the old connection task's teardown and can be silently no-op'd, wedging the relay permanently `!Connected` (issue #8 — 4 live occurrences). `teardown_barrier_and_rearm` therefore (1) awaits the old task's exit via `Relay::is_connection_task_running()` — an accessor added in **our library fork** (see [§D1](#d1--library-nostr-relay-pool-rust-nostr) / [nostr-relay-pool-limitations.md → *The whisperhash fork*](nostr-relay-pool-limitations.md)), bounded at 12 s (above the lib's 10 s `close_ws` cap) — then (2) re-drives any non-connectable leftover status through `Terminated` before calling `connect` (the lib's `connect()` silently no-ops unless status is `Initialized | Terminated | Sleeping`), counting outcomes in `teardown_barrier_total{relay, outcome}`. After the re-arm, the lib's `post_connection → resubscribe()` replays forward subs and active backfills on the new WebSocket. Regressions: nos.lol stayed Terminated for ~45 minutes on 2026-05-23 because the re-arm step was missing entirely — T21 (`probe_timeout_followed_by_relay_reconnect_and_resubscribe` in `tests/integration.rs`) pins that contract; the issue-#8 wedge is pinned by `barrier_prevents_connect_relay_race_wedge` (guard) alongside the bug-present upstream canary `repro_connect_relay_race_wedge`. `NOTICE` is intentionally **not** routed as a probe-alive signal (deliberate divergence from NDK — a NOTICE during a probe is ambient noise, not proof-of-dispatch). Probe attempts are rate-limited by `probe_min_interval` (default 60 s) anchored on issue time, so a probe-timeout→reconnect→still-silent cycle can't tight-loop. `forward_silence_threshold_seconds = 0` disables the probe entirely (kill switch preserved from the previous design). The probe tests subscription dispatch directly, so it's decoupled from per-relay event rate — a low-rate relay like `relay.zapstore.dev` (~0 ev/s for hours) and a high-rate relay like `relay.primal.net` (~1 ev/s) are handled by the same mechanism without per-relay threshold tuning. Recovery on Alive: the re-issued forward sub has `since = checkpoint - safety_margin`, so events delivered during the silent window are recovered up to the relay's retention; intermediate replaceable versions that compress within the silent window are still lost (the relay only kept the latest).

The dispatch-liveness bump (`mark_dispatch_activity`, which sets `last_activity_at`) fires from **both** the dedup'd `RelayPoolNotification::Event` channel *and* the wire-level `RelayMessage::Event` channel. Wire-level coverage is load-bearing: bumping only from the post-dedup path (as the original per-sub `last_event_at` trigger did) caused false-positive probe firings whenever a slower-arriving relay's events were already in the pool's `MemoryDatabase` LRU from a faster peer — the lib suppresses the post-dedup notification but still surfaces the raw wire-level frame. Observed in production on 2026-05-09: every relay's forward sub re-issued within 60 s of restart despite all four relays being healthy, because the dominant relay (damus) consistently won the LRU race and other relays' redeliveries never reached the bump site. (The per-sub `last_event_at` is still maintained from both channels, but is now diagnostic only — the probe reads the relay-wide `last_activity_at`.)

**Design history.** Before 2026-05-23 this watchdog was an asymmetric gate — re-issue forward subs only when the forward sub was silent AND the relay was otherwise active (any non-event frame within the same window). The gate's blind spot was the case where a relay went *fully* silent at the Nostr layer (no EOSE, no NOTICE, no CLOSED) while the WebSocket and library-level pings/pongs stayed healthy — the 5-hour stall observed on 2026-05-23 affecting three relays simultaneously. The probe replaces (not augments) the asymmetric gate; it tests dispatch directly via REQ/EOSE rather than inferring liveness from incidental traffic.

#### Server-enforced periodic disconnects

Some relays close long-lived connections on a fixed schedule independent of any activity. Observed live on 2026-05-14: `relay.damus.io` issued a transport-level close at exactly **HH:15:47–:48 UTC every hour** for the duration of the day (21 disconnects on a single connection); the cadence is precise to within one second, indicating a server-side timer. `relay.primal.net` exhibits a similar but irregular pattern (5 disconnects on the same day); `nos.lol` and `relay.zapstore.dev` did not disconnect in the observation window.

The kill happens at the Cloudflare edge in front of Damus (the relay itself runs strfry, which has no hourly-disconnect logic in its source — the `autoPingSeconds` config comment explicitly assumes "any reverse proxy idle timeouts" upstream). The pool's auto-reconnect loop then handles the reconnect — but its default settings produced a 12–34 s gap that drifted toward the 60 s `MAX_RETRY_INTERVAL` cap over a process lifetime, because `nostr-relay-pool`'s `adjust_retry_interval` path uses monotonic atomic counters that never reset. We tune those settings in `Acquirer::add_relay_internal` (`retry_interval = 2 s`, `adjust_retry_interval = false`) to bound the steady-state gap to ~3–5 s. Per-relay reconnect timing is exposed as the `relay_disconnects_total{relay}` counter and `relay_reconnect_duration_seconds{relay}` histogram (§5.D10), sampled on a 1 s tick in the relay task — finer-grained than the 30 s heartbeat so the typical few-second cycle is actually visible. The periodic `"Relay receiver exited with error"` log line surfaced by `nostr_relay_pool::relay::inner` is expected lifecycle, not a fault. The dispatch-liveness probe (above) skips its trigger predicate while the pool is reconnecting (`last_observed_state != RelayConnState::Connected`), precisely so it does not fight the pool's own reconnect during these gaps. Events that arrive during the reconnect gap are recovered either by a peer relay's forward sub or by the pool's auto-resubscribe with the original `since`; intermediate versions of replaceable events published *and* superseded entirely within the gap are still lost on that relay (a fundamental limit of pull-based archiving), partially offset by multi-relay redundancy.

**A restarting relay is a different thing, and the paragraph above will mislead you about it.**
A fixed-schedule edge timeout (damus, above) and an upstream that keeps dying are both logged by
the pool as the same `"Relay receiver exited with error"` line with the same
`Connection reset without closing handshake` text, so the message cannot tell them apart - and
only the first is "expected lifecycle, not a fault". `relay.plebeian.market` is the second case:
its relay process dies and systemd restarts it every few minutes, hundreds of times a day, at a
rate that is itself degrading
(#61 (relay.plebeian.market's relay process dies and restarts every few minutes, dropping every client)).
The discriminating test costs two sockets and a poll: hold **two or more connections of different
ages, ideally in separate processes**, keep a **control connection open to an unrelated relay**,
and poll the suspect's NIP-11 every 2 s. A timer kills each connection at its own constant age and
leaves NIP-11 answering 200 (ok) throughout; a dying upstream kills every connection in the same
instant and makes NIP-11 answer 502 (bad gateway) for the length of the restart. Either way the
reconnect gap is unmetered loss for anything that relay alone holds, so attribute the cause before
calling the churn benign.

#### Per-event size and tag-count limits

`nostr-relay-pool`'s default `RelayLimits` reject events with > 2,000 tags (with a per-`Kind::ContactList` override at 10,000) at the message-handler level. For an archiver that's lossy: some kinds routinely carry giant tag arrays (a power user's follow list holds one tag per follow), and there the tags are the data. We disable the per-event size and tag-count caps in `Acquirer::add_relay_internal` while keeping the wire-level message-size cap as a safety net against absurd payloads. Pre-fix this was logged as `Impossible to handle relay message. error=Received event with too many tags…` ERROR lines, and high-profile accounts' events were silently dropped.

### D7 — Process model

Single binary, Tokio runtime, one task per relay. Reconnect/backoff is delegated to the underlying `nostr-relay-pool` — the acquirer task does not double-wrap that loop, but we *tune* the pool's parameters via `RelayOptions` (fixed `retry_interval = 2 s`, `adjust_retry_interval = false`) so the steady-state Damus hourly-reconnect gap is bounded at ~3–5 s; see §5.D6. Events flow over a bounded `mpsc<EventEnvelope>` channel to a single consumer task that calls `Archive::put_event`; the archive demuxes to per-`(class, kind, shard)` writer tasks (one per bucket, lazily spawned on first observation). Each writer task owns its own WAL file, in-memory Arrow batch, and chunk-roll timer. Channel backpressure throttles ingest if any writer stalls.

Graceful shutdown drains the consumer, then calls `archive.shutdown()`, which signals every non-empty writer to flush its final chunk + truncate the WAL and awaits completion. Final flushes are bounded by the archive's **process-wide flush permits** (`runtime.chunk_flush_concurrency`, default 32), not run all at once: on the S3 backend a mass simultaneous fan-out once failed dozens of final PUTs together (2026-07-01; a failed flush is safe - the rows stay in the WAL). Since 2026-10-16 that bound is shared with every other roll trigger rather than living in `shutdown()`, because an aligned steady-state herd failed the same way (443 flushes in one second, 178 failed PUTs, 2026-10-15), which is #57 (Restart-synchronized buffer-age flush burst) - and the buffer-age deadline is now spread per writer by `runtime.chunk_buffer_age_jitter` so the herd does not form. Default budget is 120 s - anything not flushed by deadline survives in the WAL and is replayed on next startup. On completion `shutdown()` emits a single reconciled **`shutdown flush complete`** log line (`writers`/`flushed`/`failed`/`empty`/`aborted`/`elapsed_ms`; warn if any failed) - deliberately a log line, not a metric, since a shutdown-terminal counter rarely survives to a scrape and resets on reboot; `elapsed_ms` (the archive-flush portion of the budget) is the signal to watch for creep toward the 120 s budget.

### D8 — Checkpointing

**Granularity: per-(relay, kind).** Tracking per-relay alone wouldn't allow new kinds added later to start fresh while existing kinds keep their progress. Per-(relay, kind) gives that flexibility cleanly.

**Single JSON file** at `{archive_root}/_state/checkpoints.json` (configurable). At v1 scale (~50 bytes × 6 kinds × 30 relays ≈ 9 KB) a single file is safe. Atomicity comes from tempfile-write-then-rename. A single dedicated checkpoint task owns writes — no concurrent-writer concerns. If we ever go multi-process against the same archive root, revisit.

**Schema:**

```json
{
  "schema_version": 1,
  "updated_at": "2026-05-01T19:42:11Z",
  "relays": {
    "wss://relay.damus.io": {
      "kinds": {
        "0":     { "since": 1714579200, "last_event_id": "abc…", "backfill": { "state": "complete" } },
        "3":     { "since": 1714579180, "last_event_id": "def…", "backfill": { "state": "complete" } },
        "10002": { "since": 1714579150, "last_event_id": "ghi…", "backfill": { "state": "in_progress", "until_cursor": 1700000000 } },
        "30063": { "since": 1714579100, "last_event_id": "jkl…", "backfill": { "state": "complete" } },
        "30267": { "since": 1714578900, "last_event_id": "mno…", "backfill": { "state": "complete" } },
        "32267": { "since": 1714579000, "last_event_id": "pqr…", "backfill": { "state": "complete" } }
      }
    },
    "wss://relay.zapstore.dev": { "kinds": { /* … */ } }
  }
}
```

Per entry:
- `since` — newest `created_at` we've observed for this `(relay, kind)`. The forward subscription's REQ uses `since = min(across kinds in REQ) - safety_margin`. A kind without an observed `since` (relay never returned that kind during backfill) keeps its forward filter unrestricted, so the same multi-kind REQ that needs `since=T` for one kind doesn't accidentally clip a sparser kind.
- `last_event_id` — diagnostic only.
- `backfill` — tagged object: `{"state": "pending"}`, `{"state": "in_progress", "until_cursor": <ts>}`, or `{"state": "complete"}`. Lets a restart resume an interrupted backfill instead of re-walking history.

**Restart semantics.** On startup the relay-task consults the per-kind `backfill.state` and only opens an onboarding-backfill REQ for kinds whose state is `pending` (fresh) or `in_progress` (resume from `until_cursor`). Kinds in `complete` state are left alone — re-walking would burn round trips against the relay and risk clobbering the `complete` marker back to `in_progress`. Hot-reload-added relays/kinds enter as `pending` because no checkpoint entry exists yet, so they receive a full backfill on first observation.

Flush every `flush_interval_seconds` (default 30). Crashes lose at most that interval; the safety margin on resume covers the loss.

### D9 — Configuration and hot-reload

TOML on disk, watched via the [`notify`](https://docs.rs/notify) crate. **Three files** because relay/kind lists are edited often, the rest seldom:

```
config/
├── nozzl.toml         # main config (storage, runtime, observability, paths)
├── relays.toml        # relay list
└── kinds.toml         # kind list
```

Those names are the files' roles, used below and in the code's comments: `--config` takes any path, and `[sources]` names the other two. This repository ships a set that runs as is, `config/nozzl.toml` with `relays.toml` and `kinds.toml`: the kinds nostrology-data reads, and a few it does not read yet, archived locally. The end of `nozzl.toml` says how to pair it with nostrology-data on S3.

`nozzl.toml`:

```toml
[storage]
backend = "local"               # "local" | "s3"
root = "/data/events"
# bucket / prefix / region for s3 backend
# credentials: standard AWS chain — never in this file

[acquirer]
backfill_safety_margin_seconds = 60
backfill_default_limit = 500
backfill_empty_pages_to_complete = 8     # consecutive zero-event pages before declaring exhaustion

[checkpoint]
path = "/data/events/_state/checkpoints.json"
flush_interval_seconds = 30

[runtime]
channel_capacity = 1024
# Chunk-roll thresholds (any one fires the roll). Defaults sized for
# production backfill volume.
chunk_max_size_bytes = 67108864   # 64 MiB upper bound on the in-memory builder
chunk_max_rows = 50000
chunk_max_buffer_seconds = 3600       # roll if buffer non-empty for this long;
                                      # clock starts on first push since last
                                      # flush, so it fires under sustained
                                      # traffic too (unlike an idle trigger)
# Fraction of chunk_max_buffer_seconds subtracted per writer, derived from a
# hash of its (class, kind, shard) key, so writers whose age clocks started
# together (every writer, after a restart) do not all come due in the same
# second. 0.0 restores the old behaviour. Clamped to 0.0..=0.5.
# Restart-required.
chunk_buffer_age_jitter = 0.10
# Process-wide ceiling on chunk flushes (Parquet encode + object-store PUT) in
# flight at once, across EVERY roll trigger (not just shutdown).
# Restart-required.
chunk_flush_concurrency = 32
# nostr-relay-pool MemoryDatabase LRU capacity. ~120 bytes/entry; 5M ≈ 600 MB.
pool_dedup_max_events = 5000000
# 0 = fsync per WAL append (durable, slower); >0 = batched fsync window in ms.
wal_fsync_batch_ms = 0
# ZSTD level (1..=22) for published Parquet chunks. Restart-required. Trades
# encode CPU for on-disk size; lossless at every level. 9 ≈ best balance on
# packed/backfilled kinds (~6–16% vs the parquet-rs default of 1); no help for
# low-rate cross-version-redundant kinds (k=38422) — those need compaction (#6).
parquet_zstd_level = 9

[observability]
metrics_addr = "0.0.0.0:9090"
# Optional admin/extraction listener (POST /admin/flush/{kind} + GET /export/{kind}).
# Absent = disabled (no listener bound). Both routes are non-destructive and
# unauthenticated, so bind loopback unless you mean to expose them.
# Restart-required to change. See §5.D10.
# admin_addr = "127.0.0.1:9091"
log_format = "json"             # "json" | "pretty"
log_level = "info"
# Optional daily-rolled log file (in addition to stderr, which is always on).
# Absent = stderr only. Restart-required to change. See §5.D10.
# log_file = "/data/logs/nozzl.log"
# Rotated-file retention. OMIT for indefinite (keep every rolled file forever);
# set N >= 1 to keep the N most-recent. 0 is rejected (no clean meaning for the
# rolling appender). Only applies when log_file is set. Restart-required.
# log_file_max_files = 30

[sources]
relays_path = "/etc/nozzl/relays.toml"
kinds_path  = "/etc/nozzl/kinds.toml"
```

`relays.toml`:

```toml
[[relay]]
url = "wss://relay.damus.io"

[[relay]]
url = "wss://nos.lol"

[[relay]]
url = "wss://relay.primal.net"

# Optional per-relay kind restriction. When present, only the intersection
# of `only_kinds` and `kinds.toml` is requested from this relay. When
# absent (the default), every kind in `kinds.toml` is requested.
#
# Use case: a single-purpose relay like `relay.zapstore.dev` only hosts a
# few app-distribution kinds. Asking it for unrelated kinds drags the
# forward sub's `min(since)` back to the long-stale timestamp the relay
# happens to have for those kinds, triggering a duplicate-event storm on
# every restart.
[[relay]]
url = "wss://relay.zapstore.dev"
only_kinds = [30063, 30267, 32267]
```

`only_kinds` validation: each entry must be a kind that exists in
`kinds.toml` (otherwise validation fails at load), no duplicates, and the
list must be non-empty (use field absence to mean "all configured kinds").

`kinds.toml`:

```toml
[[kind]]
k = 0
slug = "profile-metadata"

[[kind]]
k = 3
slug = "follow-lists"

[[kind]]
k = 10002
slug = "relay-list-metadata"

[[kind]]
k = 30063
slug = "zapstore-app-releases"

[[kind]]
k = 30267
slug = "zapstore-app-stacks"

[[kind]]
k = 32267
slug = "zapstore-app-catalog"

# Optional per-kind relay restriction - the INVERSE of a relay's
# `only_kinds`. When present, this kind is requested only from these
# relays; when absent (the default), from every configured relay.
#
# Use case: a kind worth acquiring from one dedicated source only.
# `only_kinds` cannot express this, because it scopes a *relay* to a kind
# list: pinning one kind to one relay with it would mean enumerating every
# OTHER kind in every OTHER relay's allowlist, and re-editing all of them on
# every future kind onboarding - a silent-drift tax where forgetting one
# leaves the new kind acquired from nowhere.
[[kind]]
k = 30402
slug = "markets-product-listings"
only_relays = ["wss://relay.plebeian.market"]
```

`k` is the universal Nostr abbreviation for "kind" (used in REQ filters and address coordinates) and is the source of truth for layout decisions.

The snippet above is **illustrative syntax, not the shipped configuration** (`config/kinds.toml`).
`kind-30402
(markets-product-listings)` was pinned to that relay on 2026-08-20 and unpinned the
same day once it turned out to be an ecosystem-wide GammaMarkets kind rather than one
vendor's.
`crates/nozzl/tests/shipped_configs.rs` is the authority on what the shipped configs
actually scope.

`only_relays` validation: each entry must name a `[[relay]] url` in
`relays.toml` (compared after URL normalization, so `wss://host` and
`wss://host/` match; otherwise validation fails at load), no duplicates, and
the list must be non-empty (use field absence to mean "all configured
relays").

### How the two scoping directions compose

`relay.only_kinds` and `kind.only_relays` are independent allowlists that
apply as an **intersection**. A kind reaches a relay only if *both* admit it:

| relay `only_kinds` | kind `only_relays` | kind requested from that relay? |
|---|---|---|
| absent | absent | yes (the historical default) |
| admits the kind | absent | yes |
| excludes the kind | absent | no |
| absent | names the relay | yes |
| absent | omits the relay | no |
| admits the kind | names the relay | yes |
| excludes the kind | names the relay | **no** - rejected at load |

`AcquirerConfig::effective_kinds_for` is the single choke point that applies
both, so relay-task spawn, the `UpdateKinds` hot-reload path, relay-add, and
the `kind_subscribed` metric all agree by construction.

The last row is a config error, not a silent no-op: a kind that reaches **no**
relay after composing both directions is rejected at load with
`kind N is configured but reaches no relay`. Without that guard, a typo in
either allowlist yields a configured kind that is never acquired - no error,
no warning, no data.

**Both allowlists are hot-reloadable.** A reload recomputes each surviving
relay's effective kind set and services the delta: a relay that *gains* a kind
gets a scoped backfill for it (so it acquires a checkpoint, which is what keeps
its forward-sub `since` bounded), and one that *loses* a kind simply stops
requesting it. Deriving the backfill set from the per-relay effective delta
rather than from the global `kinds_added` is load-bearing: a kind can reach a
relay for the first time either by being added globally or by an allowlist
edit, and only the first is in `kinds_added`. A relay whose effective set did
not change is not touched at all.

`slug` is **required** but is **no longer on disk** — the layout is `c={class}/k={kind}/…` (slugless). Slug is functionally determined by `kind` (1:1) and surfaces only in metrics labels and operator logs, sourced from the in-memory config `kind→slug` map. Validation: must match `^[a-z0-9][a-z0-9-]*$`, max 63 bytes, unique within the file.

Slug is **restart-stable**: editing it mid-run takes effect only after a restart for the archive-side writer/scan metrics (the slug is captured when a writer is spawned and held for its lifetime; the storage scan reads the map per pass but a running writer's label is fixed). The acquirer-side event-flow metric labels DO pick up a hot-reloaded slug immediately, so a mid-run slug edit can briefly produce two series for the same `(relay, kind)` until restart. **v1 limitation (deferred):** the proper "restart-required, ignored-until-restart" treatment (a `warn_on_restart_required` entry that distinguishes a *new* kind's slug from a *changed* existing slug) is not implemented; mid-run slug edits are discouraged. Because slug is off-disk, a slug change no longer fragments on-disk history.

**Hot-reload semantics:**
- Edits to `relays.toml` apply live: added relays spin up new tasks (with onboarding backfill); removed relays gracefully tear down. A newly-added relay's `only_kinds` is honored at spawn. **Changing `only_kinds` on an already-running relay is also applied live**, as is changing a kind's `only_relays`: `AcquirerConfig::diff` recomputes every surviving relay's *effective* kind set (both allowlists composed) and emits an `UpdateKinds` only for relays whose set actually changed, carrying the kinds that relay newly *gained* as its scoped backfill. A relay whose effective set is unchanged is deliberately left alone — re-issuing there would tear down and replay a working forward sub and discard NOTICE-learned REQ-split state for no change in what is requested. Writer tasks are keyed `(class, kind, shard)` globally rather than per relay, so a relay dropping a kind needs no teardown; the writer simply stops receiving from that source.
- Edits to `kinds.toml` apply live: each relay's forward subscription is closed and re-issued with the new kind set. New kinds **automatically trigger an onboarding backfill** scoped to the new kinds against every configured relay. (No opt-out toggle; assume operators don't fat-finger.)
- Edits to `nozzl.toml` log a warning if the change requires restart (writer count, storage backend, etc.).
- A bad config load (parse error, validation failure) logs an error and keeps the old config. We never crash on reload.

### D10 — Observability

Logging and metrics are first-class — the system needs to be highly debuggable from logs alone, since incidents will inevitably happen against external relays whose behavior we don't control.

**Logging** via [`tracing`](https://docs.rs/tracing) + `tracing-subscriber`. Format and level from config: JSON in deployments, pretty at a terminal. Structured fields throughout (`relay`, `kind`, `event_id`, `pubkey`, `created_at`).

What we log:

| Level | Event                                                                  |
|-------|------------------------------------------------------------------------|
| info  | Startup banner (`nozzl starting`: version, git sha, env, kinds, relays) **and** a separate `effective config (startup snapshot)` line dumping the complete `Config` (every scalar + per-kind slugs + per-relay `only_kinds`). Not redacted — config carries no secrets. This is the authoritative record of restart-required / read-once fields, which hold these values for the whole process lifetime (only the relay/kind lists are hot-reloadable). |
| info  | Relay connection state transitions (connecting → connected → disconnected → reconnecting) |
| info  | NIP-11 doc summary per relay (name, software, advertised limits): `ws://` relays only, so none today (#49 (NIP-11 (relay information) is never read)) |
| info  | REQ open / EOSE / CLOSED with subscription id and filter summary       |
| info  | NOTICE messages from a relay (verbatim)                                |
| info  | AUTH challenges (logged as "AUTH skipped" in v1)                       |
| info  | Backfill milestones — `backfill started` / `backfill complete` markers per `(relay, kind-group)`; pair them by exact `kinds=[…]` match to derive wall-clock duration. Subsequent pages emit `REQ open: backfill next page` / `REQ open: backfill page (re-issued)` |
| info  | Backfill completeness summary - `backfill run complete (completeness summary)`, emitted **once per `(relay, kind-group)` run** at every Complete terminal (empty-page streak, epoch-floor, stuck-page floor). Carries `pages`, `full_pages`, `partial_pages`, `tie_stagnations`, `events`, and the `newest_created_at`/`oldest_created_at` span walked - the single line that answers "did this kind's walk cleanly cover its history?" without per-page log spam. Mirrored on `/metrics` by `backfill_partial_pages_total` and `backfill_tie_stagnations_total`. `events` counts wire-level EVENTs, so since 2026-10-12 it includes about one re-delivered boundary event per short page: check a walk's completeness against the archive, not this sum. |
| info  | Heartbeat every 30 s — per-relay events/sec rolling, totals (fastest signal of a silently-stuck relay) |
| info  | Config reload — diff summary (kinds added/removed, relays added/removed) |
| debug | Per-event: `relay`, `kind`, `event_id` (short), `pubkey` (short), `created_at`, outcome (`written` / `deduped` / `rejected{reason=…}`) |
| debug | Per-event signature verification result                                |
| debug | Checkpoint flush — file size, entries written, flush latency           |
| warn  | Relay disconnects with cause                                           |
| warn  | REQ rejected by relay; auto-split triggered                            |
| warn  | Concurrency-pressure NOTICE/CLOSED; back-off applied                   |
| warn  | Backfill REQ stuck without EOSE; abandoning and re-issuing             |
| warn  | Object store transient errors (will retry)                             |
| error | Object store fatal write error (after retries; event dropped)          |
| error | Config file invalid (current config retained)                          |

**Metrics** at `/metrics` on `metrics_addr`, Prometheus exposition:

- `events_received_total{relay, kind, slug}`
- `events_written_total{relay, kind, slug}` — accepted into the per-shard writer mpsc; queued for WAL fsync + Parquet roll. Not a durability ack: the WAL fsync happens asynchronously after the counter increments, and a WAL-append failure inside the writer task drops the event silently. For "did this actually land on disk" use parquet file mtimes.
- `events_deduped_total{relay, kind, slug}` — only emitted by the legacy per-event-file backend; under the chunked archive the pool's `MemoryDatabase` LRU collapses near-term duplicates before they reach the writer, and read-side `QUALIFY` collapses the rest
- `events_rejected_total{relay, kind, slug, reason}` — kind-not-configured, wrong-class (regular/ephemeral, i.e. out of archive scope), kind-rejected-at-floor / backfill-rejected-at-floor, etc. (Signature failures are not counted here — `nostr-relay-pool` verifies signatures upstream, so invalid-sig events never reach this counter.)
- `events_dropped_total{relay, kind, slug, reason}` — `put-event-failed` (writer task or channel error)
- `relay_connection_state{relay}` — gauge: 0=disconnected, 1=connected, 2=connecting. Updated on a 1 s tick from the per-relay task (plus the 30 s heartbeat sweep) so short reconnect cycles are observable.
- `relay_disconnects_total{relay}` — counter of observed `Connected → !Connected` transitions per relay. Hourly Damus drops each contribute one increment; see §5.D6.
- `relay_reconnect_duration_seconds{relay}` — histogram (buckets: 1, 2, 5, 10, 20, 30, 60, 120, 300 s) of wall-clock seconds from each observed disconnect to the next observed connect. Initial bring-up is not observed (no prior disconnect to time from). Used to dashboard P95 reconnect gap and to alert on regression away from the ~3–5 s steady state.
- `relay_req_split_count{relay}` — gauge, 1 normal, higher on auto-split
- `probe_total{relay, outcome}` — counter of dispatch-liveness probes by settlement outcome. `outcome="alive"` collapses EOSE / EVENT / CLOSED matching the probe sub_id; `outcome="timeout"` is the no-response-within-`probe_timeout` branch that also bumps `relay_disconnects_total{relay}` inline (so a timeout spike correlates ~2 s later with reconnect activity). Healthy steady state on a busy relay: 0 increments. Operational signal — see §5.D6 "forward-sub silent-drop" paragraph.
- `forward_sub_closed_total{relay, action}` - counter of forward subs the relay CLOSEd, by what nozzl did: `action="reissue"` (not a filter rejection: re-opened after the back-off above), `"split"` (a filter rejection: the kind group halved), `"floor"` (a filter rejection of a one-kind group: the kind suspended on that relay until its retry), `"echo"` (an empty CLOSED right after nozzl's own CLOSE of the id, taken as the relay acknowledging it; one never confirmed by an EOSE also counts as `reissue`). Preallocated per relay, including relays added by hot-reload. A steady `reissue` rate is a relay CLOSing us in a loop; any `floor` is a kind not being read from that relay until the retry.
- `probe_alive_latency_seconds{relay}` — histogram (buckets: `[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0]` s) of probe-issued to probe-settled-Alive wall-clock latency. Observed on the Alive path only — Timeout has a known fixed value (`probe_timeout`) and observing it would just spike one bucket. P95 expected < 0.5 s for strfry-class relays; > 1 s suggests overload or filter-shape work the relay isn't fast-pathing.
- `probe_in_flight{relay}` — gauge: `1` when a probe is outstanding for that relay, `0` otherwise. At most one probe per relay by construction; useful for dashboards that want to overlay probe activity on event-rate panels.
- `backfill_in_progress{relay, kind, slug}` — gauge 0/1
- `checkpoint_lag_seconds{relay, kind, slug}` — gauge. Only emitted for `(relay, kind)` pairs in the active subscription set (i.e. after per-relay `only_kinds` intersection); excluded pairs keep their checkpoint entry on disk but stop emitting fresh lag, so the metric reflects what we're actually still tracking.
- `kind_subscribed{relay, kind, slug}` — gauge, always 1 for every `(relay, kind)` pair currently in the active subscription set. Operator-facing tools (a status watcher, for one) intersect with the checkpoint file's `(relay, kind)` enumeration via this series, so stale rows from prior policy don't appear in the live view.
- `channel_depth` — gauge, in-flight events
- `chunks_published_total{class, kind, slug, shard}` — counter of Parquet chunks successfully PUT to the object store, incremented inside `chunk_writer::flush_and_truncate` after a successful PUT and before the WAL truncate. So an increment is a durability ack (the chunk exists in storage), independent of whether the subsequent WAL truncate succeeded. `class` is `"replaceable"`, `"addressable"`, or `"regular"`; `shard` is a single lowercase hex char `0`–`f`.
- `chunks_last_published_timestamp_seconds{class, kind, slug, shard}` — gauge holding the Unix-seconds timestamp of the most recent publish per writer, set in `flush_and_truncate` in the same block as the counter increment. Companion to the counter, needed because the natural-sounding query `time() - timestamp(chunks_published_total)` does **not** produce "seconds since last publish" — `timestamp()` returns the timestamp of the latest scrape sample, which is ~now for any active series, regardless of whether the counter incremented. The gauge stores event time explicitly so dashboards can compute `time() - chunks_last_published_timestamp_seconds{...}` for a real stalled-writer signal. Operator query: `(time() - chunks_last_published_timestamp_seconds{class=~"replaceable|addressable|regular"}) > 21600` flags any shard that hasn't rolled in 6 h, which under the buffer-age trigger (§5.D6, `chunk_max_buffer_seconds`) should be near-impossible unless the writer task is wedged.
- `chunk_flush_failures_total{class, kind, slug, trigger}` - counter of chunk flushes that did **not** publish: incremented in `chunk_writer::flush_and_truncate` on any error return (a failed Parquet encode, a failed object-store PUT, or an unarchivable class), i.e. the exact complement of `chunks_published_total`. A WAL-truncate failure *after* a successful PUT returns `Ok` and is deliberately **not** counted here - that chunk is durable, and the truncate error is on the log line. `trigger` is the roll reason, one of `size`, `rows`, `buffer_age`, `force`, `adhoc`, `shutdown`, `replay`, and it is the discriminating label: `buffer_age` spiking inside a single second is the restart-synchronized herd of #57 (Restart-synchronized buffer-age flush burst), while `adhoc` is the trigger with a same-night downstream cost (a failed shard makes `/admin/flush/{kind}` answer 502 (bad gateway), so the nightly extract correctly refuses to extract a short slice and one surface goes unrefreshed for a day). Deliberately **no `shard` label**, unlike `chunks_published_total`: the per-shard detail is on the failure's own log line, and 16 shards x 7 triggers would cost ~7,600 mostly-zero series. A failure is **not data loss** - the WAL is truncated only after a successful PUT, so the rows are republished by the next trigger or by WAL replay on the next boot; the counter measures deferred recovery and wasted encode CPU. Preallocated per (class, kind, slug) x trigger, so a healthy deployment reads a real `0`. An alert on this counter leaves out the `shutdown` trigger: a teardown-time increment rarely survives to a scrape, so the reconciled `shutdown flush complete` log line is the signal there.
- `storage_resident_bytes{class, kind, slug}` — gauge holding the bytes **currently resident** in the object store per archived kind (the compressed on-disk `.parquet` total, summed across all shards/dates). Populated by a periodic scan loop (`nozzl::spawn_storage_scan_loop`, default every 300 s, `[storage_metrics]` in §5.D9) that re-derives the totals from scratch each pass via `ChunkedArchive::scan_resident_usage` — a fresh measurement, never an accumulator — so it tracks deletions and eventual compaction automatically (a cumulative byte counter could not). Discovery uses `object_store::list_with_delimiter` on the three Hive class roots (`c=replaceable`/`c=addressable`/`c=regular`) to enumerate the `k={kind}/` prefixes that hold objects; each is summed with a recursive `list`, counting only `.parquet` objects (so an in-flight local-FS staging file never inflates a measurement). The metric's `class` label is the **bare** name (`replaceable`), tagged from `EventClass::chunk_metric_label` rather than the `c=` listing segment; the `slug` label is resolved from the config `kind→slug` map (empty if unconfigured), not from the path. Backend-agnostic: identical on `LocalFileSystem` and S3. **Deliberately not preallocated** (see below): a kind appears only once its first scan finds resident `.parquet`, so the series is absent — never a phantom `0` — until there is real data; a kind with no on-disk data, or (post-compaction) one reduced to zero chunks, simply does not emit. Scan duration is **logged** (`storage resident scan complete`, with `elapsed_ms` / `kinds` / `total_bytes` / `total_files`), not turned into a metric — that log is the early-warning signal for when scan cost grows large relative to the interval. A dashboard can chart `sum by (kind, slug) (storage_resident_bytes{...})` as the resident storage of each kind.
- `storage_resident_files{class, kind, slug}` — gauge holding the **number of resident `.parquet` chunk files** per archived kind, set from the same scan pass as `storage_resident_bytes` (the `file_count` already tallied alongside `total_bytes` in `ChunkedArchive::scan_resident_usage` — no extra directory walk). Same cadence, same labels, same fresh-re-derivation semantics, and **deliberately not preallocated** for the same reason. The total across all kinds (`sum(storage_resident_files{...})`) gives a dashboard the archive's file count, beside the byte total and its 24 h delta.
- `wal_bytes{class, kind, slug, shard}` — gauge holding the bytes **currently occupied on local disk** by each writer's `_wal` file, set from the same scan loop as the resident gauges (`nozzl::spawn_storage_scan_loop`, default every 300 s) via `ChunkedArchive::scan_wal_usage` — a fresh `read_dir` + `stat` of every `c=…-k=…-p=….wal` under the local WAL root each pass, never an accumulator. Two ways it differs from `storage_resident_bytes`: it is **local-disk** (the WAL is always on local disk even when the archive backend is S3 — see `resolve_wal_root`) and **per-writer** (`shard` is part of the key, like `chunks_published_total`, not collapsed per-kind). Because `flush_and_truncate` truncates the WAL with `set_len(0)` and **never deletes** the file, a writer's `.wal` persists for its whole life: the series reads `0` immediately after a flush / while idle and `> 0` while events are buffered, so unlike the resident gauges it stays present (a real `0`, not absent) for every live writer. A persistently large `wal_bytes` with a stale `chunks_last_published_timestamp_seconds` is the decisive **stalled-writer** signal — a writer still accepting events but no longer publishing. **Deliberately not preallocated** (same reasoning as the resident gauges — a preallocated `0` before the first scan would be a guess, not a measurement): a `(class, kind, slug, shard)` series appears once the writer's WAL file first exists on disk. One caveat to "stays present for the writer's whole life": a `.wal` file is never deleted (only `set_len(0)`-truncated), and `recover_from_wal` re-spawns a writer for *every* `.wal` it finds on restart — even kinds since removed from config (with an empty `slug`). So a decommissioned kind's WAL lingers and keeps emitting `wal_bytes …0` (and `chunks_published_total`, identically) until the stale `.wal` is manually removed; this is a benign cardinality residue, not a leak that grows on its own. Like the resident scan it is gated by `[storage_metrics] enabled` and the walk duration is **logged** (`wal size scan complete`, with `elapsed_ms` / `wal_files` / `wal_total_bytes`), not metered. The total (`sum(wal_bytes{...})`) gives a dashboard the bytes still waiting in the WAL, beside the archive's byte and file totals.
- `export_requests_total{kind, outcome}` — counter of `GET /export/{kind}` outcomes (§5.D10 Export endpoint). `outcome` ∈ {`started` (a `200` ndjson stream began), `rejected_429` (single-flight contention — another export in flight), `not_found` (no parquet on disk for the kind), `error` (duckdb spawn failure → `500`), `unsupported` (no local archive root, e.g. an S3-only deploy → `503`)}. Like `probe_total`, **not preallocated**: export is config-independent (any kind on disk is exportable), so a `(kind, outcome)` series appears on its first request rather than emitting phantom zeros for every conceivable kind. A rising `rejected_429` rate means consumers are polling faster than single-flight allows — they should back off.
- `notif_fanout_dropped_total{relay}` — counter of notifications the central listener's per-relay fan-out dropped because the relay task's bounded channel ([`NOTIF_FANOUT_CAP`] in `acquirer.rs`) was full — i.e. that relay task is hung / not draining. **One of the two and only two ways an event the pool already accepted can vanish before it reaches the archive** (the other is `notif_channel_lagged_total`). Both were `warn!`-only until this metric; together they turn the "loss ⟺ a dropped/lagged notification" invariant into a live gauge instead of a log grep. Preallocated per relay, so a healthy deployment reads a real `0`; **any non-zero value is in-pipeline loss on that relay** and the alert condition.
- `notif_channel_lagged_total` — counter, **labelless**. Total notifications skipped by the pool's single shared `tokio::sync::broadcast` channel (capacity 4096) because the central listener fell more than the capacity behind, observed as `RecvError::Lagged(n)` and **incremented by `n`** (not by 1). Labelless on purpose: the lag is a property of the one central consumer draining the pool's one broadcast channel, and `Lagged` carries no relay, so a `relay` label would invent data the call site doesn't have. `n` counts skipped `Event` *and* `Message` slots, so it is an **upper bound on lost unique events** — the exact accounting the `pool_loss_harness` regression test pins (`lost <= lagged_total`). Preallocated to `0`. Healthy steady state `0`; `> 0` is the alert condition: the archive consumer stalled and the pool lapped it.
- `backfill_partial_pages_total{relay, kind, slug}` — counter of backfill pages that came back **partial** (`received < limit`) per `(relay, kind)`. A page is attributed to every kind in its (possibly combined) group, since one page advances the cursor for all of them. A partial page is normal near a relay's retention floor (and the empty terminator pages, and the one re-read of the last second, are partial), but a **rising rate on a dense kind mid-walk** — full pages still ahead — is the dashboard-visible shadow of the per-kind backfill-completeness summary log line, i.e. the signal of a combined multi-kind REQ collecting less of a dense kind than a walk of that kind alone (one busy kind once came out about 4 % short). Preallocated per `(relay, kind, slug)` so dashboards see a real `0`.
- `backfill_tie_stagnations_total{relay, kind, slug}` - counter of backfill **tie-stagnation steps** per `(relay, kind)`: a full page whose `oldest_created_at` equalled the previous page's, so the inclusive cursor could not advance and stepped `oldest - 1` (issue #18), accepting the remainder of that one-second band as unrecoverable. Attributed to every kind in the page's group, same as `backfill_partial_pages_total`. Non-zero on a dense kind is the precise, dashboard-visible signal that the combined-REQ walk shed events at a boundary second. Preallocated to `0`. Since 2026-10-12 a short page holds the cursor inclusively too (#32 (Backfill silently drops boundary-tie events when a relay serves fewer events per REQ than backfill_default_limit); [req-semantics.md](req-semantics.md#short-pages)), so a short page can stagnate as well. It is counted when it was as large as the relay's pages get (as large as the run's largest page, on a relay that clamps or cuts below `limit`), since that second may hold more than the relay serves per page; the re-read of a walk's last second, smaller than that, is not.
- `process_start_time_seconds` — gauge, labelless. Unix timestamp (seconds) when this process started. Use `time() - process_start_time_seconds` in PromQL for uptime; a jump in the value signals a restart. A dashboard can show uptime from it.
- `build_info{version, git_sha}` — gauge, always `1`. Labels carry the `CARGO_PKG_VERSION` and the short git SHA baked in at compile time by `crates/nozzl-metrics/build.rs`. Useful for correlating dashboard state with a specific deployed commit; the `git_sha` label matches the Docker image tag, which the deploy tooling sets to the same short SHA.
- `nostr_relay_pool_*` — pass through whatever the lib exposes via `tracing`

**Series preallocation.** Prometheus `*Vec` metrics in every client library (Rust, Go, Python, …) lazy-create a time series the first time a label combination is observed. By default this means slow-to-fire counters (e.g. `chunks_published_total`, which under default `chunk_max_buffer_seconds = 3600` may not increment for up to an hour after restart) are **absent** from `/metrics` for that window — not zero. Dashboards that query `sum(...)` over an absent series fall back to the previous-restart value via Grafana's `lastNotNull` reducer, making panels look like they survived the restart.

To prevent that, the binary calls [`Metrics::preallocate_known_series`](../crates/nozzl-metrics/src/lib.rs) once at startup, after the acquirer config is loaded. This forces every label combination derivable from config (relays x kinds x slugs x {chunk classes, shards 0-f, probe outcomes alive|timeout}) to appear in `/metrics` at zero / baseline. The loss/backfill-diagnostic counters are preallocated too: `notif_channel_lagged_total` (labelless) to a single `0`, `notif_fanout_dropped_total{relay}` per relay, `backfill_partial_pages_total` / `backfill_tie_stagnations_total` per `(relay, kind, slug)`, and `chunk_flush_failures_total` per `(class, kind, slug)` x each of the seven `trigger` values - so "did the pipeline lose anything / did the walk shed boundary bands / did a chunk fail to publish?" reads a real `0`, not absent, on a healthy deployment and can be alerted on directly. The flush-failure counter's preallocation is load-bearing for consumers: an alert on it can treat no data as OK only because a healthy process emits real zeros rather than nothing. Open-ended labels (the `reason` axis of `events_rejected_total` / `events_dropped_total`) are intentionally left lazy because their value space is unbounded - only the reasons actually observed in this process lifetime appear. `storage_resident_bytes`, `storage_resident_files`, `wal_bytes`, and `checkpoint_lag_seconds` are likewise excluded, but for a semantic reason rather than cardinality: a preallocated `0` would be a *lie* (the bytes/files are already on disk, the WAL already holds bytes, or the checkpoint already has a lag - we just haven't measured yet), and the first real scan/tick would then show a misleading `0 -> N` cliff. These stay absent until genuinely measured.

The preallocation does not change metric semantics: production code still increments / sets values exactly as before. It only changes whether `/metrics` exposes a real `0` vs `absent` until the first observation. Operator implication: post-restart dashboards immediately show real-zero values for all known-at-startup label combinations, and `sum()` queries no longer carry pre-restart values forward via `lastNotNull`.

The `slug` label is the human-readable name from `kinds.toml` for that `kind` (e.g. `slug="profile-metadata"` for `kind="0"`). Slug is functionally determined by kind (1:1 in config, validated at load), so the label adds no series cardinality — it exists purely for `/metrics` legibility and dashboard queries (e.g. `events_written_total{slug=~"zapstore.*"}`). Empty `slug=""` is the sentinel used when the kind isn't in the configured set, currently only emitted on `events_rejected_total{reason="kind-not-configured"}`. Slugs are no longer on disk and are restart-stable (§5.D9); if an operator changes one mid-run, the same `(relay, kind)` may briefly carry two series — old slug and new — until process restart.

**Health endpoint** at `/health` on the same port: `200` if at least one relay is in `connected` state, `503` otherwise. ECS / container health-check friendly.

**Admin flush endpoint** at `POST /admin/flush/{kind}` on a **separate** opt-in listener (`observability.admin_addr`; absent => disabled). Publishes a single kind's buffered events as Parquet chunks *now* - for querying fresh archive data without waiting for the buffer-age roll (`chunk_max_buffer_seconds`, default 1 h). The request is **synchronous**: it fans an `AdhocFlush` message to every writer for that kind (all shards), waits for each to confirm its publish, and returns a JSON summary `{kind, writers_signaled, shards_published, rows, timed_out, errors[]}`. Status: 200 (ok) all-confirmed (incl. a configured-but-idle kind with `writers_signaled: 0`); 404 (not found) kind not currently configured for acquisition (validated against the hot-reload-tracked kind set, so a kind added via `kinds.toml` reload validates and a removed one answers 404 (not found)); 400 (bad request) non-numeric `{kind}`; 502 (bad gateway) a shard's publish failed (rows stay durable in the WAL); 504 (gateway timeout) a shard didn't confirm within the timeout (also still durable - the enqueued flush completes shortly after; the request merely stopped waiting). Downstream consumers branch on exactly these: `nostrology-data`'s `dagster_code/pipeline/flush.py` retries 504 (gateway timeout) and aborts on 502 (bad gateway), because a 502 (bad gateway) means rows are stranded in the WAL and extracting would take a short slice. It is mutating but **non-destructive** (it only publishes already-buffered events; it never deletes/alters data), kept off the scrape port on its own opt-in listener (see the Export endpoint below). Because the ad-hoc path reuses `flush_and_truncate`, an ad-hoc publish increments `chunks_published_total` and sets `chunks_last_published_timestamp_seconds` exactly like a timer/size/rows roll - the publish-rate and time-since-last-chunk panels reflect it automatically. A **failed** ad-hoc publish increments `chunk_flush_failures_total{..., trigger="adhoc"}` (added 2026-10-16; before that there was no failure metric at all, which is why the 2026-10-15 herd went unnoticed). Attribution of a *successful* publish to its trigger is still log-only, in the `chunk published` line's `trigger` field, where ad-hoc flushes log `trigger=adhoc`; only the failure path carries a `trigger` metric label. Per-request `info`-level logs (`ad-hoc flush requested` / `ad-hoc flush complete`) make the operation visible at default log levels. The listener lives in the `nozzl-http` crate alongside the metrics/health router; that crate owns nozzl's entire HTTP surface.

**Export endpoint** at `GET /export/{kind}?mode=latest|history` on the **same** admin listener. Streams a kind's archived events as newline-delimited bare NIP-01 JSON (`application/x-ndjson`) by shelling out to the `duckdb` CLI — a separate, memory-isolated process spawned with `kill_on_drop` — over the committed Parquet. This section is the canonical contract for the endpoint (downstream consumers wire against it). It is **read-only** and **does not flush**: it reads only committed chunks, leaving WAL-buffered freshness to the operator's separate flush call (decoupled by design, so an export never perturbs the writer). `latest` (default) collapses replaceable/addressable events to one row per coordinate via a read-side `QUALIFY row_number()` window (replaceable → `(kind,pubkey)`; addressable → `(kind,pubkey,d_tag)`); regular kinds ignore `mode`; id-level dedup (`PARTITION BY id ORDER BY ingested_at`) is always applied first. The duckdb statement uses `FORMAT JSON, ARRAY false` (the `ARRAY false` is what yields newline-delimited objects rather than one enclosing array) and no `hive_partitioning` (the glob pins one kind dir; the schema has no partition columns). Concurrency is bounded to **one export at a time** by a global semaphore: a second concurrent request gets `429` + `Retry-After`. The permit is held for the whole stream and released on completion **or** client disconnect — which, via `kill_on_drop`, also terminates the duckdb child (so an abandoned download frees the slot and reclaims CPU/IO). Status: `200` streaming (a present-but-empty kind streams an empty body); `400` bad `mode` / non-numeric kind; `404` no parquet on disk (unconfigured / ephemeral / WAL-only); `429` export in flight; `500` duckdb spawn failure; `503` no local archive root (S3-only deploy). Inputs are strongly typed (`u16` kind, enum `mode`) so the only externally-derived bytes reaching the SQL are the single-quote-escaped glob literal — closing the injection surface on an unauthenticated listener. The query/dedup core + a CLI bin (for running over a filesystem path, e.g. a read-only mount, local dev, or S3 later) live in the `nozzl-export` crate, wrapped by the `nozzl-http` handler. **Security invariant:** because this listener is unauthenticated and may be reachable beyond loopback, every route on it must stay non-destructive, resource-bounded, and strongly typed; any future destructive operation goes on a separate loopback-only listener. **Consumer contract:** the response carries **no ordering guarantee** — consumers must not depend on line order (id-level dedup plus full-history input means an unordered stream is fine for a loader that resolves latest-per-pubkey at query time). A **mid-stream failure truncates an already-committed `200` body** (the duckdb cause is logged by the stderr drainer), so treat an abnormally-ended stream as a soft failure and retry rather than ingesting a partial file. Large kinds stream for a while — a small host computes the latest-per-coordinate window and streams as it goes at roughly a few MB/s, so a full kind-10002 (relay-list-metadata) pull is tens of seconds to a couple of minutes; use a **generous (or no) client read timeout** (`curl --max-time 0`, or omit it), since a short timeout aborts the stream and is indistinguishable from a truncation. Reachability (the same admin listener that hosts `/admin/flush/{kind}`): loopback `http://127.0.0.1:9091` (local native binary), or port `9091` of nozzl's container from another on the same Docker network. Examples: `curl -fsS "http://127.0.0.1:9091/export/10002?mode=latest" -o relay_lists.jsonl` (replaceable, latest-per-pubkey); `curl -fsS "http://127.0.0.1:9091/export/9735" -o zap_receipts.jsonl` (regular, `mode` ignored).

### D11 — NIP-42 AUTH-required relays

Out of scope for v1. If a configured relay sends `AUTH` challenge, log a warning and continue — events that arrive before AUTH-gating are still archived; AUTH-gated subscriptions simply yield nothing.

---

## 6. d-tag handling

Confirmed via direct reading of NIP-01 and rust-nostr docs: the spec places **no constraints** on the d-tag value — no length limit, no character set, no formal handling for empty/missing. See §13 for sources.

Practical reality (cf. [nips#555](https://github.com/nostr-protocol/nips/issues/555)): authors put arbitrary UTF-8 — including slashes, spaces, control chars, emoji, very long strings — into d-tags. Our archive accepts whatever shows up.

### Storage as a column, not a path component

Under the chunked Parquet layout (§3.D3), d-tags are not part of the on-disk path — that responsibility now belongs to the partition columns (`class`, `kind`, `slug`, pubkey shard, date). The d-tag instead lives in the dedicated `d_tag STRING NULLABLE` column on every Parquet chunk, projected from the first `["d", …]` tag at write time. Dictionary encoding compresses repeated d-tags within a chunk to ~zero overhead, so this projection is effectively free; NULL rows for non-addressable kinds RLE-compress to ~1 bit per row.

The full original `tags` array (including the source `["d", …]` tag) is also retained verbatim in the `tags LIST<LIST<STRING>>` column. The `d_tag` column is a query-acceleration projection, not a substitute.

### Special cases

- Empty d-tag (`d == ""`) → `d_tag` column value is the empty string.
- Missing d-tag in a 30000-range event → `d_tag` column is NULL. Matches the dominant client interpretation including `rust-nostr`.

### Querying

To find a specific addressable coordinate via DuckDB:

```sql
SELECT * FROM read_parquet('data/events/addressable/**/*.parquet', hive_partitioning=true)
WHERE k = '30063-zapstore-app-releases'
  AND pubkey = '<hex>'
  AND d_tag = 'myapp@1.2.3';
```

Hive partition pruning narrows to one shard's directory; `pubkey` and `id` bloom filters narrow row groups within; the `d_tag` predicate filters remaining rows. Sub-second on local disk for any populated coordinate.

#### Multiple versions per coordinate

**The archive deliberately retains every distinct version of every replaceable and addressable event it observes, including superseded versions** (§1 Goal). The query above returns *all* versions of `(kind=30063, pubkey, d_tag='myapp@1.2.3')` that nozzl has ever seen — typically one row per author publish — sorted within each chunk by `(pubkey ASC, created_at DESC)`. There is no write-time supersession sweep; collapsing to "current version" is a read-side operation.

To select the **current** version of a replaceable event (latest `created_at` per `(kind, pubkey)`):

```sql
SELECT * FROM read_parquet('data/events/replaceable/**/*.parquet', hive_partitioning=true)
WHERE k = '3-follow-lists' AND pubkey = '<hex>'
QUALIFY row_number() OVER (PARTITION BY kind, pubkey ORDER BY created_at DESC) = 1;
```

For addressable kinds, partition additionally by `d_tag`:

```sql
QUALIFY row_number() OVER (PARTITION BY kind, pubkey, d_tag ORDER BY created_at DESC) = 1
```

Note this is **version-level** dedup (one row per coordinate, latest wins) and is distinct from the **id-level** dedup discussed in §5.D4 (one row per event id, collapsing the same event observed twice via different relays). Both can be composed in a single query when needed; the order is `QUALIFY` for id dedup first (cheaper, prunes via id bloom), then group/window by `(kind, pubkey [, d_tag])` over the result.

#### Regular events (no coordinate)

Regular kinds in `regular/**` have **no address coordinate**, so version-level dedup does not apply — every event is a distinct keeper. Read-side dedup is **id-level only**:

```sql
SELECT * FROM read_parquet('data/events/regular/**/*.parquet', hive_partitioning=true)
WHERE k = '9735-zap-receipts'
QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingested_at) = 1;
```

The id-`QUALIFY` collapses the same event observed via multiple relays or re-fetched across restarts. There is no "current version per `(kind, pubkey)`" window — that concept is meaningless for regular events. Queries that target a regular event's subject (e.g. zaps **to** a pubkey) filter on `tags` rather than `pubkey`/`d_tag`; the pubkey/d_tag partition pruning that accelerates addressable lookups does not apply, so these are time-partition scans.

---

## 7. Resilience and failure modes

| Failure                                       | Behavior                                                                 |
|-----------------------------------------------|--------------------------------------------------------------------------|
| Relay disconnects                             | Reconnect via `nostr-relay-pool` with fixed `retry_interval = 2 s ± 3 s jitter`, no exponential growth. Steady-state gap on the Damus hourly Cloudflare cutoff: ~3–5 s. See §5.D6. |
| Relay rejects REQ for filter shape (NOTICE / CLOSED) | Halve the kind list into multiple REQs and retry; track in metrics       |
| Relay signals concurrency / rate-limit pressure | Back off (2 s → 30 s with doubling) and re-issue the same page; reset on next clean EOSE. Splitting is *not* applied — it would amplify pressure |
| Backfill REQ stalls without EOSE              | After 60 s timeout, abandon the sub, advance `until` past any events already received (or retry same cursor if empty), and re-issue |
| Relay sends event with unknown/wrong kind     | Drop, increment `events_rejected_total{reason="kind-not-configured"}`    |
| Relay sends event with invalid signature      | Drop (rust-nostr filters by default); increment rejected counter         |
| Object store write error on chunk publish     | Writer task logs, retains buffer + WAL, retries on the next roll trigger. Persistent failures eventually backpressure the consumer via channel fill |
| Consumer or writer task error                 | Drop the event, increment `events_dropped_total{reason="put-event-failed"}`; warn loudly. WAL is unaffected so durably-captured events on disk are not lost |
| Config file invalid                           | Log error, keep old config. Never crash on reload                        |
| Checkpoint write fails                        | Log, retry on next interval. In-memory state remains authoritative       |
| Process crash                                 | On restart, resume from last checkpoint with safety margin               |

---

## 8. Testing strategy

The codebase will be the basis of a long-term project. Testing must give correctness confidence at multiple granularities so we can build on this foundation without fear of regressions.

### Unit tests (per crate)

Pure functions, no I/O. Fast. Run on every commit.

- `nozzl-archive::wal`: append + replay round-trip; truncate-and-reuse; batched fsync; both torn-write modes (mid-prefix and mid-payload); corrupt-record skip-and-continue; replay idempotency.
- `nozzl-archive::parquet_writer`: schema correctness; round-trip Arrow → Parquet → file; per-row-group sort; bloom-filter presence on `id` and `pubkey`; `d_tag` projection (NULL for non-addressable, populated for addressable).
- `nozzl-archive::path_enc`: `chunk_path` and `wal_path` exact-string assertions for all three archived classes (replaceable/addressable/regular); ephemeral resolves to `None`; ULID lex-sort by creation; `PubkeyShard` validation (lowercase hex only); date-from-unix-secs boundaries.
- `nozzl-archive::reader`: round-trip events through `ChunkBuilder::serialize` → file → `read_chunk` preserving id, pubkey, content, sig, tags, ingested_at, d_tag.
- `nozzl-archive::chunk_writer`: roll on max_rows; force-flush on demand; replay from existing WAL on writer-task startup; in-batch dedup; addressable layout under correct top-level dir.
- `nozzl-archive::demux`: route by `(class, kind, shard)`; archive regular kinds and skip ephemeral; `recover_from_wal` picks up orphan WAL files (incl. the `regular-…` filename branch); force_flush_all + shutdown.
- `nozzl-config`: TOML parsing — valid configs, every error path produces a useful error; `is_archivable_kind` accepts replaceable/addressable/regular and rejects ephemeral; default-fill semantics; hot-reload diff.
- `nozzl-checkpoint`: file format round-trip; atomic-write semantics (tempfile-then-rename); corruption recovery; serde compatibility.
- `nozzl-acquirer`: REQ filter construction, kind-list-split logic, NIP-11 limit parsing.
- `nozzl-types`: `EventClass::classify` for every NIP-01 boundary; `chunk_metric_label` (regular archived, ephemeral not); `AddressCoord::from_event` (`Some` for replaceable/addressable, `None` for regular/ephemeral).

### Integration tests (per crate)

I/O against fakes, in-memory backends, or local-FS temp dirs. Moderate speed.

- `nozzl-archive`: against `tempfile::TempDir`-backed `LocalFileSystem`. Verify many-event round trips through the full `ChunkedArchive` (1000 events of mixed kinds spanning all three archived classes including regular, force flush, read all chunks back via `reader::read_all_events`, assert event-set equality and that `replaceable/`, `addressable/`, and `regular/` top-level dirs are populated). Replaceable version-history retention. Regular-kind round-trip (`d_tag` NULL). `scan_resident_usage` reports the regular class. `d_tag` projection across mixed kinds.
- `nozzl-acquirer`: against the in-process `nozzl-test-relay` fixture, which implements a minimum NIP-01 + NIP-11 subset and supports forced NOTICE, CLOSED, disconnect, and configurable NIP-11 doc / status. Spawning `nostr-rs-relay` as an additional subprocess test was scoped out of v1 (covered by a live smoke run); revisit if the in-process fixture's NIP-01 implementation drifts from real relays' behavior.
- `nozzl-config`: write file, mutate, observe `notify` event, verify diff logic.
- `nozzl-checkpoint`: concurrent updates from multiple tasks, crash-recovery (kill mid-write, verify state).

### End-to-end tests (binary crate)

Compose all crates. Slowest, fewest tests. Run on every push.

- Spin up `nozzl-test-relay`, point a fully-wired `nozzl` binary at it, publish a sequence of events, verify archive contents match expectations including version histories for replaceable and addressable events, and that a regular kind (e.g. 9735) is captured via both forward sub and backfill into the `regular/` tree.
- Config hot-reload: change kind list at runtime, verify subscriptions update and auto-backfill triggers for new kinds.
- Crash and restart: verify checkpoint resume.

### Optional nightly smoke

A separate workflow may run a small smoke test against the actual seed relays to detect external regressions. Off the critical path; can flake without blocking PRs.

### Coverage targets (informal)

- Pure modules (`archive` path code, `config` parser, `checkpoint` serde): ≥ 95% line coverage.
- I/O modules: ≥ 80%, with the focus on protocol edge cases (NIP-11, NOTICE, CLOSED) rather than raw line count.

---

## 9. Module boundaries (workspace layout)

A Cargo workspace with crate-per-concern, chosen so multiple specialized agents can implement parts in parallel against stable trait/type contracts. Each crate has its own integration tests and `Cargo.toml` declaring only the deps it actually needs.

```
nozzl/
├── Cargo.toml                  (workspace)
├── docs/
│   ├── design.md               (this doc)
│   └── crates/                 (per-crate briefs for implementing agents)
├── crates/
│   ├── nozzl-types/            shared types: EventEnvelope, RelayUrl, AddressCoord, EventClass
│   ├── nozzl-archive/          chunked Parquet archive
│   │     ├── wal               local-disk write-ahead log (durability boundary)
│   │     ├── parquet_writer    Arrow builder + ArrowWriter (schema, sort, blooms, zstd)
│   │     ├── chunk_writer      per-(class, kind, shard) writer task; rolls on threshold
│   │     ├── demux             ChunkedArchive: routes events to writer tasks
│   │     ├── path_enc          chunk_path / wal_path / PubkeyShard
│   │     └── reader            test-only Parquet reader (production reads via DuckDB)
│   ├── nozzl-acquirer/         relay client, REQ orchestration, NIP-11, pool dedup LRU
│   ├── nozzl-checkpoint/       per-(relay, kind) since-timestamp persistence
│   ├── nozzl-config/           TOML schema, loader, file watcher
│   ├── nozzl-metrics/          Prometheus registry + typed metric handles (transport-agnostic)
│   ├── nozzl-http/             HTTP surface: /metrics, /health, POST /admin/flush/{kind}, GET /export/{kind}
│   ├── nozzl-export/           duckdb-backed per-kind event export (core + CLI bin)
│   ├── nozzl-test-relay/       in-process test relay (test-utility crate)
│   └── nozzl/                  binary: wires everything together
└── deploy/
    └── Dockerfile
```

### Contract surfaces (sketches; final signatures TBD by implementing agents)

`nozzl-types` (no I/O, no async):

```rust
pub struct EventEnvelope {
    pub relay: RelayUrl,
    pub event: nostr::Event,
    pub kind_slug: String,   // attached at acquirer emit time so a hot-reload
                             // of the slug map can't retroactively rewrite paths.
}

pub enum EventClass { Regular, Replaceable, Addressable, Ephemeral }
impl EventClass {
    pub fn classify(kind: u16) -> Self;
}

pub enum AddressCoord {
    Replaceable { kind: u16, pubkey: PublicKey },
    Addressable { kind: u16, pubkey: PublicKey, d: String },
}
impl AddressCoord {
    pub fn from_event(e: &nostr::Event) -> Option<Self>;
}
```

`nozzl-archive`:

```rust
#[async_trait]
pub trait Archive: Send + Sync {
    /// Hand off an event for durable storage. Under `ChunkedArchive`, returning
    /// `Written` means the event has been fsync'd to the writer's WAL and
    /// pushed into the in-memory Arrow buffer; it will appear in a Parquet
    /// chunk on the next roll.
    async fn put_event(&self, e: &nostr::Event, slug: &str) -> Result<PutOutcome, ArchiveError>;
}

pub enum PutOutcome {
    Written,
    AlreadyExists,    // legacy backends only; never emitted by ChunkedArchive
    Skipped { reason: SkipReason },
}

pub struct ChunkedArchive { /* … */ }
impl ChunkedArchive {
    /// Replay any orphan WAL files left from a previous crash. Call once at
    /// startup before opening the event channel.
    pub async fn recover_from_wal(&self, configured: &[(EventClass, u16, String)]) -> usize;

    /// Signal every writer to flush its final chunk + truncate the WAL and
    /// await completion. Bounded by the archive's process-wide flush permits
    /// (`chunk_flush_concurrency`), shared with every other roll trigger: a
    /// mass simultaneous S3 fan-out once failed all of them at once, in
    /// shutdown (2026-07-01) and again in steady state (2026-10-15).
    /// Emits a reconciled `shutdown flush complete` summary log on completion.
    pub async fn shutdown(&self);
}

pub fn build_archive(
    cfg: &StorageConfig,
    wal_root: PathBuf,
    chunk_cfg: ChunkWriterConfig,
) -> Result<Arc<ChunkedArchive>, ArchiveError>;
```

`nozzl-acquirer`:

```rust
pub struct AcquirerConfig {
    pub relays: Vec<RelayUrl>,
    pub kinds: Vec<u16>,
    pub kind_slugs: HashMap<u16, String>,
    pub backfill_safety_margin: Duration,
    pub backfill_default_limit: u32,
    pub backfill_empty_pages_to_complete: u32,
    /// LRU capacity for the `nostr-relay-pool` `MemoryDatabase`. Read once at
    /// `spawn`; runtime changes are ignored.
    pub pool_dedup_max_events: usize,
}

pub struct Acquirer { /* … */ }
impl Acquirer {
    pub fn spawn(cfg: AcquirerConfig,
                 sink: mpsc::Sender<EventEnvelope>,
                 ckpt: Arc<dyn CheckpointStore>,
                 metrics: Arc<Metrics>) -> Self;
    pub async fn apply_config(&self, cfg: AcquirerConfig);   // hot reload entry point
    pub async fn shutdown(self);
}
```

`nozzl-checkpoint`:

```rust
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn since(&self, relay: &RelayUrl, kind: u16) -> Option<Timestamp>;
    async fn since_snapshot(&self) -> Vec<(RelayUrl, u16, Timestamp)>;
    async fn observe(&self, relay: &RelayUrl, kind: u16, created_at: Timestamp, event_id: EventId);
    async fn backfill_state(&self, relay: &RelayUrl, kind: u16) -> BackfillState;
    async fn record_backfill(&self, relay: &RelayUrl, kind: u16, state: BackfillState);
    async fn flush(&self) -> Result<(), CheckpointError>;
}
pub enum BackfillState { Pending, InProgress { until_cursor: Timestamp }, Complete }
```

`nozzl-config`:

```rust
pub struct Config { /* nozzl.toml + resolved relays + resolved kinds */ }
pub struct ConfigWatcher { /* watch_stream: mpsc::Receiver<ConfigUpdate> */ }
pub fn load(nozzl_path: &Path) -> Result<Config, ConfigError>;
pub fn watch(nozzl_path: &Path) -> Result<ConfigWatcher, ConfigError>;
```

`nozzl-metrics` (transport-agnostic metric definitions):

```rust
pub struct Metrics { /* pre-built counters/gauges */ }
```

`nozzl-http` (the HTTP surface; depends on `nozzl-metrics` + `nozzl-archive`):

```rust
pub trait HealthSource: Send + Sync {
    async fn is_healthy(&self) -> bool;
}
pub fn install_metrics(
    addr: SocketAddr,
    health: Arc<dyn HealthSource>,
) -> Result<(Arc<Metrics>, Arc<Registry>, JoinHandle<()>), HttpError>;
// Optional admin listener; drives nozzl_archive::FlushControl.
pub fn install_admin(
    addr: SocketAddr,
    flush: Arc<dyn FlushControl>,
    configured_kinds: Arc<RwLock<HashMap<u16, String>>>,
    flush_timeout: Duration,
) -> Result<JoinHandle<()>, HttpError>;
```

These contracts let one agent build `nozzl-archive` against in-memory object_store while another builds `nozzl-acquirer` against the test relay, with no integration friction at merge time.

---

## 10. Deployment

### Container

Multi-stage build:
- **Stage 1 — builder**: `rust:slim` (latest stable). Uses `cargo-chef` to cache the dependency-compile layer so iterative rebuilds don't recompile crates.io deps.
- **Stage 2 — runtime**: `debian:bookworm-slim`. Shell + curl available for `docker exec` troubleshooting (consistent with our "ample logging" stance — operators will want to attach into the container and `curl localhost:9090/metrics`).

No `CMD` in the Dockerfile; a deployment's compose file passes `--config`.

### Compose

No compose file ships here. A deployment brings its own, and it needs:
- a volume at `/data` for the archive and checkpoint state;
- the config directory read-only at `/etc/nozzl`, bound as a directory so that hot-reload survives an atomic rename;
- a stop grace period above the 120 s shutdown budget (`SHUTDOWN_BUDGET` in `crates/nozzl/src/lib.rs`), because Docker's default of 10 s kills the drain;
- Port `9090` exposed for `/metrics` and `/health`. The optional admin/extraction listener (`observability.admin_addr`, §5.D10; hosts `POST /admin/flush/{kind}` and `GET /export/{kind}`) is best left unpublished; reach it via `docker exec … curl 127.0.0.1:<port>/…`. Both routes are non-destructive and unauthenticated, so a deployment that wants it from the host publishes it on loopback only.
- `restart: unless-stopped`.

### Architectures

Build is single-arch; `docker build` runs on the target host and produces a native image. No `buildx` / multi-arch manifest needed for v1. If we later push prebuilt images to a registry, multi-arch can be added.

### License

**AGPL-3.0-or-later** (`LICENSE`). `[workspace.package]` sets `license = "AGPL-3.0-or-later"` and `publish = false`, and every crate inherits both. Code copied from other projects keeps its own license, listed in `THIRD_PARTY_NOTICES.md`.

### Rust edition & MSRV

Edition **2024** (stabilized in Rust 1.85). MSRV pinned to a recent stable; no aggressive backwards-compat constraints.

---

## 11. Glossary

- **Replaceable event** — kind 0, 3, or in `[10000, 19999]`. Addressed by `(kind, pubkey)`. Relays SHOULD keep only the latest version.
- **Addressable event** — kind in `[30000, 39999]`. Addressed by `(kind, pubkey, d)`. Relays SHOULD keep only the latest version per coordinate.
- **d-tag** — a tag with key `"d"` whose first value identifies the addressable-event slot.
- **Address coordinate** — the tuple uniquely identifying an "address": `(kind, pubkey)` for replaceable, `(kind, pubkey, d)` for addressable.
- **Forward subscription** — long-running REQ subscription with `since=checkpoint`. Captures new events going forward.
- **Onboarding backfill** — bounded REQ at relay-add or kind-add time, paginated by `until`.
- **Checkpoint** — per-(relay, kind) last-seen `created_at` and backfill state.
- **Kind-list auto-split** — recursive halving of a multi-kind REQ in response to a *filter-shape* NOTICE/CLOSED, bottoming out at 1 kind per REQ. See §5.D6. Distinct from the back-off path used for concurrency-pressure responses.
- **Concurrency-pressure response** — a NOTICE/CLOSED whose text indicates we have too many open subscriptions or are over a rate quota (substrings: "concurrent", "subscriptions", "rate", "too many requests", "bandwidth", "budget"). Triggers a back-off, *not* an auto-split. See §5.D6.

---

## 12. References

- [NIP-01 — Basic protocol flow](https://github.com/nostr-protocol/nips/blob/master/01.md)
- [NIPs index](https://github.com/nostr-protocol/nips/blob/master/README.md)
- [nips#555 — d-tag URL-format retrieval problems](https://github.com/nostr-protocol/nips/issues/555)
- [rust-nostr — Tag](https://rust-nostr.org/sdk/event/tag.html)
- [`nostr-relay-pool` on crates.io](https://crates.io/crates/nostr-relay-pool)
- [`object_store` crate](https://docs.rs/object_store)
- [`notify` crate](https://docs.rs/notify)
- [`tracing` crate](https://docs.rs/tracing)
- `nozzl-test-relay` (workspace crate) — in-process WebSocket fixture used for acquirer integration tests
- [`cargo-chef`](https://github.com/LukeMathWalker/cargo-chef) — Docker layer caching for Rust builds
- AWS — [S3 conditional writes (If-None-Match)](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-conditional-writes/)
