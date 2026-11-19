# nozzl-acquirer - crate brief

## Purpose

Manages the relay-pool side of the system. For each configured relay, runs a relay-task with two concurrent flows — a **forward subscription** (long-lived, `since=checkpoint`) and an **onboarding backfill** (bounded, paginated by `until`) — and feeds matching events into a bounded mpsc channel toward the writer pool.

Handles relay lifecycle (connect, reconnect, NIP-11 fetch, NOTICE/CLOSED, REQ-split-on-rejection), plus hot-reload of the relay and kind lists.

The acquisition model and operational behaviors are defined in [../design.md §5.D6](../design.md#d6--acquisition-model) and [../design.md §5.D9](../design.md#d9--configuration-and-hot-reload).

## Dependencies

crates.io:
- `nostr-relay-pool` (the rust-nostr relay-pool component)
- `nostr` (event types, filters)
- `tokio` (full)
- `async-trait`
- `tracing`
- `thiserror`

sister crates:
- `nozzl-types` (`EventEnvelope`, `RelayUrl`, `EventClass`)
- `nozzl-checkpoint` (consumes `CheckpointStore` trait)
- `nozzl-metrics` (consumes `Metrics` for observation recording)
- `nozzl-test-relay` (test-only)

## Public API contract

```rust
pub struct Acquirer { /* internal: pool, per-relay tasks, control channels */ }

#[derive(Clone, Debug)]
pub struct AcquirerConfig {
    pub relays: Vec<RelayUrl>,
    pub kinds: Vec<u16>,
    /// Every entry in `kinds` MUST have a corresponding `kind → slug` entry.
    /// The binary's mapping enforces this invariant; the acquirer attaches
    /// the slug to each emitted `EventEnvelope`.
    pub kind_slugs: HashMap<u16, String>,
    /// Optional per-relay kind restriction. A relay with an entry here is
    /// only asked for the intersection of `kinds` and its allowlist; a relay
    /// without an entry is asked for the full `kinds` set. Read at spawn (and
    /// at `apply_config` for newly-added relays); restart-required for
    /// existing relays — the binary surfaces a "requires restart" warning.
    pub relay_only_kinds: HashMap<RelayUrl, Vec<u16>>,
    pub backfill_safety_margin: Duration,
    pub backfill_default_limit: u32,
    /// Consecutive zero-event backfill pages required before declaring a
    /// kind-group's history complete. Defaults to 8.
    pub backfill_empty_pages_to_complete: u32,
}

impl Acquirer {
    /// Construct and spawn relay-tasks for the initial config.
    /// `sink` receives events; `ckpt` provides since-resumption and records observations.
    pub fn spawn(
        cfg: AcquirerConfig,
        sink: mpsc::Sender<EventEnvelope>,
        ckpt: Arc<dyn CheckpointStore>,
        metrics: Arc<Metrics>,
    ) -> Self;

    /// Apply a new config. Diffs against the active state and updates only what's needed:
    ///  - Added relays:    spin up new relay-task; trigger onboarding backfill for full kind set.
    ///  - Removed relays:  gracefully tear down (close subs, drain in-flight, stop reconnecting).
    ///  - Added kinds:     close & re-issue forward sub on every relay; trigger onboarding
    ///                     backfill scoped to the new kinds against every relay.
    ///  - Removed kinds:   close & re-issue forward sub on every relay.
    pub async fn apply_config(&self, cfg: AcquirerConfig);

    /// Stop all relay-tasks. Idempotent.
    pub async fn shutdown(self);
}
```

## Behaviors / acceptance criteria

1. **Per-relay task**. Reconnect/backoff is delegated to the underlying `nostr-relay-pool`; the acquirer task does not double-wrap that loop, but it does *configure* the pool's `RelayOptions` (`retry_interval = 2 s`, `adjust_retry_interval = false`) so reconnects don't drift toward the 60 s cap over a process lifetime. Connection-state transitions are visible via `relay_connection_state` (1 s sampling tick inside the relay task — finer than the 30 s heartbeat) plus `relay_disconnects_total` (counter) and `relay_reconnect_duration_seconds` (histogram).
2. **NIP-11 fetch** runs once per (re)connect attempt and parses `limitations.max_filters` / `max_subscriptions` for logging. v1 implementation note: NIP-11 is fetched only over plain `ws://` URLs (no TLS for this advisory GET); for `wss://` relays, proceed without NIP-11 data and rely on the reactive auto-split path. The advertised limits are not used to pre-emptively split — reactive splitting on NOTICE/CLOSED converges to the same end state.
3. **Forward subscription** filter:
   - `kinds = configured_kinds`
   - `since = max(0, min_per_kind_checkpoint - safety_margin_seconds)`
   - Sub-id stable per relay-task (e.g. `"forward"`); re-used across reconnects.
4. **Onboarding backfill** flow:
   - For relay-add: kind list = the relay's *effective* set (global `kinds` intersected with that relay's `only_kinds`, if any). Empty intersection → no forward sub opened; the relay is logged and skipped.
   - For kind-add: kind list = the **newly added** kinds, further intersected with each relay's `only_kinds` (a kind added globally is **not** backfilled on a relay whose allowlist doesn't include it).
   - Initial filter has no `since`; uses `until = now`, `limit = nip11_max_or_default`.
   - On every EOSE that delivered at least one event, issue another REQ with `until = oldest_observed_created_at - 1` and the same `limit`. (Walks history regardless of page fullness — relays often return slightly under-cap pages due to timestamp-tie pagination quirks while plenty of older history remains.)
   - When `backfill_empty_pages_to_complete` consecutive EOSEs return **zero** events, declare backfill complete; record state via `CheckpointStore::record_backfill(.., Complete)`; tear down sub. Default 8: chosen to tolerate transient empty bands (rate-limit hiccups, brief shard misses, per-kind retention edges) at the cost of a few extra round trips at end-of-history.
   - State recorded on every page (`record_backfill(.., InProgress { until_cursor })`).
   - Resume from `until_cursor` on restart if state is `InProgress`.
5. **Auto-split on filter-shape rejection**: if a NOTICE or CLOSED arrives indicating the REQ was too broad (heuristics: any of "too many", "limit", "filter", "vague", "broad", or a CLOSED with "unsupported"/"invalid"), close the offending sub and re-open it as N halved subs (covering disjoint kind subsets). Floor at 1 kind per sub; if a single-kind sub still gets rejected, log loudly, increment `events_rejected_total`, and stop subscribing for that kind on that relay until the next reconnect.
   **Concurrency-pressure responses** (NOTICE/CLOSED matching "concurrent", "subscriptions", "rate", "too many requests") are classified separately and trigger a **back-off** (initial 2 s, doubling to 30 s) instead of a split — halving in-flight subs is the inverse of what's needed when the relay is rejecting because we already have too many open. The back-off must be honoured on **every** path that opens a backfill REQ, including the EOSE-driven next-page open in `handle_eose` (not just `reissue_backfill_page`). The decay rule: an EOSE that arrives **without** any pressure NOTICE/CLOSED having been observed during the page just completed clears the back-off; pressure seen mid-page survives the EOSE that ends that page so the throttle isn't immediately wiped. Pre-fix bug — Bug 1, observed live against nos.lol on 2026-05-02 17:01 UTC: every EOSE unconditionally cleared the back-off **before** the next-page open, so a sustained NOTICE storm produced ~95 REQs/s with no actual throttling.
6. **Stuck-REQ recovery**: every backfill REQ carries a 60 s deadline. A periodic scan in the relay-task `select!` loop (5 s cadence) abandons any backfill sub whose age exceeds the deadline, re-issuing it with `until = oldest_event_received - 1` (or the original `until` if no events arrived). This protects the cursor against relays that silently drop subs without sending CLOSED — observed in practice when concurrency pressure causes a relay to garbage-collect over-quota subscriptions without notification.

   **Forward-sub dispatch-liveness probe** (replaced the prior asymmetric silence watchdog on 2026-05-23): separate from the backfill-stuck path. Per forward sub_id we track `last_event_at`. On a 1 s tick (`tick_forward_sub_probe`) the relay-task runs two phases:

   - **Probe trigger.** When **any** entry in `forward_subs` has `now - last_event_at >= forward_silence_threshold` (default 60 s; 0 disables — kill switch), no probe is already in flight, the relay is `Connected`, and the per-relay rate limit (`probe_min_interval`, default 60 s, anchored on issue time) permits, issue a single one-shot probe REQ via `probe_filter()` (`kinds=[u16::MAX], limit:0`) with sub_id `probe-<nonce>`. One probe covers all post-split forward-sub groups for the relay; the recovery re-issues all of them.
   - **Probe settle (Alive).** `EOSE`, `EVENT`, or `CLOSED` matching the probe sub_id within `probe_timeout` (default 5 s) → close+reopen all forward subs against the relay. This is the actual Bug-2 recovery, equivalent to what the deleted asymmetric watchdog did when its gate fired. `NOTICE` is **not** routed as an alive signal — deliberate divergence from NDK, since NOTICE is not sub-scoped.
   - **Probe settle (Timeout).** No response within `probe_timeout` → bump `relay_disconnects_total{relay}` inline, force `pool.disconnect_relay(...)`, then **explicitly re-arm with `pool.connect_relay(...)`**. The explicit re-arm is load-bearing: `disconnect_relay` puts the relay in `RelayStatus::Terminated`, which the lib's connect-loop treats as terminal — `RelayOptions::reconnect = true` covers only network-level failures, not explicit termination. After `connect_relay`, `post_connection → resubscribe()` replays forward subs and active backfills on the new WebSocket, so no explicit `open_all_forward_subs` is needed in the timeout path. T21 in `tests/integration.rs` pins the contract; it was the regression test for a 2026-05-23 prod incident where nos.lol stayed Terminated for ~45 minutes after a probe timeout.

   The probe tests subscription dispatch directly via REQ/EOSE, so it's decoupled from per-relay event rate — works equally for `relay.zapstore.dev` (~0 ev/s baseline) and `relay.primal.net` (~1 ev/s). The 1 s tick cadence is independent of `STUCK_BACKFILL_CHECK_INTERVAL` so detection latency is bounded by ~1 s + `probe_timeout`. Cancellation paths that clear `pending_probe` (and unsubscribe its sub_id): `Shutdown`, `UpdateKinds`, and a `!Connected → Connected` transition (treated as Timeout). Operational signals: `probe_total{relay, outcome={"alive","timeout"}}`, `probe_alive_latency_seconds{relay}` (histogram), `probe_in_flight{relay}` (gauge). See [../design.md §5.D6](../design.md#d6--acquisition-model) for the rationale and [../design.md §5.D10](../design.md#d10--observability) for the metric definitions.

   `last_event_at` is bumped from **both** the post-dedup `RelayPoolNotification::Event` channel and the wire-level `RelayMessage::Event` channel. The wire-level path is the canonical liveness signal: the pool's `MemoryDatabase` LRU suppresses the post-dedup notification when an event id has already been seen via a peer relay, but the raw wire frame still arrives — so a sub whose deliveries are dominated by LRU-dedup'd events would falsely tip into "silent" territory if only the post-dedup path bumped. Observed in production on 2026-05-09 as every relay's forward sub re-issuing within 60 s of restart despite all relays being healthy.

   **Per-event size and tag-count limits**: nozzl overrides the pool's default `RelayLimits` to disable per-event size and tag-count caps (`max_size`, `max_size_per_kind`, `max_num_tags`, `max_num_tags_per_kind`) on every relay it adds. The lib's defaults reject events with > 2,000 tags (with a per-`Kind::ContactList` override at 10,000) at the message-handler level, before the event reaches our acquirer code. Some kinds routinely carry giant tag arrays (a power user's follow list holds one tag per follow), and there the tags are the data: those defaults silently lose high-profile accounts' events. The wire-level message-size cap on `RelayLimits.messages` is preserved as a safety net against absurd JSON payloads. Pre-fix: `Impossible to handle relay message. error=Received event with too many tags…` ERROR logs (the lib's `url=` field on those errors identifies the offending relay).
7. **Event handling on receive**:
   - Drop events whose `kind` is not in the configured set (defensive).
   - Drop events whose `EventClass::classify` is not Replaceable or Addressable.
   - For accepted events: increment `events_received_total{relay, kind, slug}`; call `CheckpointStore::observe`; send to `sink`. If `sink.send` would block, apply backpressure (don't drop; the channel is bounded and writers should catch up).
8. **NOTICE messages** logged at info verbatim with structured fields `relay` and `notice`.
9. **AUTH challenges** logged at warn `"AUTH skipped"` with `relay`. The sub continues; if the relay refuses to send events without AUTH, the sub yields nothing — that's fine.
10. **Hot-reload diff logic** in `apply_config` is itself unit-testable (pure function on two `AcquirerConfig` snapshots).
11. **Graceful shutdown** closes all subs cleanly (CLOSE messages sent), drains any in-flight events into `sink` (best-effort), then drops the channel sender.

## Testing

Unit (no I/O):
- `apply_config` diff logic: feed pairs of (old, new) configs, assert the produced action set (relays added, relays removed, kinds added, kinds removed) is correct.
- REQ filter construction: kinds list, since computation, limit application — pure function tests.
- Auto-split heuristic: parse representative NOTICE/CLOSED reasons against a fixture table.

Integration (`tests/`) — these depend on `nozzl-test-relay`:
- Spawn `TestRelay` preloaded with N events of kind 0 / 30023 / etc. Spawn `Acquirer` pointed at it. Assert all events arrive on the sink.
- Onboarding backfill: preload 1500 events; verify pagination walks until done; verify checkpoint state transitions Pending → InProgress → Complete.
- Reconnect: `disconnect_all()` mid-stream; verify reconnect happens and forward sub resumes from `since=checkpoint - safety_margin`; no missed events.
- NOTICE-driven split: configure test relay to send a NOTICE rejecting any REQ with > 2 kinds; verify acquirer auto-splits and events still arrive.
- Hot-reload: change kind list; verify the forward sub is re-issued with new kinds and that an onboarding-backfill REQ for the new kinds appears.
- Hot-reload: add a new relay; verify new relay-task spawns with full backfill.

Spawning `nostr-rs-relay` as an additional subprocess test was scoped out of v1 — coverage is provided by the in-process fixture plus a live smoke run against the real seed relays. Revisit if the in-process fixture's NIP-01 subset drifts from real-relay behavior.

## Out of scope

- AUTH handling (NIP-42).
- DM filters / encrypted-content special-casing (we accept ciphertext as-is).
- Outgoing publishing.
- Per-relay custom backoff parameters (use the same defaults everywhere).

## Notes

The sub-id naming convention is operationally important. Use stable, semantic names (`"forward"`, `"backfill-{nonce}"`) so logs are interpretable. Don't use random uuids except for backfill cursors.
