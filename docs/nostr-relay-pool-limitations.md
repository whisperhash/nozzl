# `nostr-relay-pool` limitations & friction log

**Purpose.** A living register of every place the rust-nostr `nostr-relay-pool`
library has fought against nozzl's needs as a *lossless archival client*. Each
entry records what the limitation is, where it bites us, what (if anything) we
did about it, and whether the workaround is sustainable. The aggregate is meant
to answer one question over time: **when does the cost of bending this library
to an archiver's needs exceed the cost of replacing it?**

This is not a bug tracker for our own code — it's specifically about the
*upstream library's* behavior and the leverage (or lack of it) we have to change
it via configuration.

- **Library:** [`nostr-relay-pool`](https://crates.io/crates/nostr-relay-pool) — the relay-pool component of `nostr-sdk`, used passively (no signing/publishing). Rationale for the choice: [design.md §D1](design.md).
- **Pinned versions (as of this writing):** `nostr-relay-pool = 0.44.1`, `nostr = 0.44.4`, `nostr-database = 0.44.0` - built from **our fork** (see next section), which tracks upstream's `v0.44` maintenance branch. Workspace pins stay `"0.44"` in [Cargo.toml](../Cargo.toml); the `[patch.crates-io]` block there swaps the source. `async-wsocket` (the pool's WebSocket layer, from crates.io) is pinned to `=0.13.2` in [crates/nozzl-acquirer/Cargo.toml](../crates/nozzl-acquirer/Cargo.toml), because entry #10 copies its connect.
- **Last updated:** 2026-10-15.

## The whisperhash fork (ACTIVE)

**As of 2026-07-05 we build against a fork:** [`whisperhash/nostr`](https://github.com/whisperhash/nostr), branch **`nozzl-v0.44`**, consumed via `[patch.crates-io]` in the workspace [Cargo.toml](../Cargo.toml) pinned to tag **`nozzl-v0.44-patch.1`**.

- **Base:** upstream `rust-nostr/nostr`'s `v0.44` **maintenance branch** (near-frozen — upstream development moved to the 0.45 restructure, which merges `nostr-relay-pool` into `nostr-sdk` and makes `RelayPool` private). Basing on the branch rather than our old lock also picked up upstream's own 0.44.x backports: pool `0.44.1` (multi-filter OR fix — a no-op for us, every sub passes one `Filter`) and nostr `0.44.3`/`0.44.4` (parsing fixes, `Drop for Keys` — benign for a passive archiver; archived bytes can't drift since `Tag` stores the raw strings).
- **The delta vs upstream is ONE additive accessor** (commit `593d045c`): `Relay::is_connection_task_running()`, exposing the lib-private `running` flag — the teardown-completion signal needed to fix entry #8's `disconnect_relay`/`connect_relay` race. Nothing else is changed; behavior is otherwise byte-identical to upstream `v0.44`.
- **All three rust-nostr crates must be patched together** (`nostr`, `nostr-database`, `nostr-relay-pool`) from the same tag — the pool's deps are workspace/path deps inside the fork repo, so patching only the pool yields two copies of `nostr` and type-mismatch errors.
- **Maintenance procedure:** if upstream backports something we want to `v0.44`: `git fetch upstream && git rebase upstream/v0.44` on the `nozzl-v0.44` branch (force-push is expected and safe — consumers pin tags/SHAs), re-tag `nozzl-v0.44-patch.N`, bump the tag in `[patch.crates-io]`, rerun the upgrade-watch on the 🟢 entries below. Tags are immutable — never force-move one.
- **Fork hygiene:** the repo must stay public (Docker/CI builds fetch it anonymously); GitHub Actions are disabled on the fork by default — leave them disabled but keep the workflow files (deleting them would be a permanent rebase-conflicting diff).
- **Reading the source:** with the patch active, the authoritative library source is the fork checkout under `~/.cargo/git/checkouts/nostr-*/`, not `~/.cargo/registry/src/`. Line-number citations elsewhere in this doc (and in issue #8) were made against **0.44.0** — 0.44.1 inserts ~3 lines around `inner.rs:1185`, so citations ≥ 1185 shift by that much (e.g. `disconnect` 1282→1285).
- **On the doc's own terms, this fires the "standing fork" migration trigger** (see *When would we migrate?*). It is accepted **consciously**: the delta is one 4-line accessor (18 lines with its doc comment), purely additive, the base branch is near-frozen, and the working assumption is that we migrate off this library rather than track it long-term — the fork is a bridge, not a commitment. **Exit strategy:** delete the `[patch.crates-io]` block (and the fork repo) whenever we upgrade past 0.44 or replace the library.
- **Side effect worth knowing:** the fork is also the ready-made vehicle for entry #1's only mechanism (a patched `nostr-relay-pool` that doesn't drop NIP-40-expired events). That *policy* decision remains open; the *mechanism* now exists.

## The recurring shape of the problem

The library is built around a **client/relay-pool** mental model: a normal app
wants deduplicated, validated, currently-valid events and is happy for the
library to silently discard anything that doesn't fit that model. An **archive**
wants the opposite — every wire-level frame, including oversized, expired, and
duplicate-across-relays events, because the "defects" are exactly the data
(historical follow lists, lapsed statuses, redundant cross-relay copies used for
liveness).

The concrete consequence, visible across multiple entries below: **we keep
having to abandon the library's high-level `RelayPoolNotification::Event` API and
read the lower-level raw `RelayMessage::Event` wire stream instead**, because the
high-level API drops data we need. Every time we reach for the wire stream is
evidence the abstraction doesn't fit our use case. Track how often that happens.

## Severity legend

- 🔴 **Blocking-class** — causes silent data loss with **no configuration knob**; only fixable by forking the library or re-architecting ingest.
- 🟠 **Footgun** — workable, but required discovering undocumented behavior and routing around the library's "nice" API; fragile across upgrades.
- 🟡 **Cost / tax** — no correctness impact, but imposes resource or tuning overhead we wouldn't choose on our own.
- 🟢 **Resolved by config** — the library exposed a knob; we set it and moved on. Listed for completeness and upgrade-watch.

## Register

| # | Issue | Severity | Config knob? | Workaround |
|---|-------|----------|--------------|------------|
| 1 | NIP-40 expired events dropped unconditionally | 🔴 | **No** | None yet — policy decision open |
| 2 | `RelayLimits` reject large / many-tag events | 🟢 | Yes | Override in `add_relay_internal` |
| 3 | `adjust_retry_interval` counters never reset → reconnect-gap drift | 🟢 | Yes | Disable + fixed `retry_interval` |
| 4 | `Event` notification LRU-dedups cross-relay & cross-sub redeliveries | 🟠 | No (must bypass API) | Read raw `Message` wire stream |
| 5 | `disconnect_relay` → `Terminated` is a permanent kill | 🟠 | No (must re-arm) | Explicit re-arm, now barrier'd (see #8) |
| 6 | No signal when a relay silently GCs a long-lived REQ | 🟠 | n/a (gap) | Custom dispatch-liveness probe |
| 7 | `MemoryDatabase` LRU memory cost for dedup | 🟡 | Capacity only | Sized at 5M (~600 MB) |
| 8 | `connect_relay` re-arm races `disconnect_relay` teardown → relay wedged `!Connected` | 🟢 (via fork) | No | Two-stage teardown barrier on the fork-exposed `is_connection_task_running()` |
| 9 | A malformed tag (non-string element) kills the whole frame before any notification | 🔴 | **No** | None - fork/upstream fix specified in #33 (rust-nostr drops the whole frame on a non-string tag element), counter proposed in #31 (Pool-level deserialize failures are unmetered event loss) |
| 10 | The WebSocket handshake carries no `User-Agent`, and some relays refuse it | 🟠 | No (pluggable transport) | Our own transport, the library's direct connect plus the header |
| 11 | `disconnect()` can lose its own termination: the task survives and reconnects | 🟠 | No (`reconnect(false)` drops auto-reconnect) | The barrier tolerates it (`timeout_task_live`); a test that needs a clean exit turns reconnect off |

---

### 1. NIP-40 expired events are dropped unconditionally — 🔴

**What.** The pool's message handler discards any event whose NIP-40
`expiration` tag is in the past, **before** the event reaches any notification
channel:

```rust
// nostr-relay-pool 0.44.0 — src/relay/inner.rs:1198
// Check if the event is expired
if event.is_expired() {
    return Err(Error::EventExpired);
}
```

`is_expired()` is simply "has an `expiration` tag with a past timestamp"
(`nostr` 0.44.2, `src/event/mod.rs:230`). Returning `Err` here means the handler
logs `Impossible to handle relay message. error=event expired` and emits
**neither** `RelayPoolNotification::Event` **nor** `::Message`
(`inner.rs:1068-1080`) — the event is fully invisible to our acquirer.

**Where it bites.** Relays legitimately replay events whose `expiration` has
lapsed (NIP-40 deletion is a MAY/SHOULD, not a MUST). Observed live on
2026-06-04 as a flood of these ERROR lines for an addressable kind whose events
carry short (~7-minute) expiration windows. For an
archive these are in-scope events - dropping them loses
history it would otherwise capture.

**Leverage.** **None via configuration.** Confirmed there is no `RelayLimits` /
`RelayOptions` flag for this, and the admit-policy hook runs *after* the
expiration check, so it can't recover the event either. This is the same
silent-drop *shape* as #2 (tag caps) but **without** #2's escape hatch — that's
what makes it blocking-class rather than resolved.

**Status: OPEN.** Two sub-decisions pending:
1. *Policy* — do we even want expired events archived (all kinds? some kinds? not ephemeral status churn?).
2. *Mechanism* — if yes, the only routes are (a) a `[patch.crates-io]` git fork of `nostr-relay-pool` with the check guarded/removed, or (b) upstreaming a config flag and waiting for a release. There is no in-tree fix.

**This is the strongest "move away" signal to date:** it's the first confirmed
silent-data-loss case the library gives us *no* configuration path out of.

### 2. `RelayLimits` reject large / many-tag events — 🟢

**What.** The library's default `RelayLimits` reject events `> 2,000` tags (with
a per-`Kind::ContactList` override at 10,000) and oversized payloads at the
message-handler level, before our code sees them.

**Where it bites.** Some kinds routinely carry giant tag arrays, and there the
tags are the data: a power-user follow list with 5,000+ `["p", …]` tags is a
real event, and the default silently drops it. Surfaced in the 2026-05-09
production incident as `Received event with too many tags: tags=4902,
max_tags=2000` ERROR lines, with high-profile accounts' events missing.

**Resolution.** Config knob exists. We clear the caps in
[`Acquirer::add_relay_internal`](../crates/nozzl-acquirer/src/acquirer.rs#L269)
(`limits.events.max_size = None`, `max_num_tags = None`, plus the per-kind maps),
keeping only the wire-level `messages.max_size` cap as a safety net. Resolved —
but note it's the *config-fixable twin* of #1, and it took a production incident
to discover the default was lossy. **Upgrade-watch:** re-verify these fields and
defaults on every `nostr-relay-pool` bump.

### 3. `adjust_retry_interval` counters never reset → reconnect-gap drift — 🟢

**What.** With defaults (`retry_interval = 10 s`, `adjust_retry_interval =
true`), the reconnect backoff multiplies the sleep by `1 + (attempts − success)
/ 2` using **monotonic atomic counters that never reset** over the process
lifetime. Any transient failure permanently lengthens *every* subsequent
reconnect, drifting toward the 60 s cap.

**Where it bites.** Damus's hourly Cloudflare-edge WebSocket cutoff produced a
12–34 s reconnect gap that crept toward 60 s over days of uptime — and during
each gap, intermediate versions of replaceable events are lost. A long-lived
instance of an earlier tool on the same library sat pinned at the 60 s cap after a
few days of DNS hiccups.

**Resolution.** Config knobs exist. We set `retry_interval = 2 s` and
`adjust_retry_interval = false` in
[`add_relay_internal`](../crates/nozzl-acquirer/src/acquirer.rs#L298), bounding
the steady-state gap to ~3–5 s.
Resolved — but the *defaults* are actively harmful for a long-running archiver,
which is a recurring theme.

### 4. `Event` notification LRU-dedups cross-relay & cross-sub redeliveries — 🟠

**What.** `RelayPoolNotification::Event` fires only the **first** time an event
id is observed per pool lifetime — it's gated by an in-memory `MemoryDatabase`
LRU. Subsequent observations of the same id (from a peer relay, or from a
different sub on the same socket) are suppressed on that channel. The raw
`RelayMessage::Event` wire channel still fires for every observation.

**Where it bites (twice).**
- **Backfill page-state.** Forward and backfill subs share one WebSocket and race; the forward sub usually wins, so the backfill sub's observations get dedup'd out of the `Event` stream. Tracking page progress (`received`, `oldest_created_at`) from `Event` made backfill "complete" after N empty pages **without ever walking history**. Fix: track page-state from the raw `Message` stream. Regression-pinned by `backfill_pagination_advances_when_forward_sub_dedupes_events_first` ([integration.rs](../crates/nozzl-acquirer/tests/integration.rs#L703)).
- **Liveness probe.** The dispatch-liveness watchdog bumps `last_event_at`. Bumping only from the dedup'd `Event` path caused false-positive "silent sub" firings whenever a slower relay's events were already LRU-cached from a faster peer (damus consistently won the race on 2026-05-09; every relay's forward sub needlessly re-issued within 60 s of startup). Fix: bump from the wire-level `Message` path too. Pinned by `watchdog_does_not_fire_when_forward_events_are_lru_deduped_from_peers` ([integration.rs](../crates/nozzl-acquirer/tests/integration.rs#L1394)).

**Leverage.** No knob makes `Event` lossless — by design it dedups. The fix in
both cases was to **stop using the high-level API and read the raw wire stream**.
This is the clearest structural mismatch: the library's headline ingest API is
unusable for a multi-relay archiver, and we increasingly treat the pool as a
glorified multiplexed WebSocket. If the list of "things we can only do via the
raw `Message` stream" keeps growing, the library is buying us less and less.

### 5. `disconnect_relay` → `Terminated` is a permanent kill — 🟠

**What.** `RelayOptions::reconnect = true` only auto-reconnects *network-level*
failures that bubble through `connect_and_run`. An **explicit**
`disconnect_relay` puts the relay into `RelayStatus::Terminated`, which the
library's reconnect loop treats as terminal (`inner.rs:568-571`) — it will never
come back on its own (unless the library loses the disconnect's termination,
entry #11, when the old task reconnects by itself).

**Where it bites.** Our probe-timeout recovery must `disconnect_relay` a wedged
relay, then reconnect. Relying on `reconnect = true` to bring it back left
nos.lol `Terminated` for ~45 minutes on 2026-05-23. Fix: **explicitly** re-arm
via `connect_relay` after the disconnect. Pinned by T21
(`probe_timeout_followed_by_relay_reconnect_and_resubscribe`). Detail in
[design.md §D6](design.md).

**Leverage.** No knob — the `reconnect` flag's scope is just narrower than its
name suggests. Workable once you know it, but it's an undocumented sharp edge.
**Caveat:** the bare `connect_relay` re-arm this originally prescribed is
itself racy — see #8, which is the failure mode of this workaround. As of
2026-07-05 the re-arm goes through `teardown_barrier_and_rearm`
([relay_task.rs](../crates/nozzl-acquirer/src/relay_task.rs)), which
sequences it safely on the fork-exposed teardown signal.

### 6. No signal when a relay silently GCs a long-lived REQ — 🟠

**What.** A relay can stop delivering events for a long-lived forward
subscription — garbage-collecting the REQ during a load spike, or going fully
silent at the Nostr layer (no EOSE/NOTICE/CLOSED) — while the WebSocket and the
library's own ping/pong stay healthy. The library surfaces **nothing**: no
error, no status change, no notification.

**Where it bites.** Observed against nos.lol on 2026-05-02 (REQ GC'd during a
concurrency-pressure storm) and as a 5-hour three-relay stall on 2026-05-23. The
library's liveness model (WebSocket + ping/pong) doesn't cover subscription-level
death, so we get silent archive gaps.

**Leverage.** Not a bug we can config away — it's a *gap* in what the abstraction
models. We built an entire **dispatch-liveness probe** subsystem on top
(per-sub `last_event_at`, one-shot probe REQ, timeout→reconnect→resubscribe),
modelled on NDK's keepalive, to compensate. See [design.md §D6](design.md). This
is real complexity the library pushed onto us; it's the kind of thing a
purpose-built ingest layer would own natively.

### 7. `MemoryDatabase` LRU memory cost for dedup — 🟡

**What.** Cross-relay dedup for `RelayPoolNotification::Event` is backed by an
in-memory LRU of seen event ids, default capacity 5,000,000 (~120 bytes/entry ≈
**600 MB**). It only covers near-term duplicates; capacity is the only knob.

**Where it bites.** Pure memory tax — ~600 MB of resident set spent on a dedup
layer whose output (#4) we've already partly abandoned for the raw wire stream.
Configured via `pool_dedup_max_events` ([design.md §](design.md)). No correctness
issue, but worth noting we're paying for a feature that doesn't fully serve us.

---

### 8. `connect_relay` re-arm races `disconnect_relay` teardown → relay wedged `!Connected` — 🟠

**What.** This is the failure mode of entry #5's workaround. `disconnect_relay()`
sets `RelayStatus::Terminated` synchronously (`inner.rs:1282`), but the connection
task's `running: AtomicBool` (`inner.rs:148`) is cleared **only at the task's exit**
(`inner.rs:608`), which happens *after* `post_connection` finishes its
`close_ws().await` handshake (`inner.rs:786`) — ~instant on localhost, tens of ms
over a real network. So the entry-#5 re-arm — calling `connect_relay()` right after
`disconnect_relay()` — races the old task's teardown:

- `connect()` (`mod.rs:288`) sets shared status `Pending` (`mod.rs:296`), then
  `spawn_connection_task()` (`mod.rs:299`) **no-ops** with a
  `"Connection task is already running."` warning when `is_running()` is still true
  (`inner.rs:509`).
- Meanwhile the old task snapshots status into a **local** variable at
  `inner.rs:566` and breaks at `inner.rs:569` if that *local* copy is `Terminated`.

The wedge is when our `connect()` runs entirely inside the window between the old
task's status snapshot (566) and its `running` clear (608): the task breaks on its
stale local `Terminated` even though shared status is now `Pending`, so it exits
leaving the relay at `Pending`/`Connecting` (our gauge `relay_connection_state = 2`)
— **or at `Disconnected` in a sibling interleaving** — with **no running connection
task and nothing to re-arm it**. `connect_relay` returns `Ok`, so the caller cannot
detect the no-op. The two competing windows (our `connect()` before line 566, or
after line 608) both recover, which is why it is rare.

**Where it bites.** nos.lol, 2026-06-06 22:17:48 UTC → stuck `Connecting`
(`relay_connection_state = 2`) for ~5 h until a manual container restart, silently
absent from `events_received_total` while the other three relays were unaffected.
The probe-timeout recovery path (`relay_task::tick_forward_sub_probe`) ran
`disconnect_relay().await` then `connect_relay().await` (the entry-#5 re-arm) and
the `connect_relay` lost the race. It is timing-sensitive — it needs a busy
multi-task runtime (the live 4-relay pool + notification listener) to jitter
`connect()`'s scheduling into the window — which is why it surfaced end-to-end only
twice in multi-week operation. Reproduced in
`repro_connect_relay_race_wedge` ([integration.rs](../crates/nozzl-acquirer/tests/integration.rs)):
a single relay cycling alone never wedges (2778 iterations, 0), but 40 relays
cycling on a 4-worker runtime wedge `!Connected` in 25-90 s, 5/5 runs, the
`Connecting` variant matching the live gauge value exactly.

**Leverage.** No config knob. The library offers no way to (a) make
`disconnect`+`connect` atomic, (b) await teardown completion before re-arming, or
(c) learn that `connect_relay`'s spawn no-op'd (`Ok` either way). Additional
findings from the 2026-07-05 deep-dive (full detail in issue #8): the earlier
"barrier on `status == Terminated`" idea is **not** a barrier at all —
`disconnect()` sets `Terminated` eagerly at entry, before teardown; and the
wedge is a **double** silent no-op, because `connect()` also guard-no-ops
unless the status is connectable (`can_connect()` = `Initialized | Terminated
| Sleeping`), which the wedged `Pending`/`Disconnected` states are not — so
recovery must always re-drive through `Terminated`. Occurrences grew to four
(nos.lol 2026-06-06 ~5 h; damus 2026-06-25 ~11.7 h and 2026-06-27; fountain.fm
2026-07-03 ~59 min, the first one an alert caught).

**RESOLVED 2026-07-05 via the fork** (see *The whisperhash fork* above): the
fork exposes the teardown ground truth (`Relay::is_connection_task_running()`
over the private `running` flag — the very bit the lib's own tests assert on),
and `teardown_barrier_and_rearm`
([relay_task.rs](../crates/nozzl-acquirer/src/relay_task.rs)) sequences the
re-arm in two stages: await task exit (bounded at 12 s, above the lib's own
10 s `close_ws` cap), then re-drive any non-connectable status through
`Terminated`, then connect. Outcomes are counted in
`teardown_barrier_total{relay, outcome}`. Regression guard:
`barrier_prevents_connect_relay_race_wedge`; the bug-present
`repro_connect_relay_race_wedge` is retained as the upstream canary (rerun on
every lib bump — when it stops wedging, upstream fixed the race and the fork
can go). The larger single-owner "connection manager" redesign is parked in
issue #8 as the long-term option. The deeper hazard stands: every operation
built on `disconnect_relay`/`connect_relay` inherits this race; the typed
status enum gives us no atomic "cycle the connection" primitive — the fork
accessor is the minimum viable escape hatch, not a fix of the lib's model.


### 9. A malformed tag kills the whole frame *before* any notification exists - 🔴

**What.** `Tag`'s `Deserialize` impl hardcodes the wire shape as `Vec<String>`
([`nostr/src/event/tag/mod.rs:557-565`](https://github.com/whisperhash/nostr/blob/nozzl-v0.44/crates/nostr/src/event/tag/mod.rs#L557)):

```rust
impl<'de> Deserialize<'de> for Tag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> {
        type Data = Vec<String>;
        let tag: Data = Data::deserialize(deserializer)?;
        Self::parse(tag).map_err(DeserializerError::custom)
    }
}
```

Any tag element that is a JSON **number** (or bool, or null) fails serde's
`String` visitor with `invalid type: integer 25000, expected a string`. That `?`
propagates out of `RelayMessage::from_json(msg)?` (`inner.rs:1106`) into
`handle_relay_message`'s `Err` arm (`inner.rs:1073-1078`), which **logs and
returns**:

```rust
Ok(Some(message)) => { ... self.send_notification(RelayNotification::Message { message }, true); }
Ok(None) => (),
Err(e) => tracing::error!(url, msg, error, "Impossible to handle relay message."),
```

One bad element in one tag therefore discards the **entire `["EVENT", ...]`
frame** - and `send_notification` is only ever reached on the `Ok(Some(_))` arm,
so the event enters *no* channel: not `RelayNotification::Event`, not
`RelayNotification::Message`. The connection itself survives (the handler is
per-frame inside the read loop at `inner.rs:875`), so this is a silent per-event
hole in an otherwise healthy stream.

**Why this is worse than #1 and #4, not just another instance.** This doc's
recurring escape hatch - *"drop to the raw `RelayMessage` wire stream"* (#4, and
the mechanism #1 would need) - **does not exist here.** The `Message`
notification is emitted *after* the parse succeeds, so the layer we normally fall
back to is downstream of the failure. There is no lower level left to reach for.
It also contrasts sharply with #1 despite sharing a drop site: #1's
`Error::EventExpired` is raised at `inner.rs:1203` *after* a successful parse, so
the library is holding a fully-formed `Event` and a fork need only decline to
return `Err`. Here the library never constructs an `Event` at all - a fork
inherits nothing but the raw `&str`, which the `Err` arm interpolates into a log
line and then drops.

**Where it bites.** #31 (Pool-level deserialize failures are unmetered event loss)
(the nozzl-side instrumentation gap) and
#33 (rust-nostr drops the whole frame on a non-string tag element) (this entry's
library root cause, with the fork/upstream change specified).
Unpinning the GammaMarkets marketplace kinds started acquiring
kind-30402 (markets-product-listings) from the public relays; a nos.lol backfill
reported 8,757 events against an independent walk's 8,845. The 88-event gap was
accounted for exactly by 89 `Impossible to handle relay message.` ERROR lines in
the same window - 100% kind-30402 (markets-product-listings), 100% failing on the
same `shipping` tag, across **64 distinct authors** with `created_at` spanning
2023-10 to 2026-07. The offending shape is a legacy inline-shipping convention
that puts the cost in as a bare number:

```json
["shipping", "Added Cost", 25000, "SAT"]
```

NIP-01 requires tags to be arrays of non-null strings, so **the publishers are
wrong and the library is right** - but an archive wants the bytes regardless, and
the current GammaMarkets spec uses a `["shipping", "30406:<pubkey>:<d-tag>"]`
coordinate reference, so these arrive from the long tail rather than dying out.

**The instrumentation blind spot is the real cost.** Every loss counter we have -
`notif_channel_lagged_total`, `notif_fanout_dropped_total`, `events_dropped_total`,
`events_rejected_total` - read **0** across that window. The invariant
(*"loss <=> a dropped/lagged notification"*) has a third case it does
not cover: **loss before the notification exists at all.** It was caught only
because an out-of-band relay walk existed to compare against. Nothing establishes
1% of one kind as a ceiling - the rate on every other kind is simply unknown.

**Still live, and it amplifies in the log.** On 2026-08-22 *one* stuck kind-30402 (markets-product-listings)
event from `wss://nostr.mom/` was re-delivered roughly every 60 s and re-failed every
time (the #4 / issue-#28 re-issue replay path - a permanently unparseable event is
re-fetched forever because it can never be checkpointed as seen). So a single
malformed event also costs an ERROR line on every re-delivery, on top of the silent
loss.

**Leverage.** **No config knob at either layer.** `RelayLimits` (#2's surface) caps
*size* and *tag count*, not element *type*, and the `Vec<String>` is a hardcoded
type in the `nostr` crate - one layer below the pool, so even a pool-level option
could not reach it. Three options, in ascending cost:

1. **Count it** (what GH #31 proposes as the cheapest useful step): a metric on
   the error path so completeness checks stop reading 0. Needs no library change -
   but from *our* side it can only be done by scraping the ERROR log, i.e. a
   parse-the-log-line dependency on an upstream message string.
2. **Expose the raw frame on the `Err` path** - an additive
   `RelayNotification::RawMessageError { raw }` (or equivalent) so nozzl can count
   and optionally archive the bytes. This is the option that fits the fork's
   additive-only hygiene, and it is the general fix: it restores the raw-wire
   escape hatch that every other 🔴/🟠 entry here depends on.
3. **A lenient `Tag` deserializer** that coerces non-string scalars. Cheapest to
   write, worst to own: it silently changes the bytes we archive (a JSON `25000`
   becomes `"25000"`), which is a semantic delta rather than an additive one, and
   would have to be defended on every rebase.

**Tracked in #33 (rust-nostr drops the whole frame on a non-string tag element)**,
which specifies option 2 and records the ordering: report upstream against
`rust-nostr/nostr` first (a plausible general-purpose request - any archiver or
relay-bridge hits it, and an accept retires this entry without growing the fork),
and carry it on `nozzl-v0.44` only if upstream declines. **Not yet reported
upstream as of 2026-08-23.**

**Upgrade-watch:** re-check whether `Tag::deserialize` still hardcodes
`Vec<String>` and whether the `Err` arm has gained any notification, on every
`nostr` / `nostr-relay-pool` bump. Line citations above are against the fork
checkout (0.44.1 base), not 0.44.0.

---

### 10. The WebSocket handshake carries no `User-Agent` - 🟠

**What.** The pool connects through `async-wsocket` 0.13.2, whose direct connect
(`native::connect_direct`) hands tungstenite the URL alone, so the opening handshake carries
`Host`, `Connection`, `Upgrade` and the two `Sec-WebSocket-*` headers and no `User-Agent`. No
knob adds one: `RelayOptions` has none, and async-wsocket takes no headers.

**Where it bites.** A relay we read answers such an upgrade with `403 Forbidden`, and the same upgrade
with any `User-Agent` with `101 Switching Protocols` (measured from a workstation, 2026-10-14). Added
to dev that day, it never connected: the pool logged one `Connection failed.`
(`HTTP error: 403 Forbidden`; it logs a repeated error once) and, on nozzl's fixed 2 s
`retry_interval` (entry #3), retried every 2 s or so until the relay was taken out again 13
minutes later: about 370 refused upgrades, computed from the interval and the attempts' own
time, not counted, since nothing counts them. Its NIP-11 (relay information) had answered the box in the
pre-flight because curl, like any HTTP client, sends a `User-Agent`. A relay refusing the
handshake is loud (`Connection failed.` in the log, and `relay_connection_state` never reads 1 (connected)), so this loses nothing silently;
it only keeps such a relay out.

**Resolution.** The pool takes a pluggable `WebSocketTransport`
(`RelayPoolBuilder::websocket_transport`). nozzl's `UserAgentTransport`
([crates/nozzl-acquirer/src/transport.rs](../crates/nozzl-acquirer/src/transport.rs)) is
async-wsocket's `connect_direct` and its Happy Eyeballs connect, copied, with
`User-Agent: nozzl/<version>` added. It calls tokio-tungstenite on async-wsocket's own release,
so the stream is async-wsocket's `WebSocket::Tokio` and everything after the handshake (TLS,
framing, ping, the error strings the pool logs) is the library's; a proxy or Tor connection mode,
which nozzl never sets, goes to the library's transport. Tested by the transport's unit tests
(the request's headers; the connect timeout, against a peer that never answers, with the
library's own error string; the IPv4 fallback when IPv6 fails fast, the path every relay with
both families takes from a host with no IPv6 route; ping support as in the library),
`handshake_carries_the_user_agent_a_relay_may_require` (against the test relay's
`require_user_agent()`, with a control that shows the library's own transport refused) and the
test relay's own test; verified live against that relay from a workstation (a partial walk, stopped
after 33 s: 18 pages, 8,981 events). Live on dev since 2026-10-15
03:57:32 UTC: no relay refused it, and that relay, back by a config reload at 04:08:17,
connected and was walked to its end.

**Upgrade-watch:** async-wsocket is pinned to `=0.13.2`, so it moves only by a deliberate bump;
on one, or on a nostr-relay-pool bump, re-diff the copy against the new `connect_direct` (and
the pool's `TransportSink`), and keep `tokio-tungstenite` in `crates/nozzl-acquirer/Cargo.toml`
on async-wsocket's release: on another, `WebSocket::Tokio` stops type-checking, so a mismatch
fails the build, not a connection. If async-wsocket ever takes headers, drop the copy.

---

### 11. `disconnect()` can lose its own termination - 🟠

**What.** `InnerRelay::disconnect()` (`relay/inner.rs:1273`) wakes the connection task
(`terminate()`, a `Notify::notify_one`, line 1282) and only then sets `RelayStatus::Terminated`
(line 1285). The woken task leaves `post_connection` (or `_try_connect`), and the auto-connect
loop decides from the status it reads next (`inner.rs:566-569`) whether to exit. A task that does
not read `Terminated` there takes the wake for a dropped connection: it sets `Disconnected`,
sleeps its retry interval in a `select!` whose terminate branch cannot fire any more (the one wake
is spent), and connects again. The task outlives the explicit disconnect, and the relay comes back
with no one asking. Three paths lead there:

- The task reads the status between the wake and `Terminated`: the disconnecting thread has to be
  delayed between two adjacent statements, preempted or slow to hand the wake to another worker.
- The disconnect lands before the task's `_try_connect` starts: the wake is stored as a permit,
  and `_try_connect` overwrites `Terminated` with `Connecting` (`inner.rs:664`) before it spends
  the permit.
- The connect and the wake are ready together and `select!` picks the connect: `Connected`
  (`inner.rs:677`) overwrites `Terminated`, and `post_connection` spends the forwarded wake.

**Where it bites.**

- `fork_accessor_tracks_connection_task_lifecycle` failed CI five times between 2026-10-09 and
  10-13 ("connection task still running 5 s after disconnect_relay"). Under CPU load on a workstation it
  failed 1 run in 320 (the first path): that run's library log has the task's
  `Disconnected from` and `Reconnecting ... in 11 secs`, then `disconnect()`'s
  `Completely disconnected`, 53 us after the task's `Disconnected from`.
- In production, the probe-timeout recovery (`teardown_barrier_and_rearm`, entry #8) waits up to
  12 s for the old task to exit. When the termination is lost, the task reconnects itself after
  nozzl's 2 s retry, the barrier times out, and it leaves recovery to that task
  (`teardown_barrier_total{outcome="timeout_task_live"}`): 10 of 2,970 barrier passes on dev
  since nozzl started on 2026-10-12 at 10:09 UTC, to 2026-10-15 03:27 UTC (relay.conduit.market 9,
  relay.zapstore.dev 1). That is about nine times the canary's rate for the first path under CPU
  saturation (below), so production probably takes the overwrite paths, which need no
  preemption. The relay is back about 2 s after the disconnect, but nozzl's task for it waits out
  the barrier's full 12 s, its notifications undrained meanwhile (the bound's documented worst
  case, `relay_task.rs`).
- A relay removed by hot-reload is disconnected the same way: by its own task's shutdown
  (`relay_task.rs`, `disconnect_relay`), then by `force_remove_relay` (`acquirer.rs`), a no-op
  once the status is `Terminated`. A task that outlived that would keep a connection nozzl no
  longer wants, and nozzl would drop its events uncounted (`run_central` no longer knows the
  relay). The log would show it: after `Completely disconnected`, a fresh `Connected to`, or a
  fresh `Connection failed.` (for a relay that never connected, the termination request is an
  error the pool has not logged yet). nozzl's log on the box has no such line after
  the `Completely disconnected` of the relay removed at 2026-10-14 18:54:18 UTC.

**Leverage.** No knob reorders `disconnect()`, and reordering it would not be enough: the second
and third paths overwrite an early `Terminated` (`inner.rs:664`, `677`). A fix needs a terminate
flag the loop checks, in upstream or the fork. `RelayOptions::reconnect(false)` avoids it at the
cost of the library's automatic reconnect: the loop then exits whatever status it reads. nozzl
could shorten the cost in production without either: when the barrier sees the status leave
`Terminated` while the old task still runs, a second `disconnect()` would wake the survivor in
its retry sleep and end it within about 2 s, not 12. Not done.

**Resolution.** None in v0.44 (the tag on disk; upstream's later releases were not checked).
nozzl's barrier tolerates it (`timeout_task_live`).
`fork_accessor_tracks_connection_task_lifecycle` runs with reconnect off, since the race is the
library's and the test pins the fork accessor alone;
`explicit_disconnect_ends_the_task_with_reconnect_on` checks production's options, failing only
if a task outlives three disconnects in a row, as a loop that no longer exits on `Terminated`
would. The canary `repro_disconnect_can_lose_its_termination`
([integration.rs](../crates/nozzl-acquirer/tests/integration.rs), ignored: two minutes of every
core) reproduces it in-process with CPU-burning threads, disconnecting half its cycles once the
relay is `Connected`, from a blocking-pool thread, and half at once. Run it alone:
`cargo test -p nozzl-acquirer --test integration repro_disconnect_can_lose_its_termination -- --ignored --exact --nocapture`.
On a workstation, disconnects of a connected relay were outlived 9 of 23,415 times over two runs
before the second way was added. With both ways, one run outlived none of 3,145 disconnects of a
connected relay and 303 of 3,145 made at once: about one in ten early disconnects is lost to the
overwrite paths, which need no preemption, and which production's rate points to. Its sibling
`with_reconnect_off_no_task_outlives_its_disconnect` found none, with reconnect off, in 34,055
disconnects of a connected relay over three runs, then none in 7,205 and 7,248 with both ways.
On a library bump, rerun the canary. If it finds no survivor, read `disconnect()` and
`_try_connect`'s status writes before dropping `reconnect(false)`, since a fix of one path may
leave the others.

---

## When would we migrate?

Signals to weigh, roughly in priority order:

1. **Accumulating 🔴 entries with no config path** (currently: #1, #9). Each one is either silent data loss we tolerate or a fork we maintain. One fork is a paper cut; a *standing* fork we must rebase every upgrade is a real tax — and the moment we fork, much of the "it's maintained upstream" benefit evaporates.
2. **Growth of the "raw `Message` stream only" list** (#4, and anything future). The more we bypass the high-level API, the more the library is just a WebSocket multiplexer we could own — at which point its surface area (and its surprises, #2/#3/#5) is net negative. **#9 sharpens this into the stronger signal:** the raw-`Message` fallback is not merely overused, it has now *failed* - a parse error drops the frame upstream of the `Message` notification, so the layer we keep retreating to does not exist on that path. A multiplexer we owned would hand us the bytes; this one cannot.
3. **Defaults that are actively wrong for a long-running archiver** (#2, #3). We've had to override several; each was discovered via a production incident, not the docs. This predicts future unknown-unknowns on every upgrade.
4. **Complexity we already had to build ourselves** (#6, #8, #10). If we're writing keepalive, liveness, *and* connection-recovery logic anyway - because the library's own `disconnect`/`connect` lifecycle races against us (#8, #11) - a thinner client gives us fewer abstraction collisions for the same total code.

**Counter-weight (why we haven't):** the library still gives us WebSocket
lifecycle, NIP-01 parsing, NIP-11 fetch, signature verification, and a
battle-tested reconnect core for free, and every workaround above is currently
*holding*. The trigger to seriously cost a migration is when a single upstream
issue forces a **standing fork** (#1 may do this) — because at that point we own
a copy of the library anyway, and the calculus flips from "patch around it" to
"why carry the rest of it."

**Status 2026-07-05: the standing-fork trigger has FIRED** — entry #8 forced
*The whisperhash fork* above. It was accepted consciously as the mildest
possible instance (4 additive lines on a near-frozen maintenance branch, with
a documented exit), precisely **because** migration off this library is the
working assumption, not despite it. The fork is the bridge that keeps 0.44
operable while the migration question is answered on its own schedule; each
new entry below this line should be read with that bias in effect.

## Maintenance

- Add an entry the moment a new limitation costs us a workaround or a debugging session — capture the version, the symptom, and whether a knob existed. The value of this doc is the *trend*, which is only visible if entries are logged when fresh.
- On every `nostr-relay-pool` upgrade, re-verify the 🟢 entries (#2, #3) still need their overrides and that field names/defaults haven't shifted, and check whether any 🔴/🟠 entry gained an upstream config knob (especially #1).
