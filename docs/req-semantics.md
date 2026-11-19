# Nostr REQ semantics — general protocol and `nozzl` usage

> **Enumerating/walking a relay?** Read [relay-acquisition-limits.md](relay-acquisition-limits.md)
> first — the source-verified per-relay caps (strfry's silent 500-`limit` clamp,
> `max_subscriptions`, the edge-layer nos.lol throttle, zapstore's `VagueFilters`)
> and the per-egress-IP pacing rules (where to run a completeness walk).

## REQ against a Nostr relay — primer

A Nostr relay speaks line-delimited JSON over a single WebSocket. A client opens a subscription with a `REQ` frame and the relay streams matching events back tagged with the same subscription id, ending the stored-history portion with `EOSE`, then keeps the subscription open to forward newly-arriving matches in real time until the client sends `CLOSE` or the relay sends `CLOSED`.

**EOSE does not mean "you've seen every stored event matching the filter".** It means "I've sent all the stored events I'm going to send you for this REQ" — bounded by the smaller of the client's `limit` and the relay's `max_limit`. A relay returning a full page of 500 events with millions of older matches still on disk will send `EOSE` after the 500th event; the client must paginate (issue further REQs stepping `until` backward to the oldest event's timestamp) to see the rest. EOSE is the page-boundary marker, not an exhaustiveness guarantee. See the [cursor strategy section](#backfill-cursor-strategy---inclusive-until) for the exact formula and why `oldest_seen - 1` has a blind spot.

> All spec quotes in this primer are from [NIP-01](https://github.com/nostr-protocol/nips/blob/master/01.md), the authoritative source for client/relay protocol semantics.

### On the wire — a complete REQ round trip

Client frame:

```
["REQ", "<sub_id>", <filter_obj>, <filter_obj>, ...]
```

- `sub_id` — arbitrary client-chosen string. It's the demux key; every server frame for this subscription echoes it back, and `CLOSE` cancels it.
- One or more filter objects. A filter is a JSON object whose fields are AND'ed together; multiple filters in a single REQ are OR'ed.

Server frames, in order:

```
["EVENT", "<sub_id>", { ...event... }]    # zero or more, for stored history (capped at limit)
["EVENT", "<sub_id>", { ...event... }]
...
["EOSE",  "<sub_id>"]                     # end of THIS PAGE of stored events
                                          # (may or may not equal end-of-history)
["EVENT", "<sub_id>", { ...event... }]    # zero or more, real-time tail (uncapped)
...
```

Termination, from either side:

```
["CLOSE",  "<sub_id>"]                            # client → relay
["CLOSED", "<sub_id>", "<human reason>"]          # relay → client (server-initiated kill or pre-emptive refusal)
["NOTICE", "<human message>"]                     # relay → client, untagged, advisory (rate limit, errors, etc.)
```

`CLOSED` may come **instead of** any events / EOSE — that's how relays reject a REQ they refuse to serve at all ("filters are too vague", "too many concurrent REQs"). `NOTICE` is connection-scoped, not sub-scoped, so figuring out which REQ it's about is heuristic.

#### Worked example 1 — small stored result, then a real-time tail

Client opens a sub for kind-1 (posts) notes from one author, newer than a timestamp:

```
C → R: ["REQ", "abc", {"kinds":[1], "authors":["3bf0c63f…"], "since": 1714579200}]
```

The relay finds two stored matches — fewer than its `max_limit` — streams them, signals end-of-page, then keeps the sub open. A new match arrives a few seconds later:

```
R → C: ["EVENT", "abc", {"id":"d4e5…","pubkey":"3bf0c63f…","created_at":1714579500,"kind":1,"tags":[],"content":"hi","sig":"…"}]
R → C: ["EVENT", "abc", {"id":"f6a7…","pubkey":"3bf0c63f…","created_at":1714579700,"kind":1,"tags":[],"content":"hello again","sig":"…"}]
R → C: ["EOSE",  "abc"]
R → C: ["EVENT", "abc", {"id":"1b2c…","pubkey":"3bf0c63f…","created_at":1714579812,"kind":1,"tags":[],"content":"live!","sig":"…"}]
```

Here EOSE *happens to coincide* with end-of-history for this filter, because the result fit comfortably under `max_limit`. The client can't tell from the protocol whether that's the case or whether more older matches exist — it can only infer it from the page size relative to the limit. Compare worked example 3.

Client tears the sub down when it no longer needs the tail:

```
C → R: ["CLOSE", "abc"]
```

#### Worked example 2 — pre-emptive rejection

Client opens an over-broad REQ:

```
C → R: ["REQ", "wide", {"kinds":[1]}]
```

A strict relay refuses to serve it at all — no events, no EOSE, just a `CLOSED`:

```
R → C: ["CLOSED", "wide", "filters are too vague: add since/until/limit or narrow by author"]
```

#### Worked example 3 — paginated history walk (EOSE ≠ end of history)

Client walks backwards in time by re-issuing with `until` set to (at most) the oldest event seen on the previous page. First page — note `since=0` is present but does **not** anchor the result; the relay still returns the 500 events nearest the `until` end of the range:

```
C → R: ["REQ", "p1", {"kinds":[30023], "since": 0, "until": 1714579200, "limit": 500}]
R → C: ["EVENT", "p1", {...created_at=1714579180...}]   # newest of the page (closest to until)
R → C: ["EVENT", "p1", {...created_at=1714579150...}]
...                                                       # 500 events, newest-first
R → C: ["EVENT", "p1", {...created_at=1712000000...}]   # oldest of the page (~30 months below until)
R → C: ["EOSE",  "p1"]
C → R: ["CLOSE", "p1"]
```

Even though we asked for the range `[0, 1714579200]` — fifty years wide — the relay only delivered the slice closest to `until`. The 500-event cap is applied to the newest-first sort, so the older end of the range stays invisible until we step `until` down. EOSE here means "the 500-event page you asked for is now complete" — it does **not** mean the relay has no events older than `1712000000`. There can be (and usually are) years more history below the cursor.

The client then re-issues with the cursor advanced. The two common approaches differ in how they handle the `limit` boundary:

```
# Approach A — exclusive cursor (oldest_seen - 1):
C → R: ["REQ", "p2", {"kinds":[30023], "since": 0, "until": 1711999999, "limit": 500}]

# Approach B — inclusive cursor (oldest_seen, requires event-id dedup):
C → R: ["REQ", "p2", {"kinds":[30023], "since": 0, "until": 1712000000, "limit": 500}]
```

See the [Backfill cursor strategy](#backfill-cursor-strategy---inclusive-until) section below for which approach nozzl uses and why.

The client only knows it has truly reached the end of history when a page returns *zero events it has not already seen*. In practice — see [docs/design.md §5.D6](design.md) — even "fewer than limit" is unreliable on relays that return `499/500` indefinitely due to `created_at` ties at the cursor; only consecutive empty pages are a robust end-of-history signal.

**What if we tried to paginate forward instead?** Setting `since=1712000000, until=1714579200, limit=500` and then bumping `since` upward would not work the way you'd hope: each page would still return the 500 events closest to `until`, so you'd see the same newest slice over and over and never advance. Forward-walking via `since` only makes sense when you expect ≤ `limit` matches in the window (e.g. a forward subscription tailing real-time, anchored at "now").

#### Worked example 4 — concurrency pressure mid-stream

Sometimes the relay starts serving a sub and then kills it because too many subs are open. The `CLOSED` arrives **after** some events but before `EOSE`:

```
C → R: ["REQ", "x", {"kinds":[1,3,7], "since": 1714000000, "limit": 500}]
R → C: ["EVENT", "x", {...}]
R → C: ["EVENT", "x", {...}]
R → C: ["NOTICE", "ERROR: too many concurrent REQs"]
R → C: ["CLOSED", "x", "rate-limited: too many subscriptions"]
```

The client never gets `EOSE` for `"x"`; the partial results are still valid, but the sub is gone and must be re-issued (after back-off).

### Filter fields (NIP-01)

A filter object can contain any of these — all optional, all AND'ed:

| Field | Type | Meaning |
|---|---|---|
| `ids` | `[hex]` | Event id is in the set. Per current NIP-01, values **MUST be full 64-character lowercase hex** — prefix matching was in an older spec revision and is no longer allowed. |
| `authors` | `[hex]` | Pubkey is in the set. Same exact-64-char-lowercase-hex requirement as `ids`. |
| `kinds` | `[int]` | `kind` is in the set. Event kinds are 0-65535 per the NIP-01 event definition; the filter field itself isn't formally bounded, but values outside that range will never match. |
| `since` | unix seconds | `created_at >= since` (inclusive — spec: `since <= created_at`). |
| `until` | unix seconds | `created_at <= until` (inclusive — spec: `created_at <= until`). Paginating with `until = oldest_seen` therefore re-delivers events at exactly that timestamp on the next page; clients that use an inclusive cursor need event-id deduplication. The simpler `oldest_seen - 1` cursor avoids re-delivery but has a blind spot: if the relay returns a full page that ends mid-way through a group of events sharing one timestamp, those remaining events are permanently missed. See the [cursor strategy section](#backfill-cursor-strategy---inclusive-until) for how nozzl handles this. |
| `limit` | non-negative int | Max **stored-history** events before the relay sends `EOSE`. Does not cap the real-time tail. Applied per filter; relay may cap below the requested value via its own `max_limit`. EOSE fires once `min(client_limit, relay_max_limit)` events have been sent, regardless of how much more matching history exists on the relay. **NIP-01 mandates that the events returned are the newest matches** — verbatim: "the events returned in the initial query will be the last `n` events ordered by the `created_at`. Newer events should appear first, and in the case of ties the event with the lowest id (first in lexical order) should be first." So with both `since` and `until` present and >`limit` matches in the range, the relay returns the `limit` events closest to `until`, not closest to `since`. This is what makes backward pagination work: each page is anchored at its `until` end, and the next page lowers `until` to step further back. The lowest-id tie-break rule is what makes timestamp-tie pagination quirks (498/500 from damus) deterministic but not necessarily forward-progressing. |
| `#<X>` | `[string]` | Indexed-tag match for tag letter `X` (NIP-01 restricts `X` to `a-zA-Z`, e.g. `#e`, `#p`, `#d`). Only the first value in any given tag on an event is indexed. |

**Relay-side defaults when fields are omitted:**

- `since` omitted → no lower time bound is set by the filter. The relay still caps how many stored events it returns: it applies its configured `max_limit` (or a "default limit" if it has a separate one for `limit`-omitted REQs) — typically 500/1000/5000, advertised in NIP-11's `limitations.max_limit`. So you don't get "everything since the beginning of time"; you get the newest `max_limit` matches within the relay's retention horizon, newest-first. To genuinely walk all of history you must paginate with `until`.
- `until` omitted → relay treats it as "now" (a real-time tail will start after EOSE).
- `limit` omitted → relay applies its own `max_limit`. It is **not** unlimited.
- `ids`, `authors`, `kinds`, `#<X>` omitted → no filtering on that dimension. **All four major dimensions omitted is the empty filter `{}` — semantically "everything"**, which most relays reject as too vague.
- Multiple values within one field are OR'ed; different fields are AND'ed.

Ordering: NIP-01's `limit` clause specifies that the returned events are "the last `n` events ordered by the `created_at`" with "newer events appear[ing] first" and ties broken by ascending event id. Streaming order is formally specified only in this `limit` context; when `limit` is absent the spec is silent on order, but in practice the relay's own `max_limit` always applies (a relay returning unbounded history would be a DOS risk), so the newest-first behaviour is effectively universal.

Because `limit` selects the **newest** matches in the filter's time range, pagination is done by walking `until` *backward* — each page returns events anchored at the `until` end of the window, and each subsequent page lowers `until` to slide further into the past. `since` does **not** anchor the result; with both `since` and `until` set and more than `limit` matches in `[since, until]`, the relay returns the `limit` events closest to `until` and you'll never see the older end of the range unless you paginate. Ties on `created_at` are the pagination quirk that bites — see [Backfill cursor strategy](#backfill-cursor-strategy---inclusive-until) and the `backfill_empty_pages_to_complete` rationale in [docs/design.md](design.md).

---

## REQs issued by `nozzl`

Per `(relay, kind-set)` we run exactly two flows, defined in [docs/design.md §5.D6](design.md):

1. A **forward subscription** — long-lived, opens once per (re)connect, no `until`, no `limit`, only `kinds` plus a computed `since`. Tails real-time events forward.
2. **Onboarding backfill** — a sequence of time-bounded paginated REQs walking history backward. Each page has `kinds + since=0 + until + limit`. Pages terminate on EOSE; we then open the next page.

All filter construction lives in [crates/nozzl-acquirer/src/filter.rs](../crates/nozzl-acquirer/src/filter.rs); subscription opening lives in [crates/nozzl-acquirer/src/relay_task.rs](../crates/nozzl-acquirer/src/relay_task.rs). Both flows go through `rust-nostr`'s `RelayPool::subscribe_with_id_to`, which serializes the `Filter` to the JSON-array `REQ` you'd see in DevTools — fields we leave unset on the `Filter` are simply omitted from the JSON object.

### 1. Forward subscription

[forward_filter](../crates/nozzl-acquirer/src/filter.rs#L41-L44) builds:

```rust
Filter::new().kinds(...).since(Timestamp::from_secs(since))   // when since is Some
Filter::new().kinds(...)                                       // when since is None
```

Opened from [open_all_forward_subs](../crates/nozzl-acquirer/src/relay_task.rs#L455-L516). `sub_id` is the constant `"forward"` when there's a single kind-group (the normal case), otherwise `"forward-<idx>"` for each group after a kind-list auto-split.

`since` value, computed in [open_all_forward_subs:466-478](../crates/nozzl-acquirer/src/relay_task.rs#L466-L478) via [since_with_safety_margin](../crates/nozzl-acquirer/src/filter.rs#L25-L34):

- Per-kind: read `checkpoint.since` for `(relay, kind)`.
- Take the `min()` across all kinds in this REQ's group.
- Subtract `acquirer.backfill_safety_margin` (default 60 s, saturating at 0).
- **If any kind in the group has no checkpoint at all, set `since=None`** — meaning we omit the field entirely so the relay sends what it has from its retention horizon up to EOSE (bounded by its `max_limit`), then tails. This is a deliberate design choice in [docs/design.md:328](design.md): "A kind without an observed since … keeps its forward filter unrestricted, so the same multi-kind REQ that needs since=T for one kind doesn't accidentally clip a sparser kind."

On-the-wire — three concrete shapes:

```jsonc
// Fresh relay, no checkpoints yet (since omitted entirely):
["REQ", "forward", {"kinds":[0,3,10002,30023,30063,30267,32267]}]

// All kinds have checkpoints, min is T:
["REQ", "forward", {"kinds":[0,3,10002,30023,30063,30267,32267], "since": <T-60>}]

// Post auto-split, two groups:
["REQ", "forward-0", {"kinds":[0,3,10002],            "since": <T0-60>}]
["REQ", "forward-1", {"kinds":[30023,30063,30267,32267],"since": <T1-60>}]
```

Semantics resulting from those parameters:

- No `until` → relay sends stored matches between `since` and "now", then keeps the sub open to forward newly-arriving matches indefinitely.
- No `limit` → relay applies its own `max_limit` to the *stored* portion. Since real systems run with `since` set close to "now" after the first run, the stored portion is typically small and we don't hit the cap. **On a fresh relay-add with `since=None`, however, the omitted `limit` means the relay caps stored history at its `max_limit`** — backfill is what makes us robust to that, not the forward sub.
- The sub lives until config reload changes the kind list, the connection drops (the pool reopens on reconnect), or the silence-watchdog re-issues it (see §5.D6 stuck-REQ recovery).

### 2. Backfill page

[backfill_filter](../crates/nozzl-acquirer/src/filter.rs#L57-L67) builds, always:

```rust
Filter::new().kinds(...).since(Timestamp::from_secs(0))   // ← always set, even when until is None
              .until(Timestamp::from_secs(until))           // when until is Some
              .limit(limit)                                  // always set
```

The unconditional `since=0` exists because some relays — notably [`relay.zapstore.dev`](https://github.com/zapstore/relay) (built on the [`rely`](https://github.com/pippellia-btc/rely) framework with zapstore's own `defender` policy layer) — reject `{kinds, until, limit}` REQs without a lower bound as "filters are too vague". The cost is one extra JSON field per backfill REQ; the regression test [filter.rs:170-185](../crates/nozzl-acquirer/src/filter.rs#L170-L185) pins it.

`sub_id` is `"backfill-<hex-nanos-nonce>"` from [new_nonce](../crates/nozzl-acquirer/src/relay_task.rs#L1452-L1457) — re-rolled per page so a relay can't get confused by a `CLOSE` racing the next page's `REQ` over the same id.

`limit` is [effective_backfill_limit](../crates/nozzl-acquirer/src/relay_task.rs#L635-L640) → `acquirer.backfill_default_limit` (default 500), floored at `BACKFILL_MIN_LIMIT=50`. NIP-11 is fetched and logged but not used to size the page in v1; the reactive auto-split is the actual back-pressure mechanism.

`until` cursor lifecycle (per kind-group, paginating backward):

| Phase | `until` value | Code |
|---|---|---|
| First page, fresh group (no checkpoint cursor) | `Some(now_secs())` — seeded explicitly so the page carries `until` (needed to clear filter-specificity floors like zapstore's `VagueFilters(3)`; a bare `{kinds, since:0, limit}` scores below it — #25 (Single-kind backfill onboarding permanently abandons against filter-specificity relays)). Semantically identical to a relay defaulting the omitted field to "now". | `start_backfill_for_kinds` |
| First page, resume after restart | min `until_cursor` across the group's kinds from the checkpoint file | `start_backfill_for_kinds` |
| Next page - non-empty page, cursor advanced (`oldest != prev_oldest`), full or short | `oldest_observed` (inclusive; closes the boundary blind spot - see cursor strategy below). Until 2026-10-12 a short page (`received < limit`) stepped `oldest_observed - 1` instead, on the reading that a short page has no boundary events left; see [Short pages](#short-pages) for why that was wrong | `handle_eose` |
| Next page - cursor stagnant (`oldest == prev_oldest`), full or short | `oldest_observed - 1` (stagnation guard; breaks the loop when >= limit events share one second, and ends the re-read of a second the previous page ended on) | `handle_eose` |
| Next page — empty EOSE | inherit previous `until - 1` | `handle_eose` |
| Re-issue after concurrency-pressure CLOSED | same `until` as the abandoned page (preserves `short_streak`) | `reissue_backfill_page` |
| Re-issue after stuck-REQ timeout (no EOSE within 60 s) | `oldest_observed - 1` if the abandoned page received events (resets `short_streak`), else the same `until` (preserves it); exclusive because the re-issued page has no `prev_oldest`, so an inclusive retry would have no stagnation guard | `check_stuck_backfills` |

Termination: after `acquirer.backfill_empty_pages_to_complete` (default 8) consecutive EOSE-with-zero-events pages we mark the kind-group `Complete` in the checkpoint and stop opening pages. Counting *empty* (not "fewer than limit") tolerates the timestamp-tie quirk where damus returns 498/500 indefinitely.

On-the-wire — three concrete shapes:

```jsonc
// First page of a brand-new backfill (until seeded to now — see issue #25):
["REQ", "backfill-19a8c…", {"kinds":[0,3,10002,30023,30063,30267,32267],
                            "since": 0, "until": 1714579200, "limit": 500}]

// Subsequent page walking backwards:
["REQ", "backfill-19a90…", {"kinds":[0,3,10002,30023,30063,30267,32267],
                            "since": 0, "until": 1714579199, "limit": 500}]

// Post auto-split, the same page issued as two:
["REQ", "backfill-19a92…A", {"kinds":[0,3,10002],            "since": 0, "until": ..., "limit": 500}]
["REQ", "backfill-19a93…B", {"kinds":[30023,30063,30267,32267],"since": 0, "until": ..., "limit": 500}]
```

Semantics resulting from those parameters:

- `since=0 + until=T + limit=N` → relay returns the newest ≤N events with `created_at` in `[0, T]`, newest-first, then `EOSE`. Because we set `limit` and a finite `until`, the relay's own `max_limit` is the ceiling on `N`, not the floor (i.e. requesting `limit=500` against a relay capping at 100 still gets you 100).
- We do **not** consume the post-EOSE real-time tail on a backfill REQ; it would be redundant with the forward sub. We send `CLOSE` (`pool.unsubscribe`) when the page completes and immediately open the next page under a fresh `sub_id`.

### What the relay sees from a fully-seeded `nozzl`

Steady state, one relay, no splits, all kinds completed-backfill:

```
["REQ", "forward", {"kinds":[...all configured kinds...], "since": <min-checkpoint-minus-60s>}]
... EVENT EVENT ... EOSE
... EVENT (real-time tail, indefinitely) ...
```

That's it — one open subscription, no `until`, no `limit`, indefinite lifetime. Backfill REQs only exist while at least one kind on that relay is `Pending` or `InProgress`; they paginate quickly and tear themselves down.

Worth flagging for accuracy: the section "Default: one big multi-kind filter per relay" in [docs/design.md:266](design.md) says "The forward subscription for the same relay stays at 1 kind per REQ" — that contradicts the rest of the doc and the code (which always opens forward subs per kind-group, exactly like backfill). Looks like a typo for "stays as a multi-kind REQ" or similar; the actual behavior in [open_all_forward_subs](../crates/nozzl-acquirer/src/relay_task.rs#L459-L514) iterates `self.kind_groups` identically to backfill.

---

## Backfill cursor strategy - inclusive `until`

### The blind spot in naïve `oldest - 1` pagination

NIP-01 mandates that when a relay returns a capped page it returns the **newest** `limit` matching events, ties broken by ascending event id. The sort order is `(created_at DESC, id ASC)`. When a page fills exactly to `limit` and the oldest event on that page shares its `created_at` with additional events that didn't fit, those remaining events are cut off at a timestamp one second above the next cursor:

```
Relay has: above1@T, above2@T, tie_a@T-5, tie_b@T-5, tie_c@T-5   (5 events total)

Page 1 (until=T, limit=3):
  relay returns: [above1@T, above2@T, tie_a@T-5]    ← newest 3; tie_b and tie_c don't fit
  oldest_seen = T-5

  Naïve cursor: next until = T-5-1 = T-6
  → tie_b@T-5 and tie_c@T-5 have created_at=T-5 > T-6; they fall outside page 2's filter.
  → tie_b and tie_c are permanently lost.
```

In practice this is not hypothetical: during the initial backfill of nos.lol for a group of five kinds, page 4 returned exactly 500 events with 4 events at the boundary second `1774435247` (one per kind). Those 4 events were lost under the naive cursor.

### The inclusive cursor fix

When a page is **non-empty** and the cursor **advanced** since the previous page (`oldest != prev_oldest`), nozzl issues the next page with `until = oldest` (inclusive), whether the page was full or short (see [Short pages](#short-pages)). Because `since/until` are both inclusive per NIP-01, events at the boundary second are re-requested. Events already seen are discarded by the `nostr-relay-pool` event-id tracker (one pool-wide `MemoryDatabase` of ids, 5,000,000 by default, shared by every relay; `acquirer.rs`). Only events at exactly the boundary second are re-delivered; events above the boundary cannot match `until = oldest` so they are never retransmitted.

Using the blind-spot scenario from above, extended with 2 events below the boundary:

Events: above1@T, above2@T, tie_a@T-5, tie_b@T-5, tie_c@T-5, below1@T-10, below2@T-15. Limit=3.

```
Page 1 (until=now, limit=3): → [above1@T, above2@T, tie_a@T-5]
  (now ≥ T, so the top of history is returned — the fresh-onboarding seed; see issue #25)
  oldest=T-5, received=3=limit, cursor_advanced(prev=None→T-5)
  next_until = T-5  ← inclusive

Page 2 (until=T-5, limit=3): → [tie_a@T-5(dup), tie_b@T-5(new), tie_c@T-5(new)]
  above1@T and above2@T have created_at=T > T-5 — they cannot match this filter.
  Pool dedup drops tie_a. Sink receives tie_b and tie_c.
  oldest=T-5, received=3=limit, cursor_stagnant(prev=T-5→T-5) ← stagnation guard
  next_until = T-5-1 = T-6  ← fall back to exclusive

Page 3 (until=T-6, limit=3): -> [below1@T-10, below2@T-15]
  oldest=T-15, received=2 < limit, cursor_advanced(prev=T-5->T-15)
  next_until = T-15  <- inclusive, although the page is short

Page 4 (until=T-15, limit=3): -> [below2@T-15(dup)]
  oldest=T-15, cursor_stagnant(prev=T-15->T-15) -> next_until = T-16

Pages 5+: 0 events -> empty streak -> complete.
All 7 events captured. Holding a short page inclusively costs the one re-read page 4.
```

### Short pages

Until 2026-10-12 only a **full** page held the cursor inclusively; a short page (`received < limit`) stepped to `oldest - 1`, on the reading that a relay which served fewer than `limit` events had nothing left at its oldest second. That reading is wrong whenever a page is short for any other reason, and every such page skipped the events of its oldest second that it did not serve: #32 (Backfill silently drops boundary-tie events when a relay serves fewer events per REQ than backfill_default_limit). Four causes have been seen live:

1. **A lower clamp.** relay.conduit.market serves 100 events when asked for 500 and advertises no limit: every page is short, and 70 of 7,353 events were lost at its 2026-08-20 onboarding. relay.nostr.net also clamps at 100 (#45 (Onboard relay.nostr.net once #32 is fixed)).
2. **Pages cut short at random.** ngit-grasp relays answer some 500-event requests with 489-499 events while older history remains (an onboarding of 2026-10-09).
3. **A read budget.** relay.damus.io cuts pages short once a per-IP read budget runs out (31 events of one kind skipped on 2026-10-09, 1 kind-17 (external-content-reactions) on 2026-10-11; #53 (Backfill ignores a pressure CLOSED that arrives after the page's EOSE)).
4. **Events the pool drops.** `received` counts what the pool hands nozzl, and the pool drops an event whose id or signature does not verify, whose `expiration` has passed (#41 (rust-nostr drops expired events before any notification)) or that does not deserialize (#31 (Pool-level deserialize failures are unmetered event loss)), after it took a slot in the relay's page. index.ngit.dev serves 22 events whose id does not hash.

Reading `limitation.max_limit` from NIP-11 (relay information) would cover only the first cause, and only on a relay that advertises it (#49 (NIP-11 (relay information) is never read)). So since 2026-10-12 the rule no longer asks whether a page was full: any non-empty page whose oldest moved holds inclusively, and the stagnation guard below ends the re-read. The cost is bounded: one extra page per walk end (the re-read of its last second), one more after any page that starts with no previous oldest (a resume, a re-issue, an empty page) when the next page is all at its `until`, and about one re-delivered boundary event per short page. Where pages are smaller than each second's ties (a read budget cutting pages to a few events) every second is read twice for the same loss. The completeness summary's `events` counts the re-delivered boundary events too, so check a re-walk against the archive, not against `events`.

Two paths still step past a second. The stagnation guard does, when a second holds more events than the relay serves per page: more than `limit` on a full page, #18 (Backfill pagination silently drops events when >limit (500) share a single created_at second), and more than the clamp on a relay that clamps below it (relay.nostr.net, clamp 100, has one second holding 283 events, so a backfill there reads 100 of them and steps past the other 183, counted; [relay-acquisition-limits.md](relay-acquisition-limits.md)). `backfill_tie_stagnations_total` counts that step when the stagnated page was as large as the relay's pages get, full or as large as the run's largest page, so the loss is not silent; the re-read of a walk's last second, smaller than that, is not counted. A page cut by a read budget below the run's largest page can still shed uncounted (#53 (Backfill ignores a pressure CLOSED that arrives after the page's EOSE)). The retry of a page abandoned without EOSE (`check_stuck_backfills`) is the other path: it has no stagnation guard and so keeps the exclusive `oldest - 1`.

### Stagnation guard

If `oldest == prev_oldest` on two consecutive pages - meaning the relay served nothing older than the boundary second the inclusive `until` asked for again - the cursor is stuck. On a page as large as the relay's pages get (a **full** page, or one as large as the run's largest page on a relay that clamps or cuts below `limit`) this happens when a single second contains more events than the relay serves per page (it always serves the same first ones, by id, never advancing). In that case the stagnation guard falls back to `oldest - 1`, accepting that the remaining events at that second are unrecoverable without an id-range cursor (which Nostr filters do not support). The same limitation exists under the naive `-1` cursor for *every* full page; the guard only fires in this degenerate edge case. On a **smaller** page it is the normal end of a re-read: the second came back whole, and the same `oldest - 1` step moves on.

### Epoch-floor guard (issue #23)

A backfill `until` must always be **strictly positive**. After the inclusive/stagnation rules compute the next cursor, if it would be `<= 0` the walk has reached the bottom: there is no event older than `created_at = 0`. That is **completion**, not another page — `next_backfill_advance` returns `Floor` and `handle_eose` marks the group `Complete`. The timeout-retry path in `check_stuck_backfills` (an abandoned page whose `oldest - 1` would be `<= 0`) applies the same floor terminal so the guard cannot be bypassed on that route either. As a last-ditch backstop, `backfill_filter` clamps any stray non-positive `until` *up* to 1 (never down to 0), so even a legacy/corrupted checkpoint that resumes at a cursor `<= 0` and slips past both terminals self-heals: it goes out as `until:1`, the relay returns only `created_at <= 1` events (or empty), and the next step floors → `Complete`, rather than saw-toothing.

This guard exists because issuing a REQ with `until <= 0` is catastrophic. The trigger is an event with `created_at = 0` - e.g. a malformed kind-9735 (zap-receipts) zap receipt whose LNURL backend never set `paid_at` (NIP-57 says `created_at` SHOULD be the invoice `paid_at`, so a missing value yields 0). The walk reaches `oldest = 0`, the naive step is `0 - 1 = -1`, and a filter built from that sends `until:0`. Two ways that loops forever, and `empty_streak` never fires because no page is ever empty:

- **NIP-01-correct relay**: `until:0` means `created_at <= 0`, so the relay keeps returning the same `created_at = 0` event(s). `oldest` stays `0` → next cursor `-1` → repeat.
- **Relays that read `0` as "unset"** (observed on **relay.fountain.fm**, 2026-06-15): `until:0` returns their **newest** events, snapping the cursor back to the forward edge — a visible saw-tooth (`until_cursor` bounces `~2yr → 1970 → ~now`).

This is **not fountain-specific**: any relay serving a `created_at = 0` (or otherwise epoch-floored) event in a backfilled kind hits it. Defense-in-depth: `backfill_filter` clamps a stray non-positive `until` **up to 1** (never down to 0) so even a corrupted/hand-edited checkpoint that bypasses the caller terminates at the floor instead of saw-toothing. Do not "simplify" that clamp back to `unwrap_or(0)`.

### Deduplication — not a new or heavyweight mechanism

The pool's event-id LRU has been present since the first version of the acquirer (it predates the inclusive-cursor fix entirely). The boundary re-delivery adds at most `limit` ID lookups per page transition — a few hundred integer comparisons against an in-memory set. It is not disk I/O and does not require any new data structure.

### Cross-project evidence

This algorithm was independently verified against major open-source Nostr projects:

| Project | Stars | Cursor used | Dedup mechanism |
|---|---|---|---|
| **`nbd-wtf/go-nostr`** (fiatjaf's lib — powers `nak` and a large fraction of the Go ecosystem) | ~500 | `until = oldest.CreatedAt` (exact, no −1) | Rolling per-page ID cache (`repeatedCache`) |
| **`mikedilger/gossip`** (Rust desktop client, most data-completeness-focused Nostr client in existence) | ~2 k | `until = oldest.created_at` (exact) | Local SQLite — events already stored are ignored |
| **`jiftechnify/nostr-fetch`** (dedicated pagination library) | ~50 | `until = oldest.created_at` (exact) | Per-relay `Set<id>` |
| **`vitorpamplona/amethyst`** (Android) | ~1 k | `until = oldest - 1` | None (cursor prevents re-delivery) — **has the blind spot** |

No major project uses `oldest - 1` except Amethyst, and Amethyst therefore has the same blind spot in any scenario where multiple events share the page-boundary second. The `go-nostr` rolling cache is equivalent to our pool dedup: it holds only the previous page's event ids — the minimum needed — because only boundary-second events can be re-delivered.

### Invariants pinned by tests

| Test | What it pins |
|---|---|
| `backfill_captures_all_events_at_boundary_timestamp` | Full page ends mid-boundary-second → all events at that second are eventually captured via inclusive cursor |
| `backfill_captures_boundary_ties_when_the_relay_clamps_below_the_limit` | A relay that serves 50 when asked for 100 (every page short): all events at each page's boundary second are captured (the first cause in [Short pages](#short-pages)) |
| `backfill_captures_boundary_ties_when_the_pool_drops_an_event_from_a_page` | A full page with one event whose id does not hash: the pool drops it, nozzl counts 49 of 50, and the boundary second is still captured whole (the fourth cause in [Short pages](#short-pages)) |
| `backfill_empty_pages_to_complete_is_honored` | Exactly 1 real page, 1 re-read of its oldest second and the configured number of empty pages before `Complete` |
| `backfill_stagnation_guard_prevents_infinite_loop` | ≥ limit events at one second → stagnation guard fires after one retry, backfill terminates without looping |
| `backfill_pagination_advances_when_forward_sub_dedupes_events_first` | Pool dedup discards events at the wire level; backfill page tracking uses the pre-dedup wire path so `received` and `oldest` are always accurate |
| `next_backfill_advance` unit tests (`relay_task.rs`) | The cursor rules + the epoch-floor guard: a next cursor `<= 0` returns `Floor` (complete), never a page |
| `backfill_completes_at_created_at_zero_floor` | A relay serving a `created_at = 0` event makes backfill terminate (reach `Complete`) instead of looping; the event is still captured |
