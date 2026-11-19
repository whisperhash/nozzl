# Relay acquisition limits & pacing — read this before walking a relay

**Read this whenever you are about to enumerate/walk a relay** — a completeness
verification, an ad-hoc `nak req` probe, a
backfill design decision, or any "how much does relay X hold for kind K" question.
It is the single source of truth for the relays we acquire from, derived from
**actual relay source code + live NIP-11** (not black-box inference — see the
"Relays are not black boxes" rule in [CLAUDE.md](../CLAUDE.md)). Verified
2026-07-04 (§1–3) / 2026-07-12 (§4); NIP-11 versions drift, so re-read live NIP-11 before hardcoding.
Re-checked 2026-10-09 while walking the NIP-34 (git collaboration) relays: the changes are dated
in place, and the new findings are in [Git-on-Nostr relays](#git-on-nostr-relays-probed-2026-10-09),
among them a silent clamp below nozzl's 500-event backfill page, which defeats our inclusive
cursor. Re-checked 2026-10-14 in a census of 38 relays: damus no
longer offers negentropy, and the new findings are in
[Relays probed 2026-10-14](#relays-probed-2026-10-14). Re-checked 2026-10-19 on the
relays nozzl reads (two not reached), for NIP-77 (negentropy), NIP-45 (event counts) COUNT and
fetching by id: the table's versions and COUNT cells are updated, on small windows, and the findings
and their limits are in
[Set-difference methods](#set-difference-methods-probed-2026-10-19).

**Hand-rolling an ad-hoc probe? Read [§4](#4-ad-hoc-probing-one-persistent-connection-never-a-looped-cli) FIRST** — looping `nak req` per page is a reconnect storm that trips the edge throttle and returns a *false clean* (empty EOSE that looks like an exhausted relay). Use one persistent connection.

Related: [req-semantics.md](req-semantics.md) (our REQ/EOSE/CLOSE contract and
backfill cursor strategy), and [nostr-relay-pool-limitations.md](nostr-relay-pool-limitations.md),
which covers #8 (Relay permanently wedged at Connecting after probe-timeout reconnect).

---

## 1. Per-relay source-verified limits

All of nos.lol / relay.damus.io / relay.primal.net run **strfry** (`hoytech/strfry`).
`relay.zapstore.dev` runs a Go relay: NIP-11 advertises `software:
github.com/zapstore/server`, but the specificity/rate code cited below lives in
the **`zapstore/relay`** repo (`pkg/relay/relay.go`, `pkg/rate/*` — verified),
built on the `pippellia-btc/rely` framework.

| Relay | Software (live 2026-07-04) | `max_limit` | `max_subscriptions` | negentropy (77) / COUNT (45) |
|---|---|---|---|---|
| nos.lol | strfry 1.1.0 (1.1.3 on 2026-10-09) | 500 | **20** | advertised but **negentropy disabled at runtime** (NIP-77 (negentropy) no longer listed on 2026-10-09); COUNT ok, exact on 2026-10-09; not reached from the VPN exit on 2026-10-19 (handshake timeout), so its `ids` handling is untested |
| relay.damus.io | strfry 1.1.0-1 (1.1.0-158 on 2026-10-09) | 500 | 200 (was 300 in Feb) | **no negentropy** by 2026-10-14 (NIP-77 (negentropy) gone from its NIP-11 (relay information), and a NEG-OPEN gets `NOTICE ERROR: bad msg: negentropy disabled`; it was honored when this table was first written); COUNT ok, and it honours `ids` up to 2,047 per filter (2026-10-19) - but **REQ pages can come back partial** (2026-10-09, see [Git-on-Nostr relays](#git-on-nostr-relays-probed-2026-10-09)), and a **per-IP read budget of about 1 MB a minute** (CLOSED `read bandwidth budget exhausted`, seen on dev since 2026-09-13; see [damus's read budget](#damuss-per-ip-read-budget-seen-since-2026-09-13)) |
| relay.primal.net | strfry **1.0.3** (version string `no-git-commits` on 2026-10-09, 1.1.3 on 2026-10-19) | 500 | **20** | negentropy disabled at runtime (NIP-77 (negentropy) no longer listed on 2026-10-09); **no COUNT in 1.0.3; COUNT answered and EXACT on 2026-10-09** (NIP-45 (event counts) listed), and it honours `ids` (2026-10-19) |
| relay.zapstore.dev | `zapstore/server` (Go) | ~1000 (observed live; `.env.example` default is 200) | n/a | no negentropy session and no COUNT (CLOSED `NIP-45 COUNT is not supported`, 2026-10-19); enforces `VagueFilters(3)` |
| relay.plebeian.market | khatru on `fiatjaf.com/nostr` (Go), custom `market-relay` | 500 (silent clamp) | none | no negentropy; **COUNT EXACT for totals, but it ignores `ids`** (one id alone counted 5,068,604, 2026-10-19) |
| nostr.mom | strfry 1.1.0 (1.1.3 on 2026-10-10) | 500 | 50 | COUNT ok, exact on 2026-10-10; not reached from the VPN exit on 2026-10-19 (handshake timeout) |
| relay.conduit.market | `congee` v2026.09.29.1 on 2026-10-19 (github.com/michmich112/congee); khatru on `fiatjaf.com/nostr` (Go) when first probed | **100, silent, NOT advertised**, measured on khatru on 2026-08-20 and not re-measured on congee | 20 on 2026-10-19 (none advertised on khatru) | NIP-77 (negentropy) answered on a small window; **no COUNT** on 2026-10-19 (`NOTICE invalid message`; it was exact on khatru) |
| relay.ngit.dev | `ngit-grasp` 3.0.7 (2026-10-09), 3.0.8 (2026-10-19) | 500 (also `default_limit`) | 500 | NIP-77 (negentropy) answered; **COUNT matched a small window and honours `ids`** on 2026-10-19, though NIP-45 (event counts) is not listed (the earlier "no COUNT" here read NIP-11 (relay information) and the 2026-10-09 walk did not test it; July records already show it answering); stores only git-connected events |
| relay.ditto.pub | `ditto-relay` 2.5.0 on 2026-10-19 (2.1.2 on 2026-10-14, 2.0.0 on 2026-10-10, `ditto-relay-rs` 0.1.0 on 2026-10-09); acquired, scoped, since 2026-10-09 | 1000 (`default_limit` 100) | 20 | COUNT ok and **EXACT**, and it honours `ids`; NIP-77 (negentropy) answered (both on small windows, 2026-10-19); serves 500-event pages (2026-10-09 on 0.1.0, 2026-10-11 on 2.0.0) |
| relay.fountain.fm | not stated in NIP-11 (relay information); acquired for kind-9735 (zap-receipts) and one more kind since 2026-06-14, and another since 2026-10-14 | 500, also the cap on a REQ with no `limit` | not advertised | **no COUNT** and no negentropy reply; **EOSE with nothing for all 7 `ids` filter shapes tried** (one addressable kind, 2 known ids, 2026-10-19), so treat fetching by id as unavailable; a REQ of kinds it does not store is CLOSED `kinds not supported`, but one that names such a kind alongside a stored one is served; stores events backdated past our backfill (#56 (Backdated events stored after the forward since has passed them are never fetched)) |
| a relay we read (sw2) | `bitvora/sw2` 0.1.0, a write-whitelisted event relay; acquired since 2026-10-14 | no `limitation` block; served all 287 events of a kind in one page, and all 307 it then held to a REQ with no `limit` | not advertised | COUNT exact for totals (287 = the walk) **but it ignores `ids`** (one id alone counted 319, 2026-10-19); no negentropy; sends an AUTH challenge on connect but serves reads without it |
| a relay we read (paid writes) | its own software, 0.3.3 (paid writes, free reads, `auth_required` false); acquired since 2026-10-15 | 1000 (serves 500-event pages; a REQ with no `limit` gets 1000) | 50 | **no COUNT**, no negentropy; completeness is a walk; **refuses a WebSocket upgrade without a `User-Agent`** (403 (forbidden); 101 (switching protocols) with one), which nozzl sends since 2026-10-15 (`docs/nostr-relay-pool-limitations.md` entry #10) |
| a relay we read (strfry 1.0.4) | strfry 1.0.4; acquired since 2026-10-14 | 500, also the cap on a REQ with no `limit` | 40 | **no COUNT** (`unknown cmd`, 2026-10-19); NIP-77 (negentropy) answered on a small window |
| relay.dreamith.to | `ditto-relay` 0.1.0; **not acquired, and must not be for an addressable kind** | 1000 | not checked | COUNT counts every stored version, but a REQ returns only the newest version of each coordinate among the newest `limit` rows: see [Relays probed 2026-10-14](#relays-probed-2026-10-14) |
| gitnostr.com, ngit.danconwaydev.com | `ngit-grasp` 3.0.7 (since 2026-10-09), 3.0.8 on 2026-10-19 | 500 (also `default_limit`; some pages short, 489-499) | 500 | NIP-77 (negentropy) answered; **COUNT matched a small window and honours `ids`** on 2026-10-19 (see relay.ngit.dev); stores only git-connected events |
| git.shakespeare.diy | `ngit-grasp` 2.0.0 (since 2026-10-09) | 500 in practice, not advertised (no `limitation` block) | not advertised | NIP-77 (negentropy) answered; **COUNT matched a small window and honours `ids`** on 2026-10-19 (see relay.ngit.dev); some pages short |
| grasp.budabit.club | `ngit-grasp` 3.0.1 (since 2026-10-09) | 500 (also `default_limit`) | 500 | NIP-77 (negentropy) answered; **COUNT matched a small window and honours `ids`** on 2026-10-19 (see relay.ngit.dev) |
| relay.snort.social | memlay 0.2.0, in memory, keeps hours (acquired for kind-17 (external-content-reactions) only, from its 2026-10-10 onboarding) | 5000; takes an index's newest `limit` before `until`, so an `until` page can come back short | 300 | **no COUNT** and no negentropy (`Unknown message type`, 2026-10-19); completeness is a walk |
| relay.nostr.net | strfry 1.1.2-148 (acquired since 2026-10-12 14:09 UTC for kind-17 (external-content-reactions) and other kinds, and since 18:55 UTC for kind-30617 (repo-announcements), whose catalog of other people's projects /git excludes) | **100, silent** | 100 | COUNT **EXACT** (106 = negentropy's 106) and honours `ids`; NIP-77 (negentropy) answered (both on a small window, 2026-10-19) |
| a relay we read (htree daemon) | an htree daemon's built-in relay (hashtree-cli), **no NIP-11** (acquired for one kind, since 2026-10-09 21:00:29 UTC) | **200 per REQ, silent, across all filters**; an unordered in-memory buffer is served first | 64 per connection, 120 REQs a minute | COUNT answered but **capped at 200 per filter**, so no completeness check; walk per author batch; it honours `ids`, no negentropy (both 2026-10-19) |
| index.ngit.dev | `ngit-indexer` 1.0.2 on 2026-10-19 (acquired since 2026-10-12) | not advertised | not advertised | NIP-77 (negentropy) answered on a small window (2026-10-19); COUNT **EXACT on kind-30617 (repo-announcements)** (7,652 = the walk, 2026-10-12) and honours `ids` (2026-10-19), but **read high on kind-5 (event-deletion-requests)**: 12,963 against about 12,255 walked (2026-10-12) |
| a relay we read (strfry master) | strfry, a `master` build, on 2026-10-19 | 500 | 200 | NIP-77 (negentropy) answered; COUNT matched a small window and honours `ids` (all 2026-10-19); 128 KB messages |
| sendit.nosflare.com | `nosflare` 4.20.69 | n/a | n/a | **write-only blastr; rejects every REQ** |

> **`relay.primal.net` is a plain strfry relay (1.0.3 in 2026-07; version string
> `no-git-commits` by 2026-10-09, 1.1.3 by 2026-10-19) - NOT the Primal caching server.** The proprietary caching API (cache verbs, not NIP-01 (protocol basics) kinds/authors)
> lives on a *separate* endpoint (`cache2.primal.net`) we do not touch. Against
> `relay.primal.net` you get vanilla NIP-01 with the 500 clamp + 20-sub cap.

### strfry behaviors that bite (source: `src/filters.h`, `src/apps/relay/*`)

- **`max_limit` = 500 is a SILENT clamp**, not a rejection: `if (limit > maxFilterLimit) limit = maxFilterLimit;`. Ask for `limit: 5000`, get 500 back with **no NOTICE**. A pager that trusts its requested page size silently under-reads. **Always drive pagination by the `until` cursor; never trust a requested limit > 500.**
- **`max_subscriptions` is count-only, freed immediately on CLOSE.** Overflow → soft `NOTICE: too many concurrent REQs`, connection stays up. A sub keeps counting **after EOSE** (handed to a live-event monitor) until you CLOSE it. This is the issue-#12 self-throttle: a paginated backfill that opens the next page without CLOSE-ing the EOSE'd one piles up against the 20-cap (nos.lol/primal) and throttles itself (~15 ev/s leaked vs ~290 ev/s disciplined). **CLOSE every page's sub on EOSE before opening the next.** Our Rust acquirer already does this (`crates/nozzl-acquirer/src/relay_task.rs` `handle_eose` → `pool.unsubscribe`; see [req-semantics.md](req-semantics.md)).
- **strfry CORE has NO per-IP connection cap and NO reconnect throttle** (read `RelayWebsocket.cpp` + `strfry.conf`: the only knobs are `maxFilterLimit`/`maxSubsPerConnection`/`queryTimesliceBudget`; the only connection-level kill is a slow-client cutoff at ~32 MiB pending outbound). So the nos.lol reconnect throttle below is **imposed at nos.lol's edge/proxy layer, not by strfry config** — it is not tunable or predictable from strfry, only avoidable by pacing + egress-IP choice. **This is a general pattern, not a nos.lol quirk:** any strfry behind a reverse proxy inherits the proxy's per-IP handshake limit. Confirmed 2026-07-12 via `curl -I` `Server:` headers — **nginx** fronts nos.lol; **Caddy** fronts the nostr.watch monitor relays (relay.nostr.watch, relaypag.es); monitorlizard.nostr1.com throttles similarly. All return the same **clean-empty-EOSE false clean** when a per-handshake reconnect storm trips them. See §4 for the ad-hoc probing technique that sidesteps this.
- **negentropy/COUNT: advertised != honored.** nos.lol and primal advertised NIP-77 (negentropy) but **reject it at runtime** (`NOTICE: negentropy disabled` - nos.lol's DB is ~242 GB and memory-pressured); by 2026-10-09 neither lists NIP-77 (negentropy) in its NIP-11 (relay information) document any more. primal 1.0.3 had **no COUNT**; by 2026-10-09 it lists NIP-45 (event counts) and its COUNT equalled a full enumeration on every NIP-34 (git collaboration) kind walked. Do not rely on negentropy for completeness on a relay where it has not been tested - fall back to `until`-cursor id-set walks - and treat COUNT as a cross-check only where it has been shown exact on that relay and that kind. [Set-difference methods](#set-difference-methods-probed-2026-10-19) lists where each answered on 2026-10-19, and how little that shows.
- **Frame size:** nos.lol `max_message_length` is 128 KB (vs 1 MB on damus/primal) — a fat multi-filter REQ can exceed it on nos.lol only.

### The nos.lol edge throttle (empirical, ~15-min cooldown)

nos.lol's fronting layer rate-limits **per source IP by connection/request rate**:
it starts refusing fresh connects after roughly **~35 rapid reconnects** (or
~545 total connects in a tight walk), with an ~**15-minute cooldown**. The
dangerous part: once throttled it can return a **clean empty EOSE** — a walk that
stops there looks *exhausted* when it was actually *throttled* (a false clean).
A walk has to defend against this: one that did not end on several
empty chunks in a row (section 3) is `INCONCLUSIVE`, never a
clean `0`. **Back off; don't retry harder** — a query that returned events on the
first try starts returning 0 after several rapid attempts.

### relay.plebeian.market (source: `PlebeianApp/market` `deploy-simple/relay`, verified 2026-08-20)

Onboarded 2026-08-20 for the NIP-99 marketplace kinds. **Not strfry** - a custom
Go `market-relay` binary built on **fiatjaf.com/nostr's khatru** (pinned
`v0.0.0-20260314085316-2cec1c943486`) over a **BoltDB** event store with a Bleve
index for NIP-50.

- **Identify it from `PlebeianApp/market`, not the NIP-11 `software` URL** - that
  URL says `PlebeianTech/market` and **404s** (the org was renamed). The
  advertised `version` `66899c512863` IS a real commit in the current repo,
  dated 2026-03-20.
- **Do not trust its advertised NIP list.** Live NIP-11 advertises
  `[1, 9, 11, 40, 42, 45, 70, 86]`; the deployed commit's own `production.env`
  sets `RELAY_SUPPORTED_NIPS="1,11,50"`. It is operator-set env, not code, and
  the two disagree in both directions.
- **`max_limit` 500 is a silent clamp**, same discipline as strfry:
  `RELAY_MAX_QUERY_LIMIT=500` -> `UseEventstore(store, 500)` -> BoltDB
  `QueryEvents` takes `min(filter limit, 500)`. Confirmed live (`limit=501`
  returned 500). Drive pagination by the `until` cursor.
- **`policies.NoComplexFilters` keys on TAG count, not kind count** - it rejects
  only when `items > 4 && len(filter.Tags) > 2`. Our kind-only filters carry
  zero tags, so a wide multi-kind REQ is never rejected here. (Worth stating
  because the zapstore `VagueFilters(3)` trap trains you to expect the
  opposite.)
- **No relay-level subscription cap and no relay-level rate limiting** -
  `main.go` wires none of khatru's `policies.*RateLimiter`. The only throttle is
  the **Caddy** edge in front of it (`via: 1.1 Caddy`; the relay itself binds
  `127.0.0.1:3334`), so the §4 rule applies in full: one persistent connection,
  never a looped fresh-connection CLI, or you get the clean-empty-EOSE false
  clean.
- **NIP-45 COUNT is supported and EXACT** - COUNT equalled a full enumeration
  for 5/5 kinds probed. This is unusually good: it makes a completeness check
  one cheap COUNT per kind instead of a full walk. (Contrast primal: no COUNT at
  all through at least 2026-07-04, exact by 2026-10-09; nos.lol: COUNT caps at 10000.)
  **But it ignores `ids`** (2026-10-19): COUNT with 100 random ids answered the window's total of 648,
  and COUNT with one id alone answered 5,068,604, so it can compare totals and never a set.
- **`until` is inclusive** (`event.CreatedAt > ef.Until` -> reject) and
  **`until == 0` means "no upper bound"** - the exact epoch-floor hazard
  `backfill_filter` already guards against.
- **The relay legitimately SHRINKS.** `UseEventstore` auto-starts khatru's
  expiration manager, so NIP-40 expiring events are really deleted, and NIP-09
  deletes are honored. "Archive has more than the relay" is the expected steady
  state; a gap analysis must only flag archive-*missing*.
- **A multi-kind union REQ is SAFE here** - unlike the strfry under-collection
  hazard, where a dense kind's `until` cursor can skip a sparse kind's newer
  events. BoltDB's `query()` runs one iterator per kind and does a proper
  globally-ordered k-way merge: each round it quickselects the top-k iterators,
  takes a `threshold` from them, emits only events at or above that threshold,
  sorts each round with `CompareEventReverse`, and stops at `limit`. So a page
  is the globally-newest `limit` events across the union, and no sparse kind is
  truncated by a dense one. This is why the 4-kind onboarding backfill needed no
  single-kind sequencing.
- **Its relay PROCESS dies and systemd restarts it, about every 5.7 min** - so the
  connection resets are its crash cycle, not a timeout and not our churn
  (`Connection reset without closing handshake`): 242-248 a day measured
  2026-10-18 to 10-24, lifetime median 344 s with a 294 s floor, plus a 7.3 s
  reconnect. It is NOT probe-driven (alive-probe counts are identical across all
  relays). **Corrected 2026-10-25: this entry previously called it "almost
  certainly a Caddy idle/lifetime timeout", which is disproven** - three
  concurrent sockets whose ages differed by 25 s all died in the same instant,
  and in that instant the relay's NIP-11 answered 502 (bad gateway) for about
  5.5 s, i.e. Caddy had no upstream to proxy to. The relay sets no
  `ReadTimeout`/`WriteTimeout`/`IdleTimeout`, its unit has no `RuntimeMaxSec`,
  and khatru's only connection killer is a 60 s read deadline our pongs keep
  refreshing. Still harmless to a nozzl forward sub, which re-requests from
  `min(since)` on reconnect - and because that `since` is days in the past and
  the relay's volume is far under its own 500 clamp, everything it STORED during
  the gap is re-delivered within seconds, so the coverage loss is near zero (only
  what a publisher tried to send while the process did not exist is gone, and the
  relay never stored that either). **The cycle is DEGRADING, so re-measure it
  rather than quoting a number from here:** about 338 s averaged over the week to
  2026-10-25 but 224 s in the last hour of it, i.e. 255/day rising to 386/day.
  **Then it stopped coming back: since 2026-10-28 the relay is effectively
  dead**, not crash-looping - Connected for 3.1% of 2026-10-28 and 2.6% of
  10-29 against 97.7-98.0% of 10-24 to 10-26, with 0 events in 24h and connect
  attempts failing `timeout` and 502 (bad gateway) rather than resetting an
  established socket. So the degrading cycle above ran to its end. Expect
  EITHER shape and measure which you have, per exact UTC day: a dead relay
  shows ~0 `relay_disconnects_total` a day (3 on 10-28, 4 on 10-29, against
  254-314 while it was crash-looping), which reads like calm. Its
  `relay_connection_state` gauge keeps reporting it, so an alert on that gauge
  would fire for as long as the relay stays configured.
  Detail, measurements and reproduction:
  #61 (relay.plebeian.market's relay process dies and restarts every few minutes,
  dropping every client).
- **No `created_at` upper bound** (`created_at_upper_limit: 0`) - future-dated
  events are accepted (one kind-30078 (NIP-78 app data) event observed 6 days ahead).

Probed over a single persistent connection, per §4.

### Marketplace-ecosystem relays (probed 2026-08-20)

Characterized while deciding whether to widen the GammaMarkets marketplace kinds
beyond one relay.

- **`nostr.mom`** - strfry 1.1.0 (1.1.3 by 2026-10-10), `max_limit` 500, `max_subscriptions` 50, NIP-45
  COUNT supported. Operationally identical to `nos.lol`; the strfry silent-clamp
  discipline in §2 applies unchanged. COUNT matched full enumeration for all
  three marketplace kinds walked.

- **`relay.conduit.market`** - **This entry describes its khatru build, probed 2026-08-20. By
  2026-10-19 it runs `congee`: its NIP-11 (relay information) has a `limitation` block
  (`default_limit` 500, 20 subscriptions), and it answers COUNT with `NOTICE invalid message`. Re-measure
  before relying on anything below.** khatru, same framework as the Plebeian relay. Its NIP-11
  has **no `limitation` block at all**, and the naive reading of that ("no cap") is
  wrong: it serves a **silent hard clamp of 100 events per REQ**. Verified live
  2026-08-20 by asking for 10 / 100 / 500 / 1000 and receiving 10 / 100 / 100 / 100,
  every one terminated by a clean EOSE. `until` is otherwise plain NIP-01 - inclusive
  (page N+1's newest equals page N's oldest) and newest-first, cursor advancing
  normally. **NIP-45 COUNT is EXACT** (7,310 = 7,310 on a full walk).
  Because nozzl backfills with 500-event pages, every page from this relay counts as
  partial, so until 2026-10-12 the cursor stepped to `oldest - 1` and could skip
  same-second ties at page boundaries (not measured here; the mechanism is under
  [Git-on-Nostr relays](#git-on-nostr-relays-probed-2026-10-09)). Since then a short page
  holds the cursor inclusively too ([req-semantics.md](req-semantics.md#short-pages)).

  **This relay is the worked example for why an unadvertised clamp is dangerous.** A
  first walk assumed 500/page from the strfry habit and capped at 40 pages; it returned
  3,909 against a COUNT of 7,301 and was written up here as "COUNT is not exact". That
  conclusion was wrong - the *walk* was truncated at 40x100, and the ~3,900 shortfall
  was exactly the missing pages. Re-walking at 100/page over 77 pages returned 7,310,
  matching COUNT precisely. **Two lessons: derive page size by measurement when NIP-11
  omits `limitation.max_limit` (absence is not "unlimited"), and treat a
  COUNT-vs-enumeration mismatch as a bug in your own walk until you have ruled that out
  -- it is far more often the walk than the relay.**

  On khatru it self-described as `"khatru conduit l2 scope2 demo"` with `conduit_l2` /
  `scope2-mvp` tags advertising custom sorts (`sort:price_asc`, `sort:updated_at_desc`)
  and a proprietary cursor (`cursor:conduit-l2-v1`). **None of that is documented in
  `Conduit-BTC/conduit-mono`** - the public monorepo carries only the relay *spec*
  (`docs/specs/relay.md`), not this deployment. We do not use those extensions; plain
  NIP-01 `until` pagination worked on that build, which is all we need.

- **NIP-40 expiry is a one-way door for backfill.** `relay.conduit.market` served 45
  `kind-30402 (markets-product-listings)` events that `rust-nostr` rejected with
  `error="event expired"` -- distinct from the numeric-tag deserialize failures of
  #31 (Pool-level deserialize failures are unmetered event loss). All 45 were exclusive to
  that relay; the others had honored NIP-40 and deleted them. The lesson generalizes past
  this relay: **an already-expired event can never be backfilled**, because the library
  refuses it regardless of source. We can only ever hold NIP-40 events captured *before*
  expiry, so "the archive outlives the relay's copy" (see the `relay.plebeian.market`
  section) holds only for what we saw in time. Onboarding a NIP-40-heavy kind late leaves
  a silent, unrecoverable hole, and no completeness check against a live relay will show
  it -- the relay does not have those events either.

- **`sendit.nosflare.com`** - a **blastr**, not a readable relay. It answers every
  REQ with `NOTICE "Denied! This relay does not accept REQs."` plus
  `CLOSED: restricted: this relay does not accept REQs`. It matters only because
  Shopstr and Plebeian Market both route publishes through it, which is *why*
  marketplace events reach relays no client lists explicitly. Never a source.

- **`relay.nostr.net` and `relay.nostr.band` were UNREACHABLE from a workstation's VPN
  exit**: WebSocket opening-handshake timeout on the initial
  walk and again on a spaced retry, and neither served NIP-11 over HTTPS. **This
  is not the throttle signature** documented in §1 - a throttled handshake yields
  a *clean empty EOSE*, not a timeout - so it reads as an ASN block on the VPN
  exit or an outage, not as us being rate-limited. Treat these two as
  **unmeasured, not empty**: both appear in Conduit's and Plebeian Market's
  publish lists, so any union computed without them is a lower bound. Re-probing
  from a different egress would settle it.

  **Settled by 2026-10-09:** relay.nostr.net answers from other VPN exits (on
  2026-10-02 and 2026-10-09) - strfry 1.1.2-148, a silent
  `max_limit` of 100, COUNT exact. relay.nostr.band is defunct (offline since before
  2026-04), not blocked; it timed out again on 2026-10-09.

### Git-on-Nostr relays (probed 2026-10-09)

Walked for NIP-34 (git collaboration) kinds over 166 candidate relays (114 answered), from a
workstation with the VPN on.

- **A silent clamp below nozzl's backfill page defeated the inclusive cursor until
  2026-10-12.** `handle_eose` in [relay_task.rs](../crates/nozzl-acquirer/src/relay_task.rs)
  held the cursor inclusively only on a page with `received >= limit`, and `limit` is the
  global `backfill_default_limit` (500 by default). On a relay that silently
  serves fewer, every page was "partial", so the cursor always took the `oldest - 1` step and
  dropped the rest of any second the clamp cut. Replaying that rule over a walk of
  relay.nostr.net's NIP-34 (git collaboration) events (clamp 100) skips 288 of 16,322 events, 274
  of them from one 297-event second (2026-05-02 21:59:46Z). Since the fix of #32 (Backfill silently drops
  boundary-tie events when a relay serves fewer events per REQ than backfill_default_limit),
  a short page holds the cursor inclusively too
  ([req-semantics.md](req-semantics.md#short-pages)), so a clamp costs pages, not the ties
  at page boundaries. What it still costs is any one second holding more events than the
  clamp: an `until`-only cursor can never read past that second's first 100, the loss of #18
  (Backfill pagination silently drops events when >limit (500) share a single created_at
  second) at the clamp's size, now counted. On relay.nostr.net itself that second holds 283
  events (297 was its count over the union of relays on 2026-10-09), so a backfill there reads
  100 of them and steps past the other 183, counted as one tie stagnation.
  `RelayEntry` has only `url` and `only_kinds`, so the page size cannot be changed per relay.
- **Ditto relays serve 500-event pages.** relay.ditto.pub (`ditto-relay-rs` 0.1.0) and
  relay.dreamith.to (`ditto-relay` 0.1.0, gitlab.com/soapbox-pub): NIP-11 (relay information) `max_limit` 1000,
  `default_limit` 100 (a REQ with no `limit` gets 100), `max_subscriptions` 20,
  `max_filters` 100, `created_at_upper_limit` 900. A 500-event page of one
  NIP-34 (git collaboration) kind came back in 2.7 s and one of another in 0.8 s, each with a
  clean EOSE. The 2026-10-02 walk had seen 500-event pages of the first time out there; that did not
  reproduce, though a long receive timeout and no client keepalive pings cost nothing. COUNT
  matched enumeration (relay.dreamith.to: one event short of 2,382). relay.mostr.pub and
  gleasonator.dev/relay now redirect to relay.ditto.pub. By 2026-10-10 relay.ditto.pub's
  NIP-11 said `ditto-relay` 2.0.0, with the same limits. On 2026-10-11 it still served a
  500-event kind-17 (external-content-reactions) page in full (0.9 s) and `limit` 1000 in
  full, and a REQ with no `limit` still got 100.
- **relay.damus.io returns partial pages.** On one connection a single-kind filter with `limit`
  10 returned 9 events, then 1, and the same filter with `limit` 50 returned 0, each ending in a
  clean EOSE, while COUNT said 104 for that kind and 560 for another; a full walk got 0 and 254. A short or empty damus page is not exhaustion. nozzl's
  empty-page streak (`backfill_empty_pages_to_complete` = 8) is the defence; whether it is
  enough for the kinds nozzl backfills from damus has not been examined.
- **relay.primal.net answers COUNT now**, exactly (equal to a full enumeration on every
  kind walked; NIP-45 (event counts) listed), but keeps NIP-34 (git collaboration) events for a month at most.
  nos.lol (strfry 1.1.3) keeps git kinds only since its ~2026-09-30 purge (one kind back to
  2025-12); COUNT exact.
- **GRASP relays (`ngit-grasp`)**: relay.ngit.dev, gitnostr.com and ngit.danconwaydev.com
  (3.0.7) and grasp.budabit.club (3.0.1) advertise `max_limit` 500 and `default_limit` 500,
  no COUNT, NIP-77 (negentropy) listed. Older builds (git.orangesync.tech 1.0.2, git.shakespeare.diy
  2.0.0) advertise no `limitation`; 500-event pages worked. A GRASP relay stores only events
  connected to the repositories it hosts. **That "no COUNT" read NIP-11 (relay information) only:**
  the walker sent COUNT only where NIP-45 (event counts) was listed. On 2026-10-19 relay.ngit.dev,
  gitnostr.com, ngit.danconwaydev.com, grasp.budabit.club and git.shakespeare.diy all answered COUNT
  exactly and honoured `ids`, the last two on the same 3.0.1 and 2.0.0 builds.
- **Onboarded 2026-10-09: relay.ngit.dev, relay.ditto.pub, grasp.budabit.club, gitnostr.com,
  git.shakespeare.diy and ngit.danconwaydev.com**, for the kinds nozzl reads from them.
  Before configuring them, nozzl's backfill paging was replayed against each one with its real
  combined multi-kind REQ and compared with single-kind walks:
  - **A multi-kind union REQ is safe on `ngit-grasp` (2.0.0, 3.0.1, 3.0.7) and
    `ditto-relay-rs` 0.1.0**: every page was the globally newest events across the union, and
    no sparse kind lost an event to a dense one, unlike strfry.
  - **`ngit-grasp` sometimes answers a 500-event request with 489-499 events while older
    history remains** (gitnostr.com 499 and 498, ngit.danconwaydev.com 499 and 489,
    git.shakespeare.diy 496, 499 and 498; none on relay.ngit.dev or grasp.budabit.club). nozzl
    read any page below `limit` as partial and stepped to `oldest - 1` until 2026-10-12, so the
    rest of that page's oldest second was skipped - the clamp hazard above, firing at random
    rather than on every page (#32 (Backfill silently drops boundary-tie events when a relay
    serves fewer events per REQ than backfill_default_limit)). It cost one event on
    gitnostr.com, which relay.ngit.dev and relay.ditto.pub also serve. Since the fix a short
    page holds the cursor inclusively.
- **strfry v315 on nostr1.com** (thecitadel.nostr1.com, theforest.nostr1.com, nostr21.com):
  `max_limit` 10000.
- **denimroad.feeds.relay.tools** (`newlay` 0.3.54): COUNT reads BELOW its own enumeration
  (493 against 587 for one kind) - not exact.
- **A same-second burst wider than the clamp** must be split by author using that second's
  author set from EVERY relay walked, not just the clamped sample: the sample missed 57 events
  on relay.nostr.net (after an author-split fill, COUNT matched).
  Size a burst with COUNT `since = until = that second` where COUNT is exact.
- **Verify signatures before reporting a union.** A walk that recomputes ids does not check
  signatures, and nozzl's pool drops bad ones; all 38,057 events checked out.

### damus's per-IP read budget (seen since 2026-09-13)

- **relay.damus.io caps what one IP may read: about 1 MB a minute.** Past it, damus answers with
  CLOSED `ERROR: read bandwidth budget exhausted (1048570 bytes/min per IP) (retry in 30ms)`.
  dev's logs have it from 2026-09-13 07:11:16 UTC (none in the 82 days before), 454 to 1,889
  times a day on the full days sampled since, nearly all for the forward subscription. nozzl
  re-issued it at once each time (#40 (Forward-sub CLOSED path re-issues with no backoff), which
  recorded it on 2026-09-29) until the 2026-10-09 fix for #48 (Forward-sub CLOSED handling):
  since then it waits 2 s, doubling to 30 s, and the phrase counts as concurrency pressure.
- **What spends it on dev.** The liveness probe re-issues the damus forward subscription about
  once a minute, because of #39 (Forward-sub teardown is pool-wide). Each re-issue replays from a
  `since` frozen months back, the subject of #28 (Frozen min(since) re-issues replay-flood the
  pipeline every 61 s): on 2026-10-09, every kind nozzl reads, since 2026-06-06, which damus answers with its newest 500 events, 574 KB
  measured once from a workstation. One such replay a minute fits (1 of 935 single-re-issue minutes
  on 2026-10-09 drew the CLOSED); a second read in the same minute does not, and the immediate
  re-issue of #40 (Forward-sub CLOSED path re-issues with no backoff) then turned one CLOSED into
  a burst (69 on 2026-10-09, up to 71 re-issues in a minute). What starts most bursts is not in
  the log. The back-off now caps a burst at a few re-issues a minute.
- **Since the fix (2026-10-09 20:50:29 UTC)** of #39 (Forward-sub teardown is pool-wide) and #48
  (Forward-sub CLOSED handling), damus's forward sub stays open, with no probe and so no
  once-a-minute replay. No budget CLOSED came in the hour after the restart (to 21:53 UTC),
  against 81 in the hour before, 64 of them in bursts at 20:35 and 20:37. Over 20:51-21:21 the
  box log has 0 forward re-issues, 0 probes and 0 CLOSEDs on damus, against 36, 29 and 7 over
  19:50-20:20 and 103, 29 and 74 over 20:20-20:50 on the old binary.
- **A quiet damus may be damus's own trouble (2026-10-15 to 10-17).** From 21:00 UTC on 10-15 it
  often missed the liveness probe's 5 s (925 timeouts on 10-16, none on 10-12 to 10-14), and from
  about 23:00 on 10-16 it also reset dev's connection, a median 39 s after each connect; the
  events dev took first from it fell from 3,738-8,498 per 2 hours to near 0. Checked from a
  workstation on the VPN at 05:34 on 10-17, its newest stored kind-1 (posts) was from 00:00:13 and a 100 s
  listen got none, while nos.lol and relay.primal.net were current: damus served no newer kind-1
  (posts) to a workstation either. Before blaming the budget or nozzl for a quiet damus, compare its
  newest stored event with another relay's, from another IP. And a flapping damus brings the
  replay a minute back: the library re-sends the forward subscription on every connect, from
  the frozen `since` above, which it did on all 1,455 connects of those two days.
- **It shrinks backfill pages from a busy IP.** A reload re-issues the forward subscription too,
  so a new kind's backfill of 2026-10-09 started in the same second as a
  replay (sent with no `since`, the new kind's backfill pending): it got 451 events and then 61
  pages of 3 to 29; 41 of the 62, the first included, were answered with EOSE and then that
  CLOSED. Every short page was read as partial (the clamp hazard below, fixed on
  2026-10-12): 31 events on page-boundary seconds were skipped, the loss in #32 (Backfill
  silently drops boundary-tie events). With #39 (Forward-sub teardown is pool-wide) fixed, no replay spends the
  budget once a minute any more, so a re-backfill's pages after its first minute should be
  larger. The first minute still shares the budget with a replay: a re-backfill takes a checkpoint
  reset and a restart, and on restart `run()` opens the forward subs just before the first
  backfill page. That replay starts from the floor of #28 (Frozen min(since) re-issues
  replay-flood the pipeline every 61 s), or with no `since` at all if the reset removes the kind's
  checkpoint entry, as in stage 1. This is untested. A backfill can also spend the budget on its
  own, and the skip stays until #32 (Backfill silently drops boundary-tie events) is fixed. If the
  budget ever cut eight pages in a row to nothing, that would also end a walk early, since eight
  consecutive empty pages is nozzl's completion rule (`backfill_empty_pages_to_complete`); not
  seen so far.
- **A pre-flight from another IP cannot see it.** The same backfill replayed from a workstation (VPN on)
  got full pages and missed nothing. Expect a damus backfill on dev to page small at least in its
  first minute (above), and a kind whose events come in same-second groups to lose some at the
  boundaries until #32 (Backfill silently drops boundary-tie events) is fixed.
- **The budget CLOSED comes after the page's EOSE, so nozzl never retries the page.** On dev
  every budget CLOSED on a backfill page has arrived after that page's EOSE (median 0.12 ms,
  at most 1.8 ms later), naming the page that just finished. By then the page has left
  `active_backfills` and the next page is already requested, so `handle_closed` drops the
  CLOSED: no back-off and no retry, although since 2026-10-09 the phrase counts as pressure. A
  kind-add's damus pages therefore all come back short:
  - 163 pages, a median of 3 events each, in 13 s for the kind-17 (external-content-reactions)
    reload of 2026-10-11, with 1 event lost;
  - 41 of 62 non-empty pages for a kind added on 2026-10-09, with 31 lost.

  Filed as #53 (Backfill ignores a pressure CLOSED that arrives after the page's EOSE), with the
  log excerpt and the fix directions.

### hashtree's relays (source: mmalmi/hashtree a60f3cd0, probed 2026-10-09)

Researched for a kind nozzl reads, and onboarded the same day.

- **A relay we read (htree daemon)** is the relay built into an htree daemon (`hashtree-cli`
  `nostr_relay.rs`, `server/ws_relay.rs`). Acquired for
  one kind since 2026-10-09 21:00:29 UTC, stage 2 of its onboarding. It
  waited for the fix of #48 (Forward-sub CLOSED handling), since an upstream's CLOSED passed
  through met nozzl's forward-sub CLOSED handling. Its backfill missed 18 of the 230 events it
  held (the cap below); its probe settles in about 2.7 s.
  - **No NIP-11 (relay information)** (HTTP 400), no AUTH, no idle timeout.
  - **A REQ returns at most 200 events across its filters, and the unordered in-memory buffer
    (the last 400 events, by id, lost on restart) is read before the newest-first stores.** So
    an `until` cursor skips what the cap cut: nozzl's backfill collected 199 of the 230 events it
    held. A
    complete walk asks per author batch and splits any batch that returns 200.
  - **Its stores keep the newest version per (pubkey, kind, `d`)**; only the buffer holds older
    ones.
  - **Of what is published to it, it streams only what trusted authors publish directly** (the
    daemon's key, its allowed keys, anyone within three follows of its social-graph root). Events
    it fetches or mirrors are stored without being streamed; anyone else's go to a spambox that
    is never served. A subscription also carries its upstreams' matches (next item).
  - **Every REQ is also forwarded to the daemon's upstream relays**, over a new connection to
    each, and its EOSE waits until every upstream has answered, closed or failed to connect,
    with no timeout of its own. nozzl's liveness probe therefore takes a steady 2.9 s
    (relay.ditto.pub: 0.35 s), against the default `probe_timeout_seconds` of 5. Upstream matches are
    streamed to the subscription and cached, on top of the 200 cap. An upstream's CLOSED is
    passed through to the client. On 2026-10-09 single REQs for the stores got no upstream
    events, but a fresh `since = now - 120` REQ once a minute got a new version of key
    5d17ad66's daemon index root on 29 of 30 tries, versions the other relays nozzl reads
    mostly lack. Those REQs took 4.6 to 6.0 s to EOSE,
    so a slow upstream can push nozzl's probe past the default 5 s timeout.
  - **It applies NIP-01 tag filters:** `"#l":["git"]` returned only `l=git` roots (50 of 50,
    against 22 of 50 without it).
  - **A client CLOSE draws no CLOSED**, so the pool-wide CLOSE of #39 (Forward-sub teardown is
    pool-wide) cannot trip #40 (Forward-sub CLOSED path re-issues with no backoff) by itself. An
    open-ended forward REQ replays 199 stored events.
  - **Limits:** 64 subscriptions and 120 REQs a minute per connection.
- **Considered and left out** (operator, 2026-10-09), the git
  helper's other default relays:
  - **temp.iris.to** (strfry 1.1.3, `max_limit` 500, 20 subscriptions, COUNT): Iris's relay for
    short-lived traffic (deletions, Iris Chat messages, FIPS ops); it keeps hours (13,123 of
    176,620 events older than 6 h).
  - **relay.snort.social** (memlay 0.2.0, `max_limit` 5000, 300 subscriptions, no COUNT): in
    memory, about 8 hours, evicts the oldest `created_at` first, empty after a restart. Its
    advertised `idle_timeout: 10` fires only on a connection that has sent nothing, and it takes
    an index's newest `limit` entries before applying `since`/`until`, so an `until` page can
    come back short (v0l/memlay e28f14c, `src/relay.rs:329-370`). Since the 2026-10-10
    onboarding of kind-17 (external-content-reactions) it is acquired for that kind alone,
    for Iris Git's stars.

### Relays probed 2026-10-14

From a coverage audit, a census of 38 relays from a workstation on the VPN,
and the onboarding that followed.

- **relay.dreamith.to (`ditto-relay` 0.1.0) answers a REQ by taking the newest `limit` rows and
  then keeping only the newest version of each coordinate among them**, while COUNT counts every
  stored row. A window COUNTed at 101 rows was served as 3 events, one per room; one author's 216
  versions as 1. A newest-first `until` walk there advances only to the oldest event served, so on
  a busy addressable kind it re-reads the same rooms and stalls (the census walk of
  kind-30312 (NIP-53 meeting space) stopped at 2026-08-22 after 372 events). Read it in windows
  that COUNT under one page: 41,236 rows in 120 windows,
  1,268 rooms. **nozzl's backfill would crawl the same way, so never acquire an addressable kind
  from it.** relay.ditto.pub (`ditto-relay` 2.1.2) does not do this.
- **relay.damus.io: no negentropy** (the table above), and it keeps about 5 weeks of
  a busy chat kind; **nos.lol and relay.primal.net about 2.** nos.lol also deleted
  part of its history around 2026-09-29: some kinds start that day, kind-10312 (NIP-53 room
  presence) among them, and its chat a day later but for 20 older stragglers, while its
  kind-30312 (NIP-53 meeting space) still reaches back to 2025-05. A relay's oldest event is
  therefore not its retention: read weekly counts before calling one window "reach".
- **relay.fountain.fm** caps a REQ with no `limit` at 500. A single-kind REQ for a kind it does
  not store is CLOSED `kinds not supported` (6 of the census's specs), but a REQ naming such a kind
  alongside a stored one is served. It stores old events after our backfill has passed them: 41
  versions of an addressable kind, most of them the final versions of 2025 events,
  arrived after our 2026-06-15 backfill read it, so 40 coordinates hold a superseded version in the archive (#56
  (Backdated events stored after the forward since has passed them are never fetched)).
- **relay.divine.video (`divine-funnelcake` 0.1.0): COUNT is not a row count.** kind-30313
  (NIP-53 meeting room event) walked 2 events against a COUNT of 1, kind-37777 (zap.stream stream
  card) 9 against 6.
- **relay.shosho.live (`bitvora/wot-relay`) and nostrelites.org limit the message rate**: at 0.7 s
  between REQs they answered "rate-limited: there is a bug in the client, no one should be making
  so many requests" (11 and 5 times); a back-off of 20 s, then 40 s, let both walks finish.
- **relay.hivetalk.org (khatru) ignores `until`** (every page was the same 4 events), so it cannot
  be walked, and it answers a REQ naming many kinds with NOTICE `too many kinds`.
  beeswax.hivetalk.org, which holds Hivetalk's rooms too, walks cleanly.
- **Unreachable or dead on 2026-10-14:** relay.tunestr.io resolves (138.197.40.145) but its
  WebSocket handshake timed out on 6 tries and its NIP-11 (relay information) request timed out;
  relay.current.fyi has no address; relay.nostr.bg is NXDOMAIN.
- **Two relays we read (paid writes; strfry 1.0.4) page nozzl's combined backfill cleanly.** Replayed
  for two kinds together, 217 and 155 pages collected every event the exact single-kind
  walks found (103,669 and 72,612). The first served full 500-event pages under its advertised
  1000.
- **What the three new relays send for nozzl's forward REQ** (no `since`, no `limit`; measured
  2026-10-14 16:10 UTC): the paid-writes relay 1,000 events whatever the kinds, the strfry 1.0.4
  one 500, the sw2 one everything it holds (307); each answered the liveness probe
  `{"kinds":[65535],"limit":0}` with EOSE, and each served its NIP-11 (relay information) to the
  dev box as to a workstation. A replay of more than 500 events new to the pool can overflow nozzl's
  per-relay notification channel while other backfills fill the sink, so the paid-writes relay was
  added alone.
- **The paid-writes relay refuses a WebSocket upgrade that carries no `User-Agent`** with 403
  (forbidden), and serves the same upgrade with any `User-Agent` (101 (switching protocols); measured
  2026-10-14 from a workstation and
  from a deployment's host). The pre-flight's NIP-11 (relay information) GET could not show it, because
  curl sends one. rust-nostr's pool sent none until nozzl's own transport added it (2026-10-15,
  `docs/nostr-relay-pool-limitations.md` entry #10); before that, it never connected.
  A probe that sends no
  User-Agent (`curl -A ''`) sees the refusal; any default client does not.

### Set-difference methods (probed 2026-10-19)

Can a relay say what it holds that the archive lacks? Tested on the relays nozzl reads, all but two of
them reached.

| What answered | Relays |
|---|---|
| NIP-77 (negentropy) and COUNT with `ids` | relay.ngit.dev, gitnostr.com, git.shakespeare.diy, ngit.danconwaydev.com, grasp.budabit.club, index.ngit.dev, relay.ditto.pub, relay.nostr.net, a relay we read (strfry master) |
| NIP-77 (negentropy) only | a relay we read (strfry 1.0.4), relay.conduit.market |
| COUNT with `ids` only | relay.damus.io, relay.primal.net, a relay we read (htree daemon; only under 200 events: its COUNT is capped at 200 per filter) |
| COUNT of totals only, `ids` ignored | a relay we read (sw2), relay.plebeian.market |
| Neither | relay.zapstore.dev, relay.fountain.fm, relay.snort.social, a relay we read (paid writes) |
| Not reached from the VPN exit | nos.lol, nostr.mom |

- **This table shows a relay answers, not that it is safe to rely on.** Each relay was tested on one
  kind in a small window. Negentropy sessions covered 1 to 371 events, each from an empty set in one
  round, so none reconciled a real set; COUNT comparisons covered 1 to 349 events. These are not the
  full enumerations that **EXACT** means elsewhere in this doc.
- **NIP-11 (relay information) is a partial guide.** Of the relays that served one, listing 77 in
  `supported_nips` matched every session, but strfry's separate `negentropy: 1` field did not (3 answered,
  2 refused). All 8 listing 45 answered COUNT, and so did 5 of the 11 that do not. Nothing in it predicts
  `ids`, so test that.
- **Test `ids` with a control that cannot pass by accident.** COUNT with 100 random ids must be 0, COUNT
  with 1 served id must be 1, and COUNT with 1 served id and nothing else must be 1. Padding served ids
  with random ones passes a relay that ignores `ids` whenever the served ids are the whole window, which
  is how the sw2 relay we read first passed.
- **Fetching by id is mostly unproven.** 18 relays returned the ids, but only relay.primal.net,
  relay.ditto.pub, relay.nostr.net and relay.plebeian.market were asked for part of a window; on the other
  14 the same false pass applies, with no control. No filter held more than 50 real ids. relay.fountain.fm
  answered EOSE with nothing, a false empty, so fill it by re-walking windows. relay.snort.social had
  nothing to fetch, and nos.lol and nostr.mom were not reached.
- **COUNT with `ids` says how many, not which.** R = COUNT of the window; H = the sum of COUNT over
  batches of the archive's ids; R - H is the gap, provided R is exact. On relay.damus.io it matched a full
  re-walk, 172 = 172, for one hour of kind-1 (posts).
- **strfry takes at most 2,047 ids in a filter.** `FilterSetBytes` refuses more than 65,535 bytes of items
  (`src/filters.h:42`) with `total filter items too large`, and an id is 32 bytes. relay.damus.io was
  bisected to exactly that.

### zapstore relay (source: `zapstore/relay` `pkg/relay/relay.go`, `pkg/rate/*`)

- **`VagueFilters(3)` — enforced cleanly and consistently; it is the subtle trap behind #25 (Single-kind backfill onboarding permanently abandons against filter-specificity relays).** The relay (NIP-11 `software: github.com/zapstore/server`) rejects (`CLOSED: filters are too vague`) any REQ filter scoring < 3 specificity; the live behavior matches `specificity()` in `zapstore/relay` `pkg/relay/relay.go` **exactly** (re-verified live 2026-07-04, not black-box-guessed). Scoring: `ids`+10, `search`+3, `authors`+2, `tags`+2, `kinds`+1, `since`+1, `until`+1, and the **`limit` term: `limit:0` +5, else `limit<100` +2, else (≥100) +0**.
  - **The trap: an ABSENT `limit` scores +2 (counted as `limit<100`); an explicit `limit:500` scores +0.** So `{kinds}` with no limit = **3 → passes**, but `{kinds, since:0, limit:500}` = **2 → rejected**. Live probe 2026-07-04 fits `specificity()<3` on the nose: `{kinds:[3063]}`→1000 ev, `{kinds:[3063],limit:500}`→rejected, `{kinds:[3063],since:1,limit:500}`→rejected, `{}`/`{limit:3}`→rejected, `{authors}`/`{#t}`/`{#h}`/`{since,until}`→accepted.
  - **This is #25 (Single-kind backfill onboarding permanently abandons against filter-specificity relays).** nozzl's first backfill page for a *freshly-onboarded* kind is `{kinds, since:0, limit:500}` (no `until`) = score 2 -> rejected -> a **singleton** onboarding silently abandons (multi-kind onboarding self-heals via the split-retry that adds `until` -> score 3). An earlier single-kind onboarding's note that the relay "rejects bare `kinds`" was actually this - nozzl's `limit:500` page, not a bare `nak` probe. **Fixed:** `start_backfill_for_kinds` now seeds the fresh-onboarding first page's `until = now` (score 3), so singleton onboarding is accepted; verified live against the relay (see [crates/nozzl-acquirer/src/relay_task.rs](../crates/nozzl-acquirer/src/relay_task.rs) + the `singleton_onboarding_completes_against_filter_specificity_relay` test).
  - **For a `nak --paginate` walk:** it passes **no** `limit`, so its wire filters `{kinds}` / `{kinds,until}` score 3–4 → **it walks zapstore fine**. (If you ever pass `-l`, keep it **< 100** — e.g. `-l 99` — because a `limit ≥ 100` *removes* the +2 and can drop you below the floor. And note `nak` omits `since:0` from the wire entirely — verified v0.18.5 — so `-s 0` adds no specificity; it only sets nak's client-side paginate floor.)
- **Per-IP token bucket** (`pippellia-btc/rate`): 100 initial / 300 max / +100 per minute. A REQ costs **1**, or **5 if it carries >10 filters**. Overflow → **hard `client.Disconnect()`** (`"rate-limited: slow down chief"`) — a dropped socket, not a soft NOTICE. **Blossom asset fetches draw the same bucket.** Budget well under 100 cheap REQ/min; keep ≤10 filters/REQ.
- **`ResponseLimit`**: a silent per-REQ cap → enumerate by `until`-cursor walk. The `.env.example` default is 200, but the deployed relay returned **~1000 events/REQ** when probed live 2026-07-04 (single REQs for `{kinds:[3063]}` and a wide `{since,until}` both capped at exactly 1000). Don't assume a specific number — paginate by `until` regardless.

---

## 2. Egress topology — WHERE you run a walk decides WHO gets throttled

The nos.lol throttle is **per source IP**. A walk throttles whichever live
deployment shares its egress IP — so **the goal is to walk from an IP that no
live acquirer uses.**

- **A deployment's nozzl egresses from that deployment's public IP** (on AWS, the box's dedicated EIP) — and that is the **same IP any walk you run *on* the box would use.**
- **A workstation on a VPN egresses from the VPN's exit**, an IP no deployment uses.

| Where the walk runs | Egress IP | Throttle lands on → who it hurts |
|---|---|---|
| **A workstation, VPN ON** | VPN exit | the disposable VPN exit → **hurts only your walk; disturbs no deployment** ✅ |
| A workstation, **VPN OFF** | its own public IP | a deployment only if it shares that IP → can throttle that deployment's live acquisition ⚠️ |
| **On a deployment's box** | the box's dedicated EIP | shared with **that deployment's own acquisition** → throttles it ❌ |

**So a workstation on a VPN is the safe place to walk — NOT the box.** The box
looks tempting ("dedicated EIP, shared with nothing"), but it is *not* shared
with nothing: **nozzl acquires from relays over that very EIP**, so a walk
there is the same self-interference we're avoiding, relocated to the box. A VPN
exit, by contrast, is used by no acquirer, so a throttle there is
inert to production — the walk may be *starved* on a pre-penalized shared exit
(switching exit region has improved stability), but that costs
only walk wall-time, never a live deployment. You never need to run *on* a box:
the archive side can come from a local Parquet tree, or, for a deployment on S3, from its
WAL on the box and then S3, while the walk runs from the workstation. Either way the walk runs
from a safe egress.

### Practical rule

1. **Prefer running relay walks from a workstation with the VPN ON.** Its exit IP is used by no live acquirer, so a throttle can't touch a deployment. Confirm before you start: `curl -s ipinfo.io/json` should show a **VPN/non-residential ASN**. A VPN exit can also be refused outright: on 2026-10-19 nos.lol and nostr.mom timed out on every handshake from it.
2. **Do NOT run walks on a deployment's box** — its EIP is that deployment's own acquisition IP; a walk there throttles it. (Same for anything sharing a deployment's IP.)
3. **The one risky workstation case: VPN OFF on an IP a deployment shares** → you can throttle its live acquisition. If `ipinfo.io/json` shows that IP, turn the VPN on (or pace `--paginate-interval ≥1000ms` and accept the risk).
4. **Always:** strictly sequential (never concurrent walks against the same relay); treat `failed to connect` / `too many` / `closed:` as a throttle signal → back off (≥15 min for nos.lol), never as end-of-history. Zero-impact alternative regardless of egress: exact-scan a source we control (no relay walk — see §3).

---

## 3. Walk pacing (defaults & knobs)

Walk a relay with **chunked `nak req --paginate`**. One run of it is a chunk: it holds one
persistent connection and re-issues REQs internally with `--paginate-interval` pacing, but
it can stop early (its `received < limit` test ends the walk at a short page while older
history remains), so never trust one chunk. Start the next from the oldest `created_at` the
last one returned (inclusive, deduping by id; one second older when a whole page ties at
that second), and call the relay exhausted only after several empty chunks in a row.
**Never** open a fresh connection per page (that trips the edge throttle: a per-page walk
died at 8.5% of a 3.2M-kind after ~545 connects). Walk relays **strictly sequentially**,
one at a time. Values that have worked:

| Knob | Value | Meaning |
|---|---|---|
| Pacing between REQs | **500 ms** | within a chunk. Raise to 1000 ms or more when walking nos.lol from an IP a deployment shares. |
| Chunk wall cap | 600 s | a timeout yields a usable partial chunk that a resume continues from |
| Backoff on refusal or rate limit | 15 s, doubling to 240 s | resume from the same cursor; never treated as the end |
| Backoff budget per stuck chunk | 1800 s | cumulative; past it the walk is INCONCLUSIVE |
| Empty chunks in a row | 3 | the relay is exhausted |
| Chunk cap | 4000 | runaway guard (nos.lol's 3.2M needs ~70) |

A throttled or truncated walk must end `INCONCLUSIVE`, never as a false clean. The
remaining discipline is *proactive*: run from the right IP (§2) so you neither disturb
the deployment nor get starved.

**An exact scan** does **no relay walk at all**: it diffs the archive against a
`strfry scan` id-set from a source we control (a local strfry serving an LMDB). Immune to
every throttle above; prefer it whenever the truth source is one we own.

---

## 4. Ad-hoc probing: ONE persistent connection, never a looped CLI

The chunked walk of §3 already does this right. This section is for the
**hand-rolled probe** — "how much does relay X hold for kind K", "who publishes
kind K", a quick volume/coverage check — where it's tempting to loop a CLI.

**Do not paginate by looping `nak req ...` in a shell.** `nak` opens a **fresh
WebSocket per invocation**, so a loop is a per-IP **reconnect storm** that trips the
edge proxy's handshake limit (§1). The tell is a **false clean**: the first call
returns events, then rapid subsequent calls return a **clean empty EOSE** (zero
events, no error, no NOTICE) — which reads as "relay exhausted" but is "you're
throttled". A 2026-07-12 NIP-66 session burned hours chasing these phantom 0s, and
mis-diagnosed it as a `limit`-value cap (it is not — strfry *clamps* `limit`, never
returns empty for a big one; every 0 was the proxy).

**Instead, hold ONE persistent connection and paginate REQ/CLOSE over the same
socket**, driven by the `until` cursor. No re-handshake ⇒ the throttle never fires.
Two ways:

- **`nak req --paginate`** reuses one connection internally. Good for a quick dump.
- **A persistent-connection pager** of your own (Python `websockets` is enough), when you
  need to tally as you page rather than dump.

Once on a single connection the earlier limits still apply: strfry **clamps** the
page to `maxFilterLimit` (500 on most; 10000 on monitorlizard), so drive pages by
`until` and don't trust a requested limit > the clamp. This technique moved a NIP-66
pull from "phantom 0 events" to **40,000 events in ~30 s**. Reachability caveat:
some monitor relays (history.nostr.watch, relay.nostr.band) **refuse TLS from
hosting/VPN ASNs** — use a residential (VPN-off) or on-box egress for those, which
is the opposite of the §2 walk rule (there the VPN is the *safe* egress; here it can
be a *blocked* one — check per relay).
