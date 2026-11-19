# nozzl

Rust archival service for regular/replaceable/addressable Nostr events. The canonical design is in [docs/design.md](docs/design.md).

## Building and testing

The toolchain comes from `rust-toolchain.toml`.

```sh
cargo test --workspace
cargo clippy --workspace --all-targets --locked -- -D warnings
```

- **`config/nozzl.toml` runs as shipped:** `cargo run --release -p nozzl -- --config config/nozzl.toml` archives the kinds in `config/kinds.toml`, from the relays in `config/relays.toml`, under `data/`. Those are the kinds nostrology-data reads, and a few it does not read yet, so a first backfill from every relay takes a while and a good deal of disk; trim the lists for a smaller archive. The end of `config/nozzl.toml` says how to pair it with nostrology-data on S3.
- **Format only the files you edit** (`rustfmt --edition 2024 <file>`), never `cargo fmt --all`. The tree is not rustfmt-clean, so a workspace-wide run buries your change in unrelated churn.
- **Never publish a test event to a relay you do not control.** Tests use the in-process `nozzl-test-relay`.
- **Config edits are tested.** `crates/nozzl/tests/shipped_configs.rs` loads every `config/nozzl*.toml` through the real loader and pins the per-kind scoping policy.
- **CI also runs** an image build from `deploy/Dockerfile`.
- **The export tests need the `duckdb` CLI.** Without it they skip silently, unless `NOZZL_REQUIRE_DUCKDB=1` is set, as CI does.

## Writing conventions

These hold everywhere: comments, docs, log strings, commit messages, issues.

- **Name every Nostr kind as `kind-NNNNN (slug)`**, for example `kind-9735 (zap-receipts)`. The slug is the `slug =` field of that kind's entry in `config/kinds.toml`. For a kind we do not capture, say what it is in a few words: `kind-24133 (NIP-46 remote-signing)`. `scripts/check-kind-slugs.py <file>` checks this against the config, and `--self-test` runs its corpus. A line carrying `kind-slug-ok` is exempt, for a fixture or an assertion on runtime output.
- **Name every issue as `#NN (title)`**, with the issue's own title, cut at most to its leading clause, on every mention.
- **Name every HTTP status as `NNN (reason phrase)`**, for example `502 (bad gateway)`. Consumers branch on exactly which one they get, so a bare number invites a wrong guess.
- **Write times in UTC.**
- **Use plain ASCII punctuation in new text**: `-` rather than an em-dash, `->` rather than an arrow, `...` rather than an ellipsis. Non-ASCII punctuation escapes in JSON and breaks `grep`.

## Don't reinvent the wheel - survey the Nostr OSS ecosystem

Before building any non-trivial solution from scratch - pagination, sync, connection/subscription management, signing/encryption, NIP edge cases, rate-limit handling, and the like - assume **someone has already solved it**, usually several times over, leaving a few credible, battle-tested approaches with different trade-offs to choose from. Survey established, popular open-source Nostr projects and read how they actually did it, then adopt/adapt the option whose trade-offs fit rather than rediscovering the failure modes they already hit and worked around.

Cast a **wide net across the whole ecosystem** - relays, libraries, clients, tools, and other popular projects. The names below are only a starting set, **not** a closed list; the Nostr landscape shifts continually, so search out whatever is currently popular, active, and relevant to the specific problem. The best prior art may live in a project not mentioned here, or one that didn't exist last month:

- relays & relay-side infra - strfry, khatru, nostr-rs-relay; Primal's server-side stack, incl. its **caching server** - but note `relay.primal.net` itself is plain strfry (advertised `version 1.0.3` in 2026-07, `no-git-commits` by 2026-10-09) and speaks vanilla NIP-01 (protocol basics): 500 clamp, 20-sub cap, and NIP-45 (event counts) COUNT, which was absent in 2026-07 and answers exactly by 2026-10-09; the proprietary caching API is a *separate* endpoint (`cache2.primal.net`) we don't touch (verified in [docs/relay-acquisition-limits.md](docs/relay-acquisition-limits.md)); ...
- libraries - `rust-nostr` (our `nostr`/`nostr-relay-pool`), `nostr-tools`, NDK, go-nostr, ...
- clients - damus, Amethyst, snort, Coracle, Wisp, Primal, ...
- tools - `nak`, ...

## Relays and Nostr libraries are not black boxes

When reasoning about or troubleshooting relay behavior, and how it interacts with our code and the Nostr libraries, **do not treat the relay as a black box.** Establish the ground truth first:

- **Which relay software and version** it runs (NIP-11 `software`/`version`, deployment knowledge - e.g. nos.lol/damus run strfry), then read *that* **source code** (e.g. `hoytech/strfry`) for the connection caps, per-REQ limits, cursor/pagination, and rate-limiting that actually govern behavior. Don't infer them from observed symptoms alone.
- **The advertised configuration** (NIP-11 `limitation`: `max_subscriptions`, `max_limit`, auth/payment, negentropy), *and* whether the relay actually honors it (several advertise negentropy but reject it at runtime; some apply silent per-REQ caps below what they advertise).
- **The relevant NIP text** for the protocol-level contract (NIP-01 REQ/EOSE, NIP-11 relay info, NIP-45 COUNT, NIP-77 negentropy, NIP-57 zaps, ...). Cite the actual NIP, don't reason from memory.

Apply the **same rigor to the Nostr libraries on both sides**, the relay's and ours (`nostr` / `nostr-relay-pool`; read the vendored Cargo source, not assumed behavior). That rigor is how #8 (Relay permanently wedged at Connecting after probe-timeout reconnect) and [the library's other limitations](docs/nostr-relay-pool-limitations.md) were pinned down. **Since 2026-07-05 we build the rust-nostr crates from our fork** (`whisperhash/nostr`, branch `nozzl-v0.44`, via `[patch.crates-io]`: one additive accessor over upstream's `v0.44`), so the authoritative library source on disk is the fork checkout under `~/.cargo/git/checkouts/nostr-*/`, **not** `~/.cargo/registry/src/`. See [nostr-relay-pool-limitations.md, *The whisperhash fork*](docs/nostr-relay-pool-limitations.md) for the what, why, maintenance and exit.

**Source-first, then experiment.** Lead with version + config + source analysis; use empirical validation to *confirm* the reading, not as the starting point, unless the question is genuinely trivial. [req-semantics.md](docs/req-semantics.md) is a worked example.

**Before walking/enumerating any relay, read [docs/relay-acquisition-limits.md](docs/relay-acquisition-limits.md).** It's the source-verified reference for the relays we acquire from: strfry's silent 500-`limit` clamp and 20-sub cap (freed only on CLOSE), the edge-layer nos.lol reconnect throttle (per source IP, ~15-min cooldown, can return a *clean empty EOSE*, a false clean), and zapstore's `VagueFilters(3)` + token-bucket hard-disconnect. Critically, it fixes **where** to run a completeness/verification walk: the throttle is per-egress-IP, so walk from an IP **no live acquirer uses**, never from a deployment's own box. Confirm your egress first. Always pace gently (sequential; at least 1000 ms interval on a shared IP; back off on `failed to connect`).

## Logs are an observability surface - never log secrets

The structured `tracing` logs are **not private**. They're written to `data/logs/`, shipped (every container's stdout) to a log store, and read by observers, people or agents, whose entire security model is *"can reach the observability data, cannot reach the secret stores"*. **That model holds only if secrets never enter the logs.** Treat a secret in a log line as a security bug, equivalent to committing it to git:

- **Never log credentials/keys/tokens** at any level: AWS keys (they come from the standard AWS credential chain, *never* from `Config`; keep it that way), any relay auth (NIP-42) secret or signing key, any webhook URL, etc.
- **Beware blanket `{:?}` / Debug dumps of config or environment.** The startup `effective config (startup snapshot)` line Debug-prints the whole `Config`; keep `Config` (and everything it transitively prints) free of secret fields, so credentials live *outside* it and the snapshot stays safe to ship. Adding a secret-bearing `Config` field is the easy way to regress this. The same caution applies to `#[instrument]` (it logs fn args unless `skip`'d) and `anyhow` error context that interpolates values.
- Event content from relays is *public relay data*, not our secret, but it's voluminous, which is why `nostr_relay_pool`/`tungstenite` are pinned to `info` (at `debug` they dump every event's full JSON). Don't undo that.

## TLS: what a relay connection trusts is decided by two crates

`webpki-roots` is the root set relay connections compile in, and `rustls-webpki` is the verifier. S3 connections use the same verifier and trust the image's own certificate store. `crates/nozzl-acquirer/Cargo.toml` requires a minimum of each crate, so a `--locked` build refuses a lock below it, and `crates/nozzl/tests/tls_crates.rs` holds every copy in the lock to the same numbers. Never switch certificate verification off: a relay whose certificate does not verify is unreachable.
