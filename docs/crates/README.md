# Crate briefs

One brief per crate in the workspace. Each brief is self-contained enough to hand to a specialized agent: it states the crate's responsibility, its dependencies, the public API contract, the acceptance criteria, the testing requirements, and what's out of scope.

The canonical project design is in [../design.md](../design.md). Briefs reference design.md sections by anchor for context but never duplicate decisions.

## Recommended implementation order

Crates are sequenced by their dependency graph; later crates depend on earlier ones via the public API contracts in their briefs.

1. [nozzl-types](./nozzl-types.md) — foundational types, no internal deps. Block on this first.
2. [nozzl-config](./nozzl-config.md) — independent of nozzl-types in v1.
3. [nozzl-checkpoint](./nozzl-checkpoint.md) — depends on nozzl-types.
4. [nozzl-archive](./nozzl-archive.md) — depends on nozzl-types.
5. [nozzl-metrics](./nozzl-metrics.md) — independent. (Now metric definitions only; the HTTP surface it originally served moved to nozzl-http.)
6. [nozzl-test-relay](./nozzl-test-relay.md) — independent test utility.
7. [nozzl-acquirer](./nozzl-acquirer.md) — depends on nozzl-types, nozzl-checkpoint, nozzl-metrics; uses nozzl-test-relay in tests.
8. **nozzl-http** (added 2026-06; no separate brief) — nozzl's HTTP surface (`/metrics`, `/health`, `POST /admin/flush/{kind}`); depends on nozzl-metrics + nozzl-archive. See design.md §5.D10 + §9.
9. [nozzl](./nozzl-bin.md) — binary; wires all of the above.

Once nozzl-types is merged, items 2–8 can be developed in parallel by separate agents against the contracts in design.md §9.

## Universal rules for every crate

- Rust 2024 edition.
- License: AGPL-3.0-or-later, inherited from the workspace (`license.workspace = true`), with `publish.workspace = true`. Code copied from another project keeps its license and gets an entry in `THIRD_PARTY_NOTICES.md`.
- All public types and functions documented with `///` doc comments where the meaning is non-obvious. Otherwise keep comments minimal — names should carry meaning.
- All errors via a crate-specific `thiserror::Error` enum. No `anyhow` in library crates; `anyhow` only in the binary crate.
- Async crates use `tokio` as the runtime; mark async functions `async fn` rather than returning `impl Future` unless there's a reason.
- Logging via `tracing` macros (`tracing::info!`, `tracing::debug!`, etc.). Structured fields, not interpolated strings.
- Tests organized as unit tests in-module and integration tests in `tests/`.
- Lints: `clippy::all` and `clippy::pedantic` should pass with at most a small, justified `#[allow(...)]` list. `rustfmt` defaults.
- No backwards-compatibility shims for code that doesn't yet exist.
- No comments that paraphrase code. Comments only for non-obvious *why* — invariants, workarounds, surprising constraints.

## Definition of done (per crate)

A crate is done when:
1. Its public API exactly matches the contract in this brief and in [../design.md §9](../design.md#9-module-boundaries-workspace-layout).
2. All acceptance criteria in the brief are met and have passing tests.
3. `cargo build`, `cargo test`, `cargo clippy -- -D warnings`, and `cargo fmt --check` all pass.
4. Coverage targets from [../design.md §8](../design.md#8-testing-strategy) are met (or the deficit is justified in a comment).
5. The brief itself has not been silently changed during implementation. If the brief needs to change, that's a separate PR with a written rationale.
