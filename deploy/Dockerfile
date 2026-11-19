# syntax=docker/dockerfile:1.7
# Multi-stage Rust build with cargo-chef for layer caching.

# Base-image major.minor MUST track rust-toolchain.toml's `channel` (the single
# source of truth). They're kept honest by the CI `image-build` drift guard. The
# COPY of rust-toolchain.toml below makes that file *authoritative* for the
# in-container rustc, so the image compiles with the exact same toolchain as CI
# and local dev — the skew that shipped a 1.95-only type error into a 1.93 image
# (and let it pass CI) is closed by building this Dockerfile in CI.
ARG RUST_VERSION=1.95
ARG DEBIAN_VERSION=bookworm

# Both bases are pinned by OCI INDEX digest below, which does three things: it makes the
# box's pull content-addressed (the daemon's own CA store cannot be talked into serving
# something else), it makes a rebuild reproducible, and - the reason it was added on
# 2026-10-26 - it invalidates the runtime stage's `apt-get install ca-certificates` layer,
# which a plain rebuild would otherwise hit from cache, shipping the pre-DSA-6572-1 CA
# bundle for ever.
#
# WITH A DIGEST, THE TAG ON THE FROM LINE IS DECORATIVE: Docker resolves the digest and
# ignores the tag. So bumping RUST_VERSION or DEBIAN_VERSION without re-resolving the
# digest would keep building on the old base, silently. Each `# base-pin:` line below
# records the tag its digest was resolved for, and the CI `image-build` drift guard
# compares the two - so the ARGs and the digests cannot drift apart unnoticed. To bump a
# base: change the ARG, re-resolve, and update the matching base-pin line in the same
# commit. The recipe is in the CI guard's own comment.

# ---------- Stage 1: chef base ----------
# base-pin: rust:1.95-slim-bookworm resolved 2026-10-26
FROM rust:${RUST_VERSION}-slim-${DEBIAN_VERSION}@sha256:d7482085ff5b415f84dba5647ae71606650bdef00db7aeb69f4b3d170c3e4082 AS chef
WORKDIR /build
# Pin the in-container toolchain to the repo SoT before any cargo runs. Inherited
# by the planner/builder stages (both `FROM chef`), so chef/prepare/cook/build all
# use the channel + components declared in rust-toolchain.toml.
COPY rust-toolchain.toml ./
RUN cargo install cargo-chef --locked --version ^0.1
RUN apt-get update \
 && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ---------- Stage 2: planner — emit recipe.json from manifest set ----------
# Cargo.lock is copied so the recipe (and the eventual build) is pinned to the
# committed lockfile rather than re-resolved at build time — reproducible
# deploys, and `--locked` below turns any lockfile drift into a loud error
# instead of a silent re-resolve. See #15.
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN cargo chef prepare --recipe-path recipe.json

# ---------- Stage 3: builder — compile deps, then app ----------
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
# How many rustc jobs the two compiles below may use. Unset by default, which is cargo's own
# default of one job per logical CPU. It exists for a host that builds this image beside
# other work that must not be starved: with `CARGO_BUILD_JOBS=1` the build is slower and
# gentler. `${VAR:+...}` is POSIX, so an unset OR empty value expands to nothing at all and the
# cargo command line is byte-identical to before.
#
# IT IS DELIBERATELY *NOT* NAMED CARGO_BUILD_JOBS, and this is load-bearing. Docker exports every
# ARG into the environment of the RUN steps that follow it, and CARGO_BUILD_JOBS is cargo's OWN
# environment variable for `build.jobs`. A deployment's compose file forwards the operator's variable as
# `${CARGO_BUILD_JOBS:-}`, which SETS the arg to the empty string when the operator has not set
# it - and cargo then reads an empty build.jobs and aborts the build with
#   error: could not parse ``. Number of parallel jobs should be `default` or a number.
# before it ever looks at the command line. That broke the 2026-10-26 dev deploy. It passed CI
# because CI builds with docker/build-push-action, which left the arg absent rather than empty;
# a deployment builds with `docker compose build`, which sets it. Measured both ways: an EMPTY
# CARGO_BUILD_JOBS fails `cargo build` on its own with no flag present, while the `${VAR:+...}`
# guard was never at fault - it expands to argc=0 for empty and unset alike. So the rule is that
# no ARG here may be named for a variable cargo reads as configuration, and CI enforces it.
#
# Declared HERE and not in the chef stage on purpose: an ARG is per-stage and does NOT cross a
# FROM, not even `FROM chef`, so a declaration up there would have expanded to nothing in these
# two RUNs and the knob would have looked wired while doing nothing. It sits before the cook
# layer, so changing it does bust the dependency cache - correct, since a different job count is
# a different build - unlike GIT_SHA below, which is deliberately declared after it.
ARG NOZZL_BUILD_JOBS
# Dependency compile. /build/target is deliberately NOT a cache mount: cargo-chef's
# whole value is a *committed* dependency layer that survives BuildKit cache-mount
# GC (which is what silently forced full dep recompiles under host disk pressure —
# see #16). registry/git stay as cache mounts: they hold downloaded .crate sources,
# cheap to repopulate and not worth committing into a layer.
# `--bin nozzl` scopes the cook to the deployed binary's graph, skipping deps pulled
# in only by dev/test-only members (e.g. nozzl-test-relay).
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo chef cook --release --locked --bin nozzl --recipe-path recipe.json \
      ${NOZZL_BUILD_JOBS:+--jobs "$NOZZL_BUILD_JOBS"}
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
# ARG/ENV declared here — after the dep-compile layer so that changing the SHA
# (every commit) only busts the app-build layer, not the dependency cache.
# Passed in via docker-compose build.args or `docker build --build-arg GIT_SHA=…`.
# build.rs reads NOZZL_BUILD_GIT_SHA and bakes it into the binary via env!().
ARG GIT_SHA=unknown
ENV NOZZL_BUILD_GIT_SHA=${GIT_SHA}
# No /build/target cache mount here either: the deps compiled into the committed
# cook layer above are inherited directly, and the app crates compile on top. The
# binary is already stripped (`strip = "symbols"` in [profile.release]), so no
# separate `strip` step is needed.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release --locked --bin nozzl ${NOZZL_BUILD_JOBS:+--jobs "$NOZZL_BUILD_JOBS"} \
 && cp /build/target/release/nozzl /build/nozzl

# ---------- Stage 4: runtime — minimal debian slim with shell for ops ----------
# base-pin: debian:bookworm-slim resolved 2026-10-26
FROM debian:${DEBIAN_VERSION}-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8 AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl tini \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --create-home --shell /usr/sbin/nologin --uid 10001 nozzl

# duckdb CLI — the export endpoint (`GET /export/{kind}`) shells out to it.
# Pinned version + per-arch SHA256-verified (a binary pulled into the runtime
# image). Arch is auto-detected, so an arm64 or an amd64 host builds it natively. Installed to /usr/local/bin (world-exec) as root, before
# the USER switch. `dpkg --print-architecture` emits exactly `arm64`/`amd64`,
# matching duckdb's release asset names. Adds ~30–50 MB to the image.
ARG DUCKDB_VERSION=1.5.3
# NOTE: `set -eu`, NOT `set -Eeuo pipefail` — Docker `RUN` executes under
# /bin/sh (dash), which has no `-o pipefail` (`Illegal option`). `-e` already
# aborts on a failed curl/gunzip or a SHA mismatch (sha256sum -c exits nonzero),
# so verification stays loud; the lone pipe (`echo | sha256sum`) can't fail on
# its left side. Do not "restore" pipefail here — it re-breaks the image build.
RUN set -eu; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      arm64) sha=65b3135fb25d9a46cb4752c0638dd688819e64cb1c96bc71ffb8cca04083509f ;; \
      amd64) sha=f05f3b448a9a1bc6e7ac27ff14dfe67bf5761b153c2002723365a456618ef35b ;; \
      *) echo "unsupported arch for duckdb: $arch" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/duckdb.gz \
      "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${arch}.gz"; \
    echo "${sha}  /tmp/duckdb.gz" | sha256sum -c -; \
    gunzip -c /tmp/duckdb.gz > /usr/local/bin/duckdb; \
    rm -f /tmp/duckdb.gz; \
    chmod 0755 /usr/local/bin/duckdb; \
    /usr/local/bin/duckdb --version

COPY --from=builder /build/nozzl /usr/local/bin/nozzl

USER nozzl
WORKDIR /home/nozzl

EXPOSE 9090
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:9090/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/nozzl"]
# CMD intentionally omitted — a deployment's compose file passes --config.
