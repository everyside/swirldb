# syntax=docker/dockerfile:1.9
#
# The SwirlDB sync server, as a production image. Built from the REPOSITORY
# ROOT, because the server is a member of the workspace here and the
# workspace's Cargo.lock is the truth about what was tested:
#
#     docker build -t swirldb-server:local .
#
# The root .dockerignore is an allow-list of what this build may see.
#
# Two stages: a builder on the Rust image, and a debian-slim runtime carrying
# the binary and nothing else. Every base is pinned by digest, not by tag —
# `rust:1.98-bookworm` and `debian:bookworm-slim` are both republished — with
# the tag kept beside the digest so a person can read what it is.

# rust:1.98-bookworm — the toolchain this tree builds with (rustc 1.98.0).
ARG RUST_IMAGE=rust@sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922
# debian:bookworm-slim — the same Debian as the builder, so the binary's glibc
# is the glibc it was linked against.
ARG RUNTIME_IMAGE=debian@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171


# ── builder ──────────────────────────────────────────────────────────────────
FROM ${RUST_IMAGE} AS builder

WORKDIR /build

# The whole workspace, not only the server: cargo reads every member's
# manifest to resolve the lockfile, so swirldb-client and the integration
# tests come along as manifests and sources even though nothing here builds
# them. swirldb-browser is excluded from the workspace and stays out.
COPY Cargo.toml Cargo.lock ./
COPY native native
COPY tests tests

# Plain `cargo build` with BuildKit cache mounts rather than stubbed manifests
# or cargo-chef: the cache mounts keep the registry and the compiled
# dependencies between builds on one machine, which is the same saving with
# nothing to keep in step. The workspace's release profile already strips
# the binary.
#
# `--locked`: the lockfile is the truth about what was tested.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    set -eu; \
    cargo build --release --locked -p swirldb-server; \
    cp target/release/swirldb-server /build/swirldb-server


# ── runtime ──────────────────────────────────────────────────────────────────
FROM ${RUNTIME_IMAGE} AS runtime

# ca-certificates: roots for an authority reached over TLS. curl: the
# container's own liveness check. tini: a PID 1 that forwards SIGTERM.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl tini; \
    rm -rf /var/lib/apt/lists/*

# A fixed, high, non-root uid, so a volume mounted later is readable by the
# next build of this image too. /data is where the documents live; mount a
# volume there or a restart loses every document's history.
RUN set -eux; \
    groupadd --system --gid 10001 swirldb; \
    useradd --system --uid 10001 --gid swirldb \
        --home-dir /data --shell /usr/sbin/nologin swirldb; \
    install -d -o swirldb -g swirldb -m 0700 /data

COPY --from=builder /build/swirldb-server /usr/local/bin/swirldb-server

USER swirldb
WORKDIR /data
VOLUME ["/data"]

# Defaults that belong to the IMAGE: where it listens and where it keeps its
# documents. The server binds 0.0.0.0 on PORT, which is what a container
# needs — its loopback is reachable by nobody, the health check included.
# AUTHORITY_URL and AUTHORITY_SECRET come from the deployment, because a
# default for either is a way to ship a wrong one; without them every
# connection is whoever it claims to be, and the log says so.
ENV PORT=3030 \
    STORAGE_TYPE=redb \
    STORAGE_PATH=/data/swirldb.redb \
    RUST_LOG=swirldb_server=info

EXPOSE 3030

# Liveness: the process answers. Shell form so PORT is read at check time.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD curl -fsS "http://127.0.0.1:${PORT}/health" || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/swirldb-server"]
