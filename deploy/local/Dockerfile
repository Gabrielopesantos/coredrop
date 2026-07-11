# syntax=docker/dockerfile:1
#
# Multi-stage build for the live smoke test. Build context is the REPO
# ROOT (so Cargo.toml / src are in scope), not deploy/local:
#
#   docker build -f deploy/local/Dockerfile --target runtime  -t coredrop:dev          .
#   docker build -f deploy/local/Dockerfile --target segfault -t coredrop-segfault:dev .
#
# (deploy/local/build-images.sh wraps both.) Requires BuildKit for the cache
# mounts (default in modern docker; podman build honors the same syntax).

# --- builder: release build of the coredrop binary ----------------------------
FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
# Cache the cargo registry + target dir across builds; copy the release binary
# out within the same layer (the cache mount is not part of the image).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    mkdir -p /out \
    && cargo build --release --bin coredrop \
    && cp target/release/coredrop /out/

# --- crictl: the CRI client coredrop shells out to for enrichment -------------
# NOTE: unused by the capture path - the kernel-exec'd handler runs in the host
# mount ns and calls the NODE's crictl (k3s provides /usr/local/bin/crictl), not
# this image's. Kept only so the runtime image is self-describing. Pinned to a
# v1.31 client (CRI v1). TARGETARCH is populated by BuildKit (amd64 / arm64).
FROM debian:bookworm-slim AS crictl
ARG CRICTL_VERSION=v1.31.1
ARG TARGETARCH=amd64
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSL "https://github.com/kubernetes-sigs/cri-tools/releases/download/${CRICTL_VERSION}/crictl-${CRICTL_VERSION}-linux-${TARGETARCH}.tar.gz" \
       | tar -xz -C /usr/local/bin crictl \
    && rm -rf /var/lib/apt/lists/*

# --- runtime: the coredrop agent image (daemon + kernel-exec'd handler) -------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# coredrop at /bin to match the published nix image layout (one chart, both
# images). crictl is unused by the capture path (see the crictl stage note).
COPY --from=crictl  /usr/local/bin/crictl   /usr/local/bin/crictl
COPY --from=builder /out/coredrop           /bin/coredrop
ENTRYPOINT ["/bin/coredrop"]

# --- workload-builder: compile the null-deref crash binary --------------------
FROM gcc:bookworm AS workload-builder
COPY deploy/local/workloads/segfault.c /segfault.c
# -O0 so the optimizer can't elide the faulting store; -static so the runtime
# image needs no shared libc for the binary itself.
RUN gcc -O0 -static -o /segfault /segfault.c

# --- segfault: the crash workload image ---------------------------------------
# /bin/sh comes from debian-slim; the workload manifest wraps the binary in a
# shell to set `ulimit -c unlimited` and fault it as a non-init child.
FROM debian:bookworm-slim AS segfault
COPY --from=workload-builder /segfault /usr/local/bin/segfault
