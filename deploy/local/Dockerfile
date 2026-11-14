# syntax=docker/dockerfile:1
#
# The crash workload image for the live tests. Build context is the REPO
# ROOT:
#
#   docker build -f deploy/local/Dockerfile -t coredrop-segfault:dev .
#
# The coredrop image itself comes from the flake (`nix build .#image`), so
# the tests run the static binary that ships (see build-images.sh).

# --- workload-builder: compile the null-deref crash binary --------------------
FROM gcc:bookworm AS workload-builder
COPY deploy/local/workloads/segfault.c /segfault.c
# -O0 prevents the optimizer from removing the faulting store. -static makes
# sure that the binary needs no shared libc in the runtime image.
# --build-id gives the binary the build-id that the smoke test expects in the
# manifest.
RUN gcc -O0 -static -Wl,--build-id -o /segfault /segfault.c

# --- segfault: the crash workload image ---------------------------------------
# /bin/sh comes from debian-slim; the workload manifest wraps the binary in a
# shell to set `ulimit -c unlimited` and fault it as a non-init child.
FROM debian:bookworm-slim AS segfault
COPY --from=workload-builder /segfault /usr/local/bin/segfault
