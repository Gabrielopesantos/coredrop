#!/usr/bin/env bash
# Build the coredrop agent image + the segfault crash-workload image via the
# multi-stage Dockerfile. Build context is the repo root; .dockerignore there
# keeps target/ and .git/ out.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

require docker

export DOCKER_BUILDKIT=1

log "building $COREDROP_IMAGE"
docker build -f "$SCRIPT_DIR/Dockerfile" --target runtime \
  -t "$COREDROP_IMAGE" "$REPO_ROOT"

log "building $SEGFAULT_IMAGE"
docker build -f "$SCRIPT_DIR/Dockerfile" --target segfault \
  -t "$SEGFAULT_IMAGE" "$REPO_ROOT"

log "images built: $COREDROP_IMAGE, $SEGFAULT_IMAGE"
