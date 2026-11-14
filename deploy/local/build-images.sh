#!/usr/bin/env bash
# Build the coredrop image from the flake, the same image that is published,
# so the live tests run the static musl binary that ships. Build the
# segfault crash-workload image from the Dockerfile. Build context is the
# repo root; .dockerignore there keeps target/ and .git/ out.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

require docker
require nix

log "building $COREDROP_IMAGE (nix build .#image)"
image_archive="$(nix build "$REPO_ROOT#image" --no-link --print-out-paths)"
loaded="$(docker load -i "$image_archive" | sed -n 's/^Loaded image: //p' | tail -n1)"
[ -n "$loaded" ] || die "docker load printed no image name for $image_archive"
docker tag "$loaded" "$COREDROP_IMAGE"

log "building $SEGFAULT_IMAGE"
DOCKER_BUILDKIT=1 docker build -f "$SCRIPT_DIR/Dockerfile" \
  -t "$SEGFAULT_IMAGE" "$REPO_ROOT"

log "images built: $COREDROP_IMAGE, $SEGFAULT_IMAGE"
