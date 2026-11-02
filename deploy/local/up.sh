#!/usr/bin/env bash
# Local bring-up: ensure the lima/k3s VM -> build -> load -> minio -> helm install
# coredrop -> demo crash workload. Leaves coredrop running; smoke.sh and
# lifecycle.sh run the tests. See lib.sh/README.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

cluster_up

"$SCRIPT_DIR/build-images.sh"
load_images

ns_apply() {
  kubectl create namespace "$1" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}
log "ensuring namespaces: $NAMESPACE, $DEMO_NAMESPACE"
ns_apply "$NAMESPACE"
ns_apply "$DEMO_NAMESPACE"

log "deploying minio (the object store coredrop streams cores/snapshots/manifests to)"
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/minio/minio.yaml" >/dev/null

install_coredrop
restart_demo

log "stack ready; run ./smoke.sh and ./lifecycle.sh"
