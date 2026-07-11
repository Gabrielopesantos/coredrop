#!/usr/bin/env bash
# End-to-end local bring-up: ensure the lima/k3s VM -> build -> load -> minio ->
# helm install coredrop -> demo crash workload -> smoke test. See lib.sh / README.
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

# The capture handler is kernel-exec'd in the HOST network namespace (no cluster
# DNS), so it can't reach `minio` by name - but the node's root netns CAN reach a
# ClusterIP (kube-proxy's DNAT rules live there). Point coredrop's upload endpoint
# at MinIO's numeric ClusterIP. (The smoke test reaches the same MinIO from the
# host via a port-forward.)
minio_cluster_ip="$(kubectl -n "$NAMESPACE" get svc minio -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)"
[ -n "$minio_cluster_ip" ] || die "could not read minio ClusterIP"
log "minio ClusterIP $minio_cluster_ip -> coredrop upload endpoint http://$minio_cluster_ip:9000"

# Record the node's pre-install core_pattern so smoke.sh can assert the daemon's
# shutdown restores exactly this value (the CorePatternGuard drop path).
node_exec cat /proc/sys/kernel/core_pattern > "$SCRIPT_DIR/.tmp/orig_core_pattern"

log "installing coredrop chart (release: $RELEASE)"
helm upgrade --install "$RELEASE" "$REPO_ROOT/charts/coredrop" \
  -n "$NAMESPACE" \
  -f "$SCRIPT_DIR/helm-values/coredrop.local.yaml" \
  --set "capture.objectStore.config.AWS_ENDPOINT=http://$minio_cluster_ip:9000" \
  --wait --timeout 120s

log "applying demo crash workload -> namespace $DEMO_NAMESPACE"
kubectl apply -n "$DEMO_NAMESPACE" -f "$SCRIPT_DIR/workloads/segfault.yaml" >/dev/null
# Restart the workload so re-ups get a fresh container id: the per-container
# rate-limit budget (/run/coredrop/recent.json on the node) survives helm
# uninstall, and a re-used container would start smoke already suppressed.
kubectl -n "$DEMO_NAMESPACE" rollout restart deployment/crash-segfault >/dev/null 2>&1 || true

log "workload applied; it faults on a short loop. Running smoke test (polls the bucket)"
"$SCRIPT_DIR/smoke.sh"
