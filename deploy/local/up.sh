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

# A previous run's post-delete cleanup Job can die before deleting its worker
# DaemonSet - nothing owns that DaemonSet, since kubectl applies it after the
# release is already gone. A survivor is privileged and would wipe the handler
# binary and reset core_pattern moments after the install below.
kubectl -n "$NAMESPACE" delete daemonset -l coredrop.io/cleanup=true \
  --ignore-not-found --wait --timeout=60s >/dev/null

# Record the node's pre-install core_pattern so smoke.sh can assert the daemon's
# shutdown restores exactly this value (the CorePatternGuard drop path). If a
# previous run left the node pointing at the handler, recording that would make
# the assertion meaningless, so reset to the kernel default first.
orig_core_pattern="$(node_exec cat /proc/sys/kernel/core_pattern)"
if printf '%s' "$orig_core_pattern" | grep -q 'coredrop capture'; then
  warn "node core_pattern still points at a coredrop handler ($orig_core_pattern);"
  warn "a previous run did not restore it - resetting to the kernel default 'core'"
  node_exec sh -c "printf core > /proc/sys/kernel/core_pattern"
  orig_core_pattern="$(node_exec cat /proc/sys/kernel/core_pattern)"
fi
printf '%s' "$orig_core_pattern" > "$SCRIPT_DIR/.tmp/orig_core_pattern"

log "installing coredrop chart (release: $RELEASE)"
helm upgrade --install "$RELEASE" "$REPO_ROOT/charts/coredrop" \
  -n "$NAMESPACE" \
  -f "$SCRIPT_DIR/helm-values/coredrop.local.yaml" \
  --set "capture.objectStore.config.AWS_ENDPOINT=http://$minio_cluster_ip:9000" \
  --wait --timeout 120s

log "applying demo crash workload -> namespace $DEMO_NAMESPACE"
kubectl apply -n "$DEMO_NAMESPACE" -f "$SCRIPT_DIR/workloads/segfault.yaml" >/dev/null
# Restart the workload so re-ups get a fresh pod UID. Rate-limit state persists
# across daemon restarts, so a rapid re-up that reuses the same pod UID could
# inherit a stale window; a fresh pod UID avoids that for the smoke test.
kubectl -n "$DEMO_NAMESPACE" rollout restart deployment/crash-segfault >/dev/null 2>&1 || true
# Let the restart settle before smoke.sh resolves the crash pod: mid-rollout both
# the old (terminating) and new pod match the label selector.
kubectl -n "$DEMO_NAMESPACE" rollout status deployment/crash-segfault --timeout=120s

log "workload applied; it faults on a short loop. Running smoke test (polls the bucket)"
"$SCRIPT_DIR/smoke.sh"
