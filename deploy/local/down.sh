#!/usr/bin/env bash
# Tear down the local deployment. By default removes the Helm release, demo
# workload, and namespaces but KEEPS the VM (fast re-`up`). Set DELETE_CLUSTER=1
# to also delete the lima VM.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if [ "${DELETE_CLUSTER:-0}" = "1" ]; then
  cluster_down
  exit 0
fi

kube_context
log "uninstalling Helm release '$RELEASE' (daemon restores core_pattern on shutdown; post-delete hook removes hostPath files)"
helm uninstall "$RELEASE" -n "$NAMESPACE" --ignore-not-found \
  || warn "helm uninstall failed (post-delete cleanup hook?); the namespace delete below still removes the workloads, but the node may keep $HOST_BIN_DIR / $HOST_RUN_DIR"

log "deleting demo workload + namespaces"
kubectl delete namespace "$DEMO_NAMESPACE" --ignore-not-found >/dev/null
kubectl delete namespace "$NAMESPACE" --ignore-not-found >/dev/null

log "done (cluster kept; set DELETE_CLUSTER=1 to remove it)"
