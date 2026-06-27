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
log "uninstalling Helm release '$RELEASE' (restores the node core_pattern via the daemon's shutdown)"
helm uninstall "$RELEASE" -n "$NAMESPACE" 2>/dev/null || true

log "deleting demo workload + namespaces"
kubectl delete namespace "$DEMO_NAMESPACE" --ignore-not-found >/dev/null
kubectl delete namespace "$NAMESPACE" --ignore-not-found >/dev/null

log "done (cluster kept; set DELETE_CLUSTER=1 to remove it)"
