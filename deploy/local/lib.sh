#!/usr/bin/env bash
# Shared config + helpers for the coredrop local deploy scaffold. Sourced by the
# other scripts.
#
# ONE environment: single-node k3s inside a lima VM (see cluster-lima-k3s.sh).
# Images are built on the host and imported into the VM's k3s containerd. See
# README.md.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

LIMA_INSTANCE="${LIMA_INSTANCE:-coredrop-k3s}"

# Namespaces: the coredrop DaemonSet vs the crash-generating demo workload.
NAMESPACE="${NAMESPACE:-coredrop-system}"
DEMO_NAMESPACE="${DEMO_NAMESPACE:-coredrop-demo}"

# Locally-built image tags (imported into the VM, never pushed). Fully qualified
# as docker.io/library/* so Kubernetes' default normalization matches and the
# builder does not rewrite the name to localhost/*; with pullPolicy:IfNotPresent
# the kubelet then uses the imported image and never hits the network. The Helm
# overlay in helm-values/ sets the same repository.
COREDROP_IMAGE="${COREDROP_IMAGE:-docker.io/library/coredrop:dev}"
SEGFAULT_IMAGE="${SEGFAULT_IMAGE:-docker.io/library/coredrop-segfault:dev}"

# The chart's post-delete cleanup Job image. Not built here - pulled on the host
# and imported like the others so `helm uninstall` never waits on a registry
# (the hook has a 180s deadline). Must match `cleanup.image` in the chart's
# values.yaml and in helm-values/coredrop.local.yaml.
CLEANUP_IMAGE="${CLEANUP_IMAGE:-docker.io/rancher/kubectl:v1.35.6}"

# The node paths the chart's post-delete hook is supposed to remove; smoke.sh
# asserts they are gone. Must match `corePattern.hostBinDir`/`hostRunDir`.
HOST_BIN_DIR="${HOST_BIN_DIR:-/opt/coredrop/bin}"
HOST_RUN_DIR="${HOST_RUN_DIR:-/run/coredrop}"

# Helm release name and the object-store bucket the overlay points at.
RELEASE="${RELEASE:-coredrop}"
BUCKET="${BUCKET:-coredrop-cores}"

# Host kubeconfig exported from the VM's k3s (see cluster-lima-k3s.sh).
KUBECONFIG_OUT="${KUBECONFIG_OUT:-$SCRIPT_DIR/.tmp/kubeconfig}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

require() {
  local missing=0 t
  for t in "$@"; do
    have "$t" || { warn "missing required tool: $t"; missing=1; }
  done
  [ "$missing" -eq 0 ] || die "install the missing tools (the Nix dev shell provides them) and retry"
}

source "$SCRIPT_DIR/cluster-lima-k3s.sh"

# A node core_pattern that contains this points at our handler.
HANDLER_MARK="coredrop capture"
# The node's pre-install core_pattern, recorded by install_coredrop. The test
# scripts assert that an uninstall gives this value back.
ORIG_FILE="$SCRIPT_DIR/.tmp/orig_core_pattern"

# Die unless the coredrop release is installed.
require_release() {
  helm status "$RELEASE" -n "$NAMESPACE" >/dev/null 2>&1 \
    || die "coredrop is not installed; run ./up.sh"
}

# Install (or upgrade) the coredrop release against the in-cluster MinIO and
# wait for it. Records the node's pre-install core_pattern to $ORIG_FILE.
install_coredrop() {
  # The capture handler is kernel-exec'd in the HOST network namespace (no cluster
  # DNS), so it can't reach `minio` by name - but the node's root netns CAN reach a
  # ClusterIP (kube-proxy's DNAT rules live there). Point coredrop's upload endpoint
  # at MinIO's numeric ClusterIP. (The smoke test reaches the same MinIO from the
  # host via a port-forward.)
  local minio_cluster_ip orig_core_pattern
  minio_cluster_ip="$(kubectl -n "$NAMESPACE" get svc minio -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)"
  [ -n "$minio_cluster_ip" ] || die "could not read minio ClusterIP"
  log "minio ClusterIP $minio_cluster_ip -> coredrop upload endpoint http://$minio_cluster_ip:9000"

  # A previous run's post-delete cleanup Job can die before deleting its worker
  # DaemonSet - nothing owns that DaemonSet, since kubectl applies it after the
  # release is already gone. A survivor would wipe the handler
  # binary and reset core_pattern moments after the install below.
  kubectl -n "$NAMESPACE" delete daemonset -l coredrop.io/cleanup=true \
    --ignore-not-found --wait --timeout=60s >/dev/null

  # Record the node's pre-install core_pattern so the tests can assert the
  # daemon's shutdown restores exactly this value (the CorePatternGuard drop
  # path). If a previous run left the node pointing at the handler, recording
  # that would make the assertion meaningless, so reset to the kernel default
  # first. On an upgrade of a running release, keep the recorded value.
  if ! helm status "$RELEASE" -n "$NAMESPACE" >/dev/null 2>&1; then
    orig_core_pattern="$(node_exec cat /proc/sys/kernel/core_pattern)"
    if printf '%s' "$orig_core_pattern" | grep -q "$HANDLER_MARK"; then
      warn "node core_pattern still points at a coredrop handler ($orig_core_pattern);"
      warn "a previous run did not restore it - resetting to the kernel default 'core'"
      node_exec sh -c "printf core > /proc/sys/kernel/core_pattern"
      orig_core_pattern="$(node_exec cat /proc/sys/kernel/core_pattern)"
    fi
    mkdir -p "$(dirname "$ORIG_FILE")"
    printf '%s' "$orig_core_pattern" > "$ORIG_FILE"
  fi

  log "installing coredrop chart (release: $RELEASE)"
  helm upgrade --install "$RELEASE" "$REPO_ROOT/charts/coredrop" \
    -n "$NAMESPACE" \
    -f "$SCRIPT_DIR/helm-values/coredrop.local.yaml" \
    --set "capture.objectStore.config.AWS_ENDPOINT=http://$minio_cluster_ip:9000" \
    --wait --timeout 120s
}

# Apply the demo crash workload and restart it, so it gets a fresh pod UID.
# The pod UID is the rate-limit key, and the rate-limit state persists across
# daemon restarts: without a fresh UID, a rerun could start already suppressed.
restart_demo() {
  log "applying demo crash workload -> namespace $DEMO_NAMESPACE"
  kubectl apply -n "$DEMO_NAMESPACE" -f "$SCRIPT_DIR/workloads/segfault.yaml" >/dev/null
  kubectl -n "$DEMO_NAMESPACE" rollout restart deployment/crash-segfault >/dev/null 2>&1 || true
  # Let the restart settle before a test resolves the crash pod: mid-rollout both
  # the old (terminating) and new pod match the label selector.
  kubectl -n "$DEMO_NAMESPACE" rollout status deployment/crash-segfault --timeout=120s
}
