#!/usr/bin/env bash
# Shared config + helpers for the coredrop local deploy scaffold. Sourced by the
# other scripts.
#
# ONE environment: single-node k3s inside a lima VM. The VM is a real kernel
# boundary with kubelet running directly on it, so hostPID, the host-global
# core_pattern sysctl, and the kernel's init-namespace coredump-handler
# invocation all line up in the SAME namespace - which is exactly what coredrop's
# capture path needs and a container cannot fake. Images are built on the host
# and imported straight into the VM's k3s containerd. See docs/testing.md.

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
