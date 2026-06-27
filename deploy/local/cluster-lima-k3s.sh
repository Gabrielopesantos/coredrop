#!/usr/bin/env bash
# Single-node k3s inside a lima Linux VM - the one environment coredrop's live
# smoke test uses. The VM kernel is a real node boundary (kubelet runs directly
# on it), so hostPID, the host-global core_pattern, and the kernel's
# init-namespace coredump-handler invocation all line up in the SAME namespace.
# That is what the capture path needs, and it also avoids this host's
# rootless-podman / no-docker-daemon constraints.
#
# Defines: cluster_up / cluster_down / kube_context / load_images.

cluster_up() {
  require limactl kubectl helm docker
  mkdir -p "$SCRIPT_DIR/.tmp"
  if limactl list --quiet 2>/dev/null | grep -qx "$LIMA_INSTANCE"; then
    log "lima instance '$LIMA_INSTANCE' exists; ensuring it is running"
    limactl start "$LIMA_INSTANCE" >/dev/null 2>&1 || true
  else
    log "creating lima instance '$LIMA_INSTANCE' (template: k3s, KVM-accelerated)"
    limactl start --name "$LIMA_INSTANCE" --tty=false template://k3s
  fi
  kube_context
  # Wait for the API server to answer through the forwarded port.
  local i
  for i in $(seq 1 60); do
    kubectl get --raw='/readyz' >/dev/null 2>&1 && break
    sleep 2
  done
  kubectl get --raw='/readyz' >/dev/null 2>&1 || die "k3s API not ready in the VM"
  log "k3s API reachable via $KUBECONFIG_OUT"
}

cluster_down() {
  if limactl list --quiet 2>/dev/null | grep -qx "$LIMA_INSTANCE"; then
    log "stopping + deleting lima instance '$LIMA_INSTANCE'"
    limactl stop "$LIMA_INSTANCE" >/dev/null 2>&1 || true
    limactl delete "$LIMA_INSTANCE"
  else
    log "lima instance '$LIMA_INSTANCE' not present"
  fi
  rm -f "$KUBECONFIG_OUT"
}

kube_context() {
  mkdir -p "$(dirname "$KUBECONFIG_OUT")"
  # k3s writes its kubeconfig inside the VM (server https://127.0.0.1:6443, which
  # lima forwards to the host). Export a host copy and point KUBECONFIG at it.
  limactl shell "$LIMA_INSTANCE" sudo cat /etc/rancher/k3s/k3s.yaml >"$KUBECONFIG_OUT"
  chmod 600 "$KUBECONFIG_OUT"
  export KUBECONFIG="$KUBECONFIG_OUT"
}

# Run a command on the VM node as root (used for the core_pattern assertions).
node_exec() {
  limactl shell "$LIMA_INSTANCE" sudo "$@"
}

load_images() {
  log "importing images into k3s containerd (namespace k8s.io)"
  local img
  for img in "$COREDROP_IMAGE" "$SEGFAULT_IMAGE"; do
    log "  $img"
    # `save` writes a docker-archive to stdout, piped into the VM and imported
    # into the kubelet's k8s.io containerd namespace.
    docker save "$img" | limactl shell "$LIMA_INSTANCE" sudo k3s ctr -n k8s.io images import -
  done
}
