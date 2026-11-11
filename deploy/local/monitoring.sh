#!/usr/bin/env bash
# Local monitoring stack: install kube-prometheus-stack into the VM,
# reinstall coredrop with its PodMonitor, load the alert rules and the
# Grafana dashboard, then port-forward Grafana to the host. Asserts nothing.
# Ctrl-C stops the port-forward; the stack stays until ./down.sh removes it.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

require kubectl helm yq
kube_context
require_release

GRAFANA_PORT="${GRAFANA_PORT:-3000}"

log "installing kube-prometheus-stack $KPS_VERSION (release: $MONITORING_RELEASE, namespace: $MONITORING_NAMESPACE)"
helm upgrade --install "$MONITORING_RELEASE" "$KPS_CHART" \
  --version "$KPS_VERSION" \
  -n "$MONITORING_NAMESPACE" --create-namespace \
  -f "$SCRIPT_DIR/helm-values/monitoring.yaml" \
  --wait --timeout 600s

# The PodMonitor CRD exists now, so install_coredrop renders the PodMonitor.
install_coredrop

# The alert rules, as a PrometheusRule. No Alertmanager: Prometheus shows
# them on its Alerts page.
log "loading alerts/coredrop.rules.yaml into Prometheus"
yq '{"apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule",
     "metadata": {"name": "coredrop"}, "spec": .}' \
  "$REPO_ROOT/alerts/coredrop.rules.yaml" \
  | kubectl -n "$MONITORING_NAMESPACE" apply -f - >/dev/null

# The Grafana sidecar loads ConfigMaps labelled grafana_dashboard=1.
log "loading dashboards/coredrop.json into Grafana"
kubectl -n "$MONITORING_NAMESPACE" create configmap coredrop-dashboard \
  --from-file=coredrop.json="$REPO_ROOT/dashboards/coredrop.json" \
  --dry-run=client -o yaml \
  | kubectl label --local -f - grafana_dashboard=1 -o yaml \
  | kubectl apply -f - >/dev/null

log "Grafana: http://localhost:$GRAFANA_PORT"
log "run ./smoke.sh in another shell to make captures; Ctrl-C stops the port-forward"
kubectl -n "$MONITORING_NAMESPACE" port-forward "svc/$MONITORING_RELEASE-grafana" "$GRAFANA_PORT:80"
