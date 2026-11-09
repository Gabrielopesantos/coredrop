#!/usr/bin/env bash
# Live uninstall test. Destructive, but it reinstalls coredrop at the end, so
# the stack is left as it was found. Two phases:
#
#   A  clean uninstall: the daemon restores core_pattern on SIGTERM
#      (CorePatternGuard), and the post-delete hook removes the hostPath dirs.
#   B  unclean uninstall: the daemon is frozen with SIGSTOP first. SIGTERM
#      stays pending, the kubelet SIGKILLs it after the grace period, and the
#      restore never runs. The hook's cleanup worker must then reset the stale
#      core_pattern to the kernel default. The worker and the SIGKILL can come
#      in either order; the result is the same.
#
# See README.md.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

require kubectl helm limactl
kube_context
require_release

fail=0
stopped_pid=""

[ -f "$ORIG_FILE" ] || die "no recorded pre-install core_pattern ($ORIG_FILE); run ./up.sh"
orig="$(cat "$ORIG_FILE")"

node_pattern() { node_exec cat /proc/sys/kernel/core_pattern; }
# Kernel log lines for crashes routed to a missing handler binary. The demo
# workload crashes every ~5s throughout, so a window where core_pattern points
# at a removed binary shows up here.
pipe_failures() { node_exec dmesg | grep -c "$HOST_BIN_DIR/coredrop pipe failed" || true; }
# stdin, not an argument: core_pattern values hold spaces and % characters.
set_node_pattern() { printf '%s' "$1" | node_exec tee /proc/sys/kernel/core_pattern >/dev/null; }

# On an early exit, leave the stack usable: thaw a frozen daemon, and reinstall
# a release that a failed phase left uninstalled.
cleanup() {
  if [ -n "$stopped_pid" ]; then
    node_exec kill -CONT "$stopped_pid" >/dev/null 2>&1 || true
  fi
  if ! helm status "$RELEASE" -n "$NAMESPACE" >/dev/null 2>&1; then
    warn "coredrop is not installed after a failed run; reinstalling"
    kubectl -n "$NAMESPACE" delete daemonset -l coredrop.io/cleanup=true \
      --ignore-not-found --wait --timeout=60s >/dev/null 2>&1 || true
    set_node_pattern "$orig" || true
    install_coredrop || warn "reinstall failed; run ./up.sh"
  fi
}
trap cleanup EXIT

# The host PID of the daemon. The kernel-exec'd handler runs as
# `coredrop capture ...`, so it does not match.
daemon_host_pid() {
  local pids
  pids="$(node_exec pgrep -f '^/bin/coredrop$' || true)"
  [ "$(printf '%s\n' "$pids" | grep -c .)" -eq 1 ] \
    || die "expected exactly one coredrop daemon on the node, found: '${pids//$'\n'/ }'"
  printf '%s' "$pids"
}

# Wait for the daemon, then check that core_pattern points at the handler.
assert_installed() {
  local ds cur
  ds="$(kubectl -n "$NAMESPACE" get ds -l 'app.kubernetes.io/name=coredrop,!coredrop.io/cleanup' -o name | head -n1)"
  [ -n "$ds" ] || die "coredrop DaemonSet not found after install"
  kubectl -n "$NAMESPACE" rollout status "$ds" --timeout=120s >/dev/null
  cur="$(node_pattern)"
  if printf '%s' "$cur" | grep -q "$HANDLER_MARK"; then
    log "  reinstalled; core_pattern points at the handler"
  else
    warn "  reinstalled, but core_pattern does NOT point at the handler: $cur"; fail=1
  fi
}

# helm uninstall, then wait until the daemon pods are gone. The exit status is
# an assertion: helm waits on the post-delete cleanup hook, so a failure means
# the hook did not finish - and a half-finished hook leaves an orphaned cleanup
# worker behind.
uninstall_and_wait() {
  local timeout="$1"
  if ! helm uninstall "$RELEASE" -n "$NAMESPACE" >/dev/null; then
    warn "  helm uninstall failed - the post-delete cleanup hook did not complete"; fail=1
  fi
  kubectl -n "$NAMESPACE" wait --for=delete pod \
    -l 'app.kubernetes.io/name=coredrop,!coredrop.io/cleanup' --timeout="$timeout" >/dev/null 2>&1 \
    || { warn "  daemon pods still present after ${timeout}"; fail=1; }
}

# The cleanup worker removes the handler binary, runtime config/state AND the
# hostPath directories themselves. A successful hook does not prove that the
# directories are gone, so check them on the node.
assert_node_clean() {
  local d
  for d in "$HOST_BIN_DIR" "$HOST_RUN_DIR" "$HOST_SPOOL_DIR"; do
    if node_exec test -e "$d"; then
      warn "  leftover on the node after uninstall: $d"; fail=1
    else
      log "  removed from the node: $d"
    fi
  done
}

pipe_failures_before="$(pipe_failures)"

# --- A. clean uninstall ------------------------------------------------------
log "A: clean uninstall restores core_pattern + post-delete cleanup"
uninstall_and_wait 60s
sleep 2
restored="$(node_pattern)"
if [ "$restored" = "$orig" ]; then
  log "  core_pattern matches its pre-install value: $restored"
else
  warn "  core_pattern is '$restored', pre-install value was '$orig'"; fail=1
fi
assert_node_clean
install_coredrop >/dev/null
assert_installed

# --- B. unclean uninstall ----------------------------------------------------
log "B: daemon dies without restoring core_pattern; the cleanup worker resets it"
stopped_pid="$(daemon_host_pid)"
node_exec kill -STOP "$stopped_pid"
log "  daemon (host pid $stopped_pid) frozen with SIGSTOP"
# 30s default grace period before the kubelet sends SIGKILL, plus margin.
uninstall_and_wait 90s
if node_exec test -e "/proc/$stopped_pid"; then
  warn "  daemon (host pid $stopped_pid) is still alive after uninstall"; fail=1
else
  log "  daemon died without restore"
  stopped_pid=""
fi
reset="$(node_pattern)"
if [ "$reset" = "core" ]; then
  log "  core_pattern reset to core by the cleanup worker"
else
  warn "  core_pattern is '$reset', expected the worker to reset it to 'core'"; fail=1
fi
assert_node_clean

# Give the node its pre-install value back, so the reinstall records it.
set_node_pattern "$orig"
install_coredrop >/dev/null
assert_installed

# --- no crash hit a missing handler ------------------------------------------
# The cleanup worker resets core_pattern before it removes the binary, and the
# daemon restores it before it stops. Either order keeps crashes away from a
# missing handler.
pipe_failures_after="$(pipe_failures)"
if [ "$pipe_failures_after" -eq "$pipe_failures_before" ]; then
  log "no crash hit a missing handler"
else
  warn "$((pipe_failures_after - pipe_failures_before)) crash(es) hit a missing handler (dmesg: 'pipe failed')"; fail=1
fi

if [ "$fail" -ne 0 ]; then
  die "lifecycle test FAILED"
fi
log "lifecycle test PASSED"
