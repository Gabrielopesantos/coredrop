#!/usr/bin/env bash
# Live smoke test. Asserts the full kernel-to-bucket path on a real
# node: the DaemonSet repoints core_pattern at the handler, a faulting workload
# produces a core, and coredrop streams a valid core + redacted proc-snapshot +
# manifest to MinIO - then uninstall restores core_pattern. coredrop has no
# control plane, so every assertion is made directly against the bucket (mc over
# a port-forward), not a REST API. See docs/testing.md, Layer 3.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

require kubectl helm mc zstd jq tar limactl
kube_context

LOCAL_PORT="${LOCAL_PORT:-19000}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-180}"
MC_ALIAS="coredrop-smoke"
HANDLER_MARK="coredrop capture"               # core_pattern points at our handler
SECRET_CANARY="s3cr3t-smoke-canary-do-not-store"  # the workload's SECRET_FOO value
ORIG_FILE="$SCRIPT_DIR/.tmp/orig_core_pattern"    # pre-install value (written by up.sh)
WORKDIR="$(mktemp -d)"

fail=0
PF_PID=""
cleanup() { [ -n "$PF_PID" ] && kill "$PF_PID" >/dev/null 2>&1 || true; rm -rf "$WORKDIR"; }
trap cleanup EXIT

# 1. DaemonSet ready
log "waiting for the coredrop DaemonSet to be ready"
ds="$(kubectl -n "$NAMESPACE" get ds -l app.kubernetes.io/name=coredrop -o name 2>/dev/null | head -n1)"
[ -n "$ds" ] || die "coredrop DaemonSet not found in namespace $NAMESPACE"
kubectl -n "$NAMESPACE" rollout status "$ds" --timeout=120s

# 2. core_pattern repointed at the handler
cur_pattern="$(node_exec cat /proc/sys/kernel/core_pattern)"
if printf '%s' "$cur_pattern" | grep -q "$HANDLER_MARK"; then
  log "node core_pattern points at the handler: $cur_pattern"
else
  warn "node core_pattern does NOT point at the handler: $cur_pattern"; fail=1
fi

# 3. reach MinIO from the host via a port-forward
# Wait for MinIO + the bucket Job first: a port-forward to a Service with no
# ready endpoints fails, and the first image pull in the VM is slow.
log "waiting for minio + the bucket to be ready"
kubectl -n "$NAMESPACE" rollout status deploy/minio --timeout=180s
kubectl -n "$NAMESPACE" wait --for=condition=complete job/minio-make-bucket --timeout=120s

log "port-forwarding svc/minio ($NAMESPACE) -> 127.0.0.1:$LOCAL_PORT"
kubectl -n "$NAMESPACE" port-forward svc/minio "$LOCAL_PORT:9000" >/dev/null 2>&1 &
PF_PID=$!
for _ in $(seq 1 30); do
  mc alias set "$MC_ALIAS" "http://127.0.0.1:$LOCAL_PORT" minioadmin minioadmin >/dev/null 2>&1 && break
  sleep 1
done
mc alias set "$MC_ALIAS" "http://127.0.0.1:$LOCAL_PORT" minioadmin minioadmin >/dev/null 2>&1 \
  || die "minio not reachable on 127.0.0.1:$LOCAL_PORT"

# 4. poll the bucket for a captured object set
# Start from a clean slate: stale objects from a previous run would otherwise
# satisfy (or break) the assertions below.
mc rm --recursive --force "$MC_ALIAS/$BUCKET" >/dev/null 2>&1 || true

# k8s Events (8c/8d below) get scoped to this run's actual crash pod:
# unlike the bucket, a fresh CoreDumped Event isn't guaranteed to appear
# *after* this point - it only fires on the first max-cores-per-hour
# captures of a given container (ratelimit.rs), which can easily already be
# spent by the time this script starts (the workload faults on a ~5s loop,
# and the DaemonSet/MinIO rollout waits above take longer than that).
# `regarding.name` is a selectable field on events.k8s.io/v1 Events, so
# scoping to the current pod is both correct and non-destructive.
crash_pod="$(kubectl -n "$DEMO_NAMESPACE" get pods -l app=crash-segfault \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
[ -n "$crash_pod" ] || die "could not resolve the crash-segfault pod in $DEMO_NAMESPACE"
log "crash workload pod: $crash_pod"

log "polling s3://$BUCKET for a captured manifest (up to ${SMOKE_TIMEOUT}s)"
manifest_key=""
deadline=$(( SECONDS + SMOKE_TIMEOUT ))
while :; do
  manifest_key="$(mc ls --recursive "$MC_ALIAS/$BUCKET" 2>/dev/null \
    | awk '{print $NF}' | grep -- '-manifest\.json$' | head -n1 || true)"
  [ -n "$manifest_key" ] && break
  [ "$SECONDS" -ge "$deadline" ] && break
  sleep 5
done
[ -n "$manifest_key" ] \
  || die "no manifest appeared in s3://$BUCKET within ${SMOKE_TIMEOUT}s (core_pattern? RLIMIT_CORE? upload endpoint?)"
log "found manifest: $manifest_key"

prefix="${manifest_key%-manifest.json}"
core_key="${prefix}-core.zst"
snap_key="${prefix}-procsnapshot.tar"

# 5. all three sibling objects present
for k in "$core_key" "$snap_key" "$manifest_key"; do
  if mc stat "$MC_ALIAS/$BUCKET/$k" >/dev/null 2>&1; then
    log "  object present: $k"
  else
    warn "  object MISSING: $k"; fail=1
  fi
done

# 6. manifest content: core present, signal 11, crictl-enriched identity
mc cat "$MC_ALIAS/$BUCKET/$manifest_key" > "$WORKDIR/manifest.json" 2>/dev/null \
  || { warn "could not fetch manifest"; fail=1; }
if [ -s "$WORKDIR/manifest.json" ]; then
  present="$(jq -r '.core.present' "$WORKDIR/manifest.json" 2>/dev/null)"
  signal="$(jq -r '.signal' "$WORKDIR/manifest.json" 2>/dev/null)"
  m_ns="$(jq -r '.identity.namespace // empty' "$WORKDIR/manifest.json" 2>/dev/null)"
  m_pod="$(jq -r '.identity.pod_name // empty' "$WORKDIR/manifest.json" 2>/dev/null)"
  m_ctr="$(jq -r '.identity.container_name // empty' "$WORKDIR/manifest.json" 2>/dev/null)"

  [ "$present" = "true" ] && log "  manifest core.present=true" || { warn "  manifest core.present=$present (expected true)"; fail=1; }
  [ "$signal" = "11" ]    && log "  manifest signal=11 (SIGSEGV)" || { warn "  manifest signal=$signal (expected 11)"; fail=1; }
  if [ -n "$m_ns" ] && [ -n "$m_pod" ] && [ -n "$m_ctr" ]; then
    log "  crictl identity: namespace=$m_ns pod=$m_pod container=$m_ctr"
  else
    warn "  crictl identity incomplete (ns='$m_ns' pod='$m_pod' container='$m_ctr') - enrichment path failed"; fail=1
  fi
fi

# 7. core object is a valid, non-empty zstd frame
mc cat "$MC_ALIAS/$BUCKET/$core_key" > "$WORKDIR/core.zst" 2>/dev/null || true
if [ -s "$WORKDIR/core.zst" ] && zstd -t "$WORKDIR/core.zst" >/dev/null 2>&1; then
  log "  core.zst is a valid zstd frame ($(wc -c < "$WORKDIR/core.zst") compressed bytes)"
else
  warn "  core.zst missing, empty, or not a valid zstd frame"; fail=1
fi

# 8. proc-snapshot has the forensic files + environ is REDACTED
mc cat "$MC_ALIAS/$BUCKET/$snap_key" > "$WORKDIR/snap.tar" 2>/dev/null || true
if [ -s "$WORKDIR/snap.tar" ]; then
  members="$(tar tf "$WORKDIR/snap.tar" 2>/dev/null || true)"
  for f in maps status environ; do
    echo "$members" | grep -qx "$f" && log "  proc-snapshot has '$f'" || { warn "  proc-snapshot missing '$f'"; fail=1; }
  done
  if tar xf "$WORKDIR/snap.tar" -C "$WORKDIR" environ 2>/dev/null; then
    if grep -aq "SECRET_FOO=<redacted>" "$WORKDIR/environ"; then
      log "  environ: SECRET_FOO redacted to <redacted>"
    else
      warn "  environ: SECRET_FOO not redacted as expected"; fail=1
    fi
    if grep -aq "$SECRET_CANARY" "$WORKDIR/environ"; then
      warn "  environ: plaintext secret canary LEAKED into the stored snapshot"; fail=1
    else
      log "  environ: plaintext secret canary absent (good)"
    fi
  else
    warn "  could not extract environ from the proc-snapshot"; fail=1
  fi
else
  warn "  proc-snapshot tar missing or empty"; fail=1
fi

# 8b. rate limit: the crash-looping container is suppressed past its budget
# The workload faults every ~5s and the default budget is 3 cores/hour, so a
# suppressed manifest (core.skipped_reason=rate_limit, no core sibling) must
# appear shortly after the third full capture.
log "waiting for a rate-limit-suppressed manifest (up to ${SMOKE_TIMEOUT}s)"
suppressed_key=""
deadline=$(( SECONDS + SMOKE_TIMEOUT ))
while :; do
  for k in $(mc ls --recursive "$MC_ALIAS/$BUCKET" 2>/dev/null \
    | awk '{print $NF}' | grep -- '-manifest\.json$'); do
    reason="$(mc cat "$MC_ALIAS/$BUCKET/$k" 2>/dev/null | jq -r '.core.skipped_reason // empty')"
    if [ "$reason" = "rate_limit" ]; then suppressed_key="$k"; break; fi
  done
  [ -n "$suppressed_key" ] && break
  [ "$SECONDS" -ge "$deadline" ] && break
  sleep 5
done
if [ -n "$suppressed_key" ]; then
  log "  suppressed manifest found: $suppressed_key"
  sup_core="${suppressed_key%-manifest.json}-core.zst"
  if mc stat "$MC_ALIAS/$BUCKET/$sup_core" >/dev/null 2>&1; then
    warn "  suppressed crash stored a core anyway: $sup_core"; fail=1
  else
    log "  suppressed crash stored no core (good)"
  fi
else
  warn "  no rate-limit-suppressed manifest appeared (rate limit not exercised)"; fail=1
fi

# 8c. k8s Events emitted on the crashing pod (kubectl get events)
# The daemon posts an events.k8s.io/v1 Event on the crash workload's own pod,
# in its own namespace (not coredrop's) - regardless of the object store path.
# `events.events.k8s.io` (not the bare `events` shortname, which kubectl
# resolves to the legacy core/v1 compat schema: `involvedObject` instead of
# `regarding`, `reportingComponent` instead of `reportingController`) so the
# JSON below actually has the fields the daemon posted. `regarding.name`
# scopes to $crash_pod specifically - a CoreDumped Event from earlier in this
# same pod's life (even seconds after it started, before this script got
# here) is exactly the evidence wanted, not something to wait past.
log "checking for a CoreDumped k8s Event on $crash_pod (up to ${SMOKE_TIMEOUT}s)"
dumped_json=""
deadline=$(( SECONDS + SMOKE_TIMEOUT ))
while :; do
  dumped_json="$(kubectl -n "$DEMO_NAMESPACE" get events.events.k8s.io \
    --field-selector "reason=CoreDumped,regarding.name=$crash_pod" -o json 2>/dev/null \
    | jq -c '.items[0] // empty')"
  [ -n "$dumped_json" ] && break
  [ "$SECONDS" -ge "$deadline" ] && break
  sleep 5
done
if [ -n "$dumped_json" ]; then
  ev_pod="$(echo "$dumped_json" | jq -r '.regarding.name // empty')"
  ev_reporter="$(echo "$dumped_json" | jq -r '.reportingController // empty')"
  log "  CoreDumped event found: pod=$ev_pod reportingController=$ev_reporter"
  [ "$ev_reporter" = "coredrop" ] || { warn "  reportingController != coredrop (got '$ev_reporter')"; fail=1; }
else
  warn "  no CoreDumped k8s Event appeared for $crash_pod within ${SMOKE_TIMEOUT}s" \
       "(rate-limit budget for this container may already be spent - see ratelimit.rs)"
  fail=1
fi

# 8d. crash-loop Events aggregate via 'series', not one object per crash
# The workload keeps faulting past its rate-limit budget, so a
# CoreDumpSuppressed Event must appear and its series count must grow -
# proof the daemon is PATCHing the existing object, not spamming etcd.
# Suppression (unlike upload) has no per-hour cap, so this one's count keeps
# growing for as long as the pod keeps crash-looping.
log "waiting for a CoreDumpSuppressed k8s Event on $crash_pod (up to ${SMOKE_TIMEOUT}s)"
suppressed_json=""
count1=""
deadline=$(( SECONDS + SMOKE_TIMEOUT ))
while :; do
  suppressed_json="$(kubectl -n "$DEMO_NAMESPACE" get events.events.k8s.io \
    --field-selector "reason=CoreDumpSuppressed,regarding.name=$crash_pod" -o json 2>/dev/null \
    | jq -c '.items[0] // empty')"
  [ -n "$suppressed_json" ] && break
  [ "$SECONDS" -ge "$deadline" ] && break
  sleep 5
done
if [ -n "$suppressed_json" ]; then
  ev_name="$(echo "$suppressed_json" | jq -r '.metadata.name')"
  count1="$(echo "$suppressed_json" | jq -r '.series.count // 1')"
  log "  CoreDumpSuppressed event found: $ev_name (series count=$count1); waiting for it to grow"
  count2="$count1"
  deadline=$(( SECONDS + SMOKE_TIMEOUT ))
  while [ "$count2" -le "$count1" ] && [ "$SECONDS" -lt "$deadline" ]; do
    sleep 5
    count2="$(kubectl -n "$DEMO_NAMESPACE" get events.events.k8s.io "$ev_name" -o json 2>/dev/null | jq -r '.series.count // 1')"
  done
  if [ "$count2" -gt "$count1" ]; then
    log "  series count grew: $count1 -> $count2 (crash loop bumps the counter, not spamming etcd)"
  else
    warn "  CoreDumpSuppressed series count did not grow ($count1 -> $count2)"; fail=1
  fi
else
  warn "  no CoreDumpSuppressed k8s Event appeared in $DEMO_NAMESPACE within ${SMOKE_TIMEOUT}s"; fail=1
fi

# --- 9. uninstall restores core_pattern (CorePatternGuard drop path) ----------
log "uninstalling coredrop to exercise core_pattern restore"
helm uninstall "$RELEASE" -n "$NAMESPACE" >/dev/null 2>&1 || true
kubectl -n "$NAMESPACE" wait --for=delete pod -l app.kubernetes.io/name=coredrop --timeout=60s >/dev/null 2>&1 || true
sleep 2
restored="$(node_exec cat /proc/sys/kernel/core_pattern)"
if printf '%s' "$restored" | grep -q "$HANDLER_MARK"; then
  warn "  core_pattern STILL points at the handler after uninstall: $restored"; fail=1
else
  log "  core_pattern no longer points at the handler: $restored"
fi
if [ -f "$ORIG_FILE" ]; then
  orig="$(cat "$ORIG_FILE")"
  if [ "$restored" = "$orig" ]; then
    log "  core_pattern matches its pre-install value"
  else
    warn "  core_pattern differs from pre-install value: '$orig'"; fail=1
  fi
fi

if [ "$fail" -ne 0 ]; then
  die "smoke test FAILED"
fi
log "smoke test PASSED"
