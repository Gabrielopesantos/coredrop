#!/usr/bin/env bash
# Live smoke test. Asserts the full kernel-to-bucket path on a real
# node: the DaemonSet repoints core_pattern at the handler, a faulting workload
# produces a core, and coredrop streams a valid core + redacted proc-snapshot +
# manifest to MinIO - then uninstall restores core_pattern. coredrop has no
# control plane, so every assertion is made directly against the bucket (mc over
# a port-forward), not a REST API. See README.md.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

require kubectl helm mc zstd jq tar sha256sum limactl
kube_context

LOCAL_PORT="${LOCAL_PORT:-19000}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-180}"
MC_ALIAS="coredrop-smoke"
HANDLER_MARK="coredrop capture"               # core_pattern points at our handler
SECRET_CANARY="s3cr3t-smoke-canary-do-not-store"  # the workload's SECRET_FOO value
CMDLINE_CANARY="s3cr3t-cmdline-canary-do-not-store"  # the workload's --db-password value
ORIG_FILE="$SCRIPT_DIR/.tmp/orig_core_pattern"    # pre-install value (written by up.sh)
WORKDIR="$(mktemp -d)"

fail=0
PF_PID=""
cleanup() { [ -n "$PF_PID" ] && kill "$PF_PID" >/dev/null 2>&1 || true; rm -rf "$WORKDIR"; }
trap cleanup EXIT

# 1. DaemonSet ready
log "waiting for the coredrop DaemonSet to be ready"
# `!coredrop.io/cleanup` excludes a leftover post-delete cleanup worker, which
# carries the same name/instance labels as the daemon's own DaemonSet.
ds="$(kubectl -n "$NAMESPACE" get ds -l 'app.kubernetes.io/name=coredrop,!coredrop.io/cleanup' -o name 2>/dev/null | head -n1)"
[ -n "$ds" ] || die "coredrop DaemonSet not found in namespace $NAMESPACE"
kubectl -n "$NAMESPACE" rollout status "$ds" --timeout=120s

# 2. core_pattern repointed at the handler
cur_pattern="$(node_exec cat /proc/sys/kernel/core_pattern)"
if printf '%s' "$cur_pattern" | grep -q "$HANDLER_MARK"; then
  log "node core_pattern points at the handler: $cur_pattern"
else
  warn "node core_pattern does NOT point at the handler: $cur_pattern"; fail=1
fi

# The rollout above already waited on the readiness probe, which runs the same
# check; run it once explicitly so a probe failure reports its own reason here
# instead of only as a rollout timeout.
daemon_pod="$(kubectl -n "$NAMESPACE" get pods \
  -l 'app.kubernetes.io/name=coredrop,!coredrop.io/cleanup' \
  --field-selector status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
if [ -z "$daemon_pod" ]; then
  warn "no running coredrop daemon pod found"; fail=1
elif check_out="$(kubectl -n "$NAMESPACE" exec "$daemon_pod" -- /bin/coredrop check 2>&1)"; then
  log "health check (readiness/liveness probe) passes on $daemon_pod"
else
  warn "health check FAILED on $daemon_pod: $check_out"; fail=1
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

# Scope the k8s Events (8c/8d below) to this run's crash pod. A CoreDumped
# Event fires only for the first max-cores-per-hour captures of a pod, so it
# can be older than this point. `regarding.name` is a selectable field on
# events.k8s.io/v1 Events.
# Use the running pod of the active ReplicaSet (replicas > 0). During a
# rollout the old pod is still listed, and its Events stop. creationTimestamp
# has one-second granularity, so it cannot tell the two pods apart. Pods with a
# deletionTimestamp are terminating, so the filter removes them.
active_hash="$(kubectl -n "$DEMO_NAMESPACE" get rs -l app=crash-segfault -o json 2>/dev/null \
  | jq -r '.items[] | select(.spec.replicas > 0) | .metadata.labels["pod-template-hash"]' | head -n1)"
[ -n "$active_hash" ] || die "could not resolve the active crash-segfault ReplicaSet in $DEMO_NAMESPACE"
crash_pod="$(kubectl -n "$DEMO_NAMESPACE" get pods \
  -l "app=crash-segfault,pod-template-hash=$active_hash" -o json 2>/dev/null \
  | jq -r '.items[]
           | select(.status.phase == "Running")
           | select(.metadata.deletionTimestamp == null)
           | .metadata.name' | head -n1)"
[ -n "$crash_pod" ] || die "could not resolve a running crash-segfault pod in $DEMO_NAMESPACE"
log "crash workload pod: $crash_pod (replicaset $active_hash)"

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
  build_id="$(jq -r '.build_id // empty' "$WORKDIR/manifest.json" 2>/dev/null)"

  [ "$present" = "true" ] && log "  manifest core.present=true" || { warn "  manifest core.present=$present (expected true)"; fail=1; }
  [ "$signal" = "11" ]    && log "  manifest signal=11 (SIGSEGV)" || { warn "  manifest signal=$signal (expected 11)"; fail=1; }
  if [ -n "$m_ns" ] && [ -n "$m_pod" ] && [ -n "$m_ctr" ]; then
    log "  crictl identity: namespace=$m_ns pod=$m_pod container=$m_ctr"
  else
    warn "  crictl identity incomplete (ns='$m_ns' pod='$m_pod' container='$m_ctr') - enrichment path failed"; fail=1
  fi
  # The workload is built with -Wl,--build-id, so the handler must read a real
  # NT_GNU_BUILD_ID from /proc/<pid>/exe.
  if printf '%s' "$build_id" | grep -Eq '^[0-9a-f]+$'; then
    log "  build_id read from the faulting binary: $build_id"
  else
    warn "  manifest build_id missing or not lowercase hex (got '$build_id')"; fail=1
  fi
fi

# 7. core object: valid zstd frame, matching sha256, and a real ELF core dump
mc cat "$MC_ALIAS/$BUCKET/$core_key" > "$WORKDIR/core.zst" 2>/dev/null || true
if [ -s "$WORKDIR/core.zst" ] && zstd -t "$WORKDIR/core.zst" >/dev/null 2>&1; then
  log "  core.zst is a valid zstd frame ($(wc -c < "$WORKDIR/core.zst") compressed bytes)"

  # The manifest sha256 is the hash of the stored object (after zstd). The
  # handler calculates it during the upload.
  want_sha="$(jq -r '.core.sha256 // empty' "$WORKDIR/manifest.json" 2>/dev/null)"
  got_sha="$(sha256sum "$WORKDIR/core.zst" | awk '{print $1}')"
  if [ -z "$want_sha" ]; then
    warn "  manifest carries no core.sha256"; fail=1
  elif [ "$want_sha" = "$got_sha" ]; then
    log "  core.zst sha256 matches the manifest ($want_sha)"
  else
    warn "  core.zst sha256 MISMATCH: manifest=$want_sha stored=$got_sha"; fail=1
  fi

  # An empty or wrong stream also gives a valid zstd frame, so check the ELF
  # header of the decompressed core.
  #
  # Write to a file, not a pipe into `head`. With `set -o pipefail` (lib.sh),
  # an early close gives zstd SIGPIPE and the pipeline exits 141.
  if zstd -dc "$WORKDIR/core.zst" > "$WORKDIR/core" 2>/dev/null; then
    hdr() { dd if="$WORKDIR/core" bs=1 skip="$1" count="$2" 2>/dev/null | od -An -tx1 | tr -d ' \n'; }
    magic="$(hdr 0 4)"    # \x7fELF
    etype="$(hdr 16 2)"   # e_type; 0400 = ET_CORE, little-endian
    if [ "$magic" = "7f454c46" ]; then
      log "  decompressed core has ELF magic ($(wc -c < "$WORKDIR/core") bytes)"
    else
      warn "  decompressed core is not an ELF (magic=$magic)"; fail=1
    fi
    if [ "$etype" = "0400" ]; then
      log "  decompressed core is ET_CORE (a real core dump, not some other ELF)"
    else
      warn "  decompressed core e_type=$etype (expected 0400 = ET_CORE)"; fail=1
    fi
  else
    warn "  could not decompress core.zst"; fail=1
  fi
else
  warn "  core.zst missing, empty, or not a valid zstd frame"; fail=1
fi

# 8. proc-snapshot has the forensic files + environ and cmdline are redacted
mc cat "$MC_ALIAS/$BUCKET/$snap_key" > "$WORKDIR/snap.tar" 2>/dev/null || true
if [ -s "$WORKDIR/snap.tar" ]; then
  members="$(tar tf "$WORKDIR/snap.tar" 2>/dev/null || true)"
  for f in maps status environ cmdline; do
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
  if tar xf "$WORKDIR/snap.tar" -C "$WORKDIR" cmdline 2>/dev/null; then
    if grep -aq -- "--db-password=<redacted>" "$WORKDIR/cmdline"; then
      log "  cmdline: --db-password redacted to <redacted>"
    else
      warn "  cmdline: --db-password not redacted as expected"; fail=1
    fi
    if grep -aq "$CMDLINE_CANARY" "$WORKDIR/cmdline"; then
      warn "  cmdline: plaintext secret canary LEAKED into the stored snapshot"; fail=1
    else
      log "  cmdline: plaintext secret canary absent (good)"
    fi
  else
    warn "  could not extract cmdline from the proc-snapshot"; fail=1
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
# The daemon posts an events.k8s.io/v1 Event on the crash pod, in the pod's
# own namespace. Use `events.events.k8s.io`: the bare `events` name gives the
# core/v1 schema (`involvedObject`, `reportingComponent`), not the fields the
# daemon posted. An Event from earlier in this pod's life also counts.
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
# The uninstall's exit status is an assertion in its own right: helm waits on the
# post-delete cleanup hook, so a failure here means the hook did not finish - and
# a half-finished hook leaves an orphaned, privileged cleanup worker behind.
log "uninstalling coredrop to exercise core_pattern restore + post-delete cleanup"
if ! helm uninstall "$RELEASE" -n "$NAMESPACE" >/dev/null; then
  warn "  helm uninstall failed - the post-delete cleanup hook did not complete"; fail=1
fi
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

# --- 10. the post-delete hook left nothing on the node ------------------------
# The cleanup worker removes the handler binary, runtime config/state AND the
# hostPath directories themselves. A successful hook does not prove that the
# directories are gone, so check them on the node.
log "checking the node for leftover coredrop hostPath directories"
for d in "$HOST_BIN_DIR" "$HOST_RUN_DIR"; do
  if node_exec test -e "$d"; then
    warn "  leftover on the node after uninstall: $d"; fail=1
  else
    log "  removed from the node: $d"
  fi
done

if [ "$fail" -ne 0 ]; then
  die "smoke test FAILED"
fi
log "smoke test PASSED"
