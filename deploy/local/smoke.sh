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

# --- 1. DaemonSet ready -------------------------------------------------------
log "waiting for the coredrop DaemonSet to be ready"
ds="$(kubectl -n "$NAMESPACE" get ds -l app.kubernetes.io/name=coredrop -o name 2>/dev/null | head -n1)"
[ -n "$ds" ] || die "coredrop DaemonSet not found in namespace $NAMESPACE"
kubectl -n "$NAMESPACE" rollout status "$ds" --timeout=120s

# --- 2. core_pattern repointed at the handler ---------------------------------
cur_pattern="$(node_exec cat /proc/sys/kernel/core_pattern)"
if printf '%s' "$cur_pattern" | grep -q "$HANDLER_MARK"; then
  log "node core_pattern points at the handler: $cur_pattern"
else
  warn "node core_pattern does NOT point at the handler: $cur_pattern"; fail=1
fi

# --- 3. reach MinIO from the host via a port-forward --------------------------
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

# --- 4. poll the bucket for a captured object set -----------------------------
# Start from a clean slate: stale objects from a previous run would otherwise
# satisfy (or break) the assertions below.
mc rm --recursive --force "$MC_ALIAS/$BUCKET" >/dev/null 2>&1 || true

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

# --- 5. all three sibling objects present -------------------------------------
for k in "$core_key" "$snap_key" "$manifest_key"; do
  if mc stat "$MC_ALIAS/$BUCKET/$k" >/dev/null 2>&1; then
    log "  object present: $k"
  else
    warn "  object MISSING: $k"; fail=1
  fi
done

# --- 6. manifest content: core present, signal 11, crictl-enriched identity ---
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

# --- 7. core object is a valid, non-empty zstd frame --------------------------
mc cat "$MC_ALIAS/$BUCKET/$core_key" > "$WORKDIR/core.zst" 2>/dev/null || true
if [ -s "$WORKDIR/core.zst" ] && zstd -t "$WORKDIR/core.zst" >/dev/null 2>&1; then
  log "  core.zst is a valid zstd frame ($(wc -c < "$WORKDIR/core.zst") compressed bytes)"
else
  warn "  core.zst missing, empty, or not a valid zstd frame"; fail=1
fi

# --- 8. proc-snapshot has the forensic files + environ is REDACTED ------------
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

# --- 8b. rate limit: the crash-looping container is suppressed past its budget -
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
