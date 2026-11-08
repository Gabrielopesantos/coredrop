# coredrop

Standalone Kubernetes coredump handler: captures fault cores and `/proc`
snapshots to object storage, with a JSON manifest sidecar per crash.

When a containerized process crashes, the node's kernel invokes coredrop as
the `core_pattern` pipe handler. coredrop snapshots the faulting process's
`/proc` entry before the kernel reaps it, writes the core through zstd into a
spool on the node's disk, enriches the crash with Kubernetes identity via the
CRI, and writes a JSON manifest next to it. The coredrop daemon uploads the
spool to an object store (S3/GCS/Azure).

```
kernel --core pipe--> handler --> spool on node disk --> daemon --> object store
        (released once           (one dir per crash)   (bounded queue,
         the core is on disk)                            retries)
```

## Design

- **Local spool, queued upload** - the handler writes only to local disk, so
  a crash never waits for the network: the kernel reaps the crashed process
  as soon as the core is on disk. The daemon uploads a bounded number of
  captures at a time and retries failures with backoff. The uncompressed core
  never lands on disk or in memory: it goes stdin -> zstd -> spool in one
  pass.
- **Credentials stay in the daemon** - the kernel-exec'd handler has no store
  credentials and makes no network calls.
- **Pre-reap `/proc` snapshot** - process state is captured while the kernel
  still holds the faulting process (see [How it works](#how-it-works)).
- **Safe by default** - `environ` and `cmdline` are redacted, stored core size
  is capped, and uploads are rate limited per pod (see
  [Safety rails](#safety-rails)).
- **Loose artifacts + manifest** - core, `/proc` snapshot tar, and JSON
  manifest are separate objects under a predictable key scheme, so tooling
  can read the manifest without downloading a multi-GB core.
- **Multi-cloud via `object_store`** - S3, GCS, and Azure (including workload
  identity).

## How it works

The single `coredrop` binary runs in three modes:

- **Daemon** (`coredrop`) - the long-running DaemonSet container. At startup
  it points `/proc/sys/kernel/core_pattern` at the handler, raises
  `core_pipe_limit` so more concurrent core dumps can be held open, and writes the
  handler's config to a hostPath file. It uploads the spool for as long as it
  runs. On shutdown it restores the previous sysctl values, then keeps
  uploading for `--shutdown-flush-secs`.
- **Capture handler** (`coredrop capture %P %s %t %E`) - the short-lived
  process the kernel exec's per fault, in the host namespaces, with the core
  on stdin. Because the kernel exec's it with a clean environment, it reads
  the daemon-written hostPath config instead of env vars.
- **Health check** (`coredrop check`) - what the chart's readiness and liveness
  probes exec. Exits non-zero when `core_pattern` does not point at the
  handler, the handler config is missing or invalid, or the capture-event
  socket is gone. Startup failures already exit the daemon non-zero; this
  catches drift after startup, and the liveness restart reinstalls
  `core_pattern` and the config.

Handler flow, ordered by time-criticality:

1. **Pre-reap `/proc` snapshot** - `maps`, `smaps`, `status`, `fd`, `limits`,
   `environ`, `cmdline`, `stack`, `exe`, plus the executable's GNU build-id.
   Must complete while the kernel holds the faulting process; none of it is
   reconstructable afterwards.
2. **Core drain** - stdin -> zstd -> sha256 -> spool file, fully streaming.
   The handler then releases the core pipe: the kernel reaps the crashed
   process and frees its `core_pipe_limit` slot.
3. **Proc snapshot** - the small in-memory tar, written to the spool.
4. **CRI enrichment** - best-effort `crictl inspect` for namespace, pod name,
   container name, image, restart count. Failure degrades to cgroup-derived
   identity (pod UID + container ID), which is always present.
5. **Manifest** - the JSON record, written to the spool. The handler then
   moves the complete capture from the spool's `incoming/` to `ready/`.

The daemon uploads each ready capture in the order core, proc snapshot,
manifest, so a manifest in the bucket always points at complete artifacts,
then deletes it from the spool. Objects land at:

```sh
{cluster}/{podUID}/{containerID}/{timestamp}-{hostPID}-core.zst
{cluster}/{podUID}/{containerID}/{timestamp}-{hostPID}-procsnapshot.tar
{cluster}/{podUID}/{containerID}/{timestamp}-{hostPID}-manifest.json
```

## Safety rails

- **Secret redaction** - `environ` values are redacted by default via a
  curated keyword list plus an entropy/shape heuristic (JWTs, PEM blocks,
  high-entropy tokens), including credentials embedded in URL userinfo and
  query strings. `cmdline` gets the same treatment for `--key=value` arguments
  and `--secret value` pairs, biased to over-redact since argv has no schema.
  `--no-redact` opts out. Cores themselves are secret-bearing regardless;
  treat the bucket accordingly.
- **Size cap** - stored core bytes are capped per crash (default 2 GiB);
  the remainder of the stream is drained but not stored.
- **Spool quota** - the spool is capped (default 10 GiB) and keeps a
  free-space floor on its filesystem (default 15%). A core that does not fit
  is dropped; its proc snapshot and manifest are still spooled. The handler
  checks the spool again while it writes a core, so concurrent crashes count
  against the cap; each one can pass it by a few MiB.
- **Rate limit** - per-pod core budget (default 3/hour) so a crash loop
  can't flood the spool or the bucket. Suppressed crashes still get a proc
  snapshot and manifest, just no core. The limiter fails open: a broken
  limiter never loses a core.
- **Restore on shutdown** - the daemon restores the node's original
  `core_pattern` and `core_pipe_limit` when it stops. HostPath files are
  left in place so restarts/upgrades keep the rate-limit state; `helm uninstall`
  runs a post-delete hook that removes the binary, the runtime files and the
  hostPath directories themselves.

## Limitations

- **Captures live on the node until uploaded** - a node deleted before its
  spool is uploaded (an autoscaler scale-down, a spot reclaim) loses those
  captures. `helm uninstall` deletes the spool too.
- **Cores rest unencrypted on node disk** - in root-only directories, until
  the daemon uploads them.
- **Disk speed sets the pace** - the kernel holds the faulting process until
  the handler has written the whole core to disk.
- **Size cap truncates** - past `--max-core-size` the core is cut, not
  selected. The manifest records `truncated`.
- **Retries end** - a capture that still fails to upload after
  `--spool-max-age-secs` (default 7 days) is deleted and logged as failed.

## Capture log and Events

The kernel gives the handler no stdout, so the handler reports every capture
to the daemon over a unix datagram socket (`/run/coredrop/events.sock` by
default). The daemon logs `capture spooled` when a capture reaches the spool,
and one `capture complete` line per capture once it is done - uploaded, or
ended without an upload. Failures and non-pod crashes are included:

```sh
$ kubectl -n <namespace> logs ds/coredrop | grep 'capture complete'
INFO coredrop::k8s_events: capture complete outcome="uploaded" namespace="default" pod="my-app-7f8b9c" container="app" ... manifest_key="local/<podUID>/<containerID>/1717000000-4242-manifest.json"
WARN coredrop::k8s_events: capture complete outcome="failed" ... error="draining core stream: ..."
```

A failed upload is logged as `bundle upload failed; will retry` with the
store's error; the capture stays in the spool until it uploads.

A crash the handler never sees, for example because the handler binary is
missing, shows up only in the node's kernel log (`dmesg`: `coredump: ...
pipe failed`).

Most people debugging a pod start at `kubectl`, so coredrop also reports each
capture as a Kubernetes Event on the crashing pod.
`kubectl describe pod`/`kubectl get events` then surfaces the crash directly:

```sh
$ kubectl get events --field-selector reason=CoreDumped
LAST SEEN   TYPE      REASON       OBJECT              MESSAGE
2m          Warning   CoreDumped   pod/my-app-7f8b9c   core dumped (signal SIGSEGV); artifacts at local/<podUID>/<containerID>/1717000000-4242-manifest.json
```

| Outcome | Event reason |
| --- | --- |
| Core uploaded | `CoreDumped` |
| Rate-limit budget exhausted | `CoreDumpSuppressed` |
| Spool full, core dropped | `CoreDumpSpoolFull` |
| Drain deadline passed, core dropped | `CoreDumpDrainDeadline` |
| No object store configured | `CoreDiscardedNoStore` |

A spooled capture gets its Event once it is uploaded, so the manifest key in
the message always exists in the bucket.

Only the daemon touches the API server, so the ServiceAccount token never
reaches the handler. A crash-looping pod bumps one Event's `series.count` instead of
creating a new object per crash. Events require crictl-enriched identity
(namespace + pod name) - a cgroup-only identity has no pod object to target
and is skipped.

Disable Events with `--no-events`/`CAPTURE_NO_EVENTS`, or the chart's
`events.enabled: false` (also skips rendering the events RBAC). The capture
log stays on.

## Retrieving artifacts

coredrop has no retrieval command - artifacts are plain objects at a
predictable key, so any object-store CLI works. Given a manifest key (from a
k8s Event's message, or listing the bucket) or its
`{cluster}/{podUID}/{containerID}/{timestamp}-{hostPID}` prefix:

### S3/S3-compatible (MinIO, etc.)

```sh
aws s3 cp s3://crash-artifacts/<prefix>-manifest.json - | jq .
aws s3 cp s3://crash-artifacts/<prefix>-core.zst core.zst && zstd -d core.zst
```

### GCS

```sh
gsutil cat gs://crash-artifacts/<prefix>-manifest.json | jq .
gsutil cp gs://crash-artifacts/<prefix>-core.zst core.zst && zstd -d core.zst
```

### Azure

```sh
az storage blob download --container-name crash-artifacts \
  --name "<prefix>-manifest.json" --auth-mode login -f - | jq .
az storage blob download --container-name crash-artifacts \
  --name "<prefix>-core.zst" --auth-mode login -f core.zst
```

Then `zstd -d core.zst` and `gdb <binary> core`, matching the manifest's
`build_id` against your symbol store.

## Retention

coredrop only writes artifacts; it never deletes them. Expire old crashes
with your object store's native lifecycle policy, keyed on the `{cluster}/`
prefix - see [charts/coredrop](charts/coredrop/README.md#retention) for
S3/GCS/Azure examples.

## Deploying

Deploy with the Helm chart - see [charts/coredrop](charts/coredrop/README.md)
for values, object-store credentials, and workload identity setup.

```sh
helm install coredrop charts/coredrop -n coredrop-system --create-namespace \
  --set capture.objectStore.url=s3://crash-artifacts
```

The DaemonSet runs as root with all capabilities dropped. The daemon writes
the node-global `core_pattern` and `core_pipe_limit` sysctls through two
bind-mounted sysctl files. See the chart README, "Pod privileges".

## Configuration

Every knob is a CLI flag with an env-var fallback (secrets are env-only).
Sizes take a Kubernetes quantity: a byte count, or a whole number with a
binary (`Ki`, `Mi`, `Gi`, ...) or decimal (`k`, `M`, `G`, ...) suffix.
The main ones:

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--cluster` | `CAPTURE_CLUSTER` | `local` | First segment of the object key |
| `--store-url` | `CAPTURE_STORE_URL` | unset | `s3://...`/`gs://...`/`az://...`; unset = keep no cores |
| `--max-core-size` | `CAPTURE_MAX_CORE_SIZE` | `2Gi` | Stored core cap per crash; 0 = unlimited |
| `--max-cores-per-hour` | `CAPTURE_MAX_CORES_PER_HOUR` | 3 | Per-pod core budget; 0 = unlimited |
| `--drain-deadline-secs` | `CAPTURE_DRAIN_DEADLINE_SECS` | 60 | Deadline for writing one core into the spool; past it the core is dropped to free the `core_pipe_limit` slot, and the snapshot and manifest are still spooled; 0 = no deadline |
| `--spool-dir` | `CAPTURE_SPOOL_DIR` | `/var/lib/coredrop/spool` | Spool root on the node |
| `--spool-max-size` | `CAPTURE_SPOOL_MAX_SIZE` | `10Gi` | Spool size cap; 0 = no cap |
| `--spool-min-free-percent` | `CAPTURE_SPOOL_MIN_FREE_PERCENT` | 15 | Free-space floor of the spool's filesystem |
| `--spool-max-age-secs` | `CAPTURE_SPOOL_MAX_AGE_SECS` | 7 days | Age past which a capture that still fails to upload is deleted |
| `--upload-concurrency` | `CAPTURE_UPLOAD_CONCURRENCY` | 2 | Captures uploaded at the same time |
| `--upload-timeout-secs` | `CAPTURE_UPLOAD_TIMEOUT_SECS` | 600 | Time limit for one upload attempt of one capture; 0 = no limit |
| `--shutdown-flush-secs` | `CAPTURE_SHUTDOWN_FLUSH_SECS` | 25 | Time the daemon keeps uploading after SIGTERM |
| `--crictl-timeout-secs` | `CRICTL_TIMEOUT_SECS` | 10 | `crictl inspect` timeout in the handler; 0 = no timeout |
| `--pipe-limit` | `CAPTURE_PIPE_LIMIT` | 128 | `core_pipe_limit` sysctl the daemon installs |
| `--no-redact` | `CAPTURE_NO_REDACT` | off | Pass `environ` and `cmdline` through un-redacted |
| `--cri-runtime-endpoint` | `CONTAINER_RUNTIME_ENDPOINT` | unset | CRI socket for crictl enrichment |
| `--no-events` | `CAPTURE_NO_EVENTS` | off | Disable k8s Event emission on capture; captures are still logged |

Object-store credentials (`AWS_ACCESS_KEY_ID`, `GOOGLE_SERVICE_ACCOUNT_KEY`,
`AZURE_STORAGE_ACCESS_KEY`, ...) are read from the environment only - never
flags - and only an allowlist of keys is read (`src/upload.rs`,
`ALLOWED_STORE_OPTS`). Only the daemon reads them: the handler config file
holds no credentials. The daemon writes that file mode `0600` in a `0700`
directory and rewrites it on startup. On `helm uninstall`, the post-delete
hook removes it, the capture-event socket, the rate-limit state and the
spool.

## License

MIT
