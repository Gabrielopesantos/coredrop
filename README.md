# coredrop

Standalone Kubernetes coredump handler: captures fault cores and `/proc`
snapshots to object storage, with a JSON manifest sidecar per crash.

When a containerized process crashes, the node's kernel invokes coredrop as
the `core_pattern` pipe handler. coredrop snapshots the faulting process's
`/proc` entry before the kernel reaps it, streams the core through zstd into
an object store (S3/GCS/Azure), enriches the crash with Kubernetes
identity via the CRI, and writes a JSON manifest next to the artifacts.

## Relation to core-dump-handler

coredrop is inspired by [IBM/core-dump-handler](https://github.com/IBM/core-dump-handler),
a well-known implementation of the DaemonSet + `core_pattern` approach for Kubernetes. It differs
in a few deliberate ways:

- **Streaming, no node disk** - core-dump-handler's composer writes a zip
  (core + metadata) to a hostPath directory, and a separate agent watches
  that directory and uploads it. coredrop is a single binary whose handler
  streams the core stdin -> zstd -> object store in one pass; the uncompressed
  core never lands on node disk or in memory, and there is no upload agent,
  watch loop, or disk quota to manage.
- **Pre-reap `/proc` snapshot** - coredrop captures `maps`, `smaps`, `fd`,
  `environ`, `limits`, `stack`, and the executable's GNU build-id while the
  kernel still holds the faulting process - state that no longer exists once
  the process is reaped.
- **Secret redaction by default** - captured `environ` values are redacted
  via a keyword list plus an entropy/shape heuristic before they leave the
  node.
- **Built-in guards** - per-container upload rate limit and a stored core size
  cap, so a crash-looping pod cannot flood the bucket.
- **Loose artifacts + manifest, not a zip** - core, `/proc` snapshot tar, and
  JSON manifest are separate objects under a predictable key scheme, so
  tooling can read the manifest without downloading a multi-GB archive.
- **Multi-cloud via `object_store`** - S3, GCS, and Azure (including workload
  identity) natively, not only the S3 API.

## How it works

The single `coredrop` binary runs in two modes:

- **Daemon** (`coredrop`) - the long-running DaemonSet container. At startup
  it points `/proc/sys/kernel/core_pattern` at the handler, raises
  `core_pipe_limit` so more concurrent core dumps can be held open, and writes the
  handler's config to a hostPath file. On shutdown it restores the previous
  sysctl values.
- **Capture handler** (`coredrop capture %P %s %t %E`) - the short-lived
  process the kernel exec's per fault, in the host namespaces, with the core
  on stdin. Because the kernel exec's it with a clean environment, it reads
  the daemon-written hostPath config instead of env vars.

Handler flow, ordered by time-criticality:

1. **Pre-reap `/proc` snapshot** - `maps`, `smaps`, `status`, `fd`, `limits`,
   `environ`, `cmdline`, `stack`, `exe`, plus the executable's GNU build-id.
   Must complete while the kernel holds the faulting process; none of it is
   reconstructable afterwards.
2. **Core drain** - stdin -> zstd -> sha256 -> multipart upload, fully
   streaming. The uncompressed core never lands on disk or in memory.
3. **Proc snapshot upload** - the small in-memory tar, buffered PUT.
4. **CRI enrichment** - best-effort `crictl inspect` for namespace, pod name,
   container name, image, restart count. Failure degrades to cgroup-derived
   identity (pod UID + container ID), which is always present.
5. **Manifest write** - the JSON record, written last so a manifest always
   points at complete artifacts.

Objects land at:

```
{cluster}/{podUID}/{containerID}/{timestamp}-core.zst
{cluster}/{podUID}/{containerID}/{timestamp}-proc.tar
{cluster}/{podUID}/{containerID}/{timestamp}-manifest.json
```

## Safety rails

- **Secret redaction** - `environ` values are redacted by default via a
  curated keyword list plus an entropy/shape heuristic (JWTs, PEM blocks,
  high-entropy tokens). `--no-redact` opts out. Cores themselves are
  secret-bearing regardless; treat the bucket accordingly.
- **Size cap** - stored core bytes are capped per crash (default 2 GiB);
  the remainder of the stream is drained but not stored.
- **Rate limit** - per-container core-upload budget (default 3/hour) so a
  crash loop can't flood the bucket. Suppressed crashes still get a proc
  snapshot and manifest, just no core. The limiter fails open: a broken
  limiter never loses a core.
- **Restore on shutdown** - the daemon restores the node's original
  `core_pattern` and `core_pipe_limit` when it stops.

## Limitations

`core_pipe_limit` caps how many crashes the kernel will hold open for the
handler at once (coredrop sets 128). Per `man 5 core`, crashes beyond the
cap are skipped silently: the handler is never exec'd, so there is no
manifest, log line, or k8s Event - the only trace is
`Pid <N> over core_pipe_limit` in the node's kernel log. The upload
deadline (`--upload-deadline-secs`, default 300) keeps a slow store from
pinning slots and widening that window. `desm`/`journalctl -k` can be used to
check for dropped crashes.

## Events

Node logs and bucket listing are the only crash-discovery paths otherwise
available, but most people debugging a pod start at `kubectl`. coredrop also
reports each capture as a Kubernetes Event on the crashing pod, so
`kubectl describe pod` / `kubectl get events` surfaces the crash directly:

```sh
$ kubectl get events --field-selector reason=CoreDumped
LAST SEEN   TYPE      REASON       OBJECT              MESSAGE
2m          Warning   CoreDumped   pod/my-app-7f8b9c   core dumped (signal SIGSEGV); artifacts at local/<podUID>/<containerID>/1717000000-manifest.json
```

| Outcome | Event reason |
| --- | --- |
| Core uploaded | `CoreDumped` |
| Rate-limit budget exhausted | `CoreDumpSuppressed` |
| No object store configured | `CoreDiscardedNoStore` |

The kernel-exec'd handler reports the capture to the daemon over a unix
datagram socket (`/run/coredrop/events.sock` by default); only the daemon
touches the API server, so the ServiceAccount token never reaches the
handler. A crash-looping pod bumps one Event's `series.count` instead of
creating a new object per crash. Events require crictl-enriched identity
(namespace + pod name) - a cgroup-only identity has no pod object to target
and is skipped.

Disable with `--no-events` / `CAPTURE_NO_EVENTS`, or the chart's
`events.enabled: false` (also skips rendering the events RBAC).

## Retrieving artifacts

coredrop has no retrieval command - artifacts are plain objects at a
predictable key, so any object-store CLI works. Given a manifest key (from a
k8s Event's message, or listing the bucket) or its
`{cluster}/{podUID}/{containerID}/{timestamp}` prefix:

**S3 / S3-compatible (MinIO, etc.)**

```sh
aws s3 cp s3://crash-artifacts/<prefix>-manifest.json - | jq .
aws s3 cp s3://crash-artifacts/<prefix>-core.zst core.zst && zstd -d core.zst
```

**GCS**

```sh
gsutil cat gs://crash-artifacts/<prefix>-manifest.json | jq .
gsutil cp gs://crash-artifacts/<prefix>-core.zst core.zst && zstd -d core.zst
```

**Azure**

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

The DaemonSet needs `privileged: true` and `hostPID: true`: the handler is
kernel-exec'd in host namespaces and the daemon writes node-global sysctls.

## Configuration

Every knob is a CLI flag with an env-var fallback (secrets are env-only).
The main ones:

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--cluster` | `CAPTURE_CLUSTER` | `local` | First segment of the object key |
| `--store-url` | `CAPTURE_STORE_URL` | unset | `s3://…` / `gs://…` / `az://…`; unset = drain but store nothing |
| `--max-core-bytes` | `CAPTURE_MAX_CORE_BYTES` | 2 GiB | Stored core cap per crash; 0 = unlimited |
| `--max-cores-per-hour` | `CAPTURE_MAX_CORES_PER_HOUR` | 3 | Per-container upload budget; 0 = unlimited |
| `--upload-deadline-secs` | `CAPTURE_UPLOAD_DEADLINE_SECS` | 300 | Per-core upload deadline; past it the upload is abandoned to free the `core_pipe_limit` slot; 0 = no deadline |
| `--pipe-limit` | `CAPTURE_PIPE_LIMIT` | 128 | `core_pipe_limit` sysctl the daemon installs |
| `--no-redact` | `CAPTURE_NO_REDACT` | off | Pass `environ` through un-redacted |
| `--cri-runtime-endpoint` | `CONTAINER_RUNTIME_ENDPOINT` | unset | CRI socket for crictl enrichment |
| `--no-events` | `CAPTURE_NO_EVENTS` | off | Disable k8s Event emission on capture |

Object-store credentials (`AWS_ACCESS_KEY_ID`, `GOOGLE_SERVICE_ACCOUNT_KEY`,
`AZURE_STORAGE_ACCESS_KEY`, …) are read from the environment only - never
flags - and only an allowlist of keys is forwarded to the handler
(`src/upload.rs`, `ALLOWED_STORE_OPTS`).

## Development

```sh
cargo build
cargo test
```

A Nix flake provides the dev shell (`nix develop`) with all tooling.

For an end-to-end live test - a single-node k3s cluster in a lima VM, an
in-cluster MinIO, a crashing demo workload, and a smoke test that asserts the
full kernel-to-bucket path - see [deploy/local](deploy/local/README.md).

## Project layout

```
src/            the coredrop binary (daemon + kernel-exec'd capture handler)
tests/          integration tests (handler flow, crictl enrichment)
charts/coredrop Helm chart
deploy/local    local live-test stack (lima + k3s + MinIO + smoke test)
```

## TODO

When a crash storm exceeds `core_pipe_limit`, the kernel skips the excess 
dumps silently (see Limitations); the only trace is the node's kernel log.
The daemon could tail `/dev/kmsg` for `Pid <N> over core_pipe_limit` and 
surface each drop - as a metric once coredrop grows a metrics endpoint 
(it has none today), and possibly as a node-scoped k8s Event. Until then
the kernel log is the only signal.

## License

MIT
