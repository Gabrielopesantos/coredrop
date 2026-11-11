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
  a crash never waits for the network. The daemon uploads a bounded number of
  captures at a time and retries failures with backoff. The uncompressed core
  never lands on disk: it goes stdin -> zstd -> spool in one pass.
- **Credentials stay in the daemon** - the kernel-exec'd handler has no store
  credentials and makes no network calls.
- **Pre-reap `/proc` snapshot** - process state is captured while the kernel
  still holds the faulting process.
- **Safe by default** - secrets redacted, core size capped, uploads rate
  limited per pod (see [Safety rails](#safety-rails)).
- **Loose artifacts + manifest** - core, `/proc` snapshot tar, and JSON
  manifest are separate objects under a predictable key scheme, so tooling
  can read the manifest without downloading a multi-GB core.
- **Multi-cloud via `object_store`** - S3, GCS, and Azure (including workload
  identity).

## How it works

The single `coredrop` binary runs in three modes:

- **Daemon** (`coredrop`) - the DaemonSet container. It installs
  `core_pattern` and `core_pipe_limit`, writes the handler config to a
  hostPath file, and uploads the spool. On shutdown it restores the previous
  sysctl values.
- **Capture handler** (`coredrop capture %P %s %t %E`) - the process the
  kernel exec's per fault, in the host namespaces, with the core on stdin.
- **Health check** (`coredrop check`) - the chart's probes. It fails when
  `core_pattern`, the handler config or the capture-event socket drift.

Handler flow, ordered by time-criticality:

1. **Pre-reap `/proc` snapshot** - `maps`, `smaps`, `status`, `fd`, `limits`,
   `environ`, `cmdline`, `stack`, `exe`, plus the executable's GNU build-id.
2. **Core drain** - stdin -> zstd -> sha256 -> spool file. The kernel then
   reaps the crashed process.
3. **Proc snapshot** - the small in-memory tar, written to the spool.
4. **CRI enrichment** - best-effort `crictl inspect` for namespace, pod and
   container identity. Falls back to cgroup-derived pod UID + container ID.
5. **Manifest** - the JSON record. The capture moves from `incoming/` to
   `ready/`.

The daemon uploads each ready capture in the order core, proc snapshot,
manifest, so a manifest in the bucket always points at complete artifacts.
Objects land at:

```sh
{cluster}/{podUID}/{containerID}/{timestamp}-{hostPID}-core.zst
{cluster}/{podUID}/{containerID}/{timestamp}-{hostPID}-procsnapshot.tar
{cluster}/{podUID}/{containerID}/{timestamp}-{hostPID}-manifest.json
```

## Safety rails

Defaults are in [docs/configuration.md](docs/configuration.md).

- **Secret redaction** - `environ` values and `cmdline` secrets are redacted
  by keyword list plus an entropy/shape heuristic (JWTs, PEM blocks, URL
  credentials). `--no-redact` opts out. Cores themselves are secret-bearing
  regardless; treat the bucket accordingly.
- **Size cap** - stored core bytes are capped per crash; the rest of the
  stream is drained but not stored. The manifest records `truncated`.
- **Spool quota** - the spool has a size cap and a free-space floor. A core
  that does not fit is dropped or truncated; its snapshot and manifest are
  still written (see the [chart README](charts/coredrop/README.md#spool)).
- **Rate limit** - per-pod core budget, so a crash loop can't flood the spool
  or the bucket. Suppressed crashes still get a snapshot and manifest. The
  limiter fails open.
- **Restore on shutdown** - the daemon restores the node's original
  `core_pattern` and `core_pipe_limit` when it stops. `helm uninstall`
  removes the node files.

## Limitations

- **Captures live on the node until uploaded** - a node deleted before its
  spool is uploaded (an autoscaler scale-down, a spot reclaim) loses those
  captures. `helm uninstall` deletes the spool too.
- **Cores rest unencrypted on node disk** - in root-only directories, until
  the daemon uploads them.
- **Disk speed sets the pace** - the kernel holds the faulting process until
  the handler has written the whole core to disk.
- **Retries end** - a capture that still fails to upload after
  `--spool-max-age-secs` is deleted and logged as failed.

## Deploying

Deploy with the Helm chart - see [charts/coredrop](charts/coredrop/README.md)
for values, object-store credentials, workload identity, pod privileges and
retention.

```sh
helm install coredrop charts/coredrop -n coredrop-system --create-namespace \
  --set capture.objectStore.url=s3://crash-artifacts
```

Without a store URL, coredrop runs observe-only: crashes are logged and
reported, no cores are kept.

## Finding a crash

```sh
kubectl get events --field-selector reason=CoreDumped
prefix='local/<podUID>/<containerID>/1717000000-4242'  # manifest key without -manifest.json
aws s3 cp "s3://crash-artifacts/${prefix}-core.zst" core.zst && zstd -d core.zst
```

The Event message holds the manifest key. coredrop never deletes artifacts:
expire them with a bucket lifecycle policy.

## Documentation

- [Configuration](docs/configuration.md) - flags, env vars, credentials.
- [Capture log and Events](docs/capture-log-and-events.md) - log lines and
  Event reasons.
- [Metrics](docs/metrics.md) - Prometheus metrics, example alerts and the Grafana dashboard.
- [Retrieving artifacts](docs/retrieving-artifacts.md) - S3, GCS and Azure
  commands.
- [Helm chart](charts/coredrop/README.md) - values, spool, rate limit, health,
  pod privileges, retention, uninstall.

## License

MIT
