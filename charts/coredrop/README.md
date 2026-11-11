# coredrop Helm chart

Deploys the coredrop DaemonSet: a root pod per node, with no capabilities, that
installs the kernel `core_pattern` handler. The handler writes captured cores,
`/proc` snapshots, and manifests to a spool on the node's disk, and the daemon
uploads the spool to an object store.

## Install

```sh
helm install coredrop oci://ghcr.io/gabrielopesantos/charts/coredrop \
  -n coredrop-system --create-namespace \
  --set capture.objectStore.url=s3://crash-artifacts \
  --set capture.objectStore.config.AWS_REGION=us-east-1
```

Leaving `capture.objectStore.url` empty runs coredrop **observe-only**. The
handler writes nothing to the spool and does not read the core. Each crash is
still logged (`outcome="no-store-discard"`) and reported as a
`CoreDiscardedNoStore` Event. A URL that is set but unusable stops the daemon
at startup.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| capture.cluster | string | `"local"` | Cluster name - the first segment of every object key. |
| capture.crictlTimeoutSeconds | int | `10` | Timeout in seconds for `crictl inspect` in the handler; `0` = no timeout. |
| capture.drainDeadlineSeconds | int | `60` | Deadline in seconds for the handler to write a core into the spool; past it the core is dropped, and the snapshot and manifest are still spooled. `0` disables. |
| capture.filter.exclude | list | `[]` | Never capture crashes that match one of these entries; wins over `include`. Example: `[{namespace: kube-system}, {container: istio-proxy}]`. |
| capture.filter.include | list | `[]` | Capture only crashes that match one of these entries; empty captures all. Example: `[{namespace: "team-*"}]`. |
| capture.maxCoreSize | string | `"2Gi"` | Truncate stored cores at this size, as a Kubernetes quantity (`2Gi`, `500M`) or a byte count. `0` = no limit. |
| capture.maxCoresPerHour | int | `3` | Per-pod core upload budget per hour; excess crashes keep manifest + snapshot only. |
| capture.noRedact | bool | `false` | Pass `environ` and `cmdline` through un-redacted. Leave false; cores are secret-bearing. |
| capture.objectStore.config | object | `{}` | Non-secret object-store client options, rendered as plain env vars (allowlisted). |
| capture.objectStore.credentials | object | `{}` | Secret-bearing object-store options, rendered into a Secret and injected via envFrom (allowlisted). |
| capture.objectStore.url | string | `""` | Object store URL (`s3://bucket`, `gs://bucket` or `az://container`); empty disables upload. |
| cleanup.enabled | bool | `true` | Run the post-delete cleanup DaemonSet on `helm uninstall`. |
| cleanup.image | string | `"rancher/kubectl:v1.35.6"` | Image for the post-delete cleanup Job that drives the cleanup DaemonSet via kubectl. |
| corePattern.hostBinDir | string | `"/opt/coredrop/bin"` | Host path the handler binary is installed to (resolved in the host mount ns). |
| corePattern.hostRunDir | string | `"/run/coredrop"` | Host path for the handler config and rate-limit state. |
| corePattern.pipeLimit | int | `128` | `core_pipe_limit` sysctl the daemon installs alongside `core_pattern`. |
| cri.crictlPath | string | `"/usr/local/bin/crictl"` | Path to the crictl binary on the node (host mount ns), for the handler's enrichment. |
| cri.runtimeEndpoint | string | `""` | CRI endpoint for crictl enrichment; when set, its `unix://` path is also the socket mounted from the host. Empty lets crictl self-resolve on the node. |
| events.enabled | bool | `true` | Post a k8s Event on the crashing pod per capture; `false` also skips the events RBAC. |
| fullnameOverride | string | `""` | Override the full generated name of chart resources. |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy. |
| image.repository | string | `"ghcr.io/gabrielopesantos/coredrop"` | Container image repository (published on `app-v*` tags). |
| image.tag | string | `""` | Image tag; empty defaults to `.Chart.AppVersion`. |
| logLevel | string | `"info"` | `RUST_LOG` filter for the daemon. |
| metrics.enabled | bool | `true` | Serve `/metrics` on the daemon pod and add the `prometheus.io/*` scrape annotations. |
| metrics.podMonitor.enabled | bool | `false` | Render a Prometheus Operator `PodMonitor` for the daemon pods (needs the `monitoring.coreos.com` CRDs). |
| metrics.podMonitor.interval | string | `""` | Scrape interval; empty uses the Prometheus default. |
| metrics.podMonitor.labels | object | `{}` | Extra labels on the `PodMonitor`, for a Prometheus `podMonitorSelector`. |
| metrics.port | int | `9464` | Container port of the `/metrics` endpoint. |
| nameOverride | string | `""` | Override the chart name segment of resource names. |
| nodeSelector | object | `{}` | Pod node selector. |
| podLabels | object | `{}` | Extra labels merged onto the daemon pod template. |
| resources | object | `{"limits":{"memory":"256Mi"},"requests":{"cpu":"50m","memory":"64Mi"}}` | DaemonSet resource requests/limits. |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsUser":0,"seccompProfile":{"type":"RuntimeDefault"}}` | Daemon container securityContext. |
| serviceAccount.annotations | object | `{}` | Annotations on the daemon's ServiceAccount (e.g. cloud workload identity). |
| spool.hostDir | string | `"/var/lib/coredrop/spool"` | Host path of the spool. Use a directory for coredrop only: the handler creates `incoming/` and `ready/` in it, and `helm uninstall` deletes them. |
| spool.maxAgeSeconds | int | `604800` | Age in seconds past which a capture that still fails to upload is deleted (7 days default). |
| spool.maxSize | string | `"10Gi"` | Cap on the spool's total size, as a Kubernetes quantity (`10Gi`, `500M`) or a byte count. Only core writes check it: a core that does not fit is dropped or truncated, and snapshots and manifests are still written. `0` = no cap. |
| spool.minFreePercent | int | `15` | Free-space floor of the spool's filesystem, in percent; below it cores are dropped. |
| tolerations | list | `[{"operator":"Exists"}]` | Pod tolerations (defaults to tolerating everything so it runs on all nodes). |
| upload.concurrency | int | `2` | Captures uploaded at the same time. |
| upload.shutdownFlushSeconds | int | `25` | On pod shutdown, seconds the daemon keeps uploading the spool. The pod's termination grace period is this plus 5. |
| upload.timeoutSeconds | int | `600` | Time limit in seconds for one upload attempt of one capture; it is retried after. `0` = no limit. |

Only allowlisted object-store keys are read by the daemon. An unrecognized
key in either map fails `helm install`/`template` naming it and
the valid set - a typo can't reach the cluster and silently drop out:

- `capture.objectStore.config` (non-secret):
  - `AWS_REGION`
  - `AWS_DEFAULT_REGION`
  - `AWS_ENDPOINT`
  - `AWS_ALLOW_HTTP`
  - `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`
  - `AWS_ROLE_ARN`
  - `AWS_WEB_IDENTITY_TOKEN_FILE`
  - `GOOGLE_SERVICE_ACCOUNT`
  - `AZURE_STORAGE_ACCOUNT_NAME`
  - `AZURE_STORAGE_CLIENT_ID`
  - `AZURE_STORAGE_TENANT_ID`
  - `AZURE_CLIENT_ID`
  - `AZURE_TENANT_ID`
  - `AZURE_FEDERATED_TOKEN_FILE`
  - `AZURE_AUTHORITY_HOST`
- `capture.objectStore.credentials` (Secret):
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_SESSION_TOKEN`
  - `GOOGLE_SERVICE_ACCOUNT_KEY`
  - `AZURE_STORAGE_ACCESS_KEY`
  - `AZURE_STORAGE_CLIENT_SECRET`

### Plaintext endpoints

The MinIO/S3-compatible pattern (`AWS_ENDPOINT: http://...` plus
`AWS_ALLOW_HTTP: "true"`) uploads over unencrypted HTTP. A core is a full
memory image, so everything the process held - credentials, tokens, customer
data - crosses the cluster network in the clear. A plaintext endpoint can be
used for in-cluster or dev stores only; anywhere else, TLS should be used.
The daemon logs a warning at startup when it is configured with one.

## Events RBAC

`events.enabled: true` (default) renders a `ClusterRole` +
`ClusterRoleBinding` granting `create`/`patch` on `events` in the
`events.k8s.io` API group, bound to the DaemonSet's `ServiceAccount`. It's
cluster-scoped because a crash can happen in any namespace the node hosts
pods for - see [Capture log and Events](../../docs/capture-log-and-events.md)
for what gets posted. Set `events.enabled: false` to skip both the RBAC objects and the
daemon's socket listener; the handler then skips the report with no error.

## Retention

coredrop only writes artifacts - it never deletes them or sweeps the bucket.
Expire old crashes with your object store's native lifecycle policy, scoped
to the `capture.cluster` prefix so it doesn't reach into unrelated bucket
contents:

**S3** (bucket lifecycle rule, e.g. via Terraform/`aws s3api`):

```json
{
  "Rules": [{
    "ID": "coredrop-expire",
    "Filter": { "Prefix": "local/" },
    "Status": "Enabled",
    "Expiration": { "Days": 30 },
    "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
  }]
}
```

**GCS** (bucket lifecycle rule):

```json
{
  "rule": [{
    "action": { "type": "Delete" },
    "condition": { "age": 30, "matchesPrefix": ["local/"] }
  }]
}
```

**Azure** (Blob Storage lifecycle management policy):

```json
{
  "rules": [{
    "name": "coredrop-expire",
    "type": "Lifecycle",
    "definition": {
      "filters": { "blobTypes": ["blockBlob"], "prefixMatch": ["local/"] },
      "actions": { "baseBlob": { "delete": { "daysAfterModificationGreaterThan": 30 } } }
    }
  }]
}
```

Replace `local/` with your `capture.cluster` value.

The daemon aborts a multipart upload that fails or passes
`upload.timeoutSeconds`. Some cases can still leave an incomplete multipart
upload. A normal listing does not show it, but it is billed:

- The time limit passes during the final complete-multipart-upload call. The
  upload cannot be aborted at that point.
- The daemon is killed mid-upload, or the shutdown flush ends mid-upload.

The capture stays in the spool in each case, and its next upload starts over.

Add incomplete-multipart cleanup to the expiry rule: S3's
`AbortIncompleteMultipartUpload` (shown above) or GCS's
`AbortIncompleteMultipartUpload` lifecycle action. Azure deletes uncommitted
blocks on its own after about a week.

## Spool

```
crash --> handler --> spool.hostDir/incoming/<capture>/   (being written)
                  --> spool.hostDir/ready/<capture>/      (complete)
daemon --> uploads ready/ (upload.concurrency at a time) --> deletes the capture
```

- **Size.** `spool.maxSize` caps the spool, and `spool.minFreePercent` keeps
  free space on its filesystem. The spool shares `/var/lib` with the kubelet,
  which evicts pods under disk pressure, so keep both conservative. Only core
  writes check them: snapshots and manifests are still written when the spool
  is full. When the spool is full before a core starts, the core is dropped;
  its snapshot and manifest are still spooled, with
  `skipped_reason: spool_full`, and the pod gets a `CoreDumpSpoolFull` Event.
  When the spool fills while a core is written, the core is kept up to that
  point, marked `truncated_reason: spool_full`, and uploaded as usual. The
  handler checks the spool again while it writes a core, so concurrent crashes
  count against the cap; each one can pass it by a few MiB.
- **Retries.** A capture that fails to upload stays in the spool and is retried
  with exponential backoff (10s, doubling, up to 10 minutes). Past
  `spool.maxAgeSeconds` it is deleted and logged as `outcome="failed"`.
- **Shutdown.** On SIGTERM the daemon restores `core_pattern` first, then
  keeps uploading for `upload.shutdownFlushSeconds`. What is left stays on
  the node for the next daemon.
- **Node loss.** A capture on a node that is deleted before its upload (an
  autoscaler scale-down, a spot reclaim) is lost.
- **At rest.** Spooled cores are unencrypted on the node's disk, in
  directories readable only by root.

## Rate limit

`capture.maxCoresPerHour` (default 3) is a per-pod sliding-window budget. The
daemon stores the window state in `recent.json` on the same hostPath as the
handler config (`corePattern.hostRunDir`). The daemon keeps that
state across its own restarts, upgrades and node drains: otherwise a
crash-looping pod could evade its budget by forcing a daemon restart. Only
`helm uninstall` clears it, via the post-delete cleanup hook below. A workload
that reuses the same pod UID across a fast daemon restart therefore resumes
its existing window rather than getting a new one.

## Workload identity

For IRSA/GKE Workload Identity/AKS Workload Identity, leave
`capture.objectStore.credentials` empty and annotate the service account:

```yaml
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/coredrop
```

Each provider's admission webhook injects credentials as env vars into the
daemon pod, which does all the uploads; the daemon reads only the
allowlisted keys (above):

- **IRSA (EKS):** the pod-identity webhook injects `AWS_ROLE_ARN` +
  `AWS_WEB_IDENTITY_TOKEN_FILE` (+ `AWS_DEFAULT_REGION`) - wired to
  assume-role-with-web-identity.
- **AKS workload identity:** the webhook only injects credentials into pods
  labeled `azure.workload.identity/use: "true"`; set that via `podLabels` and
  annotate the ServiceAccount with `azure.workload.identity/client-id` per
  Azure's docs:

  ```yaml
  podLabels:
    azure.workload.identity/use: "true"
  serviceAccount:
    annotations:
      azure.workload.identity/client-id: "<client-id>"
  ```

  The webhook then injects `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
  `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_AUTHORITY_HOST` - all read.
- **GKE workload identity:** no env vars needed - the daemon authenticates
  through the GKE metadata server as the pod's Kubernetes ServiceAccount,
  same as any other workload.

## Health

Pod status is the health surface - the daemon serves no health endpoint,
having no control plane to serve one from.

Startup failures are fatal. The daemon exits non-zero when it cannot bind
the capture-event socket, write the handler config, or install `core_pattern`,
so a node whose capture path never came up shows `CrashLoopBackOff` rather than
a Ready pod that silently captures nothing.

Drift after startup is caught by the readiness and liveness probes, which
exec `coredrop check`. It verifies that `core_pattern` still routes to our
handler, that the handler config is present and valid, and that the capture-event
socket is still bound. Another agent on the node (a competing coredump handler, a
node-config daemon, a wiped `/run`) can change this state while the daemon looks
healthy. A failed probe restarts the pod, and at startup the daemon reinstalls
`core_pattern` and rewrites the config. In-flight captures are separate
kernel-exec'd processes and survive the restart.

## Pod privileges

The pods run as root with all capabilities dropped. `install-handler` copies
the binary to `corePattern.hostBinDir`. The daemon writes `handler.json`,
binds `events.sock` in `corePattern.hostRunDir`, reads and deletes captures
in `spool.hostDir`, and writes two node-global sysctls:

```
node                                     daemon container
/proc/sys/kernel/core_pattern     --->   /host/sysctl/core_pattern     (rw bind)
/proc/sys/kernel/core_pipe_limit  --->   /host/sysctl/core_pipe_limit  (rw bind)
                                         /proc/sys                     (read-only)
```

A sysctl has one value for the whole node, so a write through the bind
mount changes it on the node. The kernel accepts the write from uid 0 in the
host user namespace with no capability. Do not set `hostUsers: false`: a user
namespace makes the write fail. On SELinux nodes, the container type may also
need `securityContext.seLinuxOptions.type: spc_t`.

This is defense in depth, not a security boundary. A process that can write
`core_pattern` can make the kernel run any program as root on the node. The
hostPath volumes also put the namespace at the Pod Security `privileged`
level.

The chart renders the daemon's settings into the `<release>-config`
ConfigMap, mounted at `/etc/coredrop/config.yaml`; a change restarts the
daemons through the `checksum/config` pod annotation. Store credentials stay
in the Secret.

The kernel-exec'd handler does not see env vars on the pod, so its config
and the rate-limit state live on a hostPath (`corePattern.hostRunDir`). The
config holds no credentials. The handler never reaches the network; the
daemon uploads from its pod, so the object-store endpoint can be a cluster
DNS name.

Uninstalling the release stops the daemon, which restores the node's original
`core_pattern` on shutdown.

## CRI socket

`cri.runtimeEndpoint` is empty by default, and that is the recommended
setting. The handler is kernel-exec'd in the host mount namespace, so it shells
out to the node's own crictl against the node's own socket - it never goes
through a mount in this pod. Leaving the endpoint unset means crictl resolves
it the way it already knows how: `/etc/crictl.yaml` and then its built-in
fallbacks for containerd, CRI-O and cri-dockerd.

Set it when you want the endpoint fixed and checked at install time. A set
`unix://` value is passed to crictl as `CONTAINER_RUNTIME_ENDPOINT` and,
minus the prefix, mounted read-only from the node as `hostPath: type: Socket` -
so a wrong path makes the kubelet refuse to start the pod rather than running
it with crictl pointed at nothing. This makes a best-effort feature block the
install, so use it on a fleet with one known runtime, not on a mixed one.

A non-`unix://` endpoint (e.g. `tcp://`) is never mounted and cannot be checked
that way; the daemon logs a warning at startup when a configured socket does
not exist, and an info line when no endpoint is configured at all.

Enrichment never blocks a capture: without it the manifest still carries the
cgroup-derived `podUID` and `containerID`, but not namespace, pod name,
container name, image, or restart count.

## Uninstall cleanup

The daemon does not remove its node-local files on shutdown - a shutdown is
usually a restart, upgrade or node drain, not an uninstall. Removing them is
instead a `post-delete` hook (`cleanup.enabled`, on by default): a Job that
applies a short-lived DaemonSet, waits for it to roll out on every
node, then deletes it. On each node that worker removes the handler binary,
`handler.json`, `events.sock` and `recent.json`, the spool's `incoming/` and
`ready/` directories, then the `corePattern.hostBinDir`,
`corePattern.hostRunDir` and `spool.hostDir` directories themselves.
If the daemon was killed without a clean shutdown, the worker also resets
`core_pattern` to the kernel default `core` if it still points at the handler.

Uninstalling deletes every capture still in the spool. Wait for the spool to
drain first if those captures matter: the daemon logs `capture complete` for
each upload.

Two details:

- The worker mounts the **parent** of each configured directory, not the
  directory itself. A hostPath mounted at its own path is a mount point inside
  the container, and `rmdir` on a mount point fails with `EBUSY` - the files
  would be removed but the directories would survive.
- Nothing owns that worker DaemonSet: `kubectl` creates it after the release is
  already deleted. If the hook fails partway (its deadline is 180s), the worker
  can be left running. The next `helm uninstall` reaps it before starting, but
  if you reinstall in between, delete it first - it will wipe
  the handler you just installed:

  ```console
  kubectl -n <namespace> delete daemonset -l coredrop.io/cleanup=true
  ```

Set `cleanup.enabled: false` to skip the hook entirely and leave the node
files in place.
