# coredrop Helm chart

Deploys the coredrop DaemonSet: a privileged, `hostPID` pod per node that
installs the kernel `core_pattern` handler and streams captured cores,
`/proc` snapshots, and manifests to an object store.

## Install

```sh
helm install coredrop oci://ghcr.io/gabrielopesantos/charts/coredrop \
  -n coredrop-system --create-namespace \
  --set capture.objectStore.url=s3://crash-artifacts \
  --set capture.objectStore.config.AWS_REGION=us-east-1
```

Leaving `capture.objectStore.url` empty runs coredrop with no upload: cores
are drained (so the kernel completes the dump) but nothing is stored.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| capture.cluster | string | `"local"` | Cluster name - the first segment of every object key. |
| capture.maxCoreBytes | int | `2147483648` | Truncate stored cores at this many bytes (2 GiB default). |
| capture.maxCoresPerHour | int | `3` | Per-container core upload budget per hour; excess crashes keep manifest + snapshot only. |
| capture.noRedact | bool | `false` | Pass `environ` through un-redacted. Leave false; cores are secret-bearing. |
| capture.objectStore.config | object | `{}` | Non-secret object-store client options, rendered as plain env vars (allowlisted). |
| capture.objectStore.credentials | object | `{}` | Secret-bearing object-store options, rendered into a Secret and injected via envFrom (allowlisted). |
| capture.objectStore.url | string | `""` | Object store URL (`s3://bucket`, `gs://bucket` or `az://container`); empty disables upload. |
| capture.uploadDeadlineSeconds | int | `300` | Core upload deadline in seconds; past it the upload is abandoned. 0 disables. |
| corePattern.hostBinDir | string | `"/opt/coredrop/bin"` | Host path the handler binary is installed to (resolved in the host mount ns). |
| corePattern.hostRunDir | string | `"/run/coredrop"` | Host path for the handler config and rate-limit state. |
| corePattern.pipeLimit | int | `128` | `core_pipe_limit` sysctl the daemon installs alongside `core_pattern`. |
| cri.crictlPath | string | `"/usr/local/bin/crictl"` | Path to the crictl binary on the node (host mount ns), for the handler's enrichment. |
| cri.runtimeEndpoint | string | `"unix:///run/containerd/containerd.sock"` | CRI endpoint for crictl enrichment. |
| cri.socketHostPath | string | `"/run/containerd/containerd.sock"` | Host path of the CRI socket, mounted so the in-pod crictl can reach it. |
| events.enabled | bool | `true` | Post a k8s Event on the crashing pod per capture; `false` also skips the events RBAC. |
| fullnameOverride | string | `""` | Override the full generated name of chart resources. |
| hostPID | bool | `true` | Share the host PID namespace (the kernel exec's the handler there). |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy. |
| image.repository | string | `"ghcr.io/gabrielopesantos/coredrop"` | Container image repository (published on `app-v*` tags). |
| image.tag | string | `""` | Image tag; empty defaults to `.Chart.AppVersion`. |
| logLevel | string | `"info"` | `RUST_LOG` filter for the daemon. |
| nameOverride | string | `""` | Override the chart name segment of resource names. |
| nodeSelector | object | `{}` | Pod node selector. |
| podLabels | object | `{}` | Extra labels merged onto the daemon pod template. |
| resources | object | `{"limits":{"memory":"256Mi"},"requests":{"cpu":"50m","memory":"64Mi"}}` | DaemonSet resource requests/limits. |
| securityContext.privileged | bool | `true` | Run the daemon privileged (required to write node-global sysctls). |
| serviceAccount.annotations | object | `{}` | Annotations on the daemon's ServiceAccount (e.g. cloud workload identity). |
| tolerations | list | `[{"operator":"Exists"}]` | Pod tolerations (defaults to tolerating everything so it runs on all nodes). |

Only allowlisted object-store keys are forwarded (see `src/upload.rs`,
`ALLOWED_STORE_OPTS`):

- `capture.objectStore.config` (non-secret):
  - `AWS_REGION`
  - `AWS_DEFAULT_REGION`
  - `AWS_ENDPOINT`
  - `AWS_ALLOW_HTTP`
  - `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`
  - `AWS_ROLE_ARN` (IRSA)
  - `AWS_WEB_IDENTITY_TOKEN_FILE` (IRSA)
  - `GOOGLE_SERVICE_ACCOUNT`
  - `AZURE_STORAGE_ACCOUNT_NAME`
  - `AZURE_STORAGE_CLIENT_ID`
  - `AZURE_STORAGE_TENANT_ID`
  - `AZURE_CLIENT_ID` (AKS workload identity)
  - `AZURE_TENANT_ID` (AKS workload identity)
  - `AZURE_FEDERATED_TOKEN_FILE` (AKS workload identity)
  - `AZURE_AUTHORITY_HOST` (AKS workload identity)
- `capture.objectStore.credentials` (Secret):
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_SESSION_TOKEN`
  - `GOOGLE_SERVICE_ACCOUNT_KEY`
  - `AZURE_STORAGE_ACCESS_KEY`
  - `AZURE_STORAGE_CLIENT_SECRET`

## Events RBAC

`events.enabled: true` (default) renders a `ClusterRole` +
`ClusterRoleBinding` granting `create`/`patch` on `events` in the
`events.k8s.io` API group, bound to the DaemonSet's `ServiceAccount`. It's
cluster-scoped because a crash can happen in any namespace the node hosts
pods for - see the main [README](../../README.md#events) for what gets
posted. Set `events.enabled: false` to skip both the RBAC objects and the
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

A capture abandoned mid-upload (the handler's upload deadline firing, or
the handler dying) can leave an incomplete multipart upload behind -
invisible to a normal listing but still billed. Pair the expiry rule with
incomplete-multipart cleanup: S3's `AbortIncompleteMultipartUpload` (shown
above), GCS's `AbortIncompleteMultipartUpload` lifecycle action; Azure
garbage-collects uncommitted blocks on its own after about a week.

## Workload identity

For IRSA / GKE Workload Identity / AKS Workload Identity, leave
`capture.objectStore.credentials` empty and annotate the service account:

```yaml
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/coredrop
```

Each provider's admission webhook injects credentials as env vars into the
daemon pod; only the keys in `ALLOWED_STORE_OPTS` (above) are forwarded on to
the kernel-exec'd handler:

- **IRSA (EKS):** the pod-identity webhook injects `AWS_ROLE_ARN` +
  `AWS_WEB_IDENTITY_TOKEN_FILE` (+ `AWS_DEFAULT_REGION`) - forwarded and wired
  to assume-role-with-web-identity.
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
  `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_AUTHORITY_HOST` - all forwarded.
- **GKE workload identity:** no env vars needed or forwarded - the handler
  authenticates via the node's metadata server automatically, same as any
  other GCE-hosted workload.

## Why privileged + hostPID

The daemon writes node-global sysctls (`core_pattern`, `core_pipe_limit`) and
installs the handler binary on a hostPath. The kernel exec's the handler in
the host PID/mount/network namespaces, so the handler's config and the
rate-limit state also live on a hostPath (`corePattern.hostRunDir`) - env vars on
the pod never reach the kernel-exec'd handler. Note the handler runs in the
host network namespace: the object-store endpoint must be resolvable and
reachable from the node itself (cluster DNS names won't resolve there).

Uninstalling the release stops the daemon, which restores the node's original
`core_pattern` on shutdown.
