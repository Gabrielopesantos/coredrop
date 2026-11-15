# Configuration

Every knob is a CLI flag, and can also be set in a YAML config file (see
[Config file](#config-file)). No knob reads an env var. Secrets are
env-only.
Sizes take a Kubernetes quantity: a byte count, or a whole number with a
binary (`Ki`, `Mi`, `Gi`, ...) or decimal (`k`, `M`, `G`, ...) suffix.
For the Helm values that set these flags, see the
[chart README](../charts/coredrop/README.md#values).

| Flag | Default | Purpose |
| --- | --- | --- |
| `--config` | unset | YAML config file that sets the other flags |
| `--cluster-name` | unset | Name of the cluster: the first segment of the object keys and the manifest's `cluster_name`; ASCII letters, digits, `.`, `_` and `-`. Unset leaves both out |
| `--store-url` | unset | `s3://bucket`/`gs://...`/`az://...`; a path after the bucket (`s3://bucket/coredumps`) is a key prefix, and keys in manifests and Events are relative to it; unset = observe-only: crashes are logged and reported, no cores are kept |
| `--max-core-size` | `2Gi` | Stored core cap per crash; 0 = unlimited |
| `--max-cores-per-hour` | 3 | Per-pod core budget; 0 = unlimited |
| `--drain-deadline-secs` | 60 | Deadline for writing one core into the spool; past it the core is dropped to free the `core_pipe_limit` slot, and the snapshot and manifest are still spooled. A core already truncated by the size cap or the spool budget keeps its stored part; 0 = no deadline |
| `--spool-dir` | `/var/lib/coredrop/spool` | Spool root on the node |
| `--spool-max-size` | `10Gi` | Spool size cap; 0 = no cap |
| `--spool-min-free-percent` | 15 | Free-space floor of the spool's filesystem |
| `--spool-max-age-secs` | 7 days | Age past which a capture that still fails to upload is deleted; 0 = never |
| `--upload-concurrency` | 2 | Captures uploaded at the same time |
| `--upload-timeout-secs` | 600 | Time limit for one upload attempt of one capture; 0 = no limit |
| `--shutdown-flush-secs` | 25 | Time the daemon keeps uploading after SIGTERM |
| `--crictl-timeout-secs` | 10 | `crictl inspect` timeout in the handler; 0 = no timeout |
| `--capture-filter` | empty | JSON (in the config file, YAML) include/exclude lists of crashes to capture; empty = all. See [Capture filter](#capture-filter) |
| `--pipe-limit` | 128 | `core_pipe_limit` sysctl the daemon installs |
| `--no-redact` | off | Pass `environ` and `cmdline` through un-redacted |
| `--cri-runtime-endpoint` | unset | CRI socket for crictl enrichment |
| `--no-events` | off | Disable k8s Event emission on capture; captures are still logged |
| `--metrics-addr` | `0.0.0.0:9464` | Address of the Prometheus `/metrics` endpoint; empty = no endpoint. See [Metrics](metrics.md) |
| `--handler-path` | `/opt/coredrop/bin/coredrop` | Host path of the handler binary that `core_pattern` runs; absolute, no whitespace |
| `--crictl-path` | unset | Path of crictl on the node, for the handler; unset uses the first that exists of `/usr/local/bin/crictl`, `/usr/bin/crictl` and `/home/kubernetes/bin/crictl` |
| `--proc-root` | `/proc` | `/proc` root the handler reads |
| `--core-pattern-path` | `/proc/sys/kernel/core_pattern` | File the `core_pattern` sysctl is written through |
| `--core-pipe-limit-path` | `/proc/sys/kernel/core_pipe_limit` | File the `core_pipe_limit` sysctl is written through |
| `--node-name` | the host name | Node name on k8s Events. The chart passes `spec.nodeName` |

## Config file

`--config <file>` reads a YAML mapping. Each key is a long flag name without
`--`, and each value is what that flag takes. `capture-filter` takes a YAML
mapping instead of JSON. A `null` value keeps the default. The daemon stops at
startup on an unknown key or a bad value.

```yaml
cluster-name: prod
store-url: s3://crash-artifacts
max-core-size: 2Gi
max-cores-per-hour: 3
no-events: false
capture-filter:
  exclude:
    - namespace: kube-system
```

A value comes from the first of these that sets it:

```
flag on the command line
  -> config file
    -> built-in default
```

The daemon reads the file once, at startup. `coredrop check` (the probes)
takes the same flags, so it finds the same paths. The Helm chart renders
every setting into a ConfigMap mounted at `/etc/coredrop/config.yaml`, and a
`checksum/config` pod annotation restarts the daemons when it changes. Env
on the pod holds only `RUST_LOG`, `NODE_NAME` (downward API, passed as
`--node-name=$(NODE_NAME)`) and the object-store client options and
credentials. Do not put credentials in the
file: anyone who can read the namespace can read a ConfigMap.

## Observe-only mode

Without `--store-url`, coredrop runs **observe-only**. The handler writes
nothing to the spool and does not read the core. Each crash is still logged
(`outcome="no-store-discard"`). It is also reported as a
`CoreDiscardedNoStore` Event when crictl resolves the pod's namespace and
name; without that identity, the daemon skips the Event.
Set a store URL to keep cores. A store URL that is set but unusable (not a
URL, an unknown scheme, a store that cannot be built) stops the daemon at
startup.

## Capture filter

The capture filter selects the crashes that coredrop captures. It has two
lists of entries, `include` and `exclude`:

- An entry holds glob patterns for `namespace`, `pod`, `container`, `image`
  and `exe`. `*` matches any run of characters, `?` one character. All
  patterns of one entry must match.
- A crash is captured when it matches an `include` entry (or `include` is
  empty) and matches no `exclude` entry. `exclude` wins.
- `exe` is the executable path, for example `/usr/bin/server`. Use
  `*/server` to match it in any directory.
- `image` is the image as the container runtime reports it, for example
  `docker.io/library/nginx:1.27`.

In the chart:

```yaml
capture:
  filter:
    include:
      - namespace: "team-*"
    exclude:
      - container: istio-proxy
      - namespace: ci
        pod: "flaky-test-*"
```

As a flag, the same filter is JSON:

```sh
--capture-filter '{"include":[{"namespace":"team-*"}],"exclude":[{"container":"istio-proxy"},{"namespace":"ci","pod":"flaky-test-*"}]}'
```

The daemon stops at startup when the filter is not valid: bad JSON, an
unknown key, an empty pattern, or an entry with no patterns.

The handler applies the filter before it reads the core:

```
cgroup identity (pod UID, container ID)
  -> crictl inspect          only when an entry uses namespace, pod,
                             container or image
  -> filter
       drop: core not read, nothing spooled, no Event,
             outcome="filtered" in the capture log
       keep: normal capture; crictl does not run again
```

When a filter uses namespace, pod, container or image, crictl runs while the
kernel holds the crashed process. That run times out after
`--crictl-timeout-secs`, but after 10 seconds at most, also when the flag is
0. When crictl fails, those fields are unknown, and the crash is captured:
an unknown field never matches an `exclude` entry, and never stops an
`include` entry from matching.

## Credentials

Object-store credentials (`AWS_ACCESS_KEY_ID`, `GOOGLE_SERVICE_ACCOUNT_KEY`,
`AZURE_STORAGE_ACCESS_KEY`, ...) are read from the environment only - never
flags - and only an allowlist of keys is read (`src/daemon/upload.rs`,
`ALLOWED_STORE_OPTS`). Only the daemon reads them: the handler config file
holds no credentials. The daemon writes that file mode `0600` in a `0700`
directory and rewrites it on startup. On `helm uninstall`, the post-delete
hook removes it, the capture-event socket, the rate-limit state and the
spool.
