# Configuration

Every knob is a CLI flag with an env-var fallback, and can also be set in a
YAML config file (see [Config file](#config-file)). Secrets are env-only.
Sizes take a Kubernetes quantity: a byte count, or a whole number with a
binary (`Ki`, `Mi`, `Gi`, ...) or decimal (`k`, `M`, `G`, ...) suffix.
For the Helm values that set these flags, see the
[chart README](../charts/coredrop/README.md#values).

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--config` | `CAPTURE_CONFIG_FILE` | unset | YAML config file that sets the other flags |
| `--cluster` | `CAPTURE_CLUSTER` | `local` | First segment of the object key |
| `--store-url` | `CAPTURE_STORE_URL` | unset | `s3://...`/`gs://...`/`az://...`; unset = observe-only: crashes are logged and reported, no cores are kept |
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
| `--capture-filter` | `CAPTURE_FILTER` | empty | JSON (in the config file, YAML) include/exclude lists of crashes to capture; empty = all. See [Capture filter](#capture-filter) |
| `--pipe-limit` | `CAPTURE_PIPE_LIMIT` | 128 | `core_pipe_limit` sysctl the daemon installs |
| `--no-redact` | `CAPTURE_NO_REDACT` | off | Pass `environ` and `cmdline` through un-redacted |
| `--cri-runtime-endpoint` | `CONTAINER_RUNTIME_ENDPOINT` | unset | CRI socket for crictl enrichment |
| `--no-events` | `CAPTURE_NO_EVENTS` | off | Disable k8s Event emission on capture; captures are still logged |
| `--metrics-addr` | `CAPTURE_METRICS_ADDR` | `0.0.0.0:9464` | Address of the Prometheus `/metrics` endpoint; empty = no endpoint. See [Metrics](metrics.md) |

## Config file

`--config <file>` reads a YAML mapping. Each key is a long flag name without
`--`, and each value is what that flag takes. `capture-filter` takes a YAML
mapping instead of JSON. A `null` value keeps the default. The daemon stops at
startup on an unknown key or a bad value.

```yaml
cluster: prod
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
  -> env var
    -> config file
      -> built-in default
```

The daemon reads the file once, at startup. `coredrop check` (the probes)
takes the same flags, so it finds the same paths. The Helm chart renders
every setting into a ConfigMap mounted at `/etc/coredrop/config.yaml`, and a
`checksum/config` pod annotation restarts the daemons when it changes. Env
on the pod holds only `RUST_LOG`, `NODE_NAME` (downward API) and the
object-store client options and credentials. Do not put credentials in the
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
flags - and only an allowlist of keys is read (`src/upload.rs`,
`ALLOWED_STORE_OPTS`). Only the daemon reads them: the handler config file
holds no credentials. The daemon writes that file mode `0600` in a `0700`
directory and rewrites it on startup. On `helm uninstall`, the post-delete
hook removes it, the capture-event socket, the rate-limit state and the
spool.
