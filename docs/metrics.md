# Metrics

The daemon serves Prometheus metrics on `:9464/metrics` (OpenMetrics text
format). Use them for alerts and dashboards across the fleet. Use the
[capture log and Events](capture-log-and-events.md) to find one crash: the
metrics do not hold pod names, object keys or errors.

```
 handler --datagram--> daemon listener --> coredrop_captures_total{outcome}
                                       |-> coredrop_core_bytes_total, coredrop_stored_bytes_total
                                       |-> coredrop_cores_truncated_total
                                       \-> coredrop_events_posted_total{result}
 spool   <--scan------ daemon uploader --> coredrop_upload_attempts_total{result}
                                       |-> coredrop_upload_duration_seconds
                                       \-> coredrop_spool_bundles, coredrop_spool_bytes,
                                           coredrop_spool_oldest_bundle_age_seconds
 core_pattern <--read every 30 s-- daemon --> coredrop_core_pattern_installed
 Prometheus --GET /metrics--> daemon pod
```

The kernel-exec'd handler keeps no metrics. It reports every capture to the
daemon, and the daemon counts it.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `coredrop_captures_total` | counter | `outcome` | Captures by final outcome (see below) |
| `coredrop_core_bytes_total` | counter | - | Uncompressed core bytes that done captures read from the kernel |
| `coredrop_stored_bytes_total` | counter | - | Compressed core bytes that done captures stored |
| `coredrop_cores_truncated_total` | counter | - | Done captures with a truncated core: size cap, full spool or stream error. The manifest has the reason |
| `coredrop_upload_attempts_total` | counter | `result` = `ok`/`error` | Upload attempts of spooled captures |
| `coredrop_upload_duration_seconds` | histogram | - | Time to upload one spooled capture: core, proc snapshot and manifest |
| `coredrop_bundles_expired_total` | counter | - | Spooled captures deleted because they were past `--spool-max-age-secs` |
| `coredrop_spool_bundles` | gauge | - | Captures in the spool waiting for upload |
| `coredrop_spool_bytes` | gauge | - | Size of the spool, `incoming/` and `ready/` |
| `coredrop_spool_oldest_bundle_age_seconds` | gauge | - | Age of the oldest capture waiting for upload; 0 when the spool is empty |
| `coredrop_spool_max_bytes` | gauge | - | `--spool-max-size`; 0 = no limit |
| `coredrop_core_pattern_installed` | gauge | - | 1 when the node's `core_pattern` routes cores to the handler, else 0 |
| `coredrop_events_posted_total` | counter | `result` = `ok`/`error` | k8s Event create and patch requests |
| `coredrop_event_payloads_dropped_total` | counter | - | Capture datagrams the daemon could not parse |
| `coredrop_build_info` | gauge | `version` | Always 1 |

`outcome` is one of `uploaded`, `suppressed-rate-limit`, `spool-full`,
`drain-deadline`, `skipped-non-k8s`, `no-store-discard`, `failed`. A spooled
capture is counted once, when it is done: `uploaded`, its handler outcome
(`suppressed-rate-limit`, `spool-full`, `drain-deadline`) after its upload,
or `failed` when it expires. The core byte counters and
`coredrop_cores_truncated_total` count at the same time. Every series starts
at 0, so `increase()` sees the first capture. Prometheus sees only the
captures after its first scrape of a daemon: a daemon that ran before
Prometheus scraped it shows fewer captures than its counter holds.

The uploader updates the spool gauges when it scans the spool: at startup,
every 30 s, and when the handler reports a spooled capture.

The daemon reads `core_pattern` every 30 s. When another tool (for example
systemd-coredump or apport) has replaced it, the gauge goes to 0 and the
daemon logs one warning with the new value. The daemon does not write the
pattern back. With the chart, the liveness probe (`coredrop check`) fails,
and after about 90 s the kubelet restarts the daemon, which installs the
pattern again. If the other tool keeps writing, the daemon keeps
restarting. Use `min_over_time()` on the gauge, because it is 0 only for a
short time.

## What the metrics cannot see

The daemon counts only the captures that the handler reports. These failures
do not change a metric:

- The handler binary is missing on the node. The kernel logs
  `coredump: ... pipe failed` in the node's `dmesg`.
- The handler stops before its bundle moves to `ready/`, for example when
  it is killed. The uploader deletes the bundle from `incoming/` later, and
  no metric counts it.

`coredrop_core_pattern_installed` and the `CoredropDaemonMissing` alert
catch the most frequent cause: no handler routing on a node.

## Alerts

[`alerts/coredrop.rules.yaml`](../alerts/coredrop.rules.yaml) holds
Prometheus alert rules. Load it with `rule_files`, or put its `groups` into a
Prometheus Operator `PrometheusRule`. The Helm chart does not install it.

| Alert | Fires when |
| --- | --- |
| `CoredropCorePatternNotInstalled` | `core_pattern` did not route to the handler at some time in the last 15 min |
| `CoredropCaptureFailed` | A capture failed in the last hour |
| `CoredropUploadsFailing` | All upload attempts of a daemon failed in the last 30 min |
| `CoredropUploadBacklog` | A capture waits for upload for more than 15 min |
| `CoredropSpoolFull` | A capture kept no core, or a truncated core, because the spool was full |
| `CoredropSpoolFilling` | The spool is above 80% of `coredrop_spool_max_bytes` for 10 min |
| `CoredropEventPostsFailing` | k8s Event requests failed in the last 30 min |
| `CoredropPayloadsDropped` | The daemon could not parse a capture report |
| `CoredropDaemonMissing` | A node has no available coredrop pod for 15 min (needs kube-state-metrics) |

## Dashboard

[`dashboards/coredrop.json`](../dashboards/coredrop.json) is a Grafana
dashboard for one or more coredrop deployments. It shows captures by outcome,
core sizes, uploads, the spool, `core_pattern` state, Kubernetes Event posts,
and the daemon pods with their versions. The Helm chart does not install it.

Load it in one of two ways:

- Grafana UI: Dashboards, New, Import, then upload the file.
- Grafana provisioning: add the file to your dashboard provider. With the
  Grafana dashboard sidecar (for example in kube-prometheus-stack), put it in
  a ConfigMap that has the sidecar's label:

  ```sh
  kubectl -n monitoring create configmap coredrop-dashboard \
    --from-file=coredrop.json=dashboards/coredrop.json
  kubectl -n monitoring label configmap coredrop-dashboard grafana_dashboard=1
  ```

The dashboard has these variables:

| Variable | Label | Source |
| --- | --- | --- |
| `datasource` | - | A Prometheus data source |
| `cluster` | `cluster` | The `PodMonitor` sets it from `capture.cluster`. A Thanos or Mimir setup can also add it from external labels |
| `namespace` | `namespace` | The scrape |
| `pod` | `pod` | The scrape |

The node panels use the `node` label, which the `PodMonitor` sets from the
pod's node. A scrape by the `prometheus.io` annotations usually gives only
`namespace` and `pod`. The filters still work: the value All also matches a
series that has no `cluster` label.

## Scraping

The chart (`metrics.enabled`, on by default) opens container port `metrics`
and adds the `prometheus.io/scrape`, `prometheus.io/port` and
`prometheus.io/path` pod annotations. With the Prometheus Operator, set
`metrics.podMonitor.enabled: true` to render a `PodMonitor`. Add the labels
your Prometheus selects on with `metrics.podMonitor.labels`. The `PodMonitor`
adds the label `node` (the pod's node) and, when `capture.cluster` is set,
the label `cluster` to every series.

Turn the endpoint off with `--metrics-addr=` (empty) or the chart's
`metrics.enabled: false`. If the daemon cannot bind the address, it logs an
error and runs on without the endpoint: capture does not depend on it.
