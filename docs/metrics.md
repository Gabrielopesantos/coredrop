# Metrics

The daemon serves Prometheus metrics on `:9464/metrics` (OpenMetrics text
format). Use them for alerts and dashboards across the fleet. Use the
[capture log and Events](capture-log-and-events.md) to find one crash: the
metrics do not hold pod names, object keys or errors.

```
 handler --datagram--> daemon listener --> coredrop_captures_total{outcome}
                                       \-> coredrop_events_posted_total{result}
 spool   <--scan------ daemon uploader --> coredrop_upload_attempts_total{result}
                                       \-> coredrop_spool_bundles, coredrop_spool_bytes
 Prometheus --GET /metrics--> daemon pod
```

The kernel-exec'd handler keeps no metrics. It reports every capture to the
daemon, and the daemon counts it.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `coredrop_captures_total` | counter | `outcome` | Captures by final outcome (see below) |
| `coredrop_upload_attempts_total` | counter | `result` = `ok`/`error` | Upload attempts of spooled captures |
| `coredrop_bundles_expired_total` | counter | - | Spooled captures deleted because they were past `--spool-max-age-secs` |
| `coredrop_spool_bundles` | gauge | - | Captures in the spool waiting for upload |
| `coredrop_spool_bytes` | gauge | - | Size of the spool, `incoming/` and `ready/` |
| `coredrop_events_posted_total` | counter | `result` = `ok`/`error` | k8s Event create and patch requests |
| `coredrop_event_payloads_dropped_total` | counter | - | Capture datagrams the daemon could not parse |
| `coredrop_build_info` | gauge | `version` | Always 1 |

`outcome` is one of `uploaded`, `suppressed-rate-limit`, `spool-full`,
`drain-deadline`, `skipped-non-k8s`, `no-store-discard`, `failed`. A spooled
capture is counted once, when it is done: `uploaded`, its handler outcome
(`suppressed-rate-limit`, `spool-full`, `drain-deadline`) after its upload,
or `failed` when it expires. Every series starts at 0, so `increase()` sees
the first capture.

The uploader updates the spool gauges when it scans the spool: at startup,
every 30 s, and when the handler reports a spooled capture.

## Example alerts

```yaml
# A capture failed. The capture log has the error.
- alert: CoredropCaptureFailed
  expr: increase(coredrop_captures_total{outcome="failed"}[1h]) > 0

# Uploads fail and none succeed: the store is down or the credentials are bad.
- alert: CoredropUploadsFailing
  expr: >
    increase(coredrop_upload_attempts_total{result="error"}[30m]) > 0
    and increase(coredrop_upload_attempts_total{result="ok"}[30m]) == 0

# Cores are dropped because the spool is full.
- alert: CoredropSpoolFull
  expr: increase(coredrop_captures_total{outcome="spool-full"}[1h]) > 0

# The spool is above 80% of the chart's spool.maxSize (10Gi here).
- alert: CoredropSpoolFilling
  expr: coredrop_spool_bytes > 0.8 * 10 * 1024 * 1024 * 1024
```

## Scraping

The chart (`metrics.enabled`, on by default) opens container port `metrics`
and adds the `prometheus.io/scrape`, `prometheus.io/port` and
`prometheus.io/path` pod annotations. With the Prometheus Operator, set
`metrics.podMonitor.enabled: true` to render a `PodMonitor`. Add the labels
your Prometheus selects on with `metrics.podMonitor.labels`.

Turn the endpoint off with `--metrics-addr=` (empty) or the chart's
`metrics.enabled: false`. If the daemon cannot bind the address, it logs an
error and runs on without the endpoint: capture does not depend on it.
