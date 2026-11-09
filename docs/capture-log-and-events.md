# Capture log and Events

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

## Kubernetes Events

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
reaches the handler. A crash-looping pod bumps one Event's `series.count`
instead of creating a new object per crash. Events require crictl-enriched
identity (namespace + pod name) - a cgroup-only identity has no pod object to
target and is skipped.

Disable Events with `--no-events`/`CAPTURE_NO_EVENTS`, or the chart's
`events.enabled: false` (also skips rendering the events RBAC). The capture
log stays on.
