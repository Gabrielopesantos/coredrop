# Local live-test stack

End-to-end environment for coredrop's kernel-to-bucket path: a single-node
k3s cluster inside a lima VM, an in-cluster MinIO as the object store, a
crash-looping demo workload, a smoke test that asserts real captures land
in the bucket, and a lifecycle test for the uninstall paths.

The stack uses a VM, not kind or minikube in docker. The VM kernel is a real
node boundary with kubelet running directly on it, so the host-global
`core_pattern` sysctl and the kernel's init-namespace handler invocation
line up in the same namespace. The capture path needs this,
and a container cannot provide it.

## Prerequisites

`limactl`, `docker`, `kubectl`, `helm`, `mc`, `zstd`, `jq`, `tar` - all
provided by the repo's Nix dev shell (`nix develop`).

## Usage

```sh
./up.sh         # VM -> build images -> MinIO -> helm install -> crash workload
./smoke.sh      # live capture checks; coredrop stays installed
./lifecycle.sh  # clean + unclean uninstall checks; reinstalls coredrop at the end
./monitoring.sh # Prometheus + Grafana with the coredrop dashboard on localhost:3000
./down.sh       # remove release + namespaces, keep the VM (fast re-up)
DELETE_CLUSTER=1 ./down.sh   # also delete the lima VM
```

Both tests can run any number of times, in any order. Each one leaves
coredrop installed.

## Monitoring stack

`./monitoring.sh` installs kube-prometheus-stack (Prometheus, the operator
and Grafana only) into the `monitoring` namespace. It reinstalls coredrop
with its `PodMonitor`, loads `alerts/coredrop.rules.yaml` as a
`PrometheusRule` and `dashboards/coredrop.json` into Grafana as a sidecar
ConfigMap, then forwards
Grafana to `http://localhost:3000`. Grafana has no login (every visitor is
an anonymous Admin), and its home page is the coredrop dashboard. Run
`./smoke.sh` in another shell to make captures for the dashboard. Ctrl-C
stops the port-forward only; run `./monitoring.sh` again to open it. It
asserts nothing. There is no Alertmanager: see the alerts on the Prometheus
Alerts page (`kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090`).

When the `PodMonitor` CRD is in the cluster, `install_coredrop` always
enables the `PodMonitor`, so the reinstalls in `smoke.sh`
and `lifecycle.sh` keep them. The stack needs the images from the internet
and about 1 GiB of the VM's memory.

## What the smoke test asserts

1. The coredrop DaemonSet is ready.
2. The node's `core_pattern` points at the handler.
3. A manifest appears in the bucket for the segfaulting demo workload.
4. The core object is a valid zstd stream. Its sha256 is the same as the
   sha256 in the manifest. It decompresses to a real ELF `ET_CORE` dump. The
   proc-snapshot tar is well-formed. The manifest contains the build-id of the
   faulting binary.
5. The workload's `SECRET_FOO` canary value was redacted from the captured
   `environ`, and its `--db-password` canary from the captured `cmdline`.
6. The crash-looping workload is rate-limited after its budget: a manifest
   with `core.skipped_reason=rate_limit` and no core object.
7. The crash pod has a `CoreDumped` Event, and a `CoreDumpSuppressed` Event
   whose `series.count` grows.
8. The daemon log has a `capture spooled` line for the crash pod, and
   `capture complete` lines with outcome `uploaded` and
   `suppressed-rate-limit`.
9. The handler config on the node holds no store credentials.
10. The daemon's `/metrics` shows `coredrop_captures_total` above 0 for the
    outcomes `uploaded` and `suppressed-rate-limit`.
11. With a capture filter that excludes the demo namespace, the handler
    config holds the filter, and a new crash pod gets only `capture complete`
    lines with outcome `filtered`. The pod has no objects in the bucket and no
    coredrop Event. The test then upgrades the release back to no filter.

## What the lifecycle test asserts

1. Clean uninstall: `helm uninstall` succeeds (the post-delete hook
   finished). The node's `core_pattern` is restored to its pre-install value,
   recorded when coredrop was installed. The three hostPath directories
   (binary, runtime, spool) are gone from the node.
2. Unclean uninstall: the daemon is frozen with `SIGSTOP` on the node, then
   the release is uninstalled. SIGTERM stays pending, and the kubelet kills
   the daemon with SIGKILL after the 30s grace period, so the daemon never
   restores `core_pattern`. The post-delete hook's cleanup worker must reset
   it to `core`. The three hostPath directories are gone.
3. After each phase, coredrop is reinstalled, and `core_pattern` points at
   the handler again. If a phase fails, the exit trap thaws the daemon and
   reinstalls.
4. No crash hit a missing handler during the run: the node's `dmesg` has no
   new `coredrop pipe failed` lines.

## Pieces

| File | Purpose |
| --- | --- |
| `lib.sh` | Shared config (namespaces, image tags, release/bucket names) + helpers |
| `cluster-lima-k3s.sh` | lima VM lifecycle: `cluster_up`/`cluster_down`/`load_images` |
| `build-images.sh` | Builds the coredrop image + the segfault workload image (multi-stage `Dockerfile`) |
| `minio/minio.yaml` | In-cluster MinIO + bucket-creation Job |
| `helm-values/coredrop.local.yaml` | Chart overlay: local image, MinIO store, k3s CRI socket path |
| `workloads/segfault.{c,yaml}` | Demo workload that faults on a short loop |
| `smoke.sh` | The live capture assertions, made directly against the bucket via `mc` |
| `lifecycle.sh` | The uninstall assertions, made on the node |
| `monitoring.sh` | Installs kube-prometheus-stack and forwards Grafana to the host |
| `helm-values/monitoring.yaml` | kube-prometheus-stack overlay: no Alertmanager or cluster scrapes, all `PodMonitor`s and `PrometheusRule`s, dashboard sidecar, Grafana without login |

## Gotchas baked into the scripts

- `install_coredrop` (`lib.sh`) injects MinIO's numeric ClusterIP as the
  upload endpoint. The daemon uploads from its pod network, which reaches it.
- k3s runs its own containerd, with its CRI socket under `/run/k3s/containerd/`
  rather than `/run/containerd/`. The chart default for `cri.runtimeEndpoint`
  is empty. The overlay sets it to the k3s socket, so the smoke test covers the
  pinned path and the socket mount.
- The per-pod rate-limit state (`/run/coredrop/recent.json` on the
  node) persists across daemon restarts and is only removed by the
  `helm uninstall` post-delete hook, so `smoke.sh` restarts the demo deployment
  to get a fresh **pod UID** (the rate-limit key) - otherwise a rerun could
  start already suppressed.
- That post-delete hook's worker DaemonSet is applied by `kubectl` after the
  release is gone, so nothing owns it: a hook that fails partway leaves a
  worker running, which would wipe the handler binary and reset
  `core_pattern` right after the next install. `install_coredrop` deletes any
  leftover (`-l coredrop.io/cleanup=true`) before installing, and `lifecycle.sh` treats
  a failed `helm uninstall` as a test failure rather than swallowing it.
- `install_coredrop` records the node's pre-install `core_pattern` only when
  the release is not installed yet. Re-running `up.sh` on a running stack
  upgrades the release and keeps the recorded value.
- Images are fully qualified as `docker.io/library/*` and imported straight
  into the VM's containerd; with `pullPolicy: IfNotPresent` the kubelet never
  hits the network.
