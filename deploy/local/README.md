# Local live-test stack

End-to-end environment for coredrop's kernel-to-bucket path: a single-node
k3s cluster inside a lima VM, an in-cluster MinIO as the object store, a
crash-looping demo workload, and a smoke test that asserts real captures land
in the bucket.

A VM (not kind/minikube-in-docker) is deliberate: the VM kernel is a real
node boundary with kubelet running directly on it, so `hostPID`, the
host-global `core_pattern` sysctl, and the kernel's init-namespace handler
invocation all line up in the same namespace - which is exactly what the
capture path needs and a container cannot fake.

## Prerequisites

`limactl`, `docker`, `kubectl`, `helm`, `mc`, `zstd`, `jq`, `tar` - all
provided by the repo's Nix dev shell (`nix develop`).

## Usage

```sh
./up.sh      # VM -> build images -> MinIO -> helm install -> crash workload -> smoke test
./smoke.sh   # re-run just the smoke test
./down.sh    # remove release + namespaces, keep the VM (fast re-up)
DELETE_CLUSTER=1 ./down.sh   # also delete the lima VM
```

## What the smoke test asserts

1. The coredrop DaemonSet is ready.
2. The node's `core_pattern` points at the handler.
3. A manifest appears in the bucket for the segfaulting demo workload.
4. The core object is a valid zstd stream and the proc-snapshot tar is
   well-formed.
5. The workload's `SECRET_FOO` canary value was redacted from the captured
   `environ`.
6. After `helm uninstall`, the node's `core_pattern` is restored to its
   pre-install value (recorded by `up.sh`).

## Pieces

| File | Purpose |
| --- | --- |
| `lib.sh` | Shared config (namespaces, image tags, release/bucket names) + helpers |
| `cluster-lima-k3s.sh` | lima VM lifecycle: `cluster_up` / `cluster_down` / `load_images` |
| `build-images.sh` | Builds the coredrop image + the segfault workload image (multi-stage `Dockerfile`) |
| `minio/minio.yaml` | In-cluster MinIO + bucket-creation Job |
| `helm-values/coredrop.local.yaml` | Chart overlay: local image, MinIO store, k3s CRI socket path |
| `workloads/segfault.{c,yaml}` | Demo workload that faults on a short loop |
| `smoke.sh` | The assertions above, made directly against the bucket via `mc` |

## Gotchas baked into the scripts

- The kernel-exec'd handler runs in the **host network namespace**, so it
  can't resolve cluster DNS. `up.sh` injects MinIO's numeric ClusterIP as the
  upload endpoint (the node's root netns can reach a ClusterIP via
  kube-proxy's DNAT rules).
- k3s runs its own containerd; the CRI socket is under
  `/run/k3s/containerd/`, not the upstream `/run/containerd/` default. The
  overlay sets both `cri.runtimeEndpoint` and `cri.socketHostPath`.
- The per-container rate-limit state (`/run/coredrop/recent.json` on the
  node) survives `helm uninstall`, so `up.sh` restarts the demo deployment to
  get a fresh container ID - otherwise a re-up would start already
  suppressed.
- Images are fully qualified as `docker.io/library/*` and imported straight
  into the VM's containerd; with `pullPolicy: IfNotPresent` the kubelet never
  hits the network.
