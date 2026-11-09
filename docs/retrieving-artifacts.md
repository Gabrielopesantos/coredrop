# Retrieving artifacts

coredrop has no retrieval command - artifacts are plain objects at a
predictable key, so any object-store CLI works. Given a manifest key (from a
k8s Event's message, or listing the bucket) or its
`{cluster}/{podUID}/{containerID}/{timestamp}-{hostPID}` prefix:

## S3/S3-compatible (MinIO, etc.)

```sh
aws s3 cp s3://crash-artifacts/<prefix>-manifest.json - | jq .
aws s3 cp s3://crash-artifacts/<prefix>-core.zst core.zst && zstd -d core.zst
```

## GCS

```sh
gsutil cat gs://crash-artifacts/<prefix>-manifest.json | jq .
gsutil cp gs://crash-artifacts/<prefix>-core.zst core.zst && zstd -d core.zst
```

## Azure

```sh
az storage blob download --container-name crash-artifacts \
  --name "<prefix>-manifest.json" --auth-mode login -f - | jq .
az storage blob download --container-name crash-artifacts \
  --name "<prefix>-core.zst" --auth-mode login -f core.zst
```

## Debugging the core

Run `zstd -d core.zst`, then `gdb <binary> core`. Match the manifest's
`build_id` against your symbol store.
