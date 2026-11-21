#!/usr/bin/env bash
# Copy the externalgrpc protocol and its generated Go code from kubernetes/autoscaler at a tag.
set -euo pipefail
tag="${1:?usage: $0 cluster-autoscaler-<version>}"
base="https://raw.githubusercontent.com/kubernetes/autoscaler/${tag}/cluster-autoscaler/cloudprovider/externalgrpc/protos"
dir="$(cd "$(dirname "$0")/.." && pwd)/proto/externalgrpc"
for f in externalgrpc.proto externalgrpc.pb.go externalgrpc_grpc.pb.go; do
  curl -fsSL -o "${dir}/${f}" "${base}/${f}"
done
sed -i.bak "s/at tag \`cluster-autoscaler-[0-9.]*\`/at tag \`${tag}\`/" "${dir}/README.md" && rm -f "${dir}/README.md.bak"
echo "vendored ${tag}; review the diff and the generator versions in ${dir}/README.md"
