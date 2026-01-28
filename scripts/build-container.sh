#!/bin/bash

TAG="${TAG:-quay.io/$USER/kubernetes-traffic-flow-tests:latest}"

set -ex

TAG="${1:-$TAG}"

# Build base image
buildah manifest rm kubernetes-traffic-flow-tests-manifest || true
buildah manifest create kubernetes-traffic-flow-tests-manifest
buildah build --manifest kubernetes-traffic-flow-tests-manifest --platform linux/amd64,linux/arm64 --target base -t "$TAG" .
buildah manifest push --all kubernetes-traffic-flow-tests-manifest "docker://$TAG"

# Build RDMA image (with RDMA tools) - target: rdma
# Derive RDMA tag: quay.io/user/image:tag -> quay.io/user/image-rdma:tag
TAG_RDMA="${TAG%:*}-rdma:${TAG##*:}"
buildah manifest rm kubernetes-traffic-flow-tests-rdma-manifest || true
buildah manifest create kubernetes-traffic-flow-tests-rdma-manifest
buildah build --manifest kubernetes-traffic-flow-tests-rdma-manifest --platform linux/amd64,linux/arm64 --target rdma -t "$TAG_RDMA" .
buildah manifest push --all kubernetes-traffic-flow-tests-rdma-manifest "docker://$TAG_RDMA"

echo ""
echo "Built images:"
echo "  Base:  $TAG"
echo "  RDMA:  $TAG_RDMA"
