#!/usr/bin/env bash
# Build a static `chatstress` binary using a Dockerized Go toolchain, so no host
# Go install is required. The binary is fully static (CGO disabled; deps are pure
# Go) and runs directly on any Linux host.
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${GO_IMAGE:-golang:1.23-alpine}"
mkdir -p bin

docker run --rm \
  -v "$PWD":/src -w /src \
  -v chatstress-gomod:/go/pkg/mod \
  -v chatstress-gobuild:/root/.cache/go-build \
  -e CGO_ENABLED=0 -e GOSUMDB=off -e GOFLAGS=-mod=mod \
  "$IMAGE" sh -c "go mod tidy && go build -trimpath -ldflags='-s -w' -o bin/chatstress ./cmd/chatstress"

echo "built ./bin/chatstress"
