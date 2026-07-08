# chatstress — use-case-6 stress test for Redis Search on Disk (MS2)
#
# Go is not required on the host: all Go commands run inside a Docker container
# (golang:1.23-alpine). The resulting binary is static and host-runnable.

IMAGE        ?= golang:1.23-alpine
CONFIG       ?= config/smoke.yaml
ADDR         ?= 127.0.0.1:6399
# Path to a disk-index-capable redisearch.so for the local in-RAM smoke server.
REDIS_MODULE ?= /home/jonathan/CLionProjects/RediSearchDisk/build/redisearch.so
SMOKE_PORT   ?= 6399

DOCKER_GO = docker run --rm -v "$(CURDIR)":/src -w /src \
	-v chatstress-gomod:/go/pkg/mod -v chatstress-gobuild:/root/.cache/go-build \
	-e CGO_ENABLED=0 -e GOSUMDB=off -e GOFLAGS=-mod=mod $(IMAGE)

.PHONY: build tidy fmt vet clean run smoke help

build: ## Build the static ./bin/chatstress binary (via Docker Go)
	./build.sh

tidy: ## Resolve/refresh go.mod + go.sum
	$(DOCKER_GO) go mod tidy

fmt: ## gofmt the sources
	$(DOCKER_GO) gofmt -w .

vet: ## go vet
	$(DOCKER_GO) go vet ./...

run: build ## Build then run against $(ADDR) with $(CONFIG)
	./bin/chatstress run -config $(CONFIG) -addr $(ADDR)

smoke: build ## Start a local in-RAM redis+module, run a short workload, tear down
	REDIS_MODULE=$(REDIS_MODULE) PORT=$(SMOKE_PORT) ./scripts/local-redis.sh start
	- ./bin/chatstress run -config $(CONFIG) -addr 127.0.0.1:$(SMOKE_PORT)
	PORT=$(SMOKE_PORT) ./scripts/local-redis.sh stop

clean: ## Remove build output and run artifacts
	rm -rf bin out

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  %-8s %s\n", $$1, $$2}'
