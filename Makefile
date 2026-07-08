IMAGE_REPO ?= ghcr.io/jw4/node-metrics-collector
IMAGE_TAG  ?= v0.2.0
PLATFORMS  ?= linux/amd64,linux/arm64

.PHONY: build test fmt vet clean docker-build docker-push

build:
	go build -o node-metrics-collector ./cmd/collector

test:
	go test -race -count=1 ./...

fmt:
	gofumpt -w .
	goimports -w .

vet:
	go vet ./...

clean:
	rm -f node-metrics-collector

docker-build:
	docker buildx build --platform $(PLATFORMS) -t $(IMAGE_REPO):$(IMAGE_TAG) .

docker-push:
	docker buildx build --platform $(PLATFORMS) -t $(IMAGE_REPO):$(IMAGE_TAG) --push .
