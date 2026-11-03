GOPATH ?= $(shell go env GOPATH)
GOLANGCI_LINT ?= $(shell which golangci-lint 2>/dev/null || echo /opt/homebrew/bin/golangci-lint)

.PHONY: all build run test bench lint clean

all: build

build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/gofetch ./cmd/gofetch

run:
	go run cmd/gofetch/main.go

test:
	go test -v -race ./...

bench:
	go test -bench=. ./bench/...

lint:
	$(GOLANGCI_LINT) run ./...

clean:
	rm -rf bin/ *.part
