SHELL := /bin/bash
GO ?= go
BIN_DIR := bin
PROTOC ?= protoc
PROTO_INCLUDE ?= /opt/homebrew/include

.PHONY: up down build test test-race vet fmt proto bench demo

## up: start NATS JetStream and PostgreSQL with healthchecks
up:
	docker compose up -d --wait

## down: stop the local infrastructure and remove volumes
down:
	docker compose down -v

## build: compile both commands into bin/
build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/bridge ./cmd/bridge
	$(GO) build -o $(BIN_DIR)/fakewms ./cmd/fakewms

## test: run unit tests
test:
	$(GO) test ./...

## test-race: run the full unit suite with the race detector
test-race:
	$(GO) test -race ./...

## vet: run go vet
vet:
	$(GO) vet ./...

## fmt: format all Go sources
fmt:
	gofmt -s -w .

## proto: regenerate internal/taskpb from proto/task.proto
proto:
	PATH="$$PATH:$$(go env GOPATH)/bin" $(PROTOC) -I proto -I $(PROTO_INCLUDE) \
		--go_out=. --go_opt=module=github.com/local/warehousetaskbridge proto/task.proto

## bench: run scheduler assignment benchmark and record it
bench:
	$(GO) test -bench=BenchmarkAssign -benchmem -run=^$$ ./internal/scheduler/ | tee docs/bench.txt

## demo: run the reproducible fake-WMS demonstration
demo:
	./scripts/demo.sh

## integration: run Testcontainers end-to-end tests
integration:
	$(GO) test -race -tags=integration ./test/integration/...
