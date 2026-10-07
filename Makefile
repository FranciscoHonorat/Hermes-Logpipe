BINARY := logpipe
BIN_DIR := bin
PKG := ./cmd/logpipe

.DEFAULT_GOAL := build
.PHONY: build test lint bench clean

build:
	go build -o $(BIN_DIR)/$(BINARY) $(PKG)

test:
	go test -race ./...

lint:
	golangci-lint run ./...

bench:
	go test -bench=. -benchmem -run=^$$ ./...
	
clean:
	rm -rf $(BIN_DIR)