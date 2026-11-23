VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/joktan4194/mqbackrest/internal/version.Version=$(VERSION)

.PHONY: build test lint e2e clean

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o dist/mqbackrest ./cmd/mqbackrest

test:
	go test -race ./...

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

e2e:
	./test/e2e/run.sh

clean:
	rm -rf dist
