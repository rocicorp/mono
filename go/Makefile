default:

VERSION = $(shell git describe --tags --always --dirty 2>/dev/null)

.PHONY: build
build:
	mkdir -p dist
	CGO_ENABLED=1 go build -tags vfs -o dist/vfs-query ./cmd/vfs-query

.PHONY: test
test:
	CGO_ENABLED=1 go test -v -count=1 -tags vfs ./...

.PHONY: vet
vet:
	CGO_ENABLED=1 go vet -tags vfs ./...

.PHONY: clean
clean:
	rm -rf dist
