GO ?= go

.PHONY: all format check test pre-commit clean

all: test

format:
	$(GO) run golang.org/x/tools/cmd/goimports@latest -w .
	$(GO) fix ./...

check:
	$(GO) vet ./...
	$(GO) run honnef.co/go/tools/cmd/staticcheck ./...

test: check
	$(GO) clean -testcache
	$(GO) test ./...

pre-commit: format test

clean:
	$(GO) clean -testcache
