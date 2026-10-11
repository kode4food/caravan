GO ?= go

.PHONY: all format check test pre-commit clean

all: test

format:
	$(GO) tool goimports -w .
	$(GO) fix ./...

check:
	$(GO) vet ./...
	$(GO) tool staticcheck ./...

test: check
	$(GO) clean -testcache
	$(GO) test ./...

pre-commit: format test

clean:
	$(GO) clean -testcache
