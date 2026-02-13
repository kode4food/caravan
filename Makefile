GO ?= go

.PHONY: all build format check test pre-commit generate clean

all: test

format: generate
	$(GO) run golang.org/x/tools/cmd/goimports@latest -w .
	$(GO) fix ./...

check:
	$(GO) vet ./...
	$(GO) run honnef.co/go/tools/cmd/staticcheck ./...

test: generate check
	$(GO) clean -testcache
	$(GO) test ./...

pre-commit: format test

build: generate

generate:
	$(GO) generate ./...

clean:
	$(GO) clean -testcache
