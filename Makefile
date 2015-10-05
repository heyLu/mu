all: help

GOPATH=$(PWD)/.go

help:
	@echo "make cmds - build command line tools"
	@echo "make test - run all tests"

cmds: mu notes

mu:
	GOPATH=$(GOPATH) go build ./cmd/mu/mu.go

notes:
	GOPATH=$(GOPATH) go build cmd/notes/notes.go

test:
	go test ./...

.PHONY: help cmds mu notes test
