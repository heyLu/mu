all: help

GOPATH=$(PWD)/.go

help:
	@echo "make cmds - build command line tools"
	@echo "make test - run all tests"

cmds:
	GOPATH=$(GOPATH) go build cmd/mu/mu.go
	GOPATH=$(GOPATH) go build cmd/notes/notes.go

test:
	go test ./...
