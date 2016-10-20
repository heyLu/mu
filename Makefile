all: help

GOPATH=$(PWD)/.go

help:
	@echo "make cmds - build command line tools"
	@echo "make test - run all tests"

cmds: mu notes

mu:
	GOPATH=$(GOPATH) go install -v github.com/heyLu/mu/cmd/mu
	@cp $(GOPATH)/bin/mu .

notes:
	GOPATH=$(GOPATH) go install -v github.com/heyLu/mu/cmd/notes
	@cp $(GOPATH)/bin/notes .

test:
	go test ./...

.PHONY: help cmds mu notes test
