all: help

help:
	@echo "make cmds - build command line tools"
	@echo "make test - run all tests"

cmds:
	go build cmd/mu/mu.go
	go build cmd/notes/notes.go

test:
	go test ./...
