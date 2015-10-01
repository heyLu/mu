package store

import (
	"fmt"
	"log"
	"net/url"
)

type Store interface {
	Get(id string) ([]byte, error)
	Put(id string, data []byte) error
	Delete(id string) error
	Close() error
}

type CreateFn func(u *url.URL) (bool, error)
type OpenFn func(u *url.URL) (Store, error)

type store struct {
	create CreateFn
	open   OpenFn
}

var registry = map[string]store{}

func Register(name string, create CreateFn, open OpenFn) {
	if _, ok := registry[name]; ok {
		log.Fatal("[store] duplicate store: ", name)
	}

	registry[name] = store{create: create, open: open}
}

func Open(u *url.URL) (Store, error) {
	name := u.Scheme
	if store, ok := registry[name]; ok {
		return store.open(u)
	}

	return nil, fmt.Errorf("unknown store: %#v", name)
}

func Create(u *url.URL) (bool, error) {
	name := u.Scheme
	if store, ok := registry[name]; ok {
		return store.create(u)
	}

	return false, fmt.Errorf("unknown store: %#v", name)
}
