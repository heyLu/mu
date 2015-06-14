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
}

var registry = map[string]func(u *url.URL) (Store, error){}

func Register(name string, open func(u *url.URL) (Store, error)) {
	if _, ok := registry[name]; ok {
		log.Fatal("[store] duplicate store: ", name)
	}

	registry[name] = open
}

func Open(u *url.URL) (Store, error) {
	name := u.Scheme
	if open, ok := registry[name]; ok {
		return open(u)
	}

	return nil, fmt.Errorf("unknown store: %#v", name)
}
