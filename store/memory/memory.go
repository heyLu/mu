package memory

import (
	"fmt"
	"net/url"

	store ".."
)

func init() {
	store.Register("memory", open)
}

func open(u *url.URL) (store.Store, error) {
	return new(memoryStore), nil
}

type memoryStore struct {
	store map[string][]byte
}

func (s *memoryStore) Get(id string) ([]byte, error) {
	if data, ok := s.store[id]; ok {
		return data, nil
	}

	return nil, fmt.Errorf("No such object: %s", id)
}

func (s *memoryStore) Put(id string, data []byte) error {
	s.store[id] = data
	return nil
}

func (s *memoryStore) Delete(id string) error {
	delete(s.store, id)
	return nil
}
