package connection

import (
	"errors"
	"net/url"

	"../database"
	"../index"
	"../storage"
	"./memory"
)

type Connection interface {
	Db() (*database.Database, error)
	TransactDatoms(datoms []index.Datom) error
}

type PersistentConnection struct {
	store *storage.Store
}

func New(u *url.URL) (Connection, error) {
	if u.Scheme == "memory" {
		return memory.New(), nil
	}

	store, err := storage.Open(u)
	if err != nil {
		return nil, err
	}

	return &PersistentConnection{store}, nil
}

func (c *PersistentConnection) Db() (*database.Database, error) {
	return database.NewFromStore(c.store)
}

func (c *PersistentConnection) TransactDatoms(datoms []index.Datom) error {
	return errors.New("not implemented")
}
