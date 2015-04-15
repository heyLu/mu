package connection

import (
	"net/url"

	"../database"
	"../storage"
)

type Connection interface {
	Db() (*database.Database, error)
}

type PersistentConnection struct {
	store *storage.Store
}

func New(u *url.URL) (Connection, error) {
	store, err := storage.Open(u)
	if err != nil {
		return nil, err
	}

	return &PersistentConnection{store}, nil
}

func (c *PersistentConnection) Db() (*database.Database, error) {
	return database.NewFromStore(c.store)
}
