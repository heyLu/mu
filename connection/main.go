package connection

import (
	"net/url"

	"../database"
	"../storage"
)

type Connection struct {
	store *storage.Store
}

func New(u *url.URL) (*Connection, error) {
	store, err := storage.Open(u)
	if err != nil {
		return nil, err
	}

	return &Connection{store}, nil
}

func (c *Connection) Db() (*database.Database, error) {
	return database.New(c.store)
}
