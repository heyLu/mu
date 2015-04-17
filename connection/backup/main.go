package backup

import (
	"errors"
	"net/url"

	connection ".."
	"../../database"
	"../../index"
	"../../storage"
)

func init() {
	connection.Register("backup", New)
}

type Connection struct {
	store *storage.Store
}

func New(u *url.URL) (connection.Connection, error) {
	store, err := storage.Open(u)
	if err != nil {
		return nil, err
	}

	return &Connection{store}, nil
}

func (c *Connection) Db() (*database.Database, error) {
	return database.NewFromStore(c.store)
}

func (c *Connection) TransactDatoms(datoms []index.Datom) error {
	return errors.New("not implemented")
}
