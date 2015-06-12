package memory

import (
	"net/url"

	connection ".."
	"../../database"
	"../../index/"
	"../../log"
	"../../transactor"
)

func init() {
	connection.Register("memory", New)
}

type Connection struct {
	db *database.Db
}

func New(u *url.URL) (connection.Connection, error) {
	eavt := index.NewMemoryIndex(index.CompareEavt)
	aevt := index.NewMemoryIndex(index.CompareAevt)
	avet := index.NewMemoryIndex(index.CompareAvet)
	vaet := index.NewMemoryIndex(index.CompareVaet)
	db := database.NewInMemory(eavt, aevt, avet, vaet)
	return &Connection{db}, nil
}

func NewFromDb(db *database.Db) connection.Connection {
	return &Connection{db}
}

func (c *Connection) Db() *database.Db { return c.db }
func (c *Connection) Log() *log.Log    { return nil }

func (c *Connection) Index(datoms []index.Datom) error {
	c.db = c.db.WithDatoms(datoms)
	return nil
}

func (c *Connection) Transact(datoms []transactor.TxDatum) (*transactor.TxResult, error) {
	_, txResult, err := transactor.Transact(c.db, nil, datoms)
	if err != nil {
		return nil, err
	}
	c.db = txResult.DbAfter
	return txResult, nil
}
