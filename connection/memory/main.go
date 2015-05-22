package memory

import (
	"net/url"

	connection ".."
	"../../database"
	"../../index/"
	memoryIndex "../../index/memory"
)

func init() {
	connection.Register("memory", New)
}

type Connection struct {
	db *database.Database
}

func New(u *url.URL) (connection.Connection, error) {
	eavt := memoryIndex.New(index.CompareEavt)
	aevt := memoryIndex.New(index.CompareAevt)
	db := database.New(eavt, aevt, nil, nil)
	return &Connection{db}, nil
}

func NewFromDb(db *database.Database) connection.Connection {
	return &Connection{db}
}

func (c *Connection) Db() (*database.Database, error) {
	return c.db, nil
}

func (c *Connection) TransactDatoms(datoms []index.Datom) error {
	eavt := c.db.Eavt().(*memoryIndex.Index)
	eavt = eavt.AddDatoms(datoms)
	aevt := c.db.Aevt().(*memoryIndex.Index)
	aevt = aevt.AddDatoms(datoms)
	c.db = database.New(eavt, aevt, nil, nil)
	return nil
}
