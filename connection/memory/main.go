package memory

import (
	"../../database"
	"../../index/"
	memoryIndex "../../index/memory"
)

type Connection struct {
	db *database.Database
}

func New() *Connection {
	eavt := memoryIndex.New(index.CompareEavt)
	db := database.New(eavt, nil, nil, nil)
	return &Connection{db}
}

func (c *Connection) Db() (*database.Database, error) {
	return c.db, nil
}

func (c *Connection) TransactDatoms(datoms []index.Datom) error {
	eavt := c.db.Eavt().(*memoryIndex.Index)
	eavt = eavt.AddDatoms(datoms)
	c.db = database.New(eavt, nil, nil, nil)
	return nil
}
