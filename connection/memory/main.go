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
	aevt := memoryIndex.New(index.CompareAevt)
	db := database.New(eavt, aevt, nil, nil)
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
