package memory

import (
	"net/url"

	connection ".."
	"../../database"
	"../../index/"
	"../../log"
)

func init() {
	connection.Register("memory", New)
}

type Connection struct {
	db *database.Database
}

func New(u *url.URL) (connection.Connection, error) {
	eavt := index.NewMemoryIndex(index.CompareEavt)
	aevt := index.NewMemoryIndex(index.CompareAevt)
	avet := index.NewMemoryIndex(index.CompareAvet)
	vaet := index.NewMemoryIndex(index.CompareVaet)
	db := database.NewMemory(eavt, aevt, avet, vaet)
	return &Connection{db}, nil
}

func NewFromDb(db *database.Database) connection.Connection {
	return &Connection{db}
}

func (c *Connection) Db() *database.Database { return c.db }
func (c *Connection) Log() *log.Log          { return nil }

func (c *Connection) TransactDatoms(datoms []index.Datom) error {
	eavt := c.db.Eavt().(*index.MemoryIndex)
	eavt = eavt.AddDatoms(datoms)
	aevt := c.db.Aevt().(*index.MemoryIndex)
	aevt = aevt.AddDatoms(datoms)
	avet := c.db.Avet().(*index.MemoryIndex)
	avetDatoms, vaetDatoms := connection.FilterAvetAndVaet(c.db, datoms)
	avet = avet.AddDatoms(avetDatoms)
	vaet := c.db.Vaet().(*index.MemoryIndex)
	vaet = vaet.AddDatoms(vaetDatoms)
	c.db = database.NewMemory(eavt, aevt, avet, vaet)
	return nil
}
