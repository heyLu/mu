package memory

import (
	"net/url"

	connection ".."
	"../../database"
	"../../index/"
	memoryIndex "../../index/memory"
	"../../log"
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
	avet := memoryIndex.New(index.CompareAvet)
	vaet := memoryIndex.New(index.CompareVaet)
	db := database.New(eavt, aevt, avet, vaet)
	return &Connection{db}, nil
}

func NewFromDb(db *database.Database) connection.Connection {
	return &Connection{db}
}

func (c *Connection) Db() *database.Database { return c.db }
func (c *Connection) Log() *log.Log          { return nil }

func (c *Connection) TransactDatoms(datoms []index.Datom) error {
	eavt := c.db.Eavt().(*memoryIndex.Index)
	eavt = eavt.AddDatoms(datoms)
	aevt := c.db.Aevt().(*memoryIndex.Index)
	aevt = aevt.AddDatoms(datoms)
	avet := c.db.Avet().(*memoryIndex.Index)
	avet = avet.AddDatoms(filterDatoms(needsAvet, datoms))
	vaet := c.db.Vaet().(*memoryIndex.Index)
	vaet = vaet.AddDatoms(filterDatoms(needsVaet, datoms))
	c.db = database.New(eavt, aevt, avet, vaet)
	return nil
}

func needsAvet(datom index.Datom) bool {
	return datom.Attribute() == 10 // db/ident
}

func needsVaet(datom index.Datom) bool {
	return false
}

func filterDatoms(pred func(index.Datom) bool, datoms []index.Datom) []index.Datom {
	filteredDatoms := make([]index.Datom, 0, len(datoms))
	for _, datom := range datoms {
		if pred(datom) {
			filteredDatoms = append(filteredDatoms, datom)
		}
	}
	return filteredDatoms
}
