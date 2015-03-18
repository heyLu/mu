package connection

import (
	"github.com/heyLu/fressian"
	"net/url"

	"../index"
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

type Database struct {
	eavt index.Index
	aevt index.Index
	avet index.Index
	vaet index.Index
}

func (c *Connection) Db() (*Database, error) {
	indexRootRaw, err := storage.Get(c.store, c.store.IndexRootId(), nil)
	if err != nil {
		return nil, err
	}
	indexRoot := indexRootRaw.(map[interface{}]interface{})
	eavtId := indexRoot[fressian.Key{"", "eavt-main"}].(string)
	eavt, err := index.New(c.store, eavtId)
	if err != nil {
		return nil, err
	}
	aevtId := indexRoot[fressian.Key{"", "aevt-main"}].(string)
	aevt, err := index.New(c.store, aevtId)
	if err != nil {
		return nil, err
	}
	avetId := indexRoot[fressian.Key{"", "avet-main"}].(string)
	avet, err := index.New(c.store, avetId)
	if err != nil {
		return nil, err
	}
	vaetId := indexRoot[fressian.Key{"", "raet-main"}].(string)
	vaet, err := index.New(c.store, vaetId)
	if err != nil {
		return nil, err
	}
	return &Database{eavt, aevt, avet, vaet}, nil
}

func (db *Database) Eavt() index.Index { return db.eavt }
func (db *Database) Aevt() index.Index { return db.aevt }
func (db *Database) Avet() index.Index { return db.avet }
func (db *Database) Vaet() index.Index { return db.vaet }
