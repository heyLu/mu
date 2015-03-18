package database

import (
	"github.com/heyLu/fressian"

	"../index"
	"../storage"
)

type Database struct {
	eavt index.Index
	aevt index.Index
	avet index.Index
	vaet index.Index
}

func New(store *storage.Store) (*Database, error) {
	indexRootRaw, err := storage.Get(store, store.IndexRootId(), nil)
	if err != nil {
		return nil, err
	}
	indexRoot := indexRootRaw.(map[interface{}]interface{})
	eavtId := indexRoot[fressian.Key{"", "eavt-main"}].(string)
	eavt, err := index.New(store, eavtId)
	if err != nil {
		return nil, err
	}
	aevtId := indexRoot[fressian.Key{"", "aevt-main"}].(string)
	aevt, err := index.New(store, aevtId)
	if err != nil {
		return nil, err
	}
	avetId := indexRoot[fressian.Key{"", "avet-main"}].(string)
	avet, err := index.New(store, avetId)
	if err != nil {
		return nil, err
	}
	vaetId := indexRoot[fressian.Key{"", "raet-main"}].(string)
	vaet, err := index.New(store, vaetId)
	if err != nil {
		return nil, err
	}
	return &Database{eavt, aevt, avet, vaet}, nil
}

func (db *Database) Eavt() index.Index { return db.eavt }
func (db *Database) Aevt() index.Index { return db.aevt }
func (db *Database) Avet() index.Index { return db.avet }
func (db *Database) Vaet() index.Index { return db.vaet }
