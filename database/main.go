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

func New(eavt, aevt, avet, vaet index.Index) *Database {
	return &Database{eavt, aevt, avet, vaet}
}

func NewFromStore(store *storage.Store) (*Database, error) {
	indexRootRaw, err := storage.Get(store, store.IndexRootId(), nil)
	if err != nil {
		return nil, err
	}
	indexRoot := indexRootRaw.(map[interface{}]interface{})
	eavtId := indexRoot[fressian.Keyword{"", "eavt-main"}].(string)
	eavt, err := index.New(store, index.Eavt, eavtId)
	if err != nil {
		return nil, err
	}
	aevtId := indexRoot[fressian.Keyword{"", "aevt-main"}].(string)
	aevt, err := index.New(store, index.Aevt, aevtId)
	if err != nil {
		return nil, err
	}
	avetId := indexRoot[fressian.Keyword{"", "avet-main"}].(string)
	avet, err := index.New(store, index.Avet, avetId)
	if err != nil {
		return nil, err
	}
	vaetId := indexRoot[fressian.Keyword{"", "raet-main"}].(string)
	vaet, err := index.New(store, index.Vaet, vaetId)
	if err != nil {
		return nil, err
	}
	return New(eavt, aevt, avet, vaet), nil
}

func (db *Database) Eavt() index.Index { return db.eavt }
func (db *Database) Aevt() index.Index { return db.aevt }
func (db *Database) Avet() index.Index { return db.avet }
func (db *Database) Vaet() index.Index { return db.vaet }

func (db *Database) Entid(key fressian.Keyword) int {
	datoms := db.avet.Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Attribute() == 10 && datom.Value().Val() == key {
			return datom.Entity()
		}
	}

	return -1
}

func (db *Database) Ident(entity int) *fressian.Keyword {
	datoms := db.aevt.Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Entity() == entity && datom.Attribute() == 10 {
			key := datom.Value().Val().(fressian.Keyword)
			return &key
		}
	}

	return nil
}

type Entity struct {
	db *Database
	id int
}

func (db *Database) Entity(id int) Entity {
	return Entity{db, id}
}

func (e Entity) Get(key fressian.Keyword) interface{} {
	attrId := e.db.Entid(key)
	if attrId == -1 {
		return nil
	}

	datoms := e.db.eavt.Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Entity() == e.id && datom.Attribute() == attrId {
			return datom.Value().Val()
		}
	}

	return nil
}
