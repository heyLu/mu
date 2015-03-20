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
	eavt, err := index.New(store, "eavt", eavtId)
	if err != nil {
		return nil, err
	}
	aevtId := indexRoot[fressian.Key{"", "aevt-main"}].(string)
	aevt, err := index.New(store, "aevt", aevtId)
	if err != nil {
		return nil, err
	}
	avetId := indexRoot[fressian.Key{"", "avet-main"}].(string)
	avet, err := index.New(store, "avet", avetId)
	if err != nil {
		return nil, err
	}
	vaetId := indexRoot[fressian.Key{"", "raet-main"}].(string)
	vaet, err := index.New(store, "vaet", vaetId)
	if err != nil {
		return nil, err
	}
	return &Database{eavt, aevt, avet, vaet}, nil
}

func (db *Database) Eavt() index.Index { return db.eavt }
func (db *Database) Aevt() index.Index { return db.aevt }
func (db *Database) Avet() index.Index { return db.avet }
func (db *Database) Vaet() index.Index { return db.vaet }

func (db *Database) Entid(key fressian.Key) int {
	datoms := db.avet.Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Attribute() == 10 && datom.Value() == key {
			return datom.Entity()
		}
	}

	return -1
}

func (db *Database) Ident(entity int) *fressian.Key {
	datoms := db.aevt.Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Entity() == entity && datom.Attribute() == 10 {
			key := datom.Value().(fressian.Key)
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

func (e Entity) Get(key fressian.Key) interface{} {
	attrId := e.db.Entid(key)
	if attrId == -1 {
		return nil
	}

	datoms := e.db.eavt.Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Entity() == e.id && datom.Attribute() == attrId {
			return datom.Value()
		}
	}

	return nil
}
