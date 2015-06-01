package database

import (
	"github.com/heyLu/fressian"
	"log"

	"../index"
	"../storage"
)

type Database struct {
	eavt           index.Index
	aevt           index.Index
	avet           index.Index
	vaet           index.Index
	attributeCache map[int]Attribute
}

func New(eavt, aevt, avet, vaet index.Index) *Database {
	return &Database{eavt, aevt, avet, vaet, make(map[int]Attribute, 100)}
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

func (e Entity) Keys() []fressian.Keyword {
	keys := []fressian.Keyword{}
	iter := e.db.Eavt().DatomsAt(index.NewDatom(e.id, -1, "", -1, false), index.NewDatom(e.id, index.MaxDatom.A(), "", -1, false))
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		kw := e.db.Ident(datom.Attribute())
		if kw == nil {
			log.Fatal("attribute has no `:db/ident`:", datom.Attribute())
		}
		keys = append(keys, *kw)
	}
	return keys
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

type Attribute struct {
	id          int
	ident       fressian.Keyword
	cardinality int
	valueType   index.ValueType
}

func (db *Database) Attribute(id int) *Attribute {
	attr, ok := db.attributeCache[id]
	if ok {
		log.Println("attribute from cache:", attr)
		return &attr
	} else {
		iter := db.Eavt().DatomsAt(
			index.NewDatom(id, -1, "", -1, false),
			index.NewDatom(id, 10000, "", -1, false))
		found := false
		attr = Attribute{
			id: id,
		}

		for datom := iter.Next(); datom != nil; datom = iter.Next() {
			found = true

			switch datom.Attribute() {
			case 10: // :db/ident
				attr.ident = datom.Value().Val().(fressian.Keyword)
			case 41: // :db/cardinality
				attr.cardinality = datom.Value().Val().(int)
			case 40: // :db/valueType
				attr.valueType = toValueType(datom.Value().Val().(int))
			}
		}

		if !found {
			return nil
		}

		db.attributeCache[id] = attr
		log.Println("attribute from db:", attr)
		return &attr
	}
}

func (a Attribute) Id() int                 { return a.id }
func (a Attribute) Ident() fressian.Keyword { return a.ident }
func (a Attribute) Cardinality() int        { return a.cardinality }
func (a Attribute) Type() index.ValueType   { return a.valueType }

func toValueType(internalType int) index.ValueType {
	switch internalType {
	case 21:
		return index.Keyword
	case 22:
		return index.Int
	case 23:
		return index.String
	case 24:
		return index.Bool
	case 25:
		return index.Date
	default:
		log.Fatal("unknown :db/type:", internalType)
		return index.ValueType(-1)
	}
}
