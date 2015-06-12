package database

import (
	"github.com/heyLu/fressian"
	"log"

	"../index"
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

func (db *Database) Eavt() index.Index { return db.eavt }
func (db *Database) Aevt() index.Index { return db.aevt }
func (db *Database) Avet() index.Index { return db.avet }
func (db *Database) Vaet() index.Index { return db.vaet }

func (db *Database) Entid(key fressian.Keyword) int {
	// FIXME [perf]: use `.DatomsAt` and/or caching (datomic does this on `connect`)
	datoms := db.avet.Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Attribute() == 10 && datom.Value().Val() == key {
			return datom.Entity()
		}
	}

	return -1
}

func (db *Database) Ident(entity int) *fressian.Keyword {
	// FIXME [perf]: use `.DatomsAt` and/or caching (datomic does this on `connect`)
	datoms := db.aevt.Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Entity() == entity && datom.Attribute() == 10 {
			key := datom.Value().Val().(fressian.Keyword)
			return &key
		}
	}

	return nil
}

// HasLookup is an interface for things that can be uniquely resolved
// to an entity.
//
// Entity ids, `:db/ident` and pairs of unique attributes and a value
// all implement HasLookup.  This allows using them in places where
// one would usually use an entity id, simplifying access.
//
// TODO: Do it this way or just provide a `db.Lookup(...)` function
// that supports lookup for a fixed set of types?  (Doing it with an
// interface means better errors and extensibility to user types.)
type HasLookup interface {
	Lookup(db *Database) int
}

type Entity struct {
	db             *Database
	id             int
	attributeCache map[fressian.Keyword]interface{}
}

// Entity constructs a lazy, cached "view" of all datoms with a given
// entity id.
func (db *Database) Entity(id int) Entity {
	return Entity{db, id, map[fressian.Keyword]interface{}{}}
}

// Datoms returns an iterator over all datoms for this entity.
func (e Entity) Datoms() index.Iterator {
	return e.db.Eavt().DatomsAt(index.NewDatom(e.id, -1, "", -1, false), index.NewDatom(e.id, index.MaxDatom.A(), "", -1, false))
}

// Keys returns a slice of all attributes of this entity.
func (e Entity) Keys() []fressian.Keyword {
	// TODO: cache attributes here as well?
	keys := []fressian.Keyword{}
	iter := e.Datoms()
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		kw := e.db.Ident(datom.Attribute())
		if kw == nil {
			log.Fatal("attribute has no `:db/ident`:", datom.Attribute())
		}
		keys = append(keys, *kw)
	}
	return keys
}

// Get retrieves the value for the attribute.
//
// The resulting value is cached.  If no value is found, `nil` is returned.
func (e Entity) Get(key fressian.Keyword) interface{} {
	if val, ok := e.attributeCache[key]; ok {
		return val
	}

	attrId := e.db.Entid(key)
	if attrId == -1 {
		return nil
	}

	// FIXME [perf]: use `.DatomsAt` (or `e.Datoms`)
	datoms := e.db.eavt.Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Entity() == e.id && datom.Attribute() == attrId {
			val := datom.Value().Val()
			e.attributeCache[key] = val
			return val
		}
	}

	return nil
}

// Touch caches all attributes of this entity.
func (e Entity) Touch() {
	iter := e.Datoms()
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		kw := e.db.Ident(datom.Attribute())
		if kw == nil {
			log.Fatal("attribute has no `:db/ident`:", datom.Attribute())
		}
		e.attributeCache[*kw] = datom.Value().Val()
	}
}

// AsMap returns a view of this entity as a map.
//
// The returned map will only contain cached attributes.  To get a map of
// all attributes call `.Touch` first.
func (e Entity) AsMap() map[fressian.Keyword]interface{} {
	return e.attributeCache
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
	// TODO: maybe make index.ValueType start out with the correct numbers?
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
