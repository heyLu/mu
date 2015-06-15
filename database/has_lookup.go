package database

import (
	"fmt"
	"github.com/heyLu/fressian"

	"github.com/heyLu/mu/index"
)

type HasLookup interface {
	Lookup(db *Db) (int, error)
}

type Id int

func (dbId Id) Lookup(db *Db) (int, error) {
	id := int(dbId)
	if id > 0 {
		iter := db.Eavt().DatomsAt(
			index.NewDatom(id, 0, index.MinValue, 0, true),
			index.NewDatom(id, index.MaxDatom.A(), index.MinValue, 0, true))
		datom := iter.Next()
		if datom == nil || datom.E() != id {
			return -1, fmt.Errorf("no entity with id %d", id)
		}
	}
	return id, nil
}

type Keyword struct {
	fressian.Keyword
}

func (kw Keyword) Lookup(db *Db) (int, error) {
	iter := db.Avet().DatomsAt(
		index.NewDatom(0, 10, kw.Keyword, 0, true),
		index.NewDatom(0, 10, index.MaxValue, index.MaxDatom.Tx(), true))
	// FIXME: .DatomsAt should start at the right value, or return nil
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		if datom.Attribute() == 10 && datom.Value().Compare(index.NewValue(kw.Keyword)) == 0 {
			return datom.Entity(), nil
		}
	}
	return -1, fmt.Errorf("no :db/ident for %v", kw)

}

type LookupRef struct {
	Attribute Keyword
	Value     index.Value
}

func (ref LookupRef) Lookup(db *Db) (int, error) {
	attrId, err := ref.Attribute.Lookup(db)
	if err != nil {
		return -1, err
	}
	iter := db.Avet().DatomsAt(
		index.NewDatom(0, attrId, ref.Value, 0, true),
		index.NewDatom(0, attrId, index.MaxValue, 0, true))
	datom := iter.Next()
	if datom == nil || datom.A() != attrId || datom.V().Compare(ref.Value) != 0 {
		return -1, fmt.Errorf("no entity for [%v %v]\n", ref.Attribute, ref.Value)
	}
	return datom.E(), nil
}
