package transactor

import (
	"fmt"
	"github.com/heyLu/fressian"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
)

func resolveTxData(db *database.Db, txData []TxDatum) ([]RawDatum, error) {
	datums := make([]RawDatum, 0, len(txData))
	for _, txDatum := range txData {
		ds, err := txDatum.Resolve(db)
		if err != nil {
			return nil, err
		}
		datums = append(datums, ds...)
	}
	return datums, nil
}

type TxDatum interface {
	Resolve(db *database.Db) ([]RawDatum, error)
}

const (
	Add     = true
	Retract = false
)

type RawDatum struct {
	Op bool
	E  int
	A  int
	V  index.Value
}

func (d RawDatum) Retraction() RawDatum {
	return RawDatum{false, d.E, d.A, d.V}
}

func (d RawDatum) Resolve(db *database.Db) ([]RawDatum, error) {
	return []RawDatum{d}, nil
}

type Datum struct {
	Op bool
	E  TxLookup
	A  TxLookup
	V  Value
}

func (d Datum) Retraction() Datum {
	return Datum{false, d.E, d.A, d.V}
}

func (d Datum) Resolve(db *database.Db) ([]RawDatum, error) {
	eid, err := d.E.Lookup(db)
	if err != nil {
		return nil, err
	}
	aid, err := d.A.Lookup(db)
	if err != nil {
		return nil, err
	}
	val, err := d.V.Get(db)
	if err != nil {
		return nil, err
	}
	return []RawDatum{RawDatum{d.Op, eid, aid, *val}}, nil
}

type Value struct {
	val    *index.Value
	lookup *TxLookup
}

func NewValue(value interface{}) Value {
	if lookup, ok := value.(TxLookup); ok {
		return Value{val: nil, lookup: &lookup}
	}
	val := index.NewValue(value)
	return Value{val: &val, lookup: nil}
}

func (v Value) Get(db *database.Db) (*index.Value, error) {
	if v.lookup != nil {
		id, err := (*v.lookup).Lookup(db)
		if err != nil {
			return nil, err
		}
		ref := index.NewRef(id)
		return &ref, nil
	}
	return v.val, nil

}

type TxMap struct {
	Id         int
	Attributes map[fressian.Keyword][]index.Value
}

func (m TxMap) Resolve(db *database.Db) ([]RawDatum, error) {
	datums := make([]RawDatum, 0, len(m.Attributes))
	for k, vs := range m.Attributes {
		attrId := db.Entid(k)
		if attrId == -1 {
			return nil, fmt.Errorf("no such attribute: %v", k)
		}

		for _, v := range vs {
			datum := RawDatum{Add, m.Id, attrId, v}
			datums = append(datums, datum)
		}
	}
	return datums, nil
}

type TxLookup interface {
	Lookup(db *database.Db) (int, error)
}

type DbId int

func (dbId DbId) Lookup(db *database.Db) (int, error) {
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

func (kw Keyword) Lookup(db *database.Db) (int, error) {
	id := db.Entid(kw.Keyword)
	if id == -1 {
		return -1, fmt.Errorf("no :db/ident for %v", kw)
	}
	return id, nil
}

type LookupRef struct {
	Attribute Keyword
	Value     index.Value
}

func (ref LookupRef) Lookup(db *database.Db) (int, error) {
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
