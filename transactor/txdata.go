package transactor

import (
	"fmt"

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
	Assert  = true
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
	E  database.HasLookup
	A  database.HasLookup
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
	attr := db.Attribute(aid)
	if attr == nil {
		return nil, fmt.Errorf("no such attribute: %v", d.A)
	}
	val, err := d.V.Get(db, attr.Type() == index.Ref)
	if err != nil {
		return nil, err
	}
	if attr.Type() != val.Type() {
		return nil, fmt.Errorf("expected value of type %v, but got %#v of type %v",
			attr.Type(), val.Val(), val.Type())
	}
	return []RawDatum{RawDatum{d.Op, eid, aid, *val}}, nil
}

type Value struct {
	val    *index.Value
	lookup *database.HasLookup
}

func NewValue(value interface{}) Value {
	if lookup, ok := value.(database.HasLookup); ok {
		return Value{val: nil, lookup: &lookup}
	}
	val := index.NewValue(value)
	return Value{val: &val, lookup: nil}
}

func (v Value) Get(db *database.Db, isRef bool) (*index.Value, error) {
	if v.lookup != nil {
		if isRef {
			id, err := (*v.lookup).Lookup(db)
			if err != nil {
				return nil, err
			}
			ref := index.NewRef(id)
			return &ref, nil
		} else if kw, ok := (*v.lookup).(database.Keyword); ok {
			val := index.NewValue(kw.Keyword)
			return &val, nil
		} else {
			return nil, fmt.Errorf("invalid value: %v", v)
		}
	}
	return v.val, nil

}

type TxMap struct {
	Id         database.HasLookup
	Attributes map[database.Keyword][]index.Value
}

func (m TxMap) Resolve(db *database.Db) ([]RawDatum, error) {
	id, err := m.Id.Lookup(db)
	if err != nil {
		return nil, err
	}

	datums := make([]RawDatum, 0, len(m.Attributes))
	for k, vs := range m.Attributes {
		attrId := db.Entid(k)
		if attrId == -1 {
			return nil, fmt.Errorf("no such attribute: %v", k)
		}

		for _, v := range vs {
			datum := RawDatum{Assert, id, attrId, v}
			datums = append(datums, datum)
		}
	}
	return datums, nil
}
