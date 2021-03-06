package transactor

import (
	"fmt"
	"github.com/heyLu/fressian"
	"net/url"

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

	isReverseAttr := false
	a := d.A
	if attr, ok := d.A.(database.Keyword); ok && attr.Name[0] == '_' {
		isReverseAttr = true
		a = database.Keyword{fressian.Keyword{Namespace: attr.Namespace, Name: attr.Name[1:]}}
	}
	aid, err := a.Lookup(db)
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
	if attr.Type() == index.Ref && val.Type() == index.Long {
		ref, err := database.Id(val.Val().(int)).Lookup(db)
		if err != nil {
			return nil, err
		}
		*val = index.NewRef(ref)
	} else if attr.Type() == index.URI && val.Type() == index.String {
		u, err := url.Parse(val.Val().(string))
		if err != nil {
			return nil, err
		}
		*val = index.NewValue(u)
	}

	rawDatum := RawDatum{d.Op, eid, aid, *val}
	if isReverseAttr {
		rawDatum = RawDatum{d.Op, val.Val().(int), aid, index.NewRef(eid)}
	}
	return []RawDatum{rawDatum}, nil
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
	Attributes map[database.Keyword][]Value
}

func (m TxMap) Resolve(db *database.Db) ([]RawDatum, error) {
	id, err := m.Id.Lookup(db)
	if err != nil {
		return nil, err
	}
	rId := resolvedId(id)

	datums := make([]RawDatum, 0, len(m.Attributes))
	for k, vs := range m.Attributes {
		for _, v := range vs {
			datum := Datum{Op: Assert, E: rId, A: k, V: v}
			rawDatum, err := datum.Resolve(db)
			if err != nil {
				return nil, err
			}

			datums = append(datums, rawDatum[0])
		}
	}
	return datums, nil
}

type resolvedId int

func (id resolvedId) Lookup(db *database.Db) (int, error) {
	return int(id), nil
}

type TxFn func(db *database.Db) ([]RawDatum, error)

func (f TxFn) Resolve(db *database.Db) ([]RawDatum, error) {
	return f(db)
}

func FnRetractEntity(id database.HasLookup) TxFn {
	retractEntity := func(db *database.Db) ([]RawDatum, error) {
		eid, err := id.Lookup(db)
		if err != nil {
			return nil, err
		}

		datums := make([]RawDatum, 0)
		iter := db.Eavt().DatomsAt(
			index.NewDatom(eid, 0, index.MinValue, 0, true),
			index.NewDatom(eid, index.MaxDatom.A(), index.MaxValue, index.MaxDatom.Tx(), true))
		for datom := iter.Next(); datom != nil; datom = iter.Next() {
			datum := RawDatum{Op: Retract, E: datom.E(), A: datom.A(), V: datom.V()}
			datums = append(datums, datum)
		}

		return datums, nil
	}

	return TxFn(retractEntity)
}

func FnCompareAndSwap(entity database.HasLookup, attribute database.HasLookup, oldValue *index.Value, newValue *index.Value) TxFn {
	compareAndSwap := func(db *database.Db) ([]RawDatum, error) {
		eid, err := entity.Lookup(db)
		if err != nil {
			return nil, err
		}

		aid, err := attribute.Lookup(db)
		if err != nil {
			return nil, err
		}

		datums := make([]RawDatum, 0)

		iter := db.Eavt().DatomsAt(
			index.NewDatom(eid, aid, oldValue, 0, true),
			index.NewDatom(eid, aid, oldValue, index.MaxDatom.Tx(), true))
		datom := iter.Next()
		if oldValue == nil { // old value must not exist
			if datom != nil {
				return nil, fmt.Errorf("cas failed, expected nil, but got %v", datom.V())
			}
		} else {
			if datom == nil || datom.V().Compare(oldValue) != 0 {
				return nil, fmt.Errorf("cas failed, expected %v, but got %v", oldValue, datom.V())
			}

			datums = append(datums, RawDatum{Op: Retract, E: eid, A: aid, V: *oldValue})
		}

		datums = append(datums, RawDatum{Op: Assert, E: eid, A: aid, V: *newValue})
		return datums, nil
	}

	return TxFn(compareAndSwap)
}
