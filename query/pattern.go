package query

import (
	"log"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
)

type Pattern struct {
	idx index.Type
	n   int
	e   database.HasLookup
	a   database.Keyword
	v   index.Value
}

func E(entity database.HasLookup) Pattern {
	return Pattern{idx: index.Eavt, n: 1, e: entity}
}

func Ea(entity database.HasLookup, attribute database.Keyword) Pattern {
	return Pattern{idx: index.Eavt, n: 2, e: entity, a: attribute}
}

func Eav(entity database.HasLookup, attribute database.Keyword, value interface{}) Pattern {
	return Pattern{idx: index.Eavt, n: 3, e: entity, a: attribute, v: index.NewValue(value)}
}

func A(attribute database.Keyword) Pattern {
	return Pattern{idx: index.Aevt, n: 1, a: attribute}
}

func Ae(attribute database.Keyword, entity database.HasLookup) Pattern {
	return Pattern{idx: index.Aevt, n: 2, a: attribute, e: entity}
}

func Aev(attribute database.Keyword, entity database.HasLookup, value interface{}) Pattern {
	return Pattern{idx: index.Aevt, n: 3, a: attribute, e: entity, v: index.NewValue(value)}
}

func Datoms(db *database.Db, pattern Pattern) (index.Iterator, error) {
	min := index.MinDatom
	max := index.MaxDatom

	minE, maxE := min.E(), max.E()
	minA, maxA := min.A(), max.A()
	minV, maxV := index.MinValue, index.MaxValue

	var idx index.Index

	switch pattern.idx {
	case index.Eavt:
		if pattern.n >= 1 {
			e, err := pattern.e.Lookup(db)
			if err != nil {
				return nil, err
			}
			minE, maxE = e, e
		}
		if pattern.n >= 2 {
			a, err := pattern.a.Lookup(db)
			if err != nil {
				return nil, err
			}
			minA, maxA = a, a
		}
		if pattern.n >= 3 {
			minV, maxV = pattern.v, pattern.v
		}
		idx = db.Eavt()
	case index.Aevt:
		if pattern.n >= 1 {
			a, err := pattern.a.Lookup(db)
			if err != nil {
				return nil, err
			}
			minA, maxA = a, a
		}
		if pattern.n >= 2 {
			e, err := pattern.e.Lookup(db)
			if err != nil {
				return nil, err
			}
			minE, maxE = e, e
		}
		if pattern.n >= 3 {
			minV, maxV = pattern.v, pattern.v
		}
		idx = db.Aevt()
	default:
		log.Fatal("invalid index type")
		return nil, nil
	}

	return idx.DatomsAt(
		index.NewDatom(minE, minA, minV, min.Tx(), min.Added()),
		index.NewDatom(maxE, maxA, maxV, max.Tx(), max.Added())), nil
}
