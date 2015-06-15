package mu

import (
	"github.com/heyLu/fressian"
	"log"
	"net/url"

	"github.com/heyLu/mu/connection"
	_ "github.com/heyLu/mu/connection/backup"
	_ "github.com/heyLu/mu/connection/file"
	_ "github.com/heyLu/mu/connection/memory"
	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	"github.com/heyLu/mu/transactor"
)

const (
	DbIdent          = 10 // :db/ident
	DbCardinality    = 41 // :db/cardinality
	DbCardinalityOne = 35 // :db.cardinality/one
	DbType           = 12 // :db/valueType
	DbTypeString     = 23 // :db.type/string
	DbPartDb         = 0  // :db.part/db
	DbPartTx         = 3  // :db.part/tx
	DbPartUser       = 4  // :db.part/user
)

func Connect(u *url.URL) (connection.Connection, error) {
	return connection.New(u)
}

func Transact(conn connection.Connection, origDatoms []transactor.TxDatum) (*transactor.TxResult, error) {
	return conn.Transact(origDatoms)
}

func With(db *database.Db, txData []transactor.TxDatum) (*database.Db, error) {
	_, txResult, err := transactor.Transact(db, txData)
	if err != nil {
		return nil, err
	}
	return txResult.DbAfter, nil
}

func Datum(entity database.HasLookup, attribute database.HasLookup, value interface{}) transactor.Datum {
	return transactor.Datum{true, entity, attribute, transactor.NewValue(value)}
}

func RawDatum(entity int, attribute int, value interface{}) transactor.Datum {
	return transactor.Datum{true, database.Id(entity), database.Id(attribute), transactor.NewValue(value)}
}

func Datom(entity int, attribute int, value interface{}) index.Datom {
	return index.NewDatom(entity, attribute, value, -1, false)
}

func Retraction(datom index.Datom) transactor.Datum {
	return transactor.Datum{false, database.Id(datom.E()), database.Id(datom.A()), transactor.NewValue(datom.V())}
}

func Datums(datoms ...transactor.TxDatum) []transactor.TxDatum {
	return datoms
}

type DatomPattern struct {
	idx index.Type
	n   int
	e   database.HasLookup
	a   database.Keyword
	v   index.Value
}

func E(entity database.HasLookup) DatomPattern {
	return DatomPattern{idx: index.Eavt, n: 1, e: entity}
}

func Ea(entity database.HasLookup, attribute database.Keyword) DatomPattern {
	return DatomPattern{idx: index.Eavt, n: 2, e: entity, a: attribute}
}

func Eav(entity database.HasLookup, attribute database.Keyword, value interface{}) DatomPattern {
	return DatomPattern{idx: index.Eavt, n: 3, e: entity, a: attribute, v: index.NewValue(value)}
}

func A(attribute database.Keyword) DatomPattern {
	return DatomPattern{idx: index.Aevt, n: 1, a: attribute}
}

func Ae(attribute database.Keyword, entity database.HasLookup) DatomPattern {
	return DatomPattern{idx: index.Aevt, n: 2, a: attribute, e: entity}
}

func Aev(attribute database.Keyword, entity database.HasLookup, value interface{}) DatomPattern {
	return DatomPattern{idx: index.Aevt, n: 3, a: attribute, e: entity, v: index.NewValue(value)}
}

func Datoms(db *database.Db, pattern DatomPattern) (index.Iterator, error) {
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

func Id(id int) database.Id {
	return database.Id(id)
}

func Attribute(namespace, name string) database.Keyword {
	return database.Keyword{fressian.Keyword{namespace, name}}
}

func LookupRef(attribute database.Keyword, value interface{}) database.LookupRef {
	return database.LookupRef{attribute, index.NewValue(value)}
}

func Keyword(namespace, name string) database.Keyword {
	return database.Keyword{fressian.Keyword{namespace, name}}
}

func Tempid(part, id int) int {
	sign := -1
	if id > 0 {
		sign = 1
	}

	return -(part*(1<<42) + sign*id)
}

func Part(id int) int {
	sign := 1
	if id < 0 {
		sign = -1
	}

	return sign * id / (1 << 42)
}

func PartStart(part int) int {
	return part * (1 << 42)
}

func PartEnd(part int) int {
	return (part + 1) * (1 << 42)
}
