package mu

import (
	"github.com/heyLu/fressian"
	"net/url"

	"./connection"
	_ "./connection/backup"
	_ "./connection/file"
	_ "./connection/memory"
	"./index"
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

func Transact(conn connection.Connection, origDatoms []index.Datom) error {
	return conn.Transact(origDatoms)
}

func Datum(entity int, attribute int, value interface{}) index.Datom {
	return index.NewDatom(entity, attribute, value, -1, true)
}

func Datoms(datoms ...index.Datom) []index.Datom {
	return datoms
}

func Keyword(namespace, name string) fressian.Keyword {
	return fressian.Keyword{namespace, name}
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
