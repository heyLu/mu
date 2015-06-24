package mu

import (
	"github.com/heyLu/fressian"
	"net/url"

	"github.com/heyLu/mu/connection"
	_ "github.com/heyLu/mu/connection/backup"
	_ "github.com/heyLu/mu/connection/file"
	_ "github.com/heyLu/mu/connection/memory"
	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	"github.com/heyLu/mu/query"
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

// CreateDatabase creates a new database at the location given
// by the url.
//
// It returns true if a new database was created and false if
// it already existed.
func CreateDatabase(rawUrl string) (bool, error) {
	u, err := url.Parse(rawUrl)
	if err != nil {
		return false, err
	}
	return connection.CreateDatabase(u)
}

func Connect(rawUrl string) (connection.Connection, error) {
	u, err := url.Parse(rawUrl)
	if err != nil {
		return nil, err
	}
	return connection.New(u)
}

func Transact(conn connection.Connection, origDatoms []transactor.TxDatum) (*transactor.TxResult, error) {
	return conn.Transact(origDatoms)
}

func TransactString(conn connection.Connection, txDataEDN string) (*transactor.TxResult, error) {
	txData, err := transactor.TxDataFromEDN(txDataEDN)
	if err != nil {
		return nil, err
	}

	return Transact(conn, txData)
}

func With(db *database.Db, txData []transactor.TxDatum) (*database.Db, error) {
	_, txResult, err := transactor.Transact(db, txData)
	if err != nil {
		return nil, err
	}
	return txResult.DbAfter, nil
}

func NewDatum(entity database.HasLookup, attribute database.HasLookup, value interface{}) transactor.Datum {
	return transactor.Datum{true, entity, attribute, transactor.NewValue(value)}
}

func NewDatumRaw(entity int, attribute int, value interface{}) transactor.Datum {
	return transactor.Datum{true, database.Id(entity), database.Id(attribute), transactor.NewValue(value)}
}

func NewDatom(entity int, attribute int, value interface{}) index.Datom {
	return index.NewDatom(entity, attribute, value, -1, false)
}

func Retraction(datom index.Datom) transactor.Datum {
	return transactor.Datum{false, database.Id(datom.E()), database.Id(datom.A()), transactor.NewValue(datom.V())}
}

func Datums(datoms ...transactor.TxDatum) []transactor.TxDatum {
	return datoms
}

func E(entity database.HasLookup) query.Pattern {
	return query.E(entity)
}

func Ea(entity database.HasLookup, attribute database.Keyword) query.Pattern {
	return query.Ea(entity, attribute)
}

func Eav(entity database.HasLookup, attribute database.Keyword, value interface{}) query.Pattern {
	return query.Eav(entity, attribute, value)
}

func A(attribute database.Keyword) query.Pattern {
	return query.A(attribute)
}

func Ae(attribute database.Keyword, entity database.HasLookup) query.Pattern {
	return query.Ae(attribute, entity)
}

func Aev(attribute database.Keyword, entity database.HasLookup, value interface{}) query.Pattern {
	return query.Aev(attribute, entity, value)
}

func Datoms(db *database.Db, pattern query.Pattern) (index.Iterator, error) {
	return query.Datoms(db, pattern)
}

func DatomsString(db *database.Db, patternEDN string) (index.Iterator, error) {
	pattern, err := query.PatternFromEDN(patternEDN)
	if err != nil {
		return nil, err
	}

	return Datoms(db, *pattern)
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
