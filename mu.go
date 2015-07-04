// Package mu provides the user-facing api for the mu database.
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
	"github.com/heyLu/mu/pattern"
	"github.com/heyLu/mu/query"
	"github.com/heyLu/mu/transactor"
)

const (
	DbIdent          = 10 // :db/ident
	DbCardinality    = 41 // :db/cardinality
	DbCardinalityOne = 35 // :db.cardinality/one
	DbType           = 40 // :db/valueType
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

// Connect connects to the database specified at the location
// given by the url.
//
// The following formats are supported:
//
//  - memory://<path>?name=<name>
//      Connects to an in-memory database with the given name.
//  - files://<path-to-dir>?name=<name>
//      Connects to an on-disk database in the directory with
//      the given name.  A single directory may contain multiple
//      databases with different names.
//  - file://<path-to-file>
//      Connects to a single-file database.  This database will
//      not support the future history api, only the log.
//  - backup://<path-to-backup>[?root=<t>]
//      Connects to a datomic backup, with an optional root if
//      the directory contains multiple backups.
func Connect(rawUrl string) (connection.Connection, error) {
	u, err := url.Parse(rawUrl)
	if err != nil {
		return nil, err
	}
	return connection.New(u)
}

// Transact adds the datoms given by the txData to the connection.
//
// The result contains a reference to the database before and after
// the transaction, the datoms that were transacted and a map from
// tempids to the assigned ids.
func Transact(conn connection.Connection, txData []transactor.TxDatum) (*transactor.TxResult, error) {
	return conn.Transact(txData)
}

// TransactString adds the datoms given by the txData to the
// connection.
//
// The txData is given as EDN data, which is parsed and converted
// to the format accepted by Transact.
//
// Apart from the different input, the behavior is the same as
// that of Transact.
func TransactString(conn connection.Connection, txDataEDN string) (*transactor.TxResult, error) {
	txData, err := transactor.TxDataFromEDN(txDataEDN)
	if err != nil {
		return nil, err
	}

	return Transact(conn, txData)
}

// With returns a database with the txData added as if it were
// transacted.
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

func E(entity database.HasLookup) pattern.Pattern {
	return pattern.E(entity)
}

func Ea(entity database.HasLookup, attribute database.Keyword) pattern.Pattern {
	return pattern.Ea(entity, attribute)
}

func Eav(entity database.HasLookup, attribute database.Keyword, value interface{}) pattern.Pattern {
	return pattern.Eav(entity, attribute, value)
}

func A(attribute database.Keyword) pattern.Pattern {
	return pattern.A(attribute)
}

func Ae(attribute database.Keyword, entity database.HasLookup) pattern.Pattern {
	return pattern.Ae(attribute, entity)
}

func Aev(attribute database.Keyword, entity database.HasLookup, value interface{}) pattern.Pattern {
	return pattern.Aev(attribute, entity, value)
}

// Datoms returns an iterator matching the given pattern.
func Datoms(db *database.Db, p pattern.Pattern) (index.Iterator, error) {
	return pattern.Datoms(db, p)
}

// DatomsString parses a pattern from the string and returns
// an iterator.
//
// The pattern must be a string in EDN format of the form
// [e a v], where e, a and v may be variables or values.
//
// See Datoms for details.
func DatomsString(db *database.Db, patternEDN string) (index.Iterator, error) {
	pattern, err := pattern.PatternFromEDN(patternEDN)
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

// Tempid creates a temporary id in the given partition for
// use in a transaction.
func Tempid(part, id int) int {
	sign := -1
	if id > 0 {
		sign = 1
	}

	return -(part*(1<<42) + sign*id)
}

// Part returns the partition id of the given entity id.
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

func Q(q query.Query, inputs ...interface{}) (interface{}, error) {
	return query.Q(q, inputs...)
}
