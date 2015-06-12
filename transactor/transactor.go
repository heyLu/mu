package transactor

import (
	"github.com/heyLu/fressian"
	"sync"

	"../database"
	"../index"
	"../store"
)

// The transactionLock will be locked when a new transaction is
// started.
var transactionLock *sync.Mutex

type OpType bool

const (
	OpAdd     = OpType(true)
	OpRetract = OpType(false)
)

type Datum struct {
	Op OpType
	E  HasLookup
	A  HasLookup
	V  index.Value
}

func (d Datum) Resolve() []Datum { return []Datum{d} }

type DbId struct {
	Part fressian.Keyword // or maybe HasLookup?
	Id   int
}

type TxDatum interface {
	Resolve() []Datum
}

type TxMap struct {
	Id         HasLookup
	Attributes map[fressian.Keyword]Value
}

func (m TxMap) Resolve() []Datum {
	datums := make([]Datum, 0, len(m.Attributes))
	for k, v := range m.Attributes {
		datums = append(datum, Datum{OpAdd, m.Id, k, v})
	}
	return datums
}

type TxResult struct {
	Tempids  map[DbId]int
	DbBefore *database.Database // FIXME: rename this to just database.Db
	DbAfter  *database.Database
	TxData   []index.Datom // or Datum, what does Datomic do?
}

//   - acquire write lock
//   - process tx data (new entity ids, tx id, ...)
//   - write processed datoms to the log (as a LogTx)
//       (this means writing a new db root)
//   - merge datoms with in-memory index
//   - create new db with updated in-memory index
//   - (?) check if re-index is needed
//   - release write lock
func Transact(conn connection.Conn, indexRootId string, txData []TxDatum) (*TxResult, error) {
	transactionLock.Lock()
	defer transactionLock.Unlock()

	db := conn.Db()
	processedTxData := ExpandTxData(db, txData)
	datoms := RealizeDatums(db, processedTxData)

	// write to log
	// create new db

	return nil, nil
}

// ExpandTxData prepares txData so that it can be transacted.
//
// - expands TxDatum's using .Resolve
// - checks for schema compliance
// - checks (and resolves) entity references
func ExpandTxData(db *database.Database, txData []TxDatum) ([]Datum, error) {
	return nil, nil
}

// Assigns ids, adds retractions (?), ...
func RealizeDatums(db *database.Database, datums []Datum) ([]index.Datom, error) {
	return nil, nil
}

type HasLookup interface {
	Lookup(db *database.Database) (int, bool)
}

// types implementing HasLookup (in theory
//
//  - DbIds (for entities, attributes and refs)
type DbId struct {
	Part Keyword // lookup needs to verify this is a partition ...
	Id   int
}

func (kw Keyword) Lookup(db *database.Database) (int, bool) {
	return -1, false
}

//  - lookup refs (kw + value, kw refers to a unique attribute)
type LookupRef struct {
	Attribute Keyword
	Value     Value
}

func (kw Keyword) Lookup(db *database.Database) (int, bool) {
	return -1, false
}

//  - keywords (resolves via the :db/ident attribute)
//     - does not work for `fressian.Keyword`s, because we
//         can't implement interfaces for external types
//     - option 1: don't use an interface, do it via reflection
//     - option 2: do it via user-level primitives (i.e. lookup
//         ahead of time)
//     - option 3: use an internal type for keywords, convert
//         when serializing with fressian  (only necessary for
//         values of type `index.Value`)
//        - keywords are constructed with `mu.Keyword` or directly
//           (or maybe we just wrap them?)
type Keyword struct {
	fressian.Keyword
}

func (kw Keyword) Lookup(db *database.Database) (int, bool) {
	return -1, false
}
