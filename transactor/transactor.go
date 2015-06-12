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
func Transact(conn connection.Conn, txData []TxDatum) (*TxResult, error) {
	transactionLock.Lock()
	defer transactionLock.Unlock()

	db := conn.Db()
	processedTxData := ExpandTxData(db, txData)
	datoms := RealizeDatums(db, processedTxData)

	// write to log (results in a new index root)
	//  - wrap new datoms in a LogTx
	//  - serialize previous LogTail + new LogTx
	//  - write new index (db?) root (only log tail is different)

	// create new db (i.e. update in-memory indexes)

	return nil, nil
}

// database
//  - access to in-memory index (for transactions)
//  - db root (= :index/root-id, :log/root-id, :log/tail
//    - :index/root-id (:eavt, :aevt, :avet, :vaet, :nextT)
//    - :log/root-id ([#log-dir [0 <id>]], <id> => [], i.e. empty log segments)
//    - :log/tail ([LogTx = :t, :id, :data])
//  - access to merged index (for .Datoms, .DatomsAt, .SeekDatoms)
//  - [basisT (for next tx?), nextT = basisT + 1?]
//
//  - we don't *have to* make the indexes public, we could just change the
//      define `.DatomsAt` and friends on the db (like datomic does)
//
//          db.DatomsAt(mu.Eavt, ...) // this does not seem ... right?
//  - the in-memory db does not need the segmented index
//  - the backup connection should allow transactions (easy, embed the std connection,
//      and override .Transact to always return an error)
//  - transactions only need to change the in-memory index, but they *do*
//      need to merge it with the on-disk index
//  - db.With could help, but it seems to accept txData, not just "finished" datoms
//  - db.IndexMemory *would* be an alternative

// func (db *database.Db) Eavt() Index
//                        Aevt(), Avet(), Vaet()
// func (db *database.Db) WithDatoms(datoms []Datom) *database.Database
// func (db *database.Db) RootId() string // transactor can use this to read and write new root

// indexer:
//  - indexes datoms from log tail (and writes new root, dirs and segments + log segments)
//  - notifies connections of new root
//      (maybe new root + last indexed tx?)

// transactor:
//  - should lock store for transactor ("<db-id>.locked" in store?)
//  - either in-process or remote (determines the type of the connection)
//  - remote transactor notifies connections of transactions as they happen
//  - remote transactor triggers indexing
//  - in-process transactor *does* indexing

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
