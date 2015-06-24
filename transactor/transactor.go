package transactor

import (
	"log"
	"time"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	txlog "github.com/heyLu/mu/log"
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
	DbTxInstant      = 50 // :db/txInstant
)

type TxResult struct {
	DbBefore *database.Db
	DbAfter  *database.Db
	Tempids  map[int]int
	Datoms   []index.Datom
}

func Transact(db *database.Db, txData []TxDatum) (*txlog.LogTx, *TxResult, error) {
	txState := newTxState(db)
	//log.Println("max entities", txState.maxPartDbEntity, txState.maxPartUserEntity)

	datums, err := resolveTxData(db, txData)
	if err != nil {
		return nil, nil, err
	}

	datums, err = validate(db, datums)
	if err != nil {
		return nil, nil, err
	}

	datoms := assignIds(txState, db, datums)

	if !txState.hasTxInstant {
		datoms = append(datoms, index.NewDatom(txState.tx, DbTxInstant, time.Now(), txState.tx, Assert))
	}

	txResult := &TxResult{
		DbBefore: db,
		DbAfter:  db.WithDatomsT(db.NextT(), txState.nextId, datoms),
		Tempids:  txState.newEntityCache,
		Datoms:   datoms,
	}
	tx := txlog.NewTx(db.NextT(), datoms)
	return tx, txResult, nil
}

type txState struct {
	newEntityCache  map[int]int
	tx              int
	nextId          int
	nextPartDbId    int
	hasTxInstant    bool
	attributeValues map[int][]index.Value
}

func newTxState(db *database.Db) *txState {
	return &txState{
		newEntityCache:  map[int]int{},
		tx:              3*(1<<42) + db.NextT(),
		nextId:          db.NextT() + 1,
		nextPartDbId:    findMaxEntity(db, 0) + 1,
		hasTxInstant:    false,
		attributeValues: map[int][]index.Value{},
	}
}

func (txState *txState) resolveTempid(entity int) int {
	newEntity, ok := txState.newEntityCache[entity]
	if ok {
		return newEntity
	} else {
		newEntity := -1
		part := Part(entity)
		switch part {
		case DbPartDb:
			newEntity = txState.nextPartDbId
			txState.nextPartDbId += 1
		case DbPartUser:
			newEntity = part*(1<<42) + txState.nextId
			txState.nextId += 1
		case DbPartTx:
			newEntity = txState.tx
		default:
			log.Fatal("unknown partition:", Part(entity))
		}
		txState.newEntityCache[entity] = newEntity
		return newEntity
	}
}

func assignIds(txState *txState, db *database.Db, origDatoms []RawDatum) []index.Datom {
	datoms := make([]index.Datom, 0, len(origDatoms))
	for _, datom := range origDatoms {
		//log.Println("processing", datom)

		entity := datom.E
		if entity < 0 {
			if Part(entity) == DbPartTx && datom.A == DbTxInstant {
				// FIXME: ensure that :db/txInstant is greater than the last
				txState.hasTxInstant = true
			}
			entity = txState.resolveTempid(entity)
		}

		newDatom := index.NewDatom(entity, datom.A, datom.V.Val(), txState.tx, datom.Op)
		//log.Println("adding", newDatom)
		datoms = append(datoms, newDatom)
	}

	return datoms
}

func findMaxEntity(db *database.Db, part int) int {
	maxEntity := -1
	start := part * (1 << 42)
	end := (part + 1) * (1 << 42)
	// FIXME [perf]: implement `.Reverse()` for iterators
	iter := db.Eavt().DatomsAt(
		index.NewDatom(start, -1, "", -1, true),
		index.NewDatom(end, -1, "", -1, true))
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		if datom.Entity() > maxEntity {
			maxEntity = datom.Entity()
		}
	}

	if maxEntity < part*(1<<42) {
		return part*(1<<42) - 1
	} else {
		return maxEntity
	}

}

func previousValue(db *database.Db, datom RawDatum) (*index.Datom, bool) {
	// FIXME [perf]: this shouldn't be necessary.  indexes should know how to do this.
	//               probably...
	iter := db.Eavt().DatomsAt(index.NewDatom(datom.E, datom.A, "", -1, true), index.MaxDatom)
	prev := iter.Next()
	if prev == nil {
		return nil, false
	} else {
		return prev, true
	}
}

func Part(id int) int {
	sign := 1
	if id < 0 {
		sign = -1
	}

	return sign * id / (1 << 42)
}
