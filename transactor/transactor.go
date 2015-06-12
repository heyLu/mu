package transactor

import (
	"log"

	"../database"
	"../index"
	txlog "../log"
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

func Transact(db *database.Db, txLog *txlog.Log, origDatoms []index.Datom) (*txlog.Log, *database.Db, error) {
	// TODO:
	//   - check for uniqueness
	//   - check types of values
	//   - ... (a lot)
	txState := newTxState(db)
	log.Println("max entities", txState.maxPartDbEntity, txState.maxPartUserEntity)

	datoms := assignIds(txState, db, origDatoms)

	return txLog, db.WithDatoms(datoms), nil
}

type txState struct {
	newEntityCache    map[int]int
	tx                int
	maxPartDbEntity   int
	maxPartUserEntity int
}

func newTxState(db *database.Db) *txState {
	return &txState{
		newEntityCache:    map[int]int{},
		tx:                findMaxTx(db) + 1,
		maxPartDbEntity:   findMaxEntity(db, DbPartDb) + 1,
		maxPartUserEntity: findMaxEntity(db, DbPartUser) + 1,
	}
}

func assignIds(txState *txState, db *database.Db, origDatoms []index.Datom) []index.Datom {
	datoms := make([]index.Datom, 0, len(origDatoms))
	for _, datom := range origDatoms {
		log.Println("processing", datom)

		// if db already contains a value for the attribute, retract it before adding the new value.
		// (assumes the attribute has cardinality one.)
		if prev, ok := previousValue(db, datom); datom.Entity() >= 0 && ok {
			log.Println("retracting", prev)
			datoms = append(datoms, prev.Retraction())

			// retractions don't need to be `added` or get new entity ids
			if !datom.Added() {
				continue
			}
		}

		entity := datom.Entity()
		if entity < 0 {
			newEntity, ok := txState.newEntityCache[entity]
			if ok {
				entity = newEntity
			} else {
				newEntity := -1
				switch Part(entity) {
				case DbPartDb:
					newEntity = txState.maxPartDbEntity
					txState.maxPartDbEntity += 1
				case DbPartUser:
					newEntity = txState.maxPartUserEntity
					txState.maxPartUserEntity += 1
				default:
					log.Fatal("unknown partition:", Part(entity))
				}
				txState.newEntityCache[entity] = newEntity
				entity = newEntity
			}
		}

		newDatom := index.NewDatom(entity, datom.A(), datom.V().Val(), txState.tx, datom.Added())
		log.Println("adding", newDatom)
		datoms = append(datoms, newDatom)
	}

	return datoms
}

const minTx = DbPartTx * (1 << 42)

func findMaxTx(db *database.Db) int {
	maxTx := -1
	// FIXME [perf]: implement `.Reverse()` for iterators
	// FIXME [perf]: start in the correct partition (only works if transactions have some attributes, such as :db/txInstant)
	iter := db.Eavt().Datoms()
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		if datom.Tx() > maxTx {
			maxTx = datom.Tx()
		}
	}
	if maxTx < minTx {
		return minTx - 1
	} else {
		return maxTx
	}
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

func previousValue(db *database.Db, datom index.Datom) (*index.Datom, bool) {
	// FIXME [perf]: this shouldn't be necessary.  indexes should know how to do this.
	//               probably...
	iter := db.Eavt().DatomsAt(index.NewDatom(datom.E(), datom.A(), "", -1, true), index.MaxDatom)
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
