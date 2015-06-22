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
	// TODO:
	//   - automatically retract value "overwrites"  (for :db.cardinality/one
	//       attributes, :db.unique/identity attributes need special handling)
	//       - should be handled by enforcing :db.cardinality/one behavior below
	//   - enforce :db.cardinality/one, allow :db.cardinality/many
	//       - check for repeated :db.cardinality/one attributes
	//       - if there is already a :db.cardinality/one value in the db,
	//           add a retraction for it
	//       - only allow multiple values in tx for :db.cardinality/many
	//           (might be enough to check for :db.cardinality/one, and let
	//            the rest through)
	//   - enforce :db.unique/value and :db.unique/identity
	//       - for :db.unique/value return an error if there is an entity
	//           with that attribute and value, use the avet index for it
	//       - for :db.unique/identity, check if there is an entity with
	//           that attribute and value, and if so assign the attributes
	//           from this tx to that entity.  need to do other attribute
	//           checking for after doing that.  (e.g. to enforce that
	//           :db/unique attributes on the entity are valid.)
	//   - prevent the same values being asserted multiple time
	//       - after processing and checking the tx, check for duplicates
	//           in the tx itself, and for duplicate values of existing
	//           entities, and remove them from the transaction.  (the
	//           "check for already existing" check seems quite a bit
	//           expensive, though.)
	//   - assign entity ids like datomic
	//        - part * (1 << 42) + nextT + 1 + newEntityIndex
	//        - nextT becomes the basis of the the resulting db value
	//        - nextT for the new db value is nextT + numNewEntities + 1
	//        - this is quite interesting as the tx id is encoded in the
	//           entity id, and you can find entities that are created
	//           at or after a certain transactions  (and it's also easier
	//           to generate new entity ids, but that's just a bonus)
	//   - (maybe: check attribute modifications, require :db.install/attribute
	//       and friends for schema changes)
	//        - might be easiest to not allow changes to :db.part/db
	//            entities for now (and explicit in which changes are allowed,
	//            schema alterations can come later)
	//   - regarding transacting datoms in the :db.part/db
	//       - unused value with a :db/ident is allowed (for "enums"?)
	//       - entities with :db/valueType, :db/cardinality & friends
	//           are *required* to be followed by :db.install/...
	//           - e.g. "just :db/ident or you're an attribute (or maybe
	//               also a partition, db fn, ...)
	//       - partitions are entities with a :db/ident and :db.install/_partition
	//       - mhh... maybe *that's* how partitions (and attributes)
	//           are identified, via the corresponding :db.install/* attribute
	//           on the :db.part/db entity?
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

	txResult := &TxResult{
		DbBefore: db,
		DbAfter:  db.WithDatoms(datoms),
		Tempids:  txState.newEntityCache,
		Datoms:   datoms,
	}
	tx := txlog.NewTx(txState.tx, datoms)
	return tx, txResult, nil
}

type txState struct {
	newEntityCache    map[int]int
	tx                int
	maxPartDbEntity   int
	maxPartUserEntity int
	hasTxInstant      bool
	attributeValues   map[int][]index.Value
}

func newTxState(db *database.Db) *txState {
	return &txState{
		newEntityCache:    map[int]int{},
		tx:                findMaxTx(db) + 1,
		maxPartDbEntity:   findMaxEntity(db, DbPartDb) + 1,
		maxPartUserEntity: findMaxEntity(db, DbPartUser) + 1,
		hasTxInstant:      false,
		attributeValues:   map[int][]index.Value{},
	}
}

func (txState *txState) resolveTempid(entity int) int {
	newEntity, ok := txState.newEntityCache[entity]
	if ok {
		return newEntity
	} else {
		newEntity := -1
		switch Part(entity) {
		case DbPartDb:
			newEntity = txState.maxPartDbEntity
			txState.maxPartDbEntity += 1
		case DbPartUser:
			newEntity = txState.maxPartUserEntity
			txState.maxPartUserEntity += 1
		case DbPartTx:
			newEntity = txState.tx
		default:
			log.Fatal("unknown partition:", Part(entity))
		}
		txState.newEntityCache[entity] = newEntity
		return newEntity
	}
}

// TODO:
//   - cardinality checks (however, :db.cardinality/one automatically adds a retraction?)
//   - uniqueness checks
//
//   - all of the above checks seem to require one pass over the data, and can only
//       checked afterwards.  (some things can be checked while iterating over the
//       datoms, but not all, e.g. :db.cardinality/one?)
//   - meh, this is quite unclear still...
func assignIds(txState *txState, db *database.Db, origDatoms []RawDatum) []index.Datom {
	datoms := make([]index.Datom, 0, len(origDatoms))
	for _, datom := range origDatoms {
		//log.Println("processing", datom)

		// if db already contains a value for the attribute, retract it before adding the new value.
		// (assumes the attribute has cardinality one.)
		/*if prev, ok := previousValue(db, datom); datom.E >= 0 && ok {
			//log.Println("retracting", prev)
			datoms = append(datoms, prev.Retraction())

			// retractions don't need to be `added` or get new entity ids
			if !datom.Op {
				continue
			}
		}*/

		entity := datom.E
		if entity < 0 {
			if Part(entity) == DbPartTx && datom.A == DbTxInstant {
				// FIXME: check for multiple :db/txInstant values?
				//   (likely more general: check for cardinality)
				txState.hasTxInstant = true
			}
			entity = txState.resolveTempid(entity)
		}

		newDatom := index.NewDatom(entity, datom.A, datom.V.Val(), txState.tx, datom.Op)
		//log.Println("adding", newDatom)
		datoms = append(datoms, newDatom)
	}

	if !txState.hasTxInstant {
		datoms = append(datoms, index.NewDatom(txState.tx, DbTxInstant, time.Now(), txState.tx, Assert))
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
