package mu

import (
	"github.com/heyLu/fressian"
	"log"
	"net/url"

	"./connection"
	_ "./connection/backup"
	_ "./connection/file"
	_ "./connection/memory"
	"./database"
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
	// TODO:
	//   - check for uniqueness
	//   - check types of values
	//   - ... (a lot)
	db := conn.Db()

	newEntityCache := map[int]int{}

	tx := findMaxTx(db) + 1
	maxPartDbEntity := findMaxEntity(db, DbPartDb) + 1
	maxPartUserEntity := findMaxEntity(db, DbPartUser) + 1
	log.Println("max entities", maxPartDbEntity, maxPartUserEntity)

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
			newEntity, ok := newEntityCache[entity]
			if ok {
				entity = newEntity
			} else {
				newEntity := -1
				switch Part(entity) {
				case DbPartDb:
					newEntity = maxPartDbEntity
					maxPartDbEntity += 1
				case DbPartUser:
					newEntity = maxPartUserEntity
					maxPartUserEntity += 1
				default:
					log.Fatal("unknown partition:", Part(entity))
				}
				newEntityCache[entity] = newEntity
				entity = newEntity
			}
		}

		newDatom := index.NewDatom(entity, datom.A(), datom.V().Val(), tx, datom.Added())
		log.Println("adding", newDatom)
		datoms = append(datoms, newDatom)
	}

	return conn.TransactDatoms(datoms)
}

const minTx = DbPartTx * (1 << 42)

func findMaxTx(db *database.Database) int {
	maxTx := -1
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

func findMaxEntity(db *database.Database, part int) int {
	maxEntity := -1
	start := part * (1 << 42)
	end := (part + 1) * (1 << 42)
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

func previousValue(db *database.Database, datom index.Datom) (*index.Datom, bool) {
	iter := db.Eavt().DatomsAt(index.NewDatom(datom.E(), datom.A(), "", -1, true), index.MaxDatom)
	prev := iter.Next()
	if prev == nil {
		return nil, false
	} else {
		return prev, true
	}
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
