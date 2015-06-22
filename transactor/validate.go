package transactor

import (
	"fmt"
	"log"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
)

// - prevent transacting the same [e a v] twice
//     - this implicitely prevents :db.cardinality/many with the same
//         values
// - cardinality
// - uniqueness constraints
//     - merge datums for existing entities using :db.unique/identity
//     - prevent duplicate values for :db.unique/value
// - attribute types
// - :db.part/db restrictions (for new entities, either just :db/ident,
//     or more attributes + :db.install/attribute)

// validate verifies that the datums are a valid transaction.
func validate(db *database.Db, datums []RawDatum) ([]RawDatum, error) {
	err := checkTypes(db, datums)
	if err != nil {
		return nil, err
	}

	err = validateUniqueness(db, datums)
	if err != nil {
		return nil, err
	}

	newDatums, err := removeNoops(db, datums)
	if err != nil {
		return nil, err
	}

	newDatums, err = validateCardinality(db, newDatums)
	if err != nil {
		return nil, err
	}

	return newDatums, nil
}

func checkTypes(db *database.Db, datums []RawDatum) error {
	for i, datum := range datums {
		val := datum.V

		attr := db.Attribute(datum.A)
		if attr == nil {
			return fmt.Errorf("unknown attribute %d", datum.A)
		}

		if attr.Type() != val.Type() {
			return fmt.Errorf("expected value of type %v, but got %#v of type %v",
				attr.Type(), val.Val(), val.Type())
		}

		if attr.Type() == index.Ref {
			datums[i].V = index.NewValue(val.Val())
		}
	}

	return nil
}

func validateUniqueness(db *database.Db, datums []RawDatum) error {
	mergedIds := make(map[int]int)

	for i, datum := range datums {
		attr := db.Attribute(datum.A)

		switch attr.Unique() {
		case database.UniqueValue:
			prev, ok := existsUniqueValue(db, datum.A, datum.V)
			if ok {
				return fmt.Errorf("not unique, value for %v already exists: %v", attr.Ident(), prev)
			}
		case database.UniqueIdentity:
			if datum.E < 0 {
				prev, ok := existsUniqueValue(db, datum.A, datum.V)
				if ok {
					log.Printf("merging %d with %d\n", datum.E, prev.E())
					mergedIds[datum.E] = prev.E()
					datums[i].E = prev.E()
				}
			} else {
				prev, ok := existsUniqueValue(db, datum.A, datum.V)
				if ok {
					return fmt.Errorf("not unique, value for %v already exists: %v", attr.Ident(), prev)
				}
			}
		case database.UniqueNil:
		default:
			return fmt.Errorf("invalid unique value for attribute %d: %v", datum.A, attr.Unique())
		}
	}

	for i, datum := range datums {
		if id, ok := mergedIds[datum.E]; ok {
			datums[i].E = id
		}
	}

	return nil
}

func existsUniqueValue(db *database.Db, attrId int, val index.Value) (*index.Datom, bool) {
	iter := db.Avet().DatomsAt(
		index.NewDatom(0, attrId, val, 0, true),
		index.NewDatom(index.MaxDatom.E(), attrId, val, index.MaxDatom.Tx(), true))
	datom := iter.Next()
	log.Println("exists unique value?", attrId, val, datom)
	return datom, datom != nil
}

func removeNoops(db *database.Db, datums []RawDatum) ([]RawDatum, error) {
	newDatums := make([]RawDatum, 0, len(datums))
	duplicates := make(map[RawDatum]bool)

	for _, datum := range datums {
		if _, ok := duplicates[datum]; ok {
			continue
		}
		duplicates[datum] = true

		exists := alreadyExists(db, datum)
		if datum.Op == Assert && !exists {
			newDatums = append(newDatums, datum)
		} else if datum.Op == Retract && exists {
			newDatums = append(newDatums, datum)
		} else {
			log.Println("noop", datum)
		}
	}

	return newDatums, nil
}

func alreadyExists(db *database.Db, datum RawDatum) bool {
	if datum.E < 0 {
		return false
	}

	iter := db.Eavt().DatomsAt(
		index.NewDatom(datum.E, datum.A, datum.V, 0, true),
		index.NewDatom(datum.E, datum.A, datum.V, index.MaxDatom.Tx(), true))
	datom := iter.Next()
	log.Println("alreadyExists?", datom, datum)
	return datom != nil
}

type prevDatum struct {
	e int
	a int
}

func validateCardinality(db *database.Db, datums []RawDatum) ([]RawDatum, error) {
	newDatums := make([]RawDatum, 0, len(datums))
	cardinalityOneAttributes := make(map[prevDatum]bool)

	for _, datum := range datums {
		attr := db.Attribute(datum.A)

		switch attr.Cardinality() {
		case database.CardinalityOne:
			_, ok := cardinalityOneAttributes[prevDatum{e: datum.E, a: datum.A}]
			if ok {
				return nil, fmt.Errorf("duplicate value for %v: %d", attr.Ident(), datum.A)
			}
			cardinalityOneAttributes[prevDatum{e: datum.E, a: datum.A}] = true

			prev := existingAttribute(db, datum.E, datum.A)
			if prev != nil {
				retractPrev := RawDatum{
					Op: Retract,
					E:  datum.E,
					A:  datum.A,
					V:  prev.Value(),
				}
				newDatums = append(newDatums, retractPrev)
			}
			newDatums = append(newDatums, datum)
		case database.CardinalityMany:
			newDatums = append(newDatums, datum)
		default:
			return nil, fmt.Errorf("invalid cardinality for %v: %v", datum, attr.Cardinality)
		}
	}

	return newDatums, nil
}

func existingAttribute(db *database.Db, entity int, attribute int) *index.Datom {
	if entity < 0 {
		return nil
	}

	iter := db.Eavt().DatomsAt(
		index.NewDatom(entity, attribute, index.MinValue, 0, true),
		index.NewDatom(entity, attribute, index.MaxValue, index.MaxDatom.Tx(), true))
	datom := iter.Next()
	log.Println("existingAttribute", datom)
	return datom
}

// Ok, let's say we have the following attributes:
//
// (Ident, type, cardinality, uniqueness)
// :name, string, one, -
// :age, int, one, -
// :id, uuid, one, value

// A simple transaction:
//
// [:db/add -1 :name "Jane"]
// [:db/add -1 :id xyz]
//
// Ok, 2 new datoms.

// Changing the name
//
// [:db/add [:id xyz] :name "Jane Lane"]
//
// Ok, automatic retraction (because of the cardinality).
// [:db/retract [:id xyz] :name "Jane"]
// [:db/add [:id xyz] :name "Jane Lane"]

// Invalid new entity
//
// [:db/add -1 :name "Fred"]
// [:db/add -1 :id xyz]
//
// Error, existing entity with id `xyz`.

// Upserted new entity (if :name has identity uniqueness)
//
// [:db/add -1 :name "Jane Lane"]
// [:db/add -1 :age 13]
// [:db/add -1 :id xyz]
//
// Ok, retraction
