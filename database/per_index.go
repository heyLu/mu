package database

import (
	"../index"
)

// FilterAvetAndVaet returns the datoms that need to be placed in the
// avet and vaet indexes, respectively.
//
// TODO: It needs a better name
func FilterAvetAndVaet(db *Database, datoms []index.Datom) ([]index.Datom, []index.Datom) {
	avet := make([]index.Datom, 0, len(datoms))
	vaet := make([]index.Datom, 0, len(datoms))
	for _, datom := range datoms {
		if needsAvet(datom) {
			avet = append(avet, datom)
		}
		if needsVaet(datom) {
			vaet = append(vaet, datom)
		}
	}
	return avet, vaet
}

func needsAvet(datom index.Datom) bool {
	return datom.Attribute() == 10 // db/ident
}

func needsVaet(datom index.Datom) bool {
	return false
}
