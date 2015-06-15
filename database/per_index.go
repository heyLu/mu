package database

import (
	"github.com/heyLu/mu/index"
)

// FilterAvetAndVaet returns the datoms that need to be placed in the
// avet and vaet indexes, respectively.
//
// TODO: It needs a better name
func FilterAvetAndVaet(db *Db, datoms []index.Datom) ([]index.Datom, []index.Datom) {
	avet := make([]index.Datom, 0, len(datoms))
	vaet := make([]index.Datom, 0, len(datoms))
	for _, datom := range datoms {
		if needsAvet(db, datom) {
			avet = append(avet, datom)
		}
		if needsVaet(db, datom) {
			vaet = append(vaet, datom)
		}
	}
	return avet, vaet
}

func needsAvet(db *Db, datom index.Datom) bool {
	a := datom.Attribute()
	switch a {
	case 10, // :db/ident
		39, // :fressian/tag
		50: // :db/txInstant
		return true
	default:
		attr := db.Attribute(a)
		return attr != nil && (attr.Indexed() || attr.Unique().IsValid())
	}
}

func needsVaet(db *Db, datom index.Datom) bool {
	a := datom.Attribute()
	switch a {
	case 11, 12, 13, 14, // :db.install/*
		15, 16, 19, // :db/excise, :db.excise/beforeT, :db.alter/attribute
		40, // :db/valueType
		41, // :db/cardinality
		42, // :db/unique
		46: // :db/lang
		return true
	default:
		attr := db.Attribute(a)
		return attr != nil && attr.Type() == index.Ref
	}
}
