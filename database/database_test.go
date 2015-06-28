package database

import (
	tu "github.com/klingtnet/gol/util/testing"
	"testing"

	"github.com/heyLu/mu/index"
)

var datoms = []index.Datom{
	index.NewDatom(10, 1, "Jane", 1000, true),
	index.NewDatom(10, 2, 7, 1000, true),
	index.NewDatom(11, 1, "Alice", 1001, true),
	index.NewDatom(11, 2, 13, 1001, true),
	index.NewDatom(12, 1, "Fred", 1002, true),
}

func TestEavtDatoms(t *testing.T) {
	db := Empty.WithDatoms(datoms)
	expectIter(t, datoms, db.Eavt().Datoms())
}

func TestAevtDatoms(t *testing.T) {
	db := Empty.WithDatoms(datoms)
	expectIter(t, []index.Datom{
		datoms[0],
		datoms[2],
		datoms[4],
		datoms[1],
		datoms[3],
	}, db.Aevt().Datoms())
}

func expectIter(t *testing.T, expected []index.Datom, iter index.Iterator) {
	i := 0
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		expectDatom(t, *datom, expected[i])

		i += 1
	}
	tu.ExpectEqual(t, i, len(expected))
}

func expectDatom(t *testing.T, d1, d2 index.Datom) {
	tu.ExpectEqual(t, d1.Entity(), d2.Entity())
	tu.ExpectEqual(t, d1.Attribute(), d2.Attribute())
	tu.ExpectEqual(t, d1.Value().Compare(d2.Value()), 0)
	tu.ExpectEqual(t, d1.Transaction(), d2.Transaction())
	tu.ExpectEqual(t, d1.Added(), d2.Added())
}
