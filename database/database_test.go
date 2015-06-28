package database

import (
	tu "github.com/klingtnet/gol/util/testing"
	"testing"

	"github.com/heyLu/mu/index"
)

var datoms = []index.Datom{
	index.NewDatom(10, 1, "Jane", tToTx(1000), true),
	index.NewDatom(10, 2, 7, tToTx(1000), true),
	index.NewDatom(11, 1, "Alice", tToTx(1001), true),
	index.NewDatom(11, 2, 13, tToTx(1001), true),
	index.NewDatom(12, 1, "Fred", tToTx(1002), true),
}

var datoms2 = []index.Datom{
	index.NewDatom(10, 1, "Jane", tToTx(1003), false),
	index.NewDatom(10, 1, "Jane Lane", tToTx(1003), true),
}

func TestEavtDatoms(t *testing.T) {
	db := Empty.WithDatoms(datoms)
	expectIter(t, datoms, db.Eavt().Datoms())
}

func TestEavtDatomsWithRetractions(t *testing.T) {
	db := Empty.WithDatoms(datoms).WithDatoms(datoms2)

	expectIter(t,
		append([]index.Datom{datoms2[1]}, datoms[1:]...),
		db.Eavt().Datoms())
}

func TestEavtDatomsHistory(t *testing.T) {
	db := Empty.WithDatoms(datoms).WithDatoms(datoms2)
	historyDb := db.History()

	// ensure the original db wasn't changed
	tu.ExpectEqual(t, db.useHistory, false)
	expectIter(t,
		append([]index.Datom{datoms2[1]}, datoms[1:]...),
		db.Eavt().Datoms())

	// check that the history db contains all datoms (including retractions)
	tu.ExpectEqual(t, historyDb.useHistory, true)
	expectIter(t,
		append([]index.Datom{
			datoms2[0],
			datoms[0],
			datoms2[1],
		}, datoms[1:]...),
		historyDb.Eavt().Datoms())
}

func TestEavtDatomsSince(t *testing.T) {
	db := Empty.WithDatoms(datoms)
	sinceDb := db.Since(1001)

	// ensure the original db wasn't changed
	tu.ExpectEqual(t, db.since, -1)
	expectIter(t, datoms, db.Eavt().Datoms())

	// check that the since db contains the datoms since 1001
	tu.ExpectEqual(t, sinceDb.since, 1001)
	expectIter(t, datoms[2:], sinceDb.Eavt().Datoms())
}

func TestEavtDatomsAsOf(t *testing.T) {
	db := Empty.WithDatoms(datoms)
	asOfDb := db.AsOf(1001)

	// ensure the original db wasn't changed
	tu.ExpectEqual(t, db.asOf, -1)
	expectIter(t, datoms, db.Eavt().Datoms())

	// check that the as of db only contains the datom from 1001 and earlier
	tu.ExpectEqual(t, asOfDb.asOf, 1001)
	expectIter(t, datoms[0:4], asOfDb.Eavt().Datoms())
}

func TestEavtDatomsFilter(t *testing.T) {
	db := Empty.WithDatoms(datoms)
	filteredDb := db.Filter(func(db *Db, datom *index.Datom) bool {
		return datom.A() == 2
	})

	// ensure the original db wasn't changed
	tu.ExpectNil(t, db.filter)
	expectIter(t, datoms, db.Eavt().Datoms())

	// check that the filtered db contains only matching datoms
	tu.ExpectNotNil(t, filteredDb.filter)
	expectIter(t, []index.Datom{datoms[1], datoms[3]}, filteredDb.Eavt().Datoms())
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

func TestAevtDatomsWithRetractions(t *testing.T) {
	db := Empty.WithDatoms(datoms).WithDatoms(datoms2)
	expectIter(t, []index.Datom{
		datoms2[1],
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

func tToTx(t int) int {
	return 3*(1<<42) + t
}
