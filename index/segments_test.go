package index

import (
	tu "github.com/klingtnet/gol/util/testing"
	"testing"
)

type sliceIterator struct {
	datoms []Datom
	pos    int
}

func newSliceIterator(datoms ...Datom) Iterator {
	return &sliceIterator{
		datoms: datoms,
		pos:    0,
	}
}

func (i *sliceIterator) Next() *Datom {
	if i.pos < len(i.datoms) {
		i.pos += 1
		return &i.datoms[i.pos-1]
	}

	return nil
}

func TestSliceIterator(t *testing.T) {
	iter := newSliceIterator(
		NewDatom(0, 1, "Jane", 0, true),
		NewDatom(0, 2, 5, 0, true),
		NewDatom(1, 1, "Alice", 0, true),
		NewDatom(1, 2, 13, 0, true))
	c := 0
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		c += 1
	}
	tu.ExpectEqual(t, c, 4)
}

func TestFilterIterator(t *testing.T) {
	iter := newSliceIterator(
		NewDatom(0, 1, "Jane", 0, true),
		NewDatom(0, 2, 5, 0, true),
		NewDatom(1, 1, "Alice", 0, true),
		NewDatom(1, 2, 13, 0, true))
	filtered := FilterIterator(iter, func(datom *Datom) bool {
		return datom.V().Compare(NewValue("Alice")) == 0
	})
	datom := filtered.Next()
	tu.RequireNotNil(t, datom)
	tu.ExpectEqual(t, datom.V().Val(), "Alice")

	tu.RequireNil(t, filtered.Next())
}
