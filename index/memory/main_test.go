package memory

import (
	"fmt"
	tu "github.com/klingtnet/gol/util/testing"
	"testing"

	index ".."
)

var (
	d1 = index.NewDatom(0, 1, "hey", 2, true)
	d2 = index.NewDatom(1, 1, "ho", 2, true)
	d3 = index.NewDatom(2, 0, "huh", 2, true)
)

func TestEavt(t *testing.T) {
	eavt := New(index.CompareEavt)
	eavt = eavt.AddDatoms([]index.Datom{d1, d2, d3})
	iter := eavt.Datoms()
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		fmt.Println(datom)
	}
}

func TestAevt(t *testing.T) {
	aevt := New(index.CompareAevt)
	aevt = aevt.AddDatoms([]index.Datom{d1, d2, d3})
	iter := aevt.Datoms()
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		fmt.Println(datom)
	}
}

func TestSeekDatoms(t *testing.T) {
	eavt := New(index.CompareEavt)
	eavt = eavt.AddDatoms([]index.Datom{d1, d2, d3})

	// start at [1 _ _ _ _], until the end
	start := index.NewDatom(1, -1, "", -1, false)
	iter := eavt.SeekDatoms(start, index.MaxDatom)
	tu.RequireNotNil(t, iter)

	datom := iter.Next()
	tu.RequireNotNil(t, datom)
	expectEqual(t, *datom, d2)

	datom = iter.Next()
	tu.RequireNotNil(t, datom)
	expectEqual(t, *datom, d3)
}

func expectEqual(t *testing.T, d1, d2 index.Datom) {
	if d1.E() != d2.E() || d1.A() != d2.A() || d1.V().Compare(d2.V()) != 0 || d1.Tx() != d2.Tx() {
		t.Errorf("%#v != %#v", d1, d2)
	}
}
