package memory

import (
	"fmt"
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
