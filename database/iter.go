package database

import (
	"github.com/heyLu/mu/index"
)

func withoutRetractions(iter index.Iterator) index.Iterator {
	return &noRetractionsIterator{iter}
}

type noRetractionsIterator struct {
	iter index.Iterator
}

func (i *noRetractionsIterator) Next() *index.Datom {
	datom := i.iter.Next()
	if datom == nil {
		return nil
	}

	// The index is sorted such that retractions appear immediately
	// before the datom they are retracting.
	for datom == nil || !datom.Added() {
		datom = i.iter.Next()
		if datom == nil {
			panic("retraction without a value")
		}

		datom = i.iter.Next()
	}

	return datom
}

func (i *noRetractionsIterator) Reverse() index.Iterator {
	panic("not implemented")
}
