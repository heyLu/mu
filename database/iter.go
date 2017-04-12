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
	for datom != nil && !datom.Added() {
		datom = i.iter.Next()
		if datom == nil {
			panic("retraction without a value")
		}

		datom = i.iter.Next()
	}

	return datom
}

func (i *noRetractionsIterator) Reverse() index.Iterator {
	return &reverseNoRetractionsIterator{iter: i.iter.Reverse()}
}

type reverseNoRetractionsIterator struct {
	iter  index.Iterator
	datom *index.Datom
}

func (i *reverseNoRetractionsIterator) Next() *index.Datom {
	for {
		// the index is reversed, so we always need to check for two
		// datoms at once.
		datom1 := i.datom
		if datom1 == nil {
			datom1 = i.iter.Next()
		}
		datom2 := i.iter.Next()

		if datom1 == nil && datom2 == nil { // at end, no more values
			return nil
		} else if datom2 == nil { // only one more value
			i.datom = nil
			return datom1
		} else if !datom2.Added() { // retraction, skip two
			i.datom = nil
			continue
		} else { // no retraction, return first, store second
			i.datom = datom2
			return datom1
		}
	}
}

func (i *reverseNoRetractionsIterator) Reverse() index.Iterator {
	panic("not implemented")
}
