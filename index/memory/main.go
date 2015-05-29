package memory

import (
	"log"

	index ".."
	"../../collection/btset"
	"../../comparable"
)

type Index struct {
	datoms *btset.Set
}

func New(compare comparable.CompareFn) *Index {
	return &Index{btset.New(compare)}
}

func (i *Index) UseCompare(compare comparable.CompareFn) {
	i.datoms.UseCompare(compare)
}

type iterator struct {
	iter btset.SetIter
}

func (it *iterator) Next() *index.Datom {
	if it.iter == nil {
		return nil
	} else {
		cur := it.iter.First()
		it.iter = it.iter.Next()
		return cur.(*index.Datom)
	}
}

func (i *Index) AddDatoms(datoms []index.Datom) *Index {
	set := i.datoms
	for i := 0; i < len(datoms); i++ {
		datom := datoms[i]
		set = set.Conj(&datom)
	}
	return &Index{set}
}

func (i *Index) Datoms() index.Iterator {
	return &iterator{i.datoms.Iter()}
}

func (i *Index) SeekDatoms(keys ...index.Datom) index.Iterator {
	log.Fatal("not implemented")
	return nil
}
