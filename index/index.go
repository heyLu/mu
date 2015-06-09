package index

import (
	"reflect"

	"../collection/btset"
	"../comparable"
	"../store"
)

const (
	Eavt = Type("eavt")
	Aevt = Type("aevt")
	Avet = Type("avet")
	Vaet = Type("vaet")
)

type Type string

type Index interface {
	Datoms() Iterator
	DatomsAt(start, end Datom) Iterator
	SeekDatoms(start Datom) Iterator
}

type Iterator interface {
	Next() *Datom
}

type MemoryIndex struct {
	datoms *btset.Set
}

func NewMemoryIndex(compare comparable.CompareFn) *MemoryIndex {
	return &MemoryIndex{btset.New(compare)}
}

type btsetIterator struct {
	iter btset.SetIter
}

func (it *btsetIterator) Next() *Datom {
	if it.iter == nil || reflect.ValueOf(it.iter).IsNil() {
		return nil
	} else {
		cur := it.iter.First()
		it.iter = it.iter.Next()
		return cur.(*Datom)
	}
}

func (mi MemoryIndex) Datoms() Iterator {
	return mi.DatomsAt(MinDatom, MaxDatom)
}

func (mi MemoryIndex) DatomsAt(start, end Datom) Iterator {
	return &btsetIterator{btset.Slice(mi.datoms, &start, &end)}
}

func (mi MemoryIndex) SeekDatoms(start Datom) Iterator {
	return mi.DatomsAt(start, MaxDatom)
}

func (mi MemoryIndex) AddDatoms(datoms []Datom) *MemoryIndex {
	set := mi.datoms
	for i := 0; i < len(datoms); i++ {
		datom := datoms[i]
		if datom.Added() {
			set = set.Conj(&datom)
		} else {
			set = set.Disj(&datom)
		}
	}
	return &MemoryIndex{set}
}

type SegmentedIndex struct {
	root    *Root
	store   store.Store
	compare CompareFn
}

func NewSegmentedIndex(root *Root, store store.Store, compare CompareFn) *SegmentedIndex {
	return &SegmentedIndex{root, store, compare}
}

func (si SegmentedIndex) Datoms() Iterator {
	return si.DatomsAt(MinDatom, MaxDatom)
}

func (si SegmentedIndex) DatomsAt(start, end Datom) Iterator {
	return newIndexIterator(si.store, *si.root, si.compare, start, end)
}

func (si SegmentedIndex) SeekDatoms(start Datom) Iterator {
	return si.DatomsAt(start, MaxDatom)
}

type MergedIndex struct {
	memoryIndex    *MemoryIndex
	segmentedIndex *SegmentedIndex
	compare        comparable.CompareFn
}

func NewMergedIndex(mi *MemoryIndex, si *SegmentedIndex, compare comparable.CompareFn) *MergedIndex {
	return &MergedIndex{mi, si, compare}
}

func (mi MergedIndex) Datoms() Iterator {
	return mi.DatomsAt(MinDatom, MaxDatom)
}

func (mi MergedIndex) DatomsAt(start, end Datom) Iterator {
	iter1 := mi.memoryIndex.DatomsAt(start, end)
	iter2 := mi.segmentedIndex.DatomsAt(start, end)
	return newMergeIterator(mi.compare, iter1, iter2)
}

func (mi MergedIndex) SeekDatoms(start Datom) Iterator {
	return mi.DatomsAt(start, MaxDatom)
}
