package index

import (
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
