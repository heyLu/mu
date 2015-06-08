package index

import (
	"reflect"

	"../collection/btset"
	"../comparable"
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

type SegmentedIndex struct {
	root    *Root
	compare CompareFn
}

func (si SegmentedIndex) Datoms() Iterator {
	return si.DatomsAt(MinDatom, MaxDatom)
}

func (si SegmentedIndex) DatomsAt(start, end Datom) Iterator {
	return newIndexIterator(*si.root, si.compare, start, end)
}

func (si SegmentedIndex) SeekDatoms(start Datom) Iterator {
	return si.DatomsAt(start, MaxDatom)
}

type MergedIndex struct {
	memoryIndex    *MemoryIndex
	segmentedIndex *SegmentedIndex
	compare        comparable.CompareFn
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
