package index

import (
	"github.com/heyLu/fressian"
	"reflect"

	"../collection/btset"
	"../comparable"
)

type MemoryIndex struct {
	datoms *btset.Set
}

func NewMemoryIndex(compare comparable.CompareFn) *MemoryIndex {
	return &MemoryIndex{btset.New(compare)}
}

func (i *MemoryIndex) UseCompare(compare comparable.CompareFn) {
	i.datoms.UseCompare(compare)
}

func (i *MemoryIndex) Count() int {
	return i.datoms.Count()
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

var defaultHandler fressian.WriteHandler = btset.NewWriteHandler(WriteHandler)

var MemoryWriteHandler fressian.WriteHandler = func(w *fressian.Writer, val interface{}) error {
	switch val := val.(type) {
	case *MemoryIndex:
		return w.WriteExt("mu.memory.Index", val.datoms)
	default:
		return defaultHandler(w, val)
	}
}

var MemoryReadHandlers = map[string]fressian.ReadHandler{
	"mu.memory.Index": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		datomsRaw, _ := r.ReadValue()
		datoms := datomsRaw.(*btset.Set)
		return &MemoryIndex{datoms}
	},
	"btset.Set":         btset.ReadHandlers["btset.Set"],
	"btset.PointerNode": btset.ReadHandlers["btset.PointerNode"],
	"btset.LeafNode":    btset.ReadHandlers["btset.LeafNode"],
}
