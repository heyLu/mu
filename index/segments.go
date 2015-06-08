package index

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/heyLu/fressian"
	"log"

	"../comparable"
	"../store"
)

type Root struct {
	tData       TransposedData
	directories []string
}

type Directory struct {
	tData    TransposedData
	segments []string
	mystery1 []int
	mystery2 []int
}

type TransposedData struct {
	values       []interface{}
	entities     []int
	attributes   []int
	transactions []int
	addeds       []bool
}

var readHandlers = map[string]fressian.ReadHandler{
	"index-root-node": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		tData, _ := r.ReadValue()
		directoriesRaw, _ := r.ReadValue()
		directories := make([]string, len(directoriesRaw.([]interface{})))
		for i, dir := range directoriesRaw.([]interface{}) {
			directories[i] = dir.(string)
		}
		return Root{
			tData:       tData.(TransposedData),
			directories: directories,
		}
	},
	"index-tdata": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		vs, _ := r.ReadValue()
		es, _ := r.ReadValue()
		as, _ := r.ReadValue()
		txs, _ := r.ReadValue()
		addeds, _ := r.ReadValue()
		return TransposedData{
			entities:     es.([]int),
			attributes:   as.([]int),
			values:       vs.([]interface{}),
			transactions: txs.([]int),
			addeds:       addeds.([]bool),
		}
	},
	"index-dir-node": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		tData, _ := r.ReadValue()
		segmentsRaw, _ := r.ReadValue()
		mystery1, _ := r.ReadValue()
		mystery2, _ := r.ReadValue()
		segments := make([]string, len(segmentsRaw.([]interface{})))
		for i, dir := range segmentsRaw.([]interface{}) {
			segments[i] = dir.(string)
		}
		return Directory{
			tData:    tData.(TransposedData),
			segments: segments,
			mystery1: mystery1.([]int),
			mystery2: mystery2.([]int),
		}
	},
}

type CompareFn func(tData TransposedData, idx int, datom Datom) int

func compareValue(a, b interface{}) int {
	//fmt.Println("compareValue", a, b)
	switch a := a.(type) {
	case int:
		b := b.(int)
		return a - b
	case string:
		b := b.(string)
		if a < b {
			return -1
		} else if a == b {
			return 0
		} else {
			return 1
		}
	default:
		log.Fatal("compareValue: not implemented (compare ", a, ", ", b, ")")
		return -1
	}
}

func CompareEavtIndex(tData TransposedData, idx int, datom Datom) int {
	//fmt.Println("compare", datom, "at", idx)
	cmp := tData.entities[idx] - datom.entity
	if cmp != 0 {
		return cmp
	}

	cmp = tData.attributes[idx] - datom.attribute
	if cmp != 0 {
		return cmp
	}

	cmp = NewValue(tData.values[idx]).Compare(datom.value)
	if cmp != 0 {
		return cmp
	}

	return tData.transactions[idx] - datom.transaction
}

// Find finds the closest (first) datom that is greater or equal to `datom`.
//
// invariants:
//   - returns len(t.entities) if all datoms are smaller
//   - the datom at the index is greater or equal
func (t TransposedData) Find(compare CompareFn, datom Datom) int {
	l := 0
	r := len(t.entities) - 1

	for {
		if l <= r {
			m := (l + r) / 2
			cmp := compare(t, m, datom)
			if cmp < 0 {
				l = m + 1
			} else {
				r = m - 1
			}
		} else {
			return l
		}
	}
}

func (t TransposedData) FindApprox(compare CompareFn, datom Datom) int {
	idx := t.Find(compare, datom)
	if idx > 0 && idx < len(t.entities) {
		cmpPrev := compare(t, idx-1, datom)
		if cmpPrev < 0 {
			return idx - 1
		} else {
			return idx
		}
	} else {
		return idx
	}
}

func (t TransposedData) DatomAt(idx int) Datom {
	return Datom{
		entity:      t.entities[idx],
		attribute:   t.attributes[idx],
		value:       NewValue(t.values[idx]),
		transaction: 3*(1<<42) + t.transactions[idx],
		added:       t.addeds[idx],
	}
}

var tmpGlobalCache = map[string]interface{}{}

func getFromCache(id string) interface{} {
	if val, ok := tmpGlobalCache[id]; ok {
		return val
	}

	log.Printf("getFromCache: globalStore.Get(%s)\n", id)
	data, err := globalStore.Get(id)
	if err != nil {
		log.Fatal("getFromCache: globalStore.Get: ", err)
		return nil
	}

	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		log.Fatal("getFromCache: gzip.NewReader: ", err)
		return nil
	}

	r := fressian.NewReader(gz, readHandlers)
	val, err := r.ReadValue()
	if err != nil {
		log.Fatal("getFromCache: r.ReadValue: ", err)
		return nil
	}

	tmpGlobalCache[id] = val
	return val
}

func getRoot(id string) Root              { return getFromCache(id).(Root) }
func getDirectory(id string) Directory    { return getFromCache(id).(Directory) }
func getSegment(id string) TransposedData { return getFromCache(id).(TransposedData) }

func (d Directory) Find(compare CompareFn, datom Datom) (int, int) {
	dirIdx := 0
	if len(d.segments) > 1 {
		dirIdx = d.tData.FindApprox(compare, datom)
	}
	if dirIdx < len(d.segments) {
		segmentIdx := getSegment(d.segments[dirIdx]).Find(compare, datom)
		return dirIdx, segmentIdx
	} else {
		return len(d.segments), 0
	}
}

func (r Root) Find(compare CompareFn, datom Datom) (int, int, int) {
	rootIdx := 0
	if len(r.directories) > 1 {
		rootIdx = r.tData.FindApprox(compare, datom)
	}
	if rootIdx < len(r.directories) {
		dirIdx, segmentIdx := getDirectory(r.directories[rootIdx]).Find(compare, datom)
		return rootIdx, dirIdx, segmentIdx
	} else {
		return len(r.directories), 0, 0
	}
}

type emptyIterator struct{}

func (i emptyIterator) Next() *Datom {
	return nil
}

type indexIterator struct {
	rootIdx, rootStart, rootEnd          int
	dirIdx, dirStart, dirEnd             int
	segmentIdx, segmentStart, segmentEnd int
	root                                 Root
	directory                            Directory
	segment                              TransposedData
}

func newIndexIterator(root Root, compare CompareFn, start, end Datom) Iterator {
	rs, ds, ss := root.Find(compare, start)
	fmt.Println(rs, ds, ss)
	re, de, se := root.Find(compare, end)
	fmt.Println(re, de, se)
	if rs >= len(root.directories) {
		return emptyIterator{}
	}
	directory := getDirectory(root.directories[rs])
	if ds >= len(directory.segments) {
		return emptyIterator{}
	}
	segment := getSegment(directory.segments[ds])
	return &indexIterator{
		rs, rs, re,
		ds, ds, de,
		ss - 1, ss, se - 1,
		root, directory, segment,
	}
}

func (i *indexIterator) atEnd() bool {
	return i.rootIdx >= i.rootEnd && i.dirIdx >= i.dirEnd && i.segmentIdx >= i.segmentEnd
}

func (i *indexIterator) Next() *Datom {
	if i.atEnd() {
		return nil
	}

	if i.segmentIdx < len(i.segment.entities)-1 {
		i.segmentIdx += 1
	} else if i.dirIdx < len(i.directory.segments)-1 {
		i.dirIdx += 1
		i.segment = getSegment(i.directory.segments[i.dirIdx])
		i.segmentIdx = 0
	} else if i.rootIdx < i.rootEnd && i.rootIdx < len(i.root.directories)-1 {
		i.rootIdx += 1
		i.dirIdx = 0
		i.segmentIdx = 0
		i.directory = getDirectory(i.root.directories[i.rootIdx])
		i.segment = getSegment(i.directory.segments[i.dirIdx])
	} else {
		return nil
	}

	datom := i.segment.DatomAt(i.segmentIdx)
	return &datom
}

type mergeIterator struct {
	compare comparable.CompareFn
	iter1   Iterator
	datom1  *Datom
	iter2   Iterator
	datom2  *Datom
}

func newMergeIterator(compare comparable.CompareFn, iter1, iter2 Iterator) Iterator {
	datom1 := iter1.Next()
	if datom1 == nil {
		return iter2
	}
	datom2 := iter2.Next()
	if datom2 == nil {
		return iter1
	}
	return &mergeIterator{compare, iter1, datom1, iter2, datom2}
}

func (i *mergeIterator) Next() *Datom {
	if i.datom1 == nil {
		return i.iter2.Next()
	}
	if i.datom2 == nil {
		return i.iter1.Next()
	}

	cmp := i.compare(i.datom1, i.datom2)
	if cmp < 0 {
		datom := i.datom1
		i.datom1 = i.iter1.Next()
		return datom
	} else if cmp == 0 {
		datom := i.datom1
		i.datom1 = i.iter1.Next()
		i.datom2 = i.iter2.Next()
		return datom
	} else {
		datom := i.datom2
		i.datom2 = i.iter2.Next()
		return datom
	}
}

var globalStore store.Store
