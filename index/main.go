package index

import (
	"github.com/heyLu/fressian"
	"log"

	"../storage"
)

const (
	Eavt = "eavt"
	Aevt = "aevt"
	Avet = "avet"
	Vaet = "vaet"
)

var readHandlers = map[string]fressian.ReadHandler{
	"index-root-node": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		tData, _ := r.ReadObject()
		directories, _ := r.ReadObject()
		return IndexRootNode{
			nil,
			nil,
			tData.(IndexTData),
			directories.([]interface{}),
			make([]*IndexDirNode, len(directories.([]interface{}))),
		}
	},
	"index-tdata": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		vs, _ := r.ReadObject()
		es, _ := r.ReadObject()
		as, _ := r.ReadObject()
		txs, _ := r.ReadObject()
		addeds, _ := r.ReadObject()
		return IndexTData{
			vs.([]interface{}),
			es.([]int),
			as.([]int),
			txs.([]int),
			addeds.([]bool),
		}
	},
	"index-dir-node": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		tData, _ := r.ReadObject()
		segmentIds, _ := r.ReadObject()
		mystery1, _ := r.ReadObject()
		mystery2, _ := r.ReadObject()
		return IndexDirNode{
			tData.(IndexTData),
			segmentIds.([]interface{}),
			mystery1.([]int), mystery2.([]int),
			make([]*IndexTData, len(segmentIds.([]interface{}))),
		}
	},
}

type IndexRootNode struct {
	store        *storage.Store
	find         func(*IndexTData, int) int
	tData        IndexTData
	directoryIds []interface{}
	directories  []*IndexDirNode
}

func (root *IndexRootNode) directory(idx int) *IndexDirNode {
	if dir := root.directories[idx]; dir != nil {
		return dir
	}

	dirRaw, err := storage.Get(root.store, root.directoryIds[idx].(string), readHandlers)
	if err != nil {
		log.Fatal(err)
	}
	dir := dirRaw.(IndexDirNode)
	root.directories[idx] = &dir
	return &dir
}

type IndexDirNode struct {
	tData      IndexTData
	segmentIds []interface{}
	mystery1   []int
	mystery2   []int
	segments   []*IndexTData
}

func (dir *IndexDirNode) segment(store *storage.Store, idx int) *IndexTData {
	if segment := dir.segments[idx]; segment != nil {
		return segment
	}

	segmentRaw, err := storage.Get(store, dir.segmentIds[idx].(string), readHandlers)
	if err != nil {
		log.Fatal(err)
	}
	segment := segmentRaw.(IndexTData)
	dir.segments[idx] = &segment
	return &segment
}

type IndexTData struct {
	values       []interface{}
	entities     []int
	attributes   []int
	transactions []int
	addeds       []bool
}

type Index interface {
	Datoms() Iterator
	SeekDatoms(components ...interface{}) Iterator
}

func New(store *storage.Store, type_ string, id string) (Index, error) {
	indexRaw, err := storage.Get(store, id, readHandlers)
	if err != nil {
		return nil, err
	}
	indexRoot := indexRaw.(IndexRootNode)
	indexRoot.store = store
	switch type_ {
	case Eavt:
		indexRoot.find = findE
	case Aevt, Avet:
		indexRoot.find = findA
	case Vaet:
		indexRoot.find = findV
	default:
		log.Fatal("invalid index type:", type_)
	}
	return Index(&indexRoot), nil
}

type Datom struct {
	entity      int
	attribute   int
	value       interface{}
	transaction int
	added       bool
}

func (d Datom) Entity() int        { return d.entity }
func (d Datom) E() int             { return d.entity }
func (d Datom) Attribute() int     { return d.attribute }
func (d Datom) A() int             { return d.attribute }
func (d Datom) Value() interface{} { return d.value }
func (d Datom) V() interface{}     { return d.value }
func (d Datom) Transaction() int   { return d.transaction }
func (d Datom) Tx() int            { return d.transaction }
func (d Datom) Added() bool        { return d.added }

type Iterator struct {
	Next func() *Datom
}

func (root *IndexRootNode) Datoms() Iterator {
	var (
		dirIndex     = 0
		dir          = root.directory(dirIndex)
		segmentIndex = 0
		segment      = dir.segment(root.store, segmentIndex)
		datomIndex   = -1
	)

	next := func() *Datom {
		if datomIndex < len(segment.entities)-1 {
			datomIndex += 1
		} else if segmentIndex < len(dir.segments)-1 {
			segmentIndex += 1
			segment = dir.segment(root.store, segmentIndex)
			datomIndex = 0
		} else if dirIndex < len(root.directories)-1 {
			dirIndex += 1
			segmentIndex = 0
			datomIndex = 0
			dir = root.directory(dirIndex)
			segment = dir.segment(root.store, segmentIndex)
		} else {
			return nil
		}

		datom := Datom{
			segment.entities[datomIndex],
			segment.attributes[datomIndex],
			segment.values[datomIndex],
			3*(1<<42) + segment.transactions[datomIndex],
			segment.addeds[datomIndex],
		}
		return &datom
	}

	return Iterator{next}
}

func (root *IndexRootNode) SeekDatoms(components ...interface{}) Iterator {
	var (
		dirIndex     = 0
		dir          = root.directory(dirIndex)
		segmentIndex = 0
		segment      = dir.segment(root.store, segmentIndex)
		datomIndex   = -1
	)

	// if we have a leading component, find the first potential segment
	if len(components) >= 1 {
		dirIndex, dir, segmentIndex, segment, datomIndex = findStart(root, components[0].(int))
	}

	next := func() *Datom {
		if datomIndex < len(segment.entities)-1 {
			datomIndex += 1
		} else if segmentIndex < len(dir.segments)-1 {
			segmentIndex += 1
			segment = dir.segment(root.store, segmentIndex)
			datomIndex = 0
		} else if dirIndex < len(root.directories)-1 {
			dirIndex += 1
			segmentIndex = 0
			datomIndex = 0
			dir = root.directory(dirIndex)
			segment = dir.segment(root.store, segmentIndex)
		} else {
			return nil
		}

		datom := Datom{
			segment.entities[datomIndex],
			segment.attributes[datomIndex],
			segment.values[datomIndex],
			3*(1<<42) + segment.transactions[datomIndex],
			segment.addeds[datomIndex],
		}
		return &datom
	}

	return Iterator{next}
}

func findStart(root *IndexRootNode, component int) (int, *IndexDirNode, int, *IndexTData, int) {
	// TODO: fix `find` if it the component is too large (i.e. not in the index)
	dirIndex := root.find(&root.tData, component)
	dir := root.directory(dirIndex)

	segmentIndex := root.find(&dir.tData, component)
	segment := dir.segment(root.store, segmentIndex)

	datomIndex := root.find(&dir.tData, component)

	return dirIndex, dir, segmentIndex, segment, datomIndex
}

func findE(td *IndexTData, value int) int {
	idx := 0
	for i, entity := range td.entities {
		if entity > value {
			break
		}
		idx = i
	}
	return idx
}

func findA(td *IndexTData, value int) int {
	idx := 0
	for i, attr := range td.attributes {
		if attr > value {
			break
		}
		idx = i
	}
	return idx
}

func findV(td *IndexTData, value int) int {
	idx := 0
	for i, val := range td.values {
		if val.(int) > value {
			break
		}
		idx = i
	}
	return idx
}
