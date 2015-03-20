package index

import (
	"github.com/heyLu/fressian"
	"log"

	"../storage"
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
		segments, _ := r.ReadObject()
		mystery1, _ := r.ReadObject()
		mystery2, _ := r.ReadObject()
		return IndexDirNode{
			tData.(IndexTData),
			segments.([]interface{}),
			mystery1.([]int), mystery2.([]int),
		}
	},
}

type IndexRootNode struct {
	store       *storage.Store
	find        func(*IndexTData, int) int
	tData       IndexTData
	directories []interface{}
}

type IndexDirNode struct {
	tData    IndexTData
	segments []interface{}
	mystery1 []int
	mystery2 []int
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
	case "eavt":
		indexRoot.find = findE
	case "aevt", "avet":
		indexRoot.find = findA
	case "vaet":
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
		dir          = getDir(root.store, root.directories[dirIndex].(string))
		segmentIndex = 0
		segment      = getSegment(root.store, dir.segments[segmentIndex].(string))
		datomIndex   = -1
	)

	next := func() *Datom {
		if datomIndex < len(segment.entities)-1 {
			datomIndex += 1
		} else if segmentIndex < len(dir.segments)-1 {
			segmentIndex += 1
			segment = getSegment(root.store, dir.segments[segmentIndex].(string))
			datomIndex = 0
		} else if dirIndex < len(root.directories)-1 {
			dirIndex += 1
			segmentIndex = 0
			datomIndex = 0
			dir = getDir(root.store, root.directories[dirIndex].(string))
			segment = getSegment(root.store, dir.segments[segmentIndex].(string))
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
		dir          = getDir(root.store, root.directories[dirIndex].(string))
		segmentIndex = 0
		segment      = getSegment(root.store, dir.segments[segmentIndex].(string))
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
			segment = getSegment(root.store, dir.segments[segmentIndex].(string))
			datomIndex = 0
		} else if dirIndex < len(root.directories)-1 {
			dirIndex += 1
			segmentIndex = 0
			datomIndex = 0
			dir = getDir(root.store, root.directories[dirIndex].(string))
			segment = getSegment(root.store, dir.segments[segmentIndex].(string))
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

func findStart(root *IndexRootNode, component int) (int, IndexDirNode, int, IndexTData, int) {
	dirIndex := root.find(&root.tData, component)
	dir := getDir(root.store, root.directories[dirIndex].(string))

	segmentIndex := root.find(&dir.tData, component)
	segment := getSegment(root.store, dir.segments[segmentIndex].(string))

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

func getDir(store *storage.Store, id string) IndexDirNode {
	dirRaw, err := storage.Get(store, id, readHandlers)
	if err != nil {
		log.Fatal(err)
	}
	return dirRaw.(IndexDirNode)
}

func getSegment(store *storage.Store, id string) IndexTData {
	segmentRaw, err := storage.Get(store, id, readHandlers)
	if err != nil {
		log.Fatal(err)
	}
	return segmentRaw.(IndexTData)
}
