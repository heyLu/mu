package index

import (
	"github.com/heyLu/fressian"
	"log"
	"time"

	"../comparable"
	"../storage"
)

const (
	Eavt = Type("eavt")
	Aevt = Type("aevt")
	Avet = Type("avet")
	Vaet = Type("vaet")
)

type Type string

var readHandlers = map[string]fressian.ReadHandler{
	"index-root-node": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		tData, _ := r.ReadValue()
		directories, _ := r.ReadValue()
		return RootNode{
			nil,
			nil,
			tData.(TData),
			directories.([]interface{}),
			make([]*DirNode, len(directories.([]interface{}))),
		}
	},
	"index-tdata": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		vs, _ := r.ReadValue()
		es, _ := r.ReadValue()
		as, _ := r.ReadValue()
		txs, _ := r.ReadValue()
		addeds, _ := r.ReadValue()
		return TData{
			vs.([]interface{}),
			es.([]int),
			as.([]int),
			txs.([]int),
			addeds.([]bool),
		}
	},
	"index-dir-node": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		tData, _ := r.ReadValue()
		segmentIds, _ := r.ReadValue()
		mystery1, _ := r.ReadValue()
		mystery2, _ := r.ReadValue()
		return DirNode{
			tData.(TData),
			segmentIds.([]interface{}),
			mystery1.([]int), mystery2.([]int),
			make([]*TData, len(segmentIds.([]interface{}))),
		}
	},
}

type RootNode struct {
	store        *storage.Store
	find         func(*TData, int) int
	tData        TData
	directoryIds []interface{}
	directories  []*DirNode
}

func (root *RootNode) directory(idx int) *DirNode {
	if dir := root.directories[idx]; dir != nil {
		return dir
	}

	dirRaw, err := storage.Get(root.store, root.directoryIds[idx].(string), readHandlers)
	if err != nil {
		log.Fatal(err)
	}
	dir := dirRaw.(DirNode)
	root.directories[idx] = &dir
	return &dir
}

type DirNode struct {
	tData      TData
	segmentIds []interface{}
	mystery1   []int
	mystery2   []int
	segments   []*TData
}

func (dir *DirNode) segment(store *storage.Store, idx int) *TData {
	if segment := dir.segments[idx]; segment != nil {
		return segment
	}

	segmentRaw, err := storage.Get(store, dir.segmentIds[idx].(string), readHandlers)
	if err != nil {
		log.Fatal(err)
	}
	segment := segmentRaw.(TData)
	dir.segments[idx] = &segment
	return &segment
}

type TData struct { // "transposed data"?
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

func New(store *storage.Store, type_ Type, id string) (Index, error) {
	indexRaw, err := storage.Get(store, id, readHandlers)
	if err != nil {
		return nil, err
	}
	indexRoot := indexRaw.(RootNode)
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

type ValueType int

const (
	Bool ValueType = iota
	Int
	Key
	String
	Date
)

type Value struct {
	ty  ValueType
	val interface{}
}

func NewValue(val interface{}) Value {
	switch val.(type) {
	case bool:
		return Value{Bool, val}
	case int:
		return Value{Int, val}
	case fressian.Key:
		return Value{Key, val}
	case string:
		return Value{String, val}
	case time.Time:
		return Value{Date, val}
	default:
		log.Fatal("invalid datom value: ", val)
		return Value{-1, nil}
	}
}

func (v Value) Type() ValueType  { return v.ty }
func (v Value) Val() interface{} { return v.val }

func (v Value) Compare(ovc comparable.Comparable) int {
	ov := ovc.(Value)
	if v.ty == ov.ty {
		switch v.ty {
		case Bool:
			v := v.val.(bool)
			ov := ov.val.(bool)
			if v == ov {
				return 0
			} else if !v && ov {
				return -1
			} else {
				return 1
			}
		case Int:
			return v.val.(int) - ov.val.(int)
		case Key:
			v := v.val.(fressian.Key)
			ov := ov.val.(fressian.Key)
			if v.Namespace < ov.Namespace && v.Name < ov.Name {
				return -1
			} else if v.Namespace == ov.Namespace && v.Name == ov.Name {
				return 0
			} else {
				return 1
			}
		case String:
			v := v.val.(string)
			ov := ov.val.(string)
			if v < ov {
				return -1
			} else if v == ov {
				return 0
			} else {
				return 1
			}
		case Date:
			v := v.val.(time.Time)
			ov := ov.val.(time.Time)
			return int(v.Unix() - ov.Unix())
		default:
			log.Fatal("invalid values: ", v, ", ", ov)
			return 0
		}
	} else if v.ty < ov.ty {
		return -1
	} else {
		return 1
	}
}

type Datom struct {
	entity      int
	attribute   int
	value       Value
	transaction int
	added       bool
}

func (d Datom) Entity() int      { return d.entity }
func (d Datom) E() int           { return d.entity }
func (d Datom) Attribute() int   { return d.attribute }
func (d Datom) A() int           { return d.attribute }
func (d Datom) Value() Value     { return d.value }
func (d Datom) V() Value         { return d.value }
func (d Datom) Transaction() int { return d.transaction }
func (d Datom) Tx() int          { return d.transaction }
func (d Datom) Added() bool      { return d.added }

func CompareEavt(ai, bi interface{}) int {
	a := ai.(*Datom)
	b := bi.(*Datom)

	if a.entity < b.entity {
		return -1
	} else if a.attribute < b.attribute {
		return -1
	} else if comparable.Lt(a.value, b.value) {
		return -1
	} else if a.transaction < b.transaction {
		return -1
	} else if a.entity == b.entity &&
		a.attribute == b.attribute &&
		comparable.Eq(a.value, b.value) &&
		a.transaction == b.transaction {
		return 0
	} else {
		return 1
	}
}

func CompareAevt(ai, bi interface{}) int {
	a := ai.(*Datom)
	b := bi.(*Datom)

	if a.attribute < b.attribute {
		return -1
	} else if a.entity < b.entity {
		return -1
	} else if comparable.Lt(a.value, b.value) {
		return -1
	} else if a.transaction < b.transaction {
		return -1
	} else if a.entity == b.entity &&
		a.attribute == b.attribute &&
		comparable.Eq(a.value, b.value) &&
		a.transaction == b.transaction {
		return 0
	} else {
		return 1
	}
}

func CompareAvet(ai, bi interface{}) int {
	a := ai.(*Datom)
	b := bi.(*Datom)

	if a.attribute < b.attribute {
		return -1
	} else if comparable.Lt(a.value, b.value) {
		return -1
	} else if a.entity < b.entity {
		return -1
	} else if a.transaction < b.transaction {
		return -1
	} else if a.entity == b.entity &&
		a.attribute == b.attribute &&
		comparable.Eq(a.value, b.value) &&
		a.transaction == b.transaction {
		return 0
	} else {
		return 1
	}
}

func CompareVaet(ai, bi interface{}) int {
	a := ai.(*Datom)
	b := bi.(*Datom)

	if comparable.Lt(a.value, b.value) {
		return -1
	} else if a.attribute < b.attribute {
		return -1
	} else if a.entity < b.entity {
		return -1
	} else if a.transaction < b.transaction {
		return -1
	} else if a.entity == b.entity &&
		a.attribute == b.attribute &&
		comparable.Eq(a.value, b.value) &&
		a.transaction == b.transaction {
		return 0
	} else {
		return 1
	}
}

type Iterator interface {
	Next() *Datom
}

type iterator struct {
	next func() *Datom
}

func (i iterator) Next() *Datom { return i.next() }

func (root *RootNode) Datoms() Iterator {
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
			NewValue(segment.values[datomIndex]),
			3*(1<<42) + segment.transactions[datomIndex],
			segment.addeds[datomIndex],
		}
		return &datom
	}

	return iterator{next}
}

func (root *RootNode) SeekDatoms(components ...interface{}) Iterator {
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
			NewValue(segment.values[datomIndex]),
			3*(1<<42) + segment.transactions[datomIndex],
			segment.addeds[datomIndex],
		}
		return &datom
	}

	return iterator{next}
}

func findStart(root *RootNode, component int) (int, *DirNode, int, *TData, int) {
	// TODO: fix `find` if it the component is too large (i.e. not in the index)
	dirIndex := root.find(&root.tData, component)
	dir := root.directory(dirIndex)

	segmentIndex := root.find(&dir.tData, component)
	segment := dir.segment(root.store, segmentIndex)

	datomIndex := root.find(&dir.tData, component)

	return dirIndex, dir, segmentIndex, segment, datomIndex
}

func findE(td *TData, value int) int {
	idx := 0
	for i, entity := range td.entities {
		if entity > value {
			break
		}
		idx = i
	}
	return idx
}

func findA(td *TData, value int) int {
	idx := 0
	for i, attr := range td.attributes {
		if attr > value {
			break
		}
		idx = i
	}
	return idx
}

func findV(td *TData, value int) int {
	idx := 0
	for i, val := range td.values {
		if val.(int) > value {
			break
		}
		idx = i
	}
	return idx
}
