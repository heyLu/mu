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
	Datoms() []Datom
}

func New(store *storage.Store, id string) (Index, error) {
	indexRaw, err := storage.Get(store, id, readHandlers)
	if err != nil {
		return nil, err
	}
	indexRoot := indexRaw.(IndexRootNode)
	indexRoot.store = store
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

func (root *IndexRootNode) Datoms() []Datom {
	datoms := make([]Datom, 0, 1000)
	store := root.store
	for _, dirId := range root.directories {
		dir, err := storage.Get(store, dirId.(string), readHandlers)
		if err != nil {
			log.Fatal(err)
		}
		for _, segmentId := range dir.(IndexDirNode).segments {
			segmentRaw, err := storage.Get(store, segmentId.(string), readHandlers)
			if err != nil {
				log.Fatal(err)
			}
			segment := segmentRaw.(IndexTData)
			for i, _ := range segment.entities {
				datom := Datom{
					segment.entities[i],
					segment.attributes[i],
					segment.values[i],
					3*(1<<42) + segment.transactions[i],
					segment.addeds[i],
				}
				datoms = append(datoms, datom)
			}
		}
	}
	return datoms
}
