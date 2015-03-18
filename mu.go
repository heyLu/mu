package main

import (
	"compress/gzip"
	"fmt"
	"github.com/heyLu/fressian"
	"log"
	"net/url"
	"os"

	"./storage"
)

func StorageGet(s *storage.Store, id string) (interface{}, error) {
	r, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	g, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return fressian.NewReader(g, readHandlers).ReadObject()
}

type Connection struct {
	store *storage.Store
}

func NewConnection(u *url.URL) (*Connection, error) {
	store, err := storage.Open(u)
	if err != nil {
		return nil, err
	}

	return &Connection{store}, nil
}

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

func (root *IndexRootNode) Datoms() []Datom {
	datoms := make([]Datom, 0, 1000)
	store := root.store
	for _, dirId := range root.directories {
		dir, err := StorageGet(store, dirId.(string))
		if err != nil {
			log.Fatal(err)
		}
		for _, segmentId := range dir.(IndexDirNode).segments {
			segmentRaw, err := StorageGet(store, segmentId.(string))
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

type Index interface {
	Datoms() []Datom
}

func NewIndex(store *storage.Store, id string) (Index, error) {
	indexRaw, err := StorageGet(store, id)
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

type Database struct {
	eavt Index
}

func (c *Connection) Db() (*Database, error) {
	indexRootRaw, err := StorageGet(c.store, c.store.IndexRootId())
	if err != nil {
		return nil, err
	}
	indexRoot := indexRootRaw.(map[interface{}]interface{})
	eavtId := indexRoot[fressian.Key{"", "eavt-main"}].(string)
	eavt, err := NewIndex(c.store, eavtId)
	if err != nil {
		return nil, err
	}
	return &Database{eavt}, nil
}

func main() {
	u, err := url.Parse(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	conn, err := NewConnection(u)
	if err != nil {
		log.Fatal(err)
	}

	db, err := conn.Db()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(db)

	for _, datom := range db.eavt.Datoms() {
		fmt.Println(datom)
	}
}
