package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"path"

	fressian "github.com/heyLu/fressian"
)

type IndexRootNode struct {
	baseDir     string
	tData       IndexTData
	directories []interface{}
}

type IndexTData struct {
	values       []interface{}
	entities     []int
	attributes   []int
	transactions []int
	addeds       []bool
}

type IndexDirNode struct {
	tData    IndexTData
	segments []interface{}
	mystery1 []int
	mystery2 []int // might be the number of entries in each segment
}

var indexHandlers = map[string]fressian.ReadHandler{
	"index-root-node": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		tData, _ := r.ReadObject()
		segments, _ := r.ReadObject()
		return IndexRootNode{"", tData.(IndexTData), segments.([]interface{})}
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

type Datom struct {
	entity      int
	attribute   int
	value       interface{}
	transaction int
	added       bool
}

func datomReadHandler(r *fressian.Reader, tag string, fieldCount int) interface{} {
	added, _ := r.ReadObject()
	partition, _ := r.ReadObject()
	entity, _ := r.ReadObject()
	attribute, _ := r.ReadObject()
	value, _ := r.ReadObject()
	transaction, _ := r.ReadObject()
	return Datom{
		partition.(int)*(1<<42) + entity.(int),
		attribute.(int),
		value,
		3*(1<<42) + transaction.(int),
		added.(bool),
	}
}

func (d Datom) prettyPrint() {
	fmt.Printf("#datom [%d %d %#v %d %t]\n",
		d.entity, d.attribute, d.value, d.transaction, d.added)
}

func (tData IndexTData) allDatoms(baseDir string) []Datom {
	datoms := make([]Datom, 0, 500)
	for i, entityId := range tData.entities {
		datom := Datom{
			entityId,
			tData.attributes[i],
			tData.values[i],
			3*(1<<42) + tData.transactions[i],
			tData.addeds[i],
		}
		datoms = append(datoms, datom)
	}
	return datoms
}

func readTData(baseDir, id string) (*IndexTData, error) {
	raw, err := readFile(baseDir, id)
	if err != nil {
		return nil, err
	}
	obj := raw.(IndexTData)
	return &obj, nil
}

func (dir IndexDirNode) allDatoms(baseDir string) []Datom {
	datoms := make([]Datom, 0, 1000)
	for _, segmentId := range dir.segments {
		tData, err := readTData(baseDir, segmentId.(string))
		if err != nil {
			log.Fatal(err)
		}
		datoms = append(datoms, tData.allDatoms(baseDir)...)
	}
	return datoms
}

func (root IndexRootNode) allDatoms() []Datom {
	datoms := make([]Datom, 0, 10000)
	for _, dirNodeId := range root.directories {
		dirNode, err := readDirNode(root.baseDir, dirNodeId.(string))
		if err != nil {
			log.Fatal(err)
		}
		datoms = append(datoms, dirNode.allDatoms(root.baseDir)...)
	}
	return datoms
}

func readFile(baseDir, id string) (interface{}, error) {
	if val, ok := globalCache[id]; ok {
		return val, nil
	}

	l := len(id)
	p := path.Join(baseDir, "values", id[l-2:l], id)
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	g, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	r := fressian.NewReader(g, indexHandlers)
	obj, err := r.ReadObject()
	if err != nil {
		return nil, err
	}
	globalCache[id] = obj
	return obj, err
}

func readDirNode(baseDir, rootId string) (*IndexDirNode, error) {
	rawDirNode, err := readFile(baseDir, rootId)
	if err != nil {
		return nil, err
	}
	dirNode := rawDirNode.(IndexDirNode)
	return &dirNode, nil
}

func readRootNode(baseDir, rootId string) (*IndexRootNode, error) {
	rawRoot, err := readFile(baseDir, rootId)
	if err != nil {
		return nil, err
	}
	root := rawRoot.(IndexRootNode)
	root.baseDir = baseDir
	return &root, nil
}

type Db struct {
	nextT   int
	eavt    IndexRootNode
	aevt    IndexRootNode
	logTail []interface{}
}

func readDb(baseDir string) (*Db, error) {
	f, err := os.Open(path.Join(baseDir, "roots"))
	if err != nil {
		return nil, err
	}
	rootNames, err := f.Readdirnames(1)
	if err != nil {
		return nil, err
	}
	f, err = os.Open(path.Join(baseDir, "roots", rootNames[0]))
	if err != nil {
		return nil, err
	}
	rawRoot, err := fressian.NewReader(f, nil).ReadObject()
	root := rawRoot.(map[interface{}]interface{})
	indexRootId := root[fressian.Key{"index", "root-id"}].(string)
	logTailRaw := root[fressian.Key{"log", "tail"}].([]byte)
	logHandlers := map[string]fressian.ReadHandler{"datum": datomReadHandler}
	logTail, _ := fressian.NewReader(bytes.NewBuffer(logTailRaw), logHandlers).ReadObject()
	rawIndexRoot, err := readFile(baseDir, indexRootId)
	if err != nil {
		return nil, err
	}
	indexRoot := rawIndexRoot.(map[interface{}]interface{})
	nextT := indexRoot[fressian.Key{"", "nextT"}].(int)
	eavtId := indexRoot[fressian.Key{"", "eavt-main"}].(string)
	aevtId := indexRoot[fressian.Key{"", "aevt-main"}].(string)
	eavt, err := readRootNode(baseDir, eavtId)
	if err != nil {
		return nil, err
	}
	aevt, err := readRootNode(baseDir, aevtId)
	if err != nil {
		return nil, err
	}
	return &Db{nextT, *eavt, *aevt, logTail.([]interface{})}, nil
}

func (d Db) Entid(key fressian.Key) int {
	for _, datom := range d.aevt.allDatoms() {
		if datom.attribute == 10 && datom.value == key {
			return datom.entity
		}
	}

	log.Fatal("no such key", key)
	return -1
}

func (d Db) Ident(id int) *fressian.Key {
	for _, datom := range d.eavt.allDatoms() {
		if datom.attribute == 10 && datom.entity == id {
			key := datom.value.(fressian.Key)
			return &key
		}
	}
	log.Fatal("no such id", id)
	return nil
}

func (d Db) findEavt(entity, attribute int) []interface{} {
	vals := make([]interface{}, 0, 1)
	for _, datom := range d.eavt.allDatoms() {
		if datom.entity == entity && datom.attribute == attribute {
			vals = append(vals, datom.value)
		}
	}
	return vals
}

type Entity struct {
	db *Db
	id int
}

func (e Entity) Keys() []fressian.Key {
	keys := make([]fressian.Key, 0)
	for _, datom := range e.db.eavt.allDatoms() {
		if datom.entity == e.id {
			keys = append(keys, *e.db.Ident(datom.attribute))
		}
	}
	return keys
}

func (e Entity) Get(key fressian.Key) []interface{} {
	return e.db.findEavt(e.id, e.db.Entid(key))
}

var globalCache = make(map[string]interface{}, 100)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <backup-dir>\n", os.Args[0])
		os.Exit(1)
	}

	baseDir := os.Args[1]
	db, err := readDb(baseDir)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%#v\n", db)

	fmt.Println("eavt:")
	for _, datom := range db.eavt.allDatoms() {
		datom.prettyPrint()
	}

	fmt.Println()
	fmt.Println("aevt:")
	for _, datom := range db.aevt.allDatoms() {
		datom.prettyPrint()
	}

	fmt.Println()
	dbPart := Entity{db, 0}
	fmt.Printf("%#v\n", dbPart.Keys())
	fmt.Printf("%#v\n", dbPart.Get(fressian.Key{"db.install", "attribute"}))
}
