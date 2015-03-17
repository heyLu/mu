package main

import (
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"path"

	fressian "github.com/heyLu/fressian"
)

type IndexRootNode struct {
	tData    IndexTData
	segments []interface{}
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
		return IndexRootNode{tData.(IndexTData), segments.([]interface{})}
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
		fmt.Println(segmentId)
		datoms = append(datoms, tData.allDatoms(baseDir)...)
	}
	return datoms
}

func (root IndexRootNode) allDatoms(baseDir string) []Datom {
	datoms := make([]Datom, 0, 10000)
	for _, dirNodeId := range root.segments {
		dirNode, err := readDirNode(baseDir, dirNodeId.(string))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(dirNode)
		datoms = append(datoms, dirNode.allDatoms(baseDir)...)
	}
	return datoms
}

func readFile(baseDir, id string) (interface{}, error) {
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
	return &root, nil
}

type Db struct {
	eavt IndexRootNode
	aevt IndexRootNode
	log  interface{}
}

func readDb(baseDir string) (*Db, error) {
	return nil, nil
}

func main() {
	// given an index root uuid, get all the datoms in it, print them
	baseDir := "dbs/initial2"
	rootId := "5507037f-cbee-42ce-8339-c2a0edae286b"
	root, err := readRootNode(baseDir, rootId)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(root)
	for _, datom := range root.allDatoms(baseDir) {
		datom.prettyPrint()
	}
}
