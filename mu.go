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
	tData              IndexTData
	segments           []interface{}
	mystery1, mystery2 []int
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
		r.ReadObject()
		r.ReadObject()
		return IndexDirNode{
			tData.(IndexTData),
			segments.([]interface{}),
			nil, nil,
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

func (root IndexRootNode) allDatoms(baseDir string) []Datom {
	datoms := make([]Datom, 0, 100)
	for _, dirNodeId := range root.segments {
		dirNode, err := readDirNode(baseDir, dirNodeId.(string))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(dirNode)
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

func readRoot(baseDir, rootId string) (*IndexRootNode, error) {
	rawRoot, err := readFile(baseDir, rootId)
	if err != nil {
		return nil, err
	}
	root := rawRoot.(IndexRootNode)
	return &root, nil
}

func main() {
	// given an index root uuid, get all the datoms in it, print them
	baseDir := "dbs/initial2"
	rootId := "5507037f-cbee-42ce-8339-c2a0edae286b"
	root, err := readRoot(baseDir, rootId)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(root)
	for _, datom := range root.allDatoms(baseDir) {
		fmt.Println(datom)
	}
}
