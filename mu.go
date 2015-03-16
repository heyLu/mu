package main

import (
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

func main() {
}
