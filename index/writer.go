package index

import (
	"github.com/heyLu/fressian"
	"log"
)

var WriteHandler fressian.WriteHandler = func(w *fressian.Writer, val interface{}) error {
	switch val := val.(type) {
	case *Datom:
		return w.WriteExt("mu.Datom", val.entity, val.attribute, val.value, val.added, val.transaction)
	case Value:
		return w.WriteValue(val.val)
	default:
		return fressian.DefaultHandler(w, val)
	}

}

var backupReadHandlers = map[string]fressian.ReadHandler{
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

var ReadHandlers = map[string]fressian.ReadHandler{
	"mu.Datom": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		entityRaw, _ := r.ReadValue()
		attributeRaw, _ := r.ReadValue()
		valueRaw, _ := r.ReadValue()
		addedRaw, _ := r.ReadValue()
		transactionRaw, _ := r.ReadValue()
		log.Println(entityRaw, attributeRaw, valueRaw, addedRaw, transactionRaw)
		return &Datom{
			entityRaw.(int),
			attributeRaw.(int),
			NewValue(valueRaw),
			transactionRaw.(int),
			addedRaw.(bool),
		}
	},
}
