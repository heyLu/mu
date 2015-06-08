package log

import (
	"bytes"
	"github.com/heyLu/fressian"
	"log"

	"../index"
	"../store"
)

type Log struct {
	store  store.Store
	RootId string
	Tail   []LogTx
}

type LogTx struct {
	Id     string
	T      int
	Datoms []index.Datom
}

func FromStore(store store.Store, logRootId string, logTail []byte) *Log {
	r := fressian.NewReader(bytes.NewBuffer(logTail), ReadHandlers)
	tailRaw, err := r.ReadValue()
	if err != nil && tailRaw == nil {
		log.Fatal("[log] FromStore: ", err)
	}
	tail := tailRaw.([]interface{})
	txs := make([]LogTx, len(tail))
	for i, txRaw := range tail {
		tx := txRaw.(map[interface{}]interface{})
		id := tx[fressian.Keyword{"", "id"}].(string)
		t := tx[fressian.Keyword{"", "t"}].(int)
		dataRaw := tx[fressian.Keyword{"", "data"}].([]interface{})
		data := make([]index.Datom, len(dataRaw))
		for i, datom := range data {
			data[i] = datom
		}
		txs[i] = LogTx{id, t, data}
	}
	return &Log{store, logRootId, txs}
}

var ReadHandlers = map[string]fressian.ReadHandler{
	"datum": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		added, _ := r.ReadValue()
		part, _ := r.ReadValue()
		id, _ := r.ReadValue()
		attribute, _ := r.ReadValue()
		value, _ := r.ReadValue()
		tx, _ := r.ReadValue()
		datom := index.NewDatom(
			part.(int)*(1<<42)+id.(int),
			attribute.(int),
			value,
			3*(1<<42)+tx.(int),
			added.(bool))
		return &datom
	},
}
