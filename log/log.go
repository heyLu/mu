package log

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"github.com/heyLu/fressian"
	"log"
	"time"

	"github.com/heyLu/mu/index"
	"github.com/heyLu/mu/store"
)

type Log struct {
	store  store.Store
	RootId string
	Tail   []LogTx
}

func (l Log) WithTx(tx *LogTx) *Log {
	return &Log{
		store:  l.store,
		RootId: l.RootId,
		Tail:   append(l.Tail, *tx),
	}
}

type LogTx struct {
	Id     fressian.UUID
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
		var id *fressian.UUID
		// FIXME: remove this as soon as possible
		switch rawId := tx[fressian.Keyword{"", "id"}].(type) {
		case fressian.UUID:
			*id = rawId
		case string:
			id, _ = fressian.NewUUIDFromString(rawId)
		default:
			log.Fatal("invalid log id: ", rawId)
		}
		t := tx[fressian.Keyword{"", "t"}].(int)
		dataRaw := tx[fressian.Keyword{"", "data"}].([]interface{})
		data := make([]index.Datom, len(dataRaw))
		for i, datomRaw := range dataRaw {
			datom := datomRaw.(*index.Datom)
			data[i] = *datom
		}
		txs[i] = LogTx{*id, t, data}
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

func NewTx(t int, datoms []index.Datom) *LogTx {
	return &LogTx{
		Id:     Squuid(),
		T:      t,
		Datoms: datoms,
	}
}

func Squuid() fressian.UUID {
	bs := make([]byte, 16)
	now := uint32(time.Now().Unix())
	binary.BigEndian.PutUint32(bs[0:4], now)
	_, err := rand.Read(bs[4:])
	if err != nil {
		log.Fatal("squuid (rand.Read): ", err)
	}
	return fressian.NewUUIDFromBytes(bs)
}
