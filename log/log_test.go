package log

import (
	"bytes"
	"github.com/heyLu/fressian"
	"reflect"
	"testing"

	"github.com/heyLu/mu/index"
)

func TestReadWrite(t *testing.T) {
	log := &Log{
		store:  nil,
		RootId: "",
		Tail: []LogTx{
			LogTx{
				Id: fressian.NewUUIDFromBytes([]byte("2a0a1982-96b6-11e6-bf91-02423fefa4c2")),
				T:  0,
				Datoms: []index.Datom{
					index.NewDatom(0, 1, "Jane", 3*(1<<42)+1, true),
					index.NewDatom(1, 1, "Judy", 3*(1<<42)+1, true),
				},
			},
		},
	}

	buf := new(bytes.Buffer)
	w := fressian.NewWriter(buf, WriteHandler)
	w.WriteValue(log.Tail)
	w.Flush()

	log2 := FromStore(nil, "", buf.Bytes())
	if !reflect.DeepEqual(log, log2) {
		t.Errorf("%#v != %#v", log, log2)
	}
}
