package log

import (
	"github.com/heyLu/fressian"

	"github.com/heyLu/mu/index"
)

var _ fressian.WriteHandler = WriteHandler

const txOffset = 3 * (1 << 42)

func WriteHandler(w *fressian.Writer, val interface{}) error {
	switch val := val.(type) {
	case index.Datom:
		part := val.Entity() / (1 << 42)
		eid := val.Entity() - (part * (1 << 42))
		return w.WriteExt("datum",
			val.Added(),
			part,
			eid,
			val.Attribute(),
			val.Value(),
			val.Transaction()%txOffset)
	case index.Value:
		return index.WriteHandler(w, val)
	case LogTx:
		m := map[interface{}]interface{}{}
		m[fressian.Keyword{"", "id"}] = val.Id
		m[fressian.Keyword{"", "t"}] = val.T
		m[fressian.Keyword{"", "data"}] = val.Datoms
		return w.WriteValue(m)
	default:
		return fressian.DefaultHandler(w, val)
	}
}
