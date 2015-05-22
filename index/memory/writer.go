package memory

import (
	"github.com/heyLu/fressian"

	index ".."
	"../../collection/btset"
)

var defaultHandler fressian.WriteHandler = btset.NewWriteHandler(index.WriteHandler)

var WriteHandler fressian.WriteHandler = func(w *fressian.Writer, val interface{}) error {
	switch val := val.(type) {
	case *Index:
		return w.WriteExt("mu.memory.Index", val.datoms)
	default:
		return defaultHandler(w, val)
	}
}

var ReadHandlers = map[string]fressian.ReadHandler{
	"mu.memory.Index": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		datomsRaw, _ := r.ReadValue()
		datoms := datomsRaw.(*btset.Set)
		return &Index{datoms}
	},
	"btset.Set":         btset.ReadHandlers["btset.Set"],
	"btset.PointerNode": btset.ReadHandlers["btset.PointerNode"],
	"btset.LeafNode":    btset.ReadHandlers["btset.LeafNode"],
}
