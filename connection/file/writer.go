package file

import (
	"github.com/heyLu/fressian"

	"../../collection/btset"
	"../../database"
	"../../index"
	"../../index/memory"
)

var WriteHandler fressian.WriteHandler = func(w *fressian.Writer, val interface{}) error {
	switch val := val.(type) {
	case *database.Database:
		return w.WriteExt("mu.Database", val.Eavt(), val.Aevt(), nil, nil)
	default:
		return memory.WriteHandler(w, val)
	}
}

var ReadHandlers = map[string]fressian.ReadHandler{
	"mu.Database": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		eavtRaw, _ := r.ReadValue()
		aevtRaw, _ := r.ReadValue()
		r.ReadValue()
		r.ReadValue()
		eavt := eavtRaw.(*memory.Index)
		aevt := aevtRaw.(*memory.Index)
		return database.New(eavt, aevt, nil, nil)
	},
	"mu.memory.Index":   memory.ReadHandlers["mu.memory.Index"],
	"mu.Datom":          index.ReadHandlers["mu.Datom"],
	"btset.Set":         btset.ReadHandlers["btset.Set"],
	"btset.PointerNode": btset.ReadHandlers["btset.PointerNode"],
	"btset.LeafNode":    btset.ReadHandlers["btset.LeafNode"],
}
