package file

import (
	"github.com/heyLu/fressian"

	"../../collection/btset"
	"../../database"
	"../../index"
)

var WriteHandler fressian.WriteHandler = func(w *fressian.Writer, val interface{}) error {
	switch val := val.(type) {
	case *database.Db:
		return w.WriteExt("mu.Database", val.Eavt(), val.Aevt(), val.Avet(), val.Vaet())
	default:
		return index.MemoryWriteHandler(w, val)
	}
}

var ReadHandlers = map[string]fressian.ReadHandler{
	"mu.Database": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		eavtRaw, _ := r.ReadValue()
		aevtRaw, _ := r.ReadValue()
		avetRaw, _ := r.ReadValue()
		vaetRaw, _ := r.ReadValue()
		eavt := eavtRaw.(*index.MemoryIndex)
		eavt.UseCompare(index.CompareEavt)
		aevt := aevtRaw.(*index.MemoryIndex)
		aevt.UseCompare(index.CompareAevt)
		avet := avetRaw.(*index.MemoryIndex)
		avet.UseCompare(index.CompareAvet)
		vaet := vaetRaw.(*index.MemoryIndex)
		vaet.UseCompare(index.CompareVaet)
		return database.NewMemory(eavt, aevt, avet, vaet)
	},
	"mu.memory.Index":   index.MemoryReadHandlers["mu.memory.Index"],
	"mu.Datom":          index.ReadHandlers["mu.Datom"],
	"btset.Set":         btset.ReadHandlers["btset.Set"],
	"btset.PointerNode": btset.ReadHandlers["btset.PointerNode"],
	"btset.LeafNode":    btset.ReadHandlers["btset.LeafNode"],
}
