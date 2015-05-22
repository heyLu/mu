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
