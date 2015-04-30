package btset

import (
	"github.com/heyLu/fressian"
)

var WriteHandler fressian.WriteHandler = func(w *fressian.Writer, val interface{}) error {
	switch val := val.(type) {
	case *Set:
		return w.WriteExt("btset.Set", val.shift, val.cnt, val.root)
	case *pointerNode:
		return w.WriteExt("btset.PointerNode", val.keys, val.pointers)
	case *leafNode:
		return w.WriteExt("btset.LeafNode", val.keys)
	default:
		return fressian.DefaultHandler(w, val)
	}
}

var ReadHandlers = map[string]fressian.ReadHandler{
	"btset.Set": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		shift, _ := r.ReadValue()
		cnt, _ := r.ReadValue()
		root, _ := r.ReadValue()
		return &Set{root.(anyNode), shift.(int), cnt.(int), nil}
	},
	"btset.PointerNode": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		keys, _ := r.ReadValue()
		pointersRaw, _ := r.ReadValue()
		// FIXME: Can we somehow avoid this conversion?
		pointers := make([]anyNode, len(pointersRaw.([]interface{})))
		for i, node := range pointersRaw.([]interface{}) {
			pointers[i] = node.(anyNode)
		}
		return &pointerNode{keys.([]interface{}), pointers}
	},
	"btset.LeafNode": func(r *fressian.Reader, tag string, fieldCount int) interface{} {
		keys, _ := r.ReadValue()
		return &leafNode{keys.([]interface{})}
	},
}
