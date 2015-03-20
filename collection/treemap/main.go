// Package treemap implements immutable maps using red-black trees à la
// okasaki.
//
// See Okasaki (JFP99) for a description of the algorithm.
package treemap

// Ord must be implemented for values stored in the map.
type Ord interface {
	Less(other Ord) bool
	Equal(other Ord) bool
}

// An immutable map.
type Map interface {
	Get(key Ord) interface{}
	Set(key Ord, val interface{}) Map
	//Update(key Ord, f func(val interface{}) interface{}) Map
	Keys() Iterator
	Values() Iterator
}

const (
	black = true
	red   = false
)

var empty *tree = nil

type tree struct {
	c bool
	l *tree
	k Ord
	v interface{}
	r *tree
}

// New creates a new empty Map.
func New() Map {
	return empty
}

func (t *tree) Contains(key Ord) bool {
	if t == nil {
		return false
	}

	if key.Less(t.k) {
		return t.l.Contains(key)
	} else if key.Equal(t.k) {
		return true
	} else {
		return t.r.Contains(key)
	}
}

func (t *tree) Get(key Ord) interface{} {
	if t == nil {
		return nil
	}

	if key.Less(t.k) {
		return t.l.Contains(key)
	} else if key.Equal(t.k) {
		return t.v
	} else {
		return t.r.Contains(key)
	}
}

func (t *tree) Set(key Ord, val interface{}) Map {
	tt := t.insert(key, val)
	return &tree{black, tt.l, tt.k, tt.v, tt.r}
}

func (t *tree) insert(key Ord, val interface{}) *tree {
	if t == nil {
		return &tree{red, nil, key, val, nil}
	}

	if key.Less(t.k) {
		return balance(t.c, t.l.insert(key, val), t.k, t.v, t.r)
	} else if key.Equal(t.k) {
		return &tree{t.c, t.l, t.k, val, t.r}
	} else {
		return balance(t.c, t.l, t.k, t.v, t.r.insert(key, val))
	}
}

func balance(color bool, l *tree, key Ord, val interface{}, r *tree) *tree {
	newTree := func(a *tree, x Ord, xv interface{}, b *tree, y Ord, yv interface{}, c *tree, z Ord, zv interface{}, d *tree) *tree {
		return &tree{red, &tree{black, a, x, xv, b}, y, yv, &tree{black, c, z, zv, d}}
	}

	if color == black && l != nil && l.c == red && l.l != nil && l.l.c == red {
		return newTree(l.l.l, l.l.k, l.l.v, l.l.r, l.k, l.v, l.r, key, val, r)
	} else if color == black && l != nil && l.c == red && l.r != nil && l.r.c == red {
		return newTree(l.l, l.k, l.v, l.r.l, l.r.k, l.r.v, l.r.r, key, val, r)
	} else if color == black && r != nil && r.c == red && r.l != nil && r.l.c == red {
		return newTree(l, key, val, r.l.l, r.l.k, r.l.v, r.l.r, r.k, r.v, r.r)
	} else if color == black && r != nil && r.c == red && r.r != nil && r.r.c == red {
		return newTree(l, key, val, r.l, r.k, r.v, r.r.l, r.r.k, r.r.v, r.r.r)
	}

	return &tree{color, l, key, val, r}
}

type Iterator interface {
	Next() interface{}
}

type treeIterator struct {
	stack []*tree
	asc   bool
}

func newTreeIterator(t *tree, asc bool) *treeIterator {
	ti := treeIterator{[]*tree{}, asc}
	ti.push(t)
	return &ti
}

func (ti *treeIterator) push(t *tree) {
	for t != nil {
		ti.stack = append(ti.stack, t)
		if ti.asc {
			t = t.l
		} else {
			t = t.r
		}
	}
}

func (ti *treeIterator) next() *tree {
	l := len(ti.stack)
	if l == 0 {
		return nil
	}

	t := ti.stack[l-1]
	ti.stack = ti.stack[:l-1]
	if ti.asc {
		ti.push(t.r)
	} else {
		ti.push(t.l)
	}
	return t
}

type keyIterator struct {
	iterator *treeIterator
}

func (ki *keyIterator) Next() interface{} {
	return ki.iterator.next().k
}

func (t *tree) Keys() Iterator {
	return &keyIterator{newTreeIterator(t, true)}
}

type valueIterator struct {
	iterator *treeIterator
}

func (vi *valueIterator) Next() interface{} {
	return vi.iterator.next().v
}

func (t *tree) Values() Iterator {
	return &valueIterator{newTreeIterator(t, true)}
}
