// Package rbtree implements immutable red-black trees à la okasaki.
//
// See Okasaki (JFP99) for a description of the algorithm.
package rbtree

// Ord must be implemented for values stored in the tree.
type Ord interface {
	Less(other Ord) bool
	Equal(other Ord) bool
}

const (
	black = true
	red   = false
)

// An immutable red-black tree.
type Tree struct {
	c bool
	l *Tree
	v Ord
	r *Tree
}

// Empty creates an empty red-black tree.
func Empty() *Tree {
	return nil
}

// Contains checks if the tree contains a given value.
func (t *Tree) Contains(val Ord) bool {
	if t == nil {
		return false
	}

	if val.Less(t.v) {
		return t.l.Contains(val)
	} else if val.Equal(t.v) {
		return true
	} else {
		return t.r.Contains(val)
	}
}

// Add stores a new value in the tree.
func (t *Tree) Add(val Ord) *Tree {
	tt := t.insert(val)
	return &Tree{black, tt.l, tt.v, tt.r}
}

func (t *Tree) insert(x Ord) *Tree {
	if t == nil {
		return &Tree{red, nil, x, nil}
	}

	if x.Less(t.v) {
		return balance(t.c, t.l.insert(x), t.v, t.r)
	} else if x.Equal(t.v) {
		return &Tree{t.c, t.l, t.v, t.r}
	} else {
		return balance(t.c, t.l, t.v, t.r.insert(x))
	}
}

func balance(color bool, l *Tree, v Ord, r *Tree) *Tree {
	tree := func(a *Tree, x Ord, b *Tree, y Ord, c *Tree, z Ord, d *Tree) *Tree {
		return &Tree{red, &Tree{black, a, x, b}, y, &Tree{black, c, z, d}}
	}

	if color == black && l != nil && l.c == red && l.l != nil && l.l.c == red {
		return tree(l.l.l, l.l.v, l.l.r, l.v, l.r, v, r)
	} else if color == black && l != nil && l.c == red && l.r != nil && l.r.c == red {
		return tree(l.l, l.v, l.r.l, l.r.v, l.r.r, v, r)
	} else if color == black && r != nil && r.c == red && r.l != nil && r.l.c == red {
		return tree(l, v, r.l.l, r.l.v, r.l.r, r.v, r.r)
	} else if color == black && r != nil && r.c == red && r.r != nil && r.r.c == red {
		return tree(l, v, r.l, r.v, r.r.l, r.r.v, r.r.r)
	}

	return &Tree{color, l, v, r}
}

type Iterator interface {
	Next() Ord
}

type treeIterator struct {
	stack []*Tree
	asc   bool
}

func newTreeIterator(t *Tree, asc bool) *treeIterator {
	ti := treeIterator{[]*Tree{}, asc}
	ti.push(t)
	return &ti
}

func (ti *treeIterator) push(t *Tree) {
	for t != nil {
		ti.stack = append(ti.stack, t)
		if ti.asc {
			t = t.l
		} else {
			t = t.r
		}
	}
}

func (ti *treeIterator) Next() Ord {
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
	return t.v
}

// Keys returns an iterator for all keys stored in the tree. The
// ordering of the keys depends on the implementation of `Ord`.
func (t *Tree) Keys() Iterator {
	return newTreeIterator(t, true)
}

// KeysReverse returns an iterator for all the keys stored in the tree,
// in reverse order. The ordering of the keys depends on the
// implementation of `Ord`.
func (t *Tree) KeysReverse() Iterator {
	return newTreeIterator(t, false)
}