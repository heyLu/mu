package database

import (
	"github.com/heyLu/mu/index"
)

const (
	eavt = (1 << 3) + (1 << 2) + (1 << 1) + (1 << 0)
	eav_ = (1 << 3) + (1 << 2) + (1 << 1) + (0 << 0)
	ea_t = (1 << 3) + (1 << 2) + (0 << 1) + (1 << 0)
	ea__ = (1 << 3) + (1 << 2) + (0 << 1) + (0 << 0)
	e_vt = (1 << 3) + (0 << 2) + (1 << 1) + (1 << 0)
	e_v_ = (1 << 3) + (0 << 2) + (1 << 1) + (0 << 0)
	e__t = (1 << 3) + (0 << 2) + (0 << 1) + (1 << 0)
	e___ = (1 << 3) + (0 << 2) + (0 << 1) + (0 << 0)
	_avt = (0 << 3) + (1 << 2) + (1 << 1) + (1 << 0)
	_av_ = (0 << 3) + (1 << 2) + (1 << 1) + (0 << 0)
	_a_t = (0 << 3) + (1 << 2) + (0 << 1) + (1 << 0)
	_a__ = (0 << 3) + (1 << 2) + (0 << 1) + (0 << 0)
	__vt = (0 << 3) + (0 << 2) + (1 << 1) + (1 << 0)
	__v_ = (0 << 3) + (0 << 2) + (1 << 1) + (0 << 0)
	___t = (0 << 3) + (0 << 2) + (0 << 1) + (1 << 0)
	____ = (0 << 3) + (0 << 2) + (0 << 1) + (0 << 0)
)

func (db *Db) Search(pattern Pattern) index.Iterator {
	panic("not implemented")
}

type Pattern struct {
	e     HasLookup
	a     HasLookup
	v     interface{}
	tx    HasLookup
	added *bool
}

func (p Pattern) toNum() int {
	n := 0
	if p.e != nil {
		n ^= 1 << 3
	}
	if p.a != nil {
		n ^= 1 << 2
	}
	if p.v != nil {
		n ^= 1 << 1
	}
	if p.tx != nil {
		n ^= 1 << 0
	}
	return n
}
