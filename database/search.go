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
	var iter index.Iterator
	minDatom, maxDatom := pattern.bounds(db)
	switch pattern.toNum() {
	case eavt:
		iter = db.Eavt().DatomsAt(minDatom, maxDatom)
	case eav_:
		iter = db.Eavt().DatomsAt(minDatom, maxDatom)
	case ea_t:
		iter = db.Eavt().DatomsAt(minDatom, maxDatom)
		iter = index.FilterIterator(iter, func(d *index.Datom) bool {
			return d.Tx() == minDatom.Tx()
		})
	case ea__:
		iter = db.Eavt().DatomsAt(minDatom, maxDatom)
	case e_vt:
		iter = db.Eavt().DatomsAt(minDatom, maxDatom)
		iter = index.FilterIterator(iter, func(d *index.Datom) bool {
			return d.Tx() == minDatom.Tx() &&
				d.V().Compare(minDatom.V()) == 0
		})
	case e_v_:
		iter = db.Eavt().DatomsAt(minDatom, maxDatom)
		iter = index.FilterIterator(iter, func(d *index.Datom) bool {
			return d.V().Compare(minDatom.V()) == 0
		})
	case e__t:
		iter = db.Eavt().DatomsAt(minDatom, maxDatom)
		iter = index.FilterIterator(iter, func(d *index.Datom) bool {
			return d.Tx() == minDatom.Tx()
		})
	case e___:
		iter = db.Eavt().DatomsAt(minDatom, maxDatom)
	case _a_t:
		iter = db.Aevt().DatomsAt(minDatom, maxDatom)
		iter = index.FilterIterator(iter, func(d *index.Datom) bool {
			return d.Tx() == minDatom.Tx()
		})
	case _a__:
		iter = db.Aevt().DatomsAt(minDatom, maxDatom)
	default:
		panic("unknown datom pattern")
	}
	// TODO: handle p.added != nil
	return iter
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

var (
	minE = index.MinDatom.E()
	maxE = index.MaxDatom.E()
	minA = index.MinDatom.A()
	maxA = index.MaxDatom.A()
	minV = index.MinDatom.V()
	maxV = index.MaxDatom.V()
	// FIXME: fix .Tx() on MinDatom and MaxDatom
	minTx    = index.MaxDatom.Tx()
	maxTx    = index.MinDatom.Tx()
	minAdded = index.MinDatom.Added()
	maxAdded = index.MaxDatom.Added()
)

func (p Pattern) bounds(db *Db) (index.Datom, index.Datom) {
	minE, maxE := minE, maxE
	minA, maxA := minA, maxA
	minV, maxV := minV, maxV
	minTx, maxTx := minTx, maxTx
	if p.e != nil {
		e, _ := p.e.Lookup(db)
		minE, maxE = e, e
	}
	if p.a != nil {
		a, _ := p.a.Lookup(db)
		minA, maxA = a, a
	}
	if p.v != nil {
		v := index.NewValue(p.v)
		minV, maxV = v, v
	}
	if p.tx != nil {
		tx, _ := p.tx.Lookup(db)
		minTx, maxTx = tx, tx
	}
	minDatom := index.NewDatom(minE, minA, minV, minTx, minAdded)
	maxDatom := index.NewDatom(maxE, maxA, maxV, maxTx, maxAdded)
	return minDatom, maxDatom
}
