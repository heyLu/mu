package index

import (
	"fmt"
	"math"
)

type Datom struct {
	entity      int
	attribute   int
	value       Value
	transaction int
	added       bool
}

var MinDatom = Datom{0, 0, MinValue, 0, false}
var MaxDatom = Datom{math.MaxInt64, math.MaxInt64, MaxValue, math.MaxInt64, true}

func NewDatom(e int, a int, v interface{}, tx int, added bool) Datom {
	return Datom{e, a, NewValue(v), tx, added}
}

func (d Datom) Entity() int      { return d.entity }
func (d Datom) E() int           { return d.entity }
func (d Datom) Attribute() int   { return d.attribute }
func (d Datom) A() int           { return d.attribute }
func (d Datom) Value() Value     { return d.value }
func (d Datom) V() Value         { return d.value }
func (d Datom) Transaction() int { return d.transaction }
func (d Datom) Tx() int          { return d.transaction }
func (d Datom) Added() bool      { return d.added }

func (d Datom) Retraction() Datom {
	return Datom{d.entity, d.attribute, d.value, d.transaction, false}
}

func (d Datom) String() string {
	return fmt.Sprintf("index.Datom{%d %d %v %d %t}", d.entity, d.attribute, d.value, d.transaction, d.added)
}

func CompareEavt(ai, bi interface{}) int {
	a := ai.(*Datom)
	b := bi.(*Datom)

	cmp := a.entity - b.entity
	if cmp != 0 {
		return cmp
	}

	cmp = a.attribute - b.attribute
	if cmp != 0 {
		return cmp
	}

	cmp = a.value.Compare(b.value)
	if cmp != 0 {
		return cmp
	}

	return a.transaction - b.transaction
}

func CompareAevt(ai, bi interface{}) int {
	a := ai.(*Datom)
	b := bi.(*Datom)

	cmp := a.attribute - b.attribute
	if cmp != 0 {
		return cmp
	}

	cmp = a.entity - b.entity
	if cmp != 0 {
		return cmp
	}

	cmp = a.value.Compare(b.value)
	if cmp != 0 {
		return cmp
	}

	return a.transaction - b.transaction
}

func CompareAvet(ai, bi interface{}) int {
	a := ai.(*Datom)
	b := bi.(*Datom)

	cmp := a.attribute - b.attribute
	if cmp != 0 {
		return cmp
	}

	cmp = a.value.Compare(b.value)
	if cmp != 0 {
		return cmp
	}

	cmp = a.entity - b.entity
	if cmp != 0 {
		return cmp
	}

	return a.transaction - b.transaction
}

func CompareVaet(ai, bi interface{}) int {
	a := ai.(*Datom)
	b := bi.(*Datom)

	cmp := a.value.Compare(b.value)
	if cmp != 0 {
		return cmp
	}

	cmp = a.attribute - b.attribute
	if cmp != 0 {
		return cmp
	}

	cmp = a.entity - b.entity
	if cmp != 0 {
		return cmp
	}

	return a.transaction - b.transaction
}
