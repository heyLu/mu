package index

import (
	"fmt"
	"github.com/heyLu/fressian"
	"log"
	"math"
	"time"

	"../comparable"
)

type ValueType int

const (
	Ref ValueType = 20 + iota
	Keyword
	Long
	String
	Bool
	Instant
	Fn
	Bytes
	Int            = Long
	Date           = Instant
	Min  ValueType = -1
	Max  ValueType = 100
)

const (
	UUID ValueType = 56 + iota
	Double
	Float
	URI
	BigInt
	BigDec
)

type Value struct {
	ty  ValueType
	val interface{}
}

var (
	MinValue = Value{Min, nil}
	MaxValue = Value{Max, nil}
)

func NewValue(val interface{}) Value {
	// TODO: implement more types
	switch val.(type) {
	case bool:
		return Value{Bool, val}
	case int:
		return Value{Int, val}
	case fressian.Keyword:
		return Value{Keyword, val}
	case string:
		return Value{String, val}
	case time.Time:
		return Value{Date, val}
	case Value:
		return val.(Value)
	default:
		log.Fatalf("invalid datom value: %#v\n", val)
		return Value{-1, nil}
	}
}

func NewRef(id int) Value {
	return Value{Ref, id}
}

func (v Value) Type() ValueType  { return v.ty }
func (v Value) Val() interface{} { return v.val }

func (v Value) Compare(ovc comparable.Comparable) int {
	ov := ovc.(Value)
	if v.ty == ov.ty {
		switch v.ty {
		case Bool:
			v := v.val.(bool)
			ov := ov.val.(bool)
			if v == ov {
				return 0
			} else if !v && ov {
				return -1
			} else {
				return 1
			}
		case Int:
			return v.val.(int) - ov.val.(int)
		case Keyword:
			v := v.val.(fressian.Keyword)
			ov := ov.val.(fressian.Keyword)
			if v.Namespace < ov.Namespace && v.Name < ov.Name {
				return -1
			} else if v.Namespace == ov.Namespace && v.Name == ov.Name {
				return 0
			} else {
				return 1
			}
		case String:
			v := v.val.(string)
			ov := ov.val.(string)
			if v < ov {
				return -1
			} else if v == ov {
				return 0
			} else {
				return 1
			}
		case Date:
			v := v.val.(time.Time)
			ov := ov.val.(time.Time)
			return int(v.Unix() - ov.Unix())
		case Min:
			return -1
		case Max:
			return 1
		default:
			log.Fatal("invalid values: ", v, ", ", ov)
			return 0
		}
	} else if v.ty < ov.ty {
		return -1
	} else {
		return 1
	}
}

func (v Value) String() string {
	switch v.ty {
	case Bool, Int:
		return fmt.Sprintf("%v", v.val)
	case String:
		return fmt.Sprintf("%#v", v.val)
	case Keyword:
		kw := v.val.(fressian.Keyword)
		if kw.Namespace == "" {
			return fmt.Sprintf(":%s", kw.Name)
		} else {
			return fmt.Sprintf(":%s/%s", kw.Namespace, kw.Name)
		}
	case Date:
		d := v.val.(time.Time)
		return d.Format(time.RFC3339)
	case Min:
		return "index.MinValue"
	case Max:
		return "index.MaxValue"
	default:
		return "index.InvalidValue"
	}
}

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
