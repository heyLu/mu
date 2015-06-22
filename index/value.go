package index

import (
	"fmt"
	"github.com/heyLu/fressian"
	"log"
	"math/big"
	"net/url"
	"time"

	"github.com/heyLu/mu/comparable"
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

func (t ValueType) String() string {
	switch t {
	case Ref:
		return "Ref"
	case Keyword:
		return "Keyword"
	case Long:
		return "Long"
	case String:
		return "String"
	case Bool:
		return "Bool"
	case Instant:
		return "Instant"
	case Fn:
		return "Fn"
	case Bytes:
		return "Bytes"
	case UUID:
		return "UUID"
	case Double:
		return "Double"
	case Float:
		return "Float"
	case URI:
		return "URI"
	case BigInt:
		return "BigInt"
	case BigDec:
		return "BigDec"
	case Min:
		return "Min"
	case Max:
		return "Max"
	default:
		return "Invalid"
	}
}

func (t ValueType) IsValid() bool {
	return (t >= Ref && t <= Bytes) || (t >= UUID && t <= BigDec)
}

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
	case int64:
		return Value{Int, int(val.(int64))}
	case float32:
		return Value{Float, val}
	case float64:
		return Value{Double, val}
	case fressian.Keyword:
		return Value{Keyword, val}
	case fressian.UUID:
		return Value{UUID, val}
	case string:
		return Value{String, val}
	case time.Time:
		return Value{Date, val}
	case *url.URL:
		return Value{URI, val}
	case *big.Int:
		return Value{BigInt, val}
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
			if v.Namespace < ov.Namespace || (v.Namespace == ov.Namespace && v.Name < ov.Name) {
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
		case UUID:
			v := v.val.(fressian.UUID)
			ov := ov.val.(fressian.UUID)
			if v.Msb < ov.Msb {
				return -1
			} else if v.Msb == ov.Msb {
				if v.Lsb < ov.Lsb {
					return -1
				} else if v.Lsb == ov.Lsb {
					return 0
				} else {
					return 1
				}
			} else {
				return 1
			}
		case Float:
			v := v.val.(float32)
			ov := ov.val.(float32)
			if v < ov {
				return -1
			} else if v == ov {
				return 0
			} else {
				return 1
			}
		case Double:
			v := v.val.(float64)
			ov := ov.val.(float64)
			if v < ov {
				return -1
			} else if v == ov {
				return 0
			} else {
				return 1
			}
		case URI:
			v := v.val.(*url.URL).String()
			ov := ov.val.(*url.URL).String()
			if v < ov {
				return -1
			} else if v == ov {
				return 0
			} else {
				return 1
			}
		case BigInt:
			v := v.val.(*big.Int)
			ov := ov.val.(*big.Int)
			return v.Cmp(ov)
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
	case Ref:
		return fmt.Sprintf("ref(%v)", v.val)
	case Bool, Int, Float, Double, URI:
		return fmt.Sprintf("%v", v.val)
	case String:
		return fmt.Sprintf("%#v", v.val)
	case Keyword:
		return v.val.(fressian.Keyword).String()
	case Date:
		d := v.val.(time.Time)
		return d.Format(time.RFC3339)
	case UUID:
		return v.val.(fressian.UUID).String()
	case BigInt:
		return fmt.Sprintf("%vN", v.val)
	case Min:
		return "index.MinValue"
	case Max:
		return "index.MaxValue"
	default:
		return "index.InvalidValue"
	}
}
