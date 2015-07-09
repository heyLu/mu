package main

import (
	"fmt"
)

type indexed interface {
	valueAt(idx int) value
}

type tuple []value

func (t tuple) valueAt(idx int) value { return t[idx] }

type value interface{}

type variable interface{}

func getterFn(attrs map[variable]int, attr variable) func(tuple) value {
	idx := attrs[attr]
	return func(tuple tuple) value {
		return tuple.valueAt(idx)
	}
}

func hashKeyFn(getters ...func(tuple) value) func(tuple) indexed {
	return func(tuple tuple) indexed {
		vals := make([]value, len(getters))
		for i, getter := range getters {
			vals[i] = getter(tuple)
		}
		return newHashKey(vals)
	}
}

func hashAttrs(keyFn func(tuple) indexed, tuples []tuple) map[indexed][]tuple {
	m := make(map[indexed][]tuple, 0)
	for _, tuple_ := range tuples {
		key := keyFn(tuple_)
		if vals, ok := m[key]; ok {
			m[key] = append(vals, tuple_)
		} else {
			m[key] = []tuple{tuple_}
		}
	}
	return m
}

func main() {
	attrs := map[variable]int{
		"name": 0,
		"age":  1,
	}

	tuples := []tuple{
		tuple{"Jane", 13},
		tuple{"Alice", 7},
		tuple{"Fred", 3},
	}

	getName := getterFn(attrs, "name")
	getAge := getterFn(attrs, "age")
	getNameAndAge := hashKeyFn(getName, getAge)

	hash := hashAttrs(getNameAndAge, tuples)
	for k, v := range hash {
		fmt.Println(k)
		fmt.Printf("  %v\n", v)
	}
}

func newHashKey(vals []value) indexed {
	switch len(vals) {
	case 1:
		return key1{val1: vals[0]}
	case 2:
		return key2{val1: vals[0], val2: vals[1]}
	case 3:
		return key3{val1: vals[0], val2: vals[1],
			val3: vals[2]}
	default:
		panic("unsupported join arity")
	}
}

type key1 struct{ val1 value }

func (j key1) valueAt(idx int) value {
	switch idx {
	case 1:
		return j.val1
	default:
		panic("invalid index")
	}
}

type key2 struct{ val1, val2 value }

func (j key2) valueAt(idx int) value {
	switch idx {
	case 1:
		return j.val1
	case 2:
		return j.val2
	default:
		panic("invalid index")
	}
}

type key3 struct{ val1, val2, val3 value }

func (j key3) valueAt(idx int) value {
	switch idx {
	case 1:
		return j.val1
	case 2:
		return j.val2
	case 3:
		return j.val3
	default:
		panic("invalid index")
	}
}
