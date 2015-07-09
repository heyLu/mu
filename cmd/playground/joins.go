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

type relation struct {
	attrs  map[variable]int
	tuples []tuple
}

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

func intersectKeys(attrs1, attrs2 map[variable]int) []variable {
	keys := make([]variable, 0)
	for attr1, _ := range attrs1 {
		if _, ok := attrs2[attr1]; ok {
			keys = append(keys, attr1)
		}
	}
	return keys
}

// joinTuples returns a new tuple with the values from tuple1 and tuple2 at indexes idxs1 and idxs2, respectively.
func joinTuples(tuple1 tuple, idxs1 []int, tuple2 tuple, idxs2 []int) tuple {
	l1 := len(idxs1)
	l2 := len(idxs2)
	newTuple := make(tuple, l1+l2)
	for i, idx := range idxs1 {
		newTuple[i] = tuple1.valueAt(idx)
	}
	for i, idx := range idxs2 {
		newTuple[l1+i] = tuple2.valueAt(idx)
	}
	return newTuple
}

func hashJoin(rel1, rel2 relation) relation {
	// join on the attributes both have in common
	commonAttrs := intersectKeys(rel1.attrs, rel2.attrs)
	commonGetters1 := make([]func(tuple) value, len(commonAttrs))
	commonGetters2 := make([]func(tuple) value, len(commonAttrs))
	for i, attr := range commonAttrs {
		commonGetters1[i] = getterFn(rel1.attrs, attr)
		commonGetters2[i] = getterFn(rel2.attrs, attr)
	}
	// take all attributes from rel1
	keepAttrs1 := make([]variable, len(rel1.attrs))
	keepIdxs1 := make([]int, len(rel1.attrs))
	i := 0
	for attr, idx := range rel1.attrs {
		keepAttrs1[i] = attr
		keepIdxs1[i] = idx
		i += 1
	}
	// only keep attrs not in rel1 from rel2
	keepAttrs2 := make([]variable, 0)
	keepIdxs2 := make([]int, 0)
	for attr, idx := range rel2.attrs {
		if _, ok := rel1.attrs[attr]; !ok {
			keepAttrs2 = append(keepAttrs2, attr)
			keepIdxs2 = append(keepIdxs2, idx)
		}
	}
	// construct functions to get the "join key" from tuple
	keyFn1 := hashKeyFn(commonGetters1...)
	keyFn2 := hashKeyFn(commonGetters2...)
	// collect tuples from rel1 by "join key"
	hash := hashAttrs(keyFn1, rel1.tuples)
	// join tuples with a matching join key
	newTuples := make([]tuple, 0)
	for _, tuple2 := range rel2.tuples {
		key := keyFn2(tuple2)
		if tuples1, ok := hash[key]; ok {
			for _, tuple1 := range tuples1 {
				joinTuple := joinTuples(tuple1, keepIdxs1, tuple2, keepIdxs2)
				newTuples = append(newTuples, joinTuple)
			}
		}
	}
	// return the new relation
	newAttrs := make(map[variable]int, 0)
	i = 0
	for _, attr := range keepAttrs1 {
		newAttrs[attr] = i
		i += 1
	}
	for _, attr := range keepAttrs2 {
		newAttrs[attr] = i
		i += 1
	}
	return relation{attrs: newAttrs, tuples: newTuples}
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
