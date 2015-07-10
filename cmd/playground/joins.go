package main

import (
	"fmt"
	"reflect"

	"github.com/heyLu/edn"
)

// indexed is an interface for values that support access to fields by
// index.
type indexed interface {
	valueAt(idx int) value
}

// a tuple is an indexed collection of values.
type tuple []value

func (t tuple) valueAt(idx int) value { return t[idx] }

// a value is any (scalar) value we support in queries.
type value interface{}

// a pattern is a tuple of variables, placeholders and values.
type pattern []patternValue

// a patternValue is either a variable, a placeholder or a
// value (constant).
type patternValue interface{}

// a variable is a name for values to query for.
type variable edn.Symbol

// a relation contains tuples which have values for a given set of
// attributes.
//
// the relation knows at which indices the attribute values are stored
// in the tuples.
type relation struct {
	attrs  map[variable]int
	tuples []tuple
}

// getterFn returns a function that extracts the attribute from a tuple.
func getterFn(attrs map[variable]int, attr variable) func(tuple) value {
	idx := attrs[attr]
	return func(tuple tuple) value {
		return tuple.valueAt(idx)
	}
}

// hashKeyFn returns a function that given a tuple returns the
// values the getters return for it in a slice.
func hashKeyFn(getters ...func(tuple) value) func(tuple) indexed {
	return func(tuple tuple) indexed {
		vals := make([]value, len(getters))
		for i, getter := range getters {
			vals[i] = getter(tuple)
		}
		return newHashKey(vals)
	}
}

// hashAttrs groups the tuples using the key from KeyFn.
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

// intersectKeys returns a slice of keys that exist in both maps.
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

// hashJoin returns a new relation with tuples that are joined
// based on the attributes common to both relations.
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

// hashEqual compares two hashable values for equality.
func hashEqual(a, b interface{}) bool {
	// TODO: benchmark this, check if map-based comparison is faster
	return reflect.DeepEqual(a, b)
}

// matchesPattern checks if the given tuple matches the pattern.
//
// A tuple matches a pattern if the constants in the same positions
// equal.  (I.e. variable in the pattern are ignored.)
func matchesPattern(pattern pattern, tuple tuple) bool {
	i := 0
	for i < len(pattern) && i < len(tuple) {
		p := pattern[i]
		t := tuple[i]
		if _, isVar := p.(variable); !isVar && !hashEqual(p, t) {
			return false
		}
		i += 1
	}
	return true
}

// lookupPatternColl returns a relation containing all the tuples in
// the collection that match the pattern.
//
// The relation contains the variables from the pattern.
func lookupPatternColl(coll []tuple, pattern pattern) relation {
	data := make([]tuple, 0)
	for _, tuple := range coll {
		if matchesPattern(pattern, tuple) {
			data = append(data, tuple)
		}
	}
	attrs := make(map[variable]int, 0)
	for i, val := range pattern {
		if variable, ok := val.(variable); ok {
			attrs[variable] = i
		}
	}
	return relation{attrs: attrs, tuples: data}
}

func main() {
	attrs := map[variable]int{
		newVar("name"): 0,
		newVar("age"):  1,
	}

	tuples := []tuple{
		tuple{"Jane", 13},
		tuple{"Alice", 7},
		tuple{"Fred", 3},
	}

	getName := getterFn(attrs, newVar("name"))
	getAge := getterFn(attrs, newVar("age"))
	getNameAndAge := hashKeyFn(getName, getAge)

	hash := hashAttrs(getNameAndAge, tuples)
	for k, v := range hash {
		fmt.Println(k)
		fmt.Printf("  %v\n", v)
	}

	likesAttrs := map[variable]int{
		newVar("name"):  0,
		newVar("likes"): 1,
	}
	likesTuples := []tuple{
		tuple{"Jane", "pancakes"},
		tuple{"Alice", "the stars"},
		tuple{"Fred", "Alice"},
		tuple{"Fred", "Little Fred"},
	}

	namesAndAges := relation{attrs: attrs, tuples: tuples}
	likes := relation{attrs: likesAttrs, tuples: likesTuples}
	joined := hashJoin(namesAndAges, likes)

	fmt.Println()
	fmt.Println(joined.attrs)
	for _, tuple := range joined.tuples {
		fmt.Println(tuple)
	}

	fmt.Println()
	fmt.Println("7 years old, likes the stars")
	age7LikesTheStars := pattern{newVar("name"), 7, "the stars"}
	starGazers := lookupPatternColl(joined.tuples, age7LikesTheStars)
	for _, tuple := range starGazers.tuples {
		for attr, idx := range starGazers.attrs {
			fmt.Printf("%v -> %v\n", attr, tuple[idx])
		}
	}

	fmt.Println()
	fmt.Println("3 years old")
	age3 := pattern{newVar("name"), 3, newVar("likes")}
	threeYearOlds := lookupPatternColl(joined.tuples, age3)
	for _, tuple := range threeYearOlds.tuples {
		for attr, idx := range threeYearOlds.attrs {
			fmt.Printf("%v -> %v\t\t", attr, tuple[idx])
		}
		fmt.Println()
	}
}

// variable returns a new variable with the given name and namespace.
//
// If no name is given, the namespace will be empty.  If more than two
// arguments are given, newVar will panic.
func newVar(namespace string, name ...string) variable {
	switch len(name) {
	case 0:
		return variable(edn.Symbol{Namespace: "", Name: namespace})
	case 1:
		return variable(edn.Symbol{Namespace: namespace, Name: name[0]})
	default:
		panic("newVar only takes one or two arguments")
	}
}

// newHashKey returns a value implementing indexed that contains
// the given values.
//
// For now the maximum number of values supported is three.  This limit
// is arbitrary and can easily be changed.  We will change it based on
// how much joins queries need in practice.
func newHashKey(vals []value) indexed {
	switch len(vals) {
	case 1:
		return key1{val1: vals[0]}
	case 2:
		return key2{val1: vals[0], val2: vals[1]}
	case 3:
		return key3{val1: vals[0], val2: vals[1], val3: vals[2]}
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
