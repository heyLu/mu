package main

import (
	"github.com/heyLu/edn"

	"github.com/heyLu/mu/index"
)

type queryContext struct{}

// can be a pattern, rule invocation, predicate, fn w/ binding
type queryClause interface{}

type pattern struct {
	source  edn.Symbol
	pattern []patternValue
}

// can be a variable, placeholder, constant or lookup ref
type patternValue interface{}

func resolveClause(context queryContext, clause queryClause) queryContext {
	switch clause := clause.(type) {
	case pattern:
		// get pattern source from context
		// "run" pattern against the db
		// "join" resulting relation with the ones in the context
		return queryContext{}
	default:
		panic("unknown clause type")
	}
}

func intersectKeys(m1, m2 map[edn.Symbol]int) []edn.Symbol {
	keys := make([]edn.Symbol, 0)
	for k1, _ := range m1 {
		if _, ok := m2[k1]; ok {
			keys = append(keys, k1)
		}
	}
	return keys
}

func relAttrsKeys(attrs map[edn.Symbol]int) []edn.Symbol {
	keys := make([]edn.Symbol, len(attrs))
	i := 0
	for k, _ := range attrs {
		keys[i] = k
		i += 1
	}
	return keys
}

var lookupAttrs map[interface{}]bool
var lookupSource interface{}

func getterFn(attrs map[edn.Symbol]int, attr edn.Symbol) func(tuple) value {
	idx := attrs[attr]
	if _, ok := lookupAttrs[attr]; ok {
		return func(tuple tuple) value {
			eid := tuple[idx]
			if eid, ok := eid.(int); ok {
				return eid
			} else {
				panic("not implemented")
				return -1
			}
		}
	} else {
		return func(tuple tuple) value {
			return tuple[idx]
		}
	}
}

func tupleKeyFn(getters ...func(tuple) value) func(tuple) []value {
	/*if len(getters) == 1 {
		return getters[0]
	} else {*/
	return func(tuple tuple) []value {
		res := make([]value, len(getters))
		for i, getter := range getters {
			res[i] = getter(tuple)
		}
		return res
	}
	//}
}

func hashAttrs(keyFn func(tuple) []value, tuples []tuple) map[[]value][]tuple {
	m := make(map[[]value][]tuple, 0)
	for _, tuple := range tuples {
		key := keyFn(tuple)
		vals, ok := m[key]
		if ok {
			m[key] = append(vals, key)
		} else {
			m[key] = []tuple{key}
		}
	}
	return m
}

func hashJoin(rel1, rel2 relation) relation {
	commonAttrs := intersectKeys(rel1.attrs, rel2.attrs)
	commonGetters1 := make([]func([]value) value, len(commonAttrs))
	commonGetters2 := make([]func([]value) value, len(commonAttrs))
	for i, attr := range commonAttrs {
		commonGetters1[i] = getterFn(rel1.attrs, attr)
		commonGetters2[i] = getterFn(rel2.attrs, attr)
	}
	keepAttrs1 := relAttrsKeys(rel1.attrs)
	keepIds1 := make([]int, len(keepAttrs1))
	for i, attr := range keepAttrs1 {
		keepIds1[i] = rel1.attrs[attr]
	}
	keepAttrs2 := relAttrsDifference(rel2.attrs, rel1.attrs)
	keepIds2 := make([]int, len(keepAttrs2))
	for i, attr := range keepAttrs2 {
		keepIds2[i] = rel2.attrs[attr]
	}
	keyFn1 := tupleKeyFn(commonGetters1...)
	hash := hashAttrs(keyFn1, rel1.tuples)
	keyFn2 := tupleKeyFn(commonGetters2...)
	newTuples := make([][]interface{}, 0)
	for _, tuple2 := range rel2.tuples {
		key := keyFn2(tuple2)
		if tuples1, ok := hash[key]; ok {
			for _, tuple1 := range tuples1 {
				newTuples = append(newTuples, joinTuples(tuple1.([]interface{}), keepIds1, tuple2, keepIds2))
			}
		}
	}
	newAttrs := make(map[interface{}]int, len(keepAttrs1)+len(keepAttrs2))
	i := 0
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

func relAttrsDifference(attrs1, attrs2 map[edn.Symbol]int) []edn.Symbol {
	keys := make([]edn.Symbol, 0)
	for k1, _ := range attrs1 {
		if _, ok := attrs2[k1]; !ok {
			keys = append(keys, k1)
		}
	}
	return keys
}

func collapseRels(rels []relation, newRel relation) []relation {
	res := make([]relation, 0, len(rels))
	for _, rel := range rels {
		if len(intersectKeys(newRel.attrs, rel.attrs)) != 0 {
			newRel = hashJoin(rel, newRel)
		} else {
			res = append(res, rel)
		}
	}
	return append(res, newRel)
}

/*type pattern struct {
	e     value
	a     value
	v     value
	tx    value
	added value
}*/
type pattern struct {
	source edn.Symbol
	values []value
}

type tuple []value

type value interface{}

type variable struct {
	name edn.Symbol
}

type constant struct {
	value interface{}
}

type context struct {
	sources map[edn.Symbol][][]value
	rels    []relation
}

type relation struct {
	attrs  map[edn.Symbol]int
	tuples [][]value
}

func isEqual(a, b value) bool {
	ca, aOk := a.(constant)
	cb, bOk := b.(constant)
	if !aOk || !bOk {
		panic("invalid comparison")
	}
	va := index.NewValue(ca)
	vb := index.NewValue(cb)
	return va.Compare(vb) == 0
}

func isFreeVar(val value) bool {
	if variable, ok := val.(variable); ok {
		return variable.name.Name[0] == '?'
	}
	return false
}

func matchesPattern(pattern pattern, tuple []value) bool {
	i := 0
	for i < len(pattern.values) && i < len(tuple) {
		t := tuple[i]
		p := pattern.values[i]
		if isFreeVar(p) || isEqual(t, p) {
			i += 1
		} else {
			return false
		}
	}
	return true
}

func lookupPatternColl(coll [][]value, pattern pattern) relation {
	data := make([][]value, 0)
	for _, val := range coll {
		if matchesPattern(pattern, val) {
			data = append(data, val)
		}
	}

	attrToIdx := make(map[edn.Symbol]int, 0)
	i := 0
	for _, val := range pattern.values {
		if isFreeVar(val) {
			attrToIdx[val.(variable).name] = i
		}
		i += 1
	}
	return relation{attrs: attrToIdx, tuples: data}
}

func innerResolveClause(ctx context, clause pattern) context {
	// TODO predicate
	// TODO function (with binding)
	// pattern
	source := ctx.sources[clause.source]
	rel := lookupPatternColl(source, clause)
	newRels := collapseRels(ctx.rels, rel)
	newCtx := ctx
	newCtx.rels = newRels
	return newCtx
}

func main() {
}
