package query

// A few thoughts on datascript's query implementation.
// (And about reading Clojure code.)
//
// - we're probably going to have to do something about
//   immutability
// - lot's of Clojure-isms (which is pretty/awesome/clever),
//   which may or may not translate to Go well.  we may
//   have to cheat a lot.
//
// Reading Clojure thoughts & tips:
//
// - use the repl!  (i didn't, but i should have.)
// - reading bottom-up is probably best
// - but you will have to read top-down, to understand
//   how all the parts work together
// - i read `datascript.query` top-down first, after
//   trying to read it bottom-up
//
// - you'll need to infer *lots* of context
// - persist!  don't let scary code intimidate you.
// - work with examples
// - print out the code
// - comment *everything*
// - leave questions for later
// - read the whole thing
// - ...
//
// It's quite fun to read Clojure, and it makes me want to
// write more Clojure, especially this kind of code.
// But ...  without a *lot* of context/a-priori knowledge
// it's going to be hard, things won't make sense.  I don't
// know how one learns "big" Clojure code bases (datascript
// maybe *is* already big, taking the "compression factor"
// into account?), but it will likely be how one learns
// anything: by starting, and not giving up.  Working slowly,
// reading/doing things multiple times, trying lots of
// examples, using `println`s, ...
// (Taking breaks also helps.)
//
// I don't yet know how this will fit into Go, but I guess
// I'll just have to start and run with it.
//
// Onwards!

import (
	"github.com/heyLu/edn"
	"reflect"

	"github.com/heyLu/mu/database"
)

/// Utilities

func intersectKeys(m1, m2 map[interface{}]int) []interface{} {
	keys := make([]interface{}, 0)
	for k1, _ := range m1 {
		if _, ok := m2[k1]; ok {
			keys = append(keys, k1)
		}
	}
	return keys
}

// TODO concatv

// TODO looks-like?

func isSource(sym interface{}) bool {
	if sym, ok := sym.(edn.Symbol); ok {
		return sym.Name[0] == '$'
	}
	return false
}

func isFreeVar(sym interface{}) bool {
	if sym, ok := sym.(edn.Symbol); ok {
		return sym.Name[0] == '?'
	}
	return false
}

func isAttr(form interface{}) bool {
	_, ok := form.(database.Keyword)
	return ok
}

func isLookupRef(form interface{}) bool {
	_, ok := form.(database.HasLookup)
	return ok
}

/// Relation algebra

type relation struct {
	attrs  map[interface{}]int
	tuples [][]interface{}
}

// joinTuples selects values at indices ids1 and ids2 in tuples1 and tuples2.
//
// Returns a slice with the values in the order specified by ids1, ids2.
func joinTuples(tuples1 []interface{}, ids1 []int, tuples2 []interface{}, ids2 []int) []interface{} {
	l1 := len(ids1)
	l2 := len(ids2)
	res := make([]interface{}, l1+l2)

	for i := 0; i < l1; i++ {
		res[i] = tuples1[ids1[i]]
	}

	for i := 0; i < l2; i++ {
		res[l1+i] = tuples2[ids2[i]]
	}

	return res
}

// sumRel returns a new relation with attributes from a and tuples from both.
func sumRel(a, b relation) relation {
	return relation{
		attrs:  a.attrs,
		tuples: append(a.tuples, b.tuples...),
	}
}

// prodRel returns the product of the two relations
func prodRel(rel1, rel2 relation) relation {
	ids1 := relIds(rel1.attrs)
	ids2 := relIds(rel2.attrs)
	attrs := relConcatAttrs(rel1.attrs, rel2.attrs)
	tuples := make([][]interface{}, 0)
	for _, t1 := range rel1.tuples {
		for _, t2 := range rel2.tuples {
			tuples = append(tuples, joinTuples(t1, ids1, t2, ids2))
		}
	}
	return relation{attrs: attrs, tuples: tuples}
}

func relIds(attrs map[interface{}]int) []int {
	ids := make([]int, len(attrs))
	i := 0
	for _, v := range attrs {
		ids[i] = v
		i += 1
	}
	return ids
}

func relAttrsKeys(attrs map[interface{}]int) []interface{} {
	keys := make([]interface{}, len(attrs))
	i := 0
	for k, _ := range attrs {
		keys[i] = k
		i += 1
	}
	return keys
}

func relConcatAttrs(attrs1, attrs2 map[interface{}]int) map[interface{}]int {
	attrs := make(map[interface{}]int, len(attrs1)+len(attrs2))
	i := 0
	for k, _ := range attrs1 {
		attrs[k] = i
		i += 1
	}
	for k, _ := range attrs2 {
		attrs[k] = i
		i += 1
	}
	return attrs
}

/// Built-ins

func differ(xs ...interface{}) bool {
	l := len(xs)
	// FIXME: should use index.Value#Compare here?
	return reflect.DeepEqual(xs[0:(l/2)], xs[(l/2):])
}

// TODO getElse

// TODO getSome

// TODO isMissing

var builtIns map[interface{}]func(xs []interface{}) interface{}

// TODO builtInAggregates

///

// TODO parseRules

func bindableToSeq(x interface{}) bool {
	_, ok := x.([]interface{})
	return ok
}

func emptyRel(binding interface{}) relation {
	// FIXME: collect vars from binding, create rel with those as attributes
	return relation{}
}

// TODO IBinding + impls

// TODO resolveIns (should be done in parser?)

var lookupAttrs map[interface{}]bool
var lookupSource interface{}

func getterFn(attrs map[interface{}]int, attr interface{}) func([]interface{}) interface{} {
	idx := attrs[attr]
	if _, ok := lookupAttrs[attr]; ok {
		return func(tuple []interface{}) interface{} {
			eid := tuple[idx]
			if eid, ok := eid.(int); ok {
				return eid
			} else {
				panic("not implemented")
				return -1
			}
		}
	} else {
		return func(tuple []interface{}) interface{} {
			return tuple[idx]
		}
	}
}

func tupleKeyFn(getters ...func([]interface{}) interface{}) func([]interface{}) interface{} {
	if len(getters) == 1 {
		return getters[0]
	} else {
		return func(tuple []interface{}) interface{} {
			res := make([]interface{}, len(getters))
			for i, getter := range getters {
				res[i] = getter(tuple)
			}
			return res
		}
	}
}

func hashAttrs(keyFn func([]interface{}) interface{}, tuples [][]interface{}) map[interface{}][]interface{} {
	m := make(map[interface{}][]interface{}, 0)
	for _, tuple := range tuples {
		key := keyFn(tuple)
		vals, ok := m[key]
		if ok {
			m[key] = append(vals, key)
		} else {
			m[key] = []interface{}{key}
		}
	}
	return m
}

func hashJoin(rel1, rel2 relation) relation {
	commonAttrs := intersectKeys(rel1.attrs, rel2.attrs)
	commonGetters1 := make([]func([]interface{}) interface{}, len(commonAttrs))
	commonGetters2 := make([]func([]interface{}) interface{}, len(commonAttrs))
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

func relAttrsDifference(attrs1, attrs2 map[interface{}]int) []interface{} {
	keys := make([]interface{}, 0)
	for k1, _ := range attrs1 {
		if _, ok := attrs2[k1]; !ok {
			keys = append(keys, k1)
		}
	}
	return keys
}

// TODO lookupPatternDb

func matchesPattern(pattern, tuple []interface{}) bool {
	i := 0
	for i < len(pattern) && i < len(tuple) {
		if _, ok := pattern[i].(edn.Symbol); ok || reflect.DeepEqual(tuple[i], pattern[i]) {
			i += 1
		} else {
			return false
		}
	}
	return true
}

func lookupPatternColl(coll [][]interface{}, pattern []interface{}) relation {
	data := make([][]interface{}, 0)
	for _, tuple := range coll {
		if matchesPattern(pattern, tuple) {
			data = append(data, tuple)
		}
	}
	attrs := make(map[interface{}]int, len(pattern))
	i := 0
	for _, p := range pattern {
		if isFreeVar(p) {
			attrs[p] = i
		}
		i += 1
	}
	return relation{attrs: attrs, tuples: data}
}

func normalizePatternClause(clause []interface{}) []interface{} {
	if isSource(clause[0]) {
		return clause
	} else {
		return append([]interface{}{edn.Symbol{Name: "$"}}, clause...)
	}
}

func lookupPattern(source interface{}, pattern []interface{}) relation {
	return lookupPatternColl(source.([][]interface{}), pattern)
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

type context struct {
	rels    []relation
	sources map[interface{}]source
}

type source struct{}

func contextResolveVal(context context, sym interface{}) interface{} {
	var rel *relation
	for _, r := range context.rels {
		if _, ok := r.attrs[sym]; ok {
			rel = &r
			break
		}
	}
	if rel == nil || len(rel.tuples) == 0 {
		return nil
	}
	return rel.tuples[0][rel.attrs[sym]]
}

func relContainsAttrs(rel relation, attrs []interface{}) bool {
	for _, attr := range attrs {
		if _, ok := rel.attrs[attr]; !ok {
			return false
		}
	}
	return true
}

func relProdByAttrs(context context, attrs []interface{}) (context, relation) {
	rels := make([]relation, 0)
	remainingRels := make([]relation, 0)
	for _, rel := range context.rels {
		if relContainsAttrs(rel, attrs) {
			rels = append(rels, rel)
		} else {
			remainingRels = append(remainingRels, rel)
		}
	}
	if len(rels) == 0 {
		return context, relation{}
	}
	prod := rels[0]
	for _, rel := range rels[1:] {
		prod = prodRel(prod, rel)
	}
	newContext := context
	newContext.rels = remainingRels
	return newContext, prod
}

func callFn(context context, rel relation, f interface{}, args []interface{}) func([]interface{}) interface{} {
	return func(tuple []interface{}) interface{} {
		resolvedArgs := make([]reflect.Value, len(args))
		for i, arg := range args {
			var val interface{}
			if _, ok := arg.(edn.Symbol); ok {
				source, ok := context.sources[arg]
				if ok {
					val = source
				} else {
					val = tuple[rel.attrs[arg]]
				}
			} else {
				val = arg
			}
			resolvedArgs[i] = reflect.ValueOf(val)
		}
		res := reflect.ValueOf(f).Call(resolvedArgs)
		if len(res) != 1 {
			panic("invalid return value")
		}
		return res[0].Interface()
	}
}

func filterByPred(context context, clause []interface{}) context {
	predicate := clause[0].([]interface{})
	f := predicate[0]
	args := predicate[1:]

	var pred func([]interface{}) interface{}
	pred, ok := builtIns[f]
	if !ok {
		pred = contextResolveVal(context, f).(func([]interface{}) interface{})
	}

	symbolArgs := make([]interface{}, 0)
	for _, arg := range args {
		if _, ok := arg.(edn.Symbol); ok {
			symbolArgs = append(symbolArgs, arg)
		}
	}
	context, prod := relProdByAttrs(context, symbolArgs)
	if pred != nil {
		tuplePred := callFn(context, prod, pred, args)
		newTuples := make([][]interface{}, 0)
		for _, tuple := range prod.tuples {
			if tuplePred(tuple).(bool) {
				newTuples = append(newTuples, tuple)
			}
		}
		prod.tuples = newTuples
	} else {
		prod.tuples = nil
	}

	newContext := context
	newContext.rels = append(newContext.rels, prod)
	return newContext
}

// TODO bind-by-fn

/// Rules

// TODO expand-rule

// TODO remove-pairs

// TODO rule-gen-guards

// TODO walk-collect

// TODO split-guards

// TODO solve-rule

// TODO resolve-pattern-lookup-refs

// TODO dynamic-lookup-attrs

// TODO -resolve-clause

// TODO resolve-clause

// TODO -q

// TODO -collect

// TODO collect

// TODO IContextResolve

// TODO -aggregate

// TODO idxs-of

// TODO aggregate

// TODO IPostProcess

// TODO pull

// TODO memoized-parse-query

// TODO q

type Query struct{}

func Q(query Query, inputs ...interface{}) (interface{}, error) {
	return nil, nil
}
