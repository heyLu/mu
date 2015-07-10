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

// a context contains the context of a running query.
type context struct {
	sources map[variable]source
	rels    []relation
}

// a source contains the data that will be queried.
type source interface{}

// a clause selects data from a source.
type clause interface{} // predicate, fn w/ binding, pattern, rule invocation

// a patternClause is a clause that selects tuples that
// match the pattern from a source.
type patternClause struct {
	source  variable
	pattern pattern
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

// collapseRels joins relations that share variables with newRel.
func collapseRels(rels []relation, newRel relation) []relation {
	newRels := make([]relation, 0)
	for _, rel := range rels {
		sharesVars := false
		for attr, _ := range newRel.attrs {
			if _, ok := rel.attrs[attr]; ok {
				sharesVars = true
				break
			}
		}

		if sharesVars {
			newRel = hashJoin(rel, newRel)
		} else {
			newRels = append(newRels, rel)
		}
	}
	return append(newRels, newRel)
}

// resolveClause returns a new context with relations filtered
// according to the given clause.
func resolveClause(context context, clause clause) context {
	switch clause := clause.(type) {
	case patternClause:
		source := context.sources[clause.source]
		relation := lookupPatternColl(source.([]tuple), clause.pattern)
		newRels := collapseRels(context.rels, relation)

		newContext := context
		newContext.rels = newRels
		return newContext
	default:
		panic("invalid clause type")
	}
}

// query resolves the clauses sequentially.
func query(context context, clauses []clause) context {
	for _, clause := range clauses {
		context = resolveClause(context, clause)
	}
	return context
}

// cloneSlice returns a new slice with the values from the original.
func cloneSlice(vals []value) []value {
	newVals := make([]value, len(vals))
	copy(newVals, vals)
	return newVals
}

// internalCollect collects the bindings for the given symbols from the context.
func internalCollect(context context, symbols []variable) [][]value {
	acc := [][]value{make([]value, len(symbols))}
	for _, rel := range context.rels {
		keepAttrs := make(map[variable]int, 0)
		keepIdxs := make([]int, len(symbols))
		for i, symbol := range symbols {
			if idx, ok := rel.attrs[symbol]; ok {
				keepAttrs[symbol] = idx
				keepIdxs[i] = idx
			} else {
				keepIdxs[i] = -1 // not in this rel
			}
		}

		if len(keepAttrs) == 0 {
			continue
		}

		newAcc := make([][]value, 0, len(acc))
		for _, t1 := range acc {
			for _, t2 := range rel.tuples {
				res := cloneSlice(t1)
				for i := 0; i < len(symbols); i++ {
					if idx := keepIdxs[i]; idx != -1 {
						res[i] = t2[idx]
					}
				}
				newAcc = append(newAcc, res)
			}
		}
		acc = newAcc
	}
	return acc
}

// collect collects the symbols from the context, returning a set of
// result values.
func collect(context context, symbols []variable) map[indexed]bool {
	m := make(map[indexed]bool, 0)
	res := internalCollect(context, symbols)
	for _, vals := range res {
		key := newHashKey(vals)
		m[key] = true
	}
	return m
}

// newVar returns a new variable with the given name and namespace.
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

type Query struct{}

func Q(query Query, inputs ...interface{}) (interface{}, error) {
	return nil, nil
}
