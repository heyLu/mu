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

// Inventing types ...
//
// getterFn :: Map Symbol Value -> Symbol -> (Tuple -> Value)
// keyTupleFn :: [(Tuple -> Value)] -> (Tuple -> [Value])
// hashJoin :: Rel -> Rel -> Rel
// collapseRels :: [Rel] -> Rel -> [Rel]
// lookupPatternColl :: [Tuple] -> Pattern
//
// now to the interesting stuff ...
//
// Tuple = [Value] (or a type that supports indexed access to Values)
// Pattern = [PatternValue]
// PatternValue = Symbol | Value | LookupRef
//
// It should be possible to use `index.Value` as Value, in fact, to
// be able to compare values it's *required*, at least with the current
// comparsion functions.
//
// However, because Go has no sum types (or union types), we either
// have to invent an interface to support access, make it untyped
// (via `interface{}`) or use an empty internal marker interface,
// that we wrap values in when "parsing" the query.
//
// New problem: slices can't be map keys, but we need that for
// hashAttrs, to be able to join on multiple attributes.
//
// Alternatives for slices as map keys
//
// - explicit structs for n-way joins (will fail when trying to
//     join on too many attributes)
// - use a different map implementation (which?  do they exist?)
// - the really bad: convert value slice to a string
// - or maybe avoid the map completely

import (
	"fmt"
	"github.com/heyLu/edn"
	"github.com/heyLu/fressian"
	"reflect"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
)

// indexed is an interface for values that support access to fields by
// index.
type Indexed interface {
	ValueAt(idx int) value
}

// a tuple is an indexed collection of values.
type tuple interface {
	Indexed
	length() int
}

type sliceTuple []value

func (t sliceTuple) length() int           { return len(t) }
func (t sliceTuple) ValueAt(idx int) value { return t[idx] }

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
		return tuple.ValueAt(idx)
	}
}

// hashKeyFn returns a function that given a tuple returns the
// values the getters return for it in a slice.
func hashKeyFn(getters ...func(tuple) value) func(tuple) Indexed {
	return func(tuple tuple) Indexed {
		vals := make([]value, len(getters))
		for i, getter := range getters {
			vals[i] = getter(tuple)
		}
		return newHashKey(vals)
	}
}

// hashAttrs groups the tuples using the key from KeyFn.
func hashAttrs(keyFn func(tuple) Indexed, tuples []tuple) map[Indexed][]tuple {
	m := make(map[Indexed][]tuple, 0)
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
	newTuple := make(sliceTuple, l1+l2)
	for i, idx := range idxs1 {
		newTuple[i] = tuple1.ValueAt(idx)
	}
	for i, idx := range idxs2 {
		newTuple[l1+i] = tuple2.ValueAt(idx)
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

// indexedDatom is a proxy that implements the tuple interface for datoms.
type indexedDatom index.Datom

func (id indexedDatom) length() int { return 5 }

func (id indexedDatom) ValueAt(idx int) value {
	d := index.Datom(id)
	switch idx {
	case 0:
		return d.Entity()
	case 1:
		return d.Attribute()
	case 2:
		return d.Value().Val()
	case 3:
		return d.Transaction()
	case 4:
		return d.Added()
	default:
		panic("invalid index")
	}
}

var placeHolder = edn.Symbol{Namespace: "", Name: "_"}

// isPlaceHolder chesk if the value is the symbol _.
func isPlaceHolder(val interface{}) bool {
	val, ok := val.(edn.Symbol)
	return ok && val == placeHolder

}

// lookupPatternDb returns a relation containing the datoms from the db
// that match the pattern.
func lookupPatternDb(db *database.Db, pattern pattern) relation {
	dbPattern := database.Pattern{}
	attrs := make(map[variable]int, 0)
	for i, val := range pattern {
		if sym, ok := val.(variable); ok {
			attrs[sym] = i
			continue
		}

		if isPlaceHolder(val) {
			continue
		}

		switch i {
		case 0, 1, 3: // e, a, tx (lookups)
			var lookup database.HasLookup
			switch val := val.(type) {
			case int:
				lookup = database.Id(val)
			case edn.Keyword:
				lookup = database.Keyword{fressian.Keyword{Namespace: val.Namespace, Name: val.Name}}
			default:
				panic(fmt.Sprintf("can't convert %#v to database.HasLookup", val))
			}

			if i == 0 {
				dbPattern.E = lookup
			} else if i == 1 {
				dbPattern.A = lookup
			} else {
				dbPattern.Tx = lookup
			}
		case 2: // v
			dbPattern.V = val
		case 4: // added
			v := val.(bool)
			dbPattern.Added = &v
		}
	}

	datoms := make([]tuple, 0)
	iter := db.Search(dbPattern)
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		datoms = append(datoms, indexedDatom(*datom))
	}

	return relation{attrs: attrs, tuples: datoms}
}

// matchesPattern checks if the given tuple matches the pattern.
//
// A tuple matches a pattern if the constants in the same positions
// equal.  (I.e. variable in the pattern are ignored.)
func matchesPattern(pattern pattern, tuple tuple) bool {
	i := 0
	for i < len(pattern) && i < tuple.length() {
		p := pattern[i]
		t := tuple.ValueAt(i)
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

// lookupPattern returns a relation containing the tuples matching the
// pattern from the source.
func lookupPattern(source source, pattern pattern) relation {
	switch source := source.(type) {
	case *database.Db:
		return lookupPatternDb(source, pattern)
	case []tuple:
		return lookupPatternColl(source, pattern)
	default:
		panic("invalid source")
	}
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
		relation := lookupPattern(source, clause.pattern)
		newRels := collapseRels(context.rels, relation)

		newContext := context
		newContext.rels = newRels
		return newContext
	default:
		panic("invalid clause type")
	}
}

// runQuery resolves the clauses sequentially.
func runQuery(context context, clauses []clause) context {
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
						res[i] = t2.ValueAt(idx)
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
func collect(context context, symbols []variable) map[Indexed]bool {
	m := make(map[Indexed]bool, 0)
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

// toQueryMap parses a list of keywords and values into a query map.
func toQueryMap(queryList []interface{}) map[interface{}]interface{} {
	key := edn.Keyword{}
	vals := make([]interface{}, 0)
	queryMap := make(map[interface{}]interface{}, 0)
	for _, val := range queryList {
		if kw, ok := val.(edn.Keyword); ok {
			if len(vals) != 0 {
				queryMap[key] = vals
			}
			vals = make([]interface{}, 0)
			key = kw
		}
	}
	return queryMap
}

// isSource returns true if the value is a symbol beginning with "$"
func isSource(val interface{}) bool {
	if sym, ok := val.(edn.Symbol); ok && sym.Name[0] == '$' {
		return true
	}
	return false
}

// isVariable returns true if the value is a symbol beginning with "?".
func isVariable(val interface{}) bool {
	if sym, ok := val.(edn.Symbol); ok && sym.Name[0] == '?' {
		return true
	}
	return false
}

// parseQuery parses a query from untyped data.
func parseQuery(rawQuery interface{}) (*query, error) {
	var queryMap map[interface{}]interface{}
	if queryList, ok := rawQuery.([]interface{}); ok {
		queryMap = toQueryMap(queryList)
	} else {
		queryMap = rawQuery.(map[interface{}]interface{})
	}

	rawFindVars := queryMap[edn.Keyword{Namespace: "", Name: "find"}].([]interface{})
	findVars := make([]variable, len(rawFindVars))
	for i, findVar := range rawFindVars {
		findVars[i] = variable(findVar.(edn.Symbol))
	}

	rawClauses := queryMap[edn.Keyword{Namespace: "", Name: "where"}].([]interface{})
	clauses := make([]clause, len(rawClauses))
	for i, rawClause := range rawClauses {
		rawClause := rawClause.([]interface{})
		clause := patternClause{}
		var rawPattern []interface{}
		if isSource(rawClause[0]) {
			clause.source = variable(rawClause[0].(edn.Symbol))
			rawPattern = rawClause[1:]
		} else {
			clause.source = variable(edn.Symbol{Namespace: "", Name: "$"})
			rawPattern = rawClause
		}
		pattern := make(pattern, len(rawPattern))
		clause.pattern = pattern

		for i, rawValue := range rawPattern {
			if isVariable(rawValue) {
				pattern[i] = variable(rawValue.(edn.Symbol))
			} else {
				pattern[i] = rawValue
			}
		}

		clauses[i] = clause
	}

	return &query{
		find:  findVars,
		in:    []variable{variable(edn.Symbol{Namespace: "", Name: "$"})},
		where: clauses,
	}, nil
}

type query struct {
	find  []variable
	in    []variable
	where []clause
}

// Q parses the query and runs it given the inputs.
//
// Inputs can be collections of type []interface{} or databases
// (*database.Db).
func Q(query interface{}, inputs ...interface{}) (map[Indexed]bool, error) {
	q, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	sources := make(map[variable]source, 0)
	for i, in := range q.in {
		sources[in] = source(inputs[i])
	}

	context := context{sources: sources}
	context = runQuery(context, q.where)
	res := collect(context, q.find)
	return res, nil
}
