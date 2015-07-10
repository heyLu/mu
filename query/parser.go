package query

import (
	"fmt"
	"github.com/heyLu/edn"

	"github.com/heyLu/mu/index"
)

/// utils

func parseAnyOf(form interface{}, parsers ...func(interface{}) (interface{}, error)) (interface{}, error) {
	for _, parser := range parsers {
		val, err := parser(form)
		if val == nil { // parser "skipped" the form, try next
			continue
		}

		return val, err
	}
	return nil, nil
}

func parseSeq(parser func(interface{}) (interface{}, error), form interface{}) (interface{}, error) {
	forms, ok := form.([]interface{})
	if !ok {
		return nil, nil
	}

	res := make([]interface{}, len(forms))
	for i, form := range forms {
		val, err := parser(form)
		if val == nil {
			return nil, nil
		}

		if err != nil {
			return nil, err
		}
		res[i] = val
	}
	return res, nil
}

/// placeholder    = the symbol '_'
/// variable       = symbol starting with "?"
/// src-var        = symbol starting with "$"
/// rules-var      = the symbol "%"
/// constant       = any non-variable data literal
/// plain-symbol   = symbol that does not begin with "$" or "?"

type placeholder struct{}

func parsePlaceholder(form interface{}) (interface{}, error) {
	if sym, ok := form.(edn.Symbol); ok && sym.Name == "_" {
		return placeholder{}, nil
	}
	return nil, nil
}

//type variable edn.Symbol

func parseVariable(form interface{}) (interface{}, error) {
	if sym, ok := form.(edn.Symbol); ok && sym.Name[0] == '?' {
		return variable(sym), nil
	}
	return nil, nil
}

type srcVar struct {
	name edn.Symbol
}

func parseSrcVar(form interface{}) (interface{}, error) {
	if sym, ok := form.(edn.Symbol); ok && sym.Name[0] == '?' {
		return srcVar{name: sym}, nil
	}
	return nil, nil
}

type rulesVar struct{}

func parseRulesVar(form interface{}) (interface{}, error) {
	if sym, ok := form.(edn.Symbol); ok && sym.Name == "%" {
		return rulesVar{}, nil
	}
	return nil, nil
}

type constant struct {
	value index.Value
}

func parseConstant(form interface{}) (interface{}, error) {
	if _, ok := form.(edn.Symbol); ok {
		return nil, nil
	}
	return constant{value: index.NewValue(form)}, nil
}

type plainSymbol struct {
	name edn.Symbol
}

func parsePlainSymbol(form interface{}) (interface{}, error) {
	if sym, ok := form.(edn.Symbol); ok {
		val1, _ := parseVariable(form)
		val2, _ := parseSrcVar(form)
		val3, _ := parsePlaceholder(form)
		if val1 == nil && val2 == nil && val3 == nil {
			return plainSymbol{name: sym}, nil
		}
	}
	return nil, nil
}

/// fn-arg = (variable | constant | src-var)

func parseFnArg(form interface{}) (interface{}, error) {
	return parseAnyOf(form, parseVariable, parseConstant, parseSrcVar)
}

/// rule-vars = [ variable+ | ([ variable+ ] variable*) ]

type ruleVars struct {
	required []variable
	free     []variable
}

func parseRuleVars(form interface{}) (interface{}, error) {
	forms, ok := form.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected [ variable+ | ([ variable+ ] variable*) ]")
	}

	var requiredRaw []interface{}
	var freeRaw []interface{} = forms
	if required, ok := forms[0].([]interface{}); ok {
		requiredRaw = required
		freeRaw = forms[1:]
	}

	required := make([]variable, len(requiredRaw))
	for i, req := range requiredRaw {
		valRaw, _ := parseVariable(req)
		val, ok := valRaw.(variable)
		if !ok {
			return nil, fmt.Errorf("expected [ variable+ | ([ variable+ ] variable*) ]")
		}
		required[i] = val
	}

	free := make([]variable, len(freeRaw))
	for i, req := range freeRaw {
		valRaw, _ := parseVariable(req)
		val, ok := valRaw.(variable)
		if !ok {
			return nil, fmt.Errorf("expected [ variable+ | ([ variable+ ] variable*) ]")
		}
		free[i] = val
	}

	duplicates := map[variable]bool{}
	for _, v := range append(required, free...) {
		if _, ok := duplicates[v]; ok {
			return nil, fmt.Errorf("duplicate rule variable %v in %v", v, form)
		}
		duplicates[v] = true
	}

	return ruleVars{required: required, free: free}, nil
}

func (rv ruleVars) flatten() []edn.Symbol {
	numRequired := len(rv.required)
	vars := make([]edn.Symbol, numRequired+len(rv.free))
	for i, v := range rv.required {
		vars[i] = edn.Symbol(v)
	}
	for i, v := range rv.free {
		vars[numRequired+i] = edn.Symbol(v)
	}
	return vars
}

func (rv ruleVars) arity() (int, int) {
	return len(rv.required), len(rv.free)
}

/// binding        = (bind-scalar | bind-tuple | bind-coll | bind-rel)
/// bind-scalar    = variable
/// bind-tuple     = [ (binding | '_')+ ]
/// bind-coll      = [ binding '...' ]
/// bind-rel       = [ [ (binding | '_')+ ] ]

type binding interface {
	bind(val interface{})
}

type bindIgnore struct{}

func parseBindIgnore(form interface{}) (interface{}, error) {
	if sym, ok := form.(edn.Symbol); ok && sym.Name == "_" {
		return bindIgnore{}, nil
	}
	return nil, nil
}

type bindScalar struct {
	variable variable
}

func parseBindScalar(form interface{}) (interface{}, error) {
	val, _ := parseVariable(form)
	if val != nil {
		return bindScalar{variable: val.(variable)}, nil
	}
	return nil, nil
}

type bindTuple struct {
	bindings []binding
}

func parseTupleEl(form interface{}) (interface{}, error) {
	val, _ := parseBindIgnore(form)
	if val != nil {
		return val, nil
	}

	return parseBinding(form)
}

func parseBindTuple(form interface{}) (interface{}, error) {
	forms, ok := form.([]interface{})
	if !ok {
		return nil, nil
	}

	if len(forms) == 0 {
		return nil, fmt.Errorf("tuple binding cannot be empty")
	}

	bindings := make([]binding, len(forms))
	for i, form := range forms {
		val, err := parseTupleEl(form)
		if err != nil {
			return nil, err
		}

		if val == nil {
			return nil, fmt.Errorf("expected (binding | '_') but got %v", form)
		}
		bindings[i] = val.(binding)
	}

	return bindTuple{bindings: bindings}, nil
}

type bindColl struct {
	binding binding
}

func parseBindColl(form interface{}) (interface{}, error) {
	forms, ok := form.([]interface{})
	sym := edn.Symbol{Name: "..."}
	if !ok || len(forms) != 2 || forms[1] != sym {
		return nil, nil
	}

	bind, err := parseBinding(forms[0])
	if err != nil {
		return nil, err
	}

	return bindColl{binding: bind.(binding)}, nil
}

type bindRel struct {
	binding binding
}

func parseBindRel(form interface{}) (interface{}, error) {
	forms, ok := form.([]interface{})
	if !ok || len(forms) != 1 {
		return nil, nil
	}

	fform, ok := forms[0].([]interface{})
	if !ok {
		return nil, nil
	}

	bind, err := parseBindTuple(fform)
	if err != nil {
		return nil, err
	}

	if bind == nil {
		return nil, nil
	}

	return bindRel{binding: bind.(binding)}, nil
}

func parseBinding(form interface{}) (interface{}, error) {
	val, err := parseAnyOf(form, parseBindColl, parseBindRel, parseBindTuple,
		parseBindIgnore, parseBindScalar)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, fmt.Errorf("expecte (bind-scalar | bind-tuple | bind-coll | bind-rel)")
	}

	return val, nil
}

/// find-spec        = ':find' (find-rel | find-coll | find-tuple | find-scalar)
/// find-rel         = find-elem+
/// find-coll        = [ find-elem '...' ]
/// find-scalar      = find-elem '.'
/// find-tuple       = [ find-elem+ ]
/// find-elem        = (variable | pull-expr | aggregate | custom-aggregate)
/// pull-expr        = [ 'pull' src-var? variable pull-pattern ]
/// pull-pattern     = (constant | variable)
/// aggregate        = [ aggregate-fn fn-arg+ ]
/// aggregate-fn     = plain-symbol
/// custom-aggregate = [ 'aggregate' variable fn-arg+ ]

type findVars interface {
	// should return a variable?
	findVars() []edn.Symbol
}

func (v variable) findVars() []edn.Symbol {
	return []edn.Symbol{edn.Symbol(v)}
}

// TODO Aggregate

// TODO Pull

type findElements interface {
	findElements() []findVars
}

// TODO FindRel

// TODO FindColl

// TODO FindScalar

// TODO FindTuple

func findVariables(find findElements) []edn.Symbol {
	vars := make([]edn.Symbol, 0)
	for _, el := range find.findElements() {
		vars = append(vars, el.findVars()...)
	}
	return vars
}

// TODO aggregate?

// TODO pull?

// TODO parse-aggregate

// TODO parse-aggregate-custom

// TODO parse-pull-expr

func parseFindElem(form interface{}) (interface{}, error) {
	// TODO allow aggregates and pull exprs
	val, err := parseAnyOf(form, parseVariable)
	if val == nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return val.(findVars), nil
}

type findRel struct {
	elems []findVars
}

func parseFindRel(form interface{}) (interface{}, error) {
	elems, err := parseSeq(parseFindElem, form)
	if elems == nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	fvs := make([]findVars, len(elems.([]interface{})))
	for i, elem := range elems.([]interface{}) {
		fvs[i] = elem.(findVars)
	}
	return findRel{elems: fvs}, nil
}

type findColl struct {
	elem findVars
}

func parseFindColl(form interface{}) (interface{}, error) {
	forms, ok := form.([]interface{})
	if !ok || len(forms) != 1 {
		return nil, nil
	}

	inner, ok := forms[0].([]interface{})
	sym := edn.Symbol{Name: "..."}
	if !ok || len(forms) != 2 || forms[1] != sym {
		return nil, nil
	}

	elem, err := parseFindElem(inner[0])
	if elem == nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return findColl{elem: elem.(findVars)}, nil
}

type findScalar struct {
	elem findVars
}

func parseFindScalar(form interface{}) (interface{}, error) {
	forms, ok := form.([]interface{})
	sym := edn.Symbol{Name: "."}
	if !ok || len(forms) != 2 || forms[1] != sym {
		return nil, nil
	}

	elem, err := parseFindElem(forms[0])
	if elem == nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return findScalar{elem: elem.(findVars)}, nil
}

type findTuple struct {
	elems []findVars
}

func parseFindTuple(form interface{}) (interface{}, error) {
	forms, ok := form.([]interface{})
	if !ok || len(forms) != 1 {
		return nil, nil
	}

	rel, err := parseFindRel(form)
	if rel == nil || err != nil {
		return nil, err
	}

	return findTuple{elems: rel.(findRel).elems}, nil
}

func parseFind(form interface{}) (interface{}, error) {
	val, err := parseAnyOf(form, parseFindRel, parseFindColl, parseFindScalar, parseFindTuple)
	if val == nil {
		return nil, fmt.Errorf("expected (find-rel | find-coll | find-tuple | find-scalar)")
	}

	return val, err
}

/// with = [ variable+ ]

func parseWith(form interface{}) (interface{}, error) {
	val, _ := parseSeq(parseVariable, form)
	if val == nil {
		return nil, fmt.Errorf("expected [ variable+ ]")
	}

	vars := make([]variable, len(val.([]interface{})))
	for i, val := range val.([]interface{}) {
		vars[i] = val.(variable)
	}

	return vars, nil
}

/// in = [ (src-var | rules-var | binding)+ ]

func parseInBinding(form interface{}) (interface{}, error) {
	return parseAnyOf(form, parseSrcVar, parseRulesVar, parseBinding)
}

func parseIn(form []interface{}) (interface{}, error) {
	val, err := parseSeq(parseInBinding, form)
	if val == nil {
		return nil, fmt.Errorf("expected [ (src-var | rules-var | binding)+ ]")
	}

	return val, err
}

/// clause          = (data-pattern | pred-expr | fn-expr | rule-expr | not-clause | not-join-clause | or-clause | or-join-clause)
/// data-pattern    = [ src-var? (variable | constant | '_')+ ]
/// pred-expr       = [ [ pred fn-arg+ ] ]
/// pred            = (plain-symbol | variable)
/// fn-expr         = [ [ fn fn-arg+ ] binding ]
/// fn              = (plain-symbol | variable)
/// rule-expr       = [ src-var? rule-name (variable | constant | '_')+ ]
/// not-clause      = [ src-var? 'not' clause+ ]
/// not-join-clause = [ src-var? 'not-join' [ variable+ ] clause+ ]
/// or-clause       = [ src-var? 'or' (clause | and-clause)+ ]
/// or-join-clause  = [ src-var? 'or-join' rule-vars (clause | and-clause)+ ]
/// and-clause      = [ 'and' clause+ ]

func parsePatternEl(form interface{}) (interface{}, error) {
	return parseAnyOf(form, parsePlaceholder, parseVariable, parseConstant)
}

var defaultSrc = srcVar{name: edn.Symbol{Name: "$"}}

func takeSource(form interface{}) (*srcVar, interface{}) {
	forms, ok := form.([]interface{})
	if !ok {
		return nil, nil
	}

	source, _ := parseSrcVar(forms[0])
	if source == nil {
		return &defaultSrc, forms
	} else {
		src := source.(srcVar)
		return &src, forms[1:]
	}
}

type pattern_ struct {
	source  srcVar
	pattern []interface{} // placeholder, variable or constant
}

func parsePattern(form interface{}) (interface{}, error) {
	source, nextForm := takeSource(form)
	if source == nil {
		return nil, nil
	}

	patternRaw, _ := parseSeq(parsePatternEl, nextForm)
	if patternRaw == nil {
		return nil, nil
	}

	if len(patternRaw.([]interface{})) == 0 {
		return nil, fmt.Errorf("empty pattern")
	}

	return pattern_{source: *source, pattern: patternRaw.([]interface{})}, nil
}

func parseCall(form interface{}) (interface{}, interface{}) {
	forms, ok := form.([]interface{})
	if !ok || len(forms) == 0 {
		return nil, nil
	}

	fn, _ := parseAnyOf(form, parsePlainSymbol, parseVariable)
	args, _ := parseSeq(parseFnArg, forms[1:])
	return fn, args
}

type predicate struct {
	name interface{}   // plainSymbol or variable
	args []interface{} // variables, constants or src-vars
}

func parsePredicate(form interface{}) (interface{}, error) {
	forms, ok := form.([]interface{})
	if !ok || len(forms) != 1 {
		return nil, nil
	}

	name, args := parseCall(forms[0])
	if name == nil || args == nil {
		return nil, nil
	}

	return predicate{name: name, args: args.([]interface{})}, nil
}

type function struct {
	name    interface{}
	args    []interface{}
	binding binding
}

func parseFunction(form interface{}) (interface{}, error) {
	return nil, nil
}
