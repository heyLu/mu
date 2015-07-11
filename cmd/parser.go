package main

import (
	"github.com/heyLu/edn"
)

// find-spec                  = ':find' (find-rel | find-coll | find-tuple | find-scalar)
// find-rel                   = find-elem+
// find-coll                  = [find-elem '...']
// find-scalar                = find-elem '.'
// find-tuple                 = [find-elem+]
// find-elem                  = (variable | pull-expr | aggregate)
type findSpec interface{}

type findRel struct {
	elems []findElem
}

type findColl struct {
	elem findElem
}

type findScalar struct {
	elem findElem
}

type findTuple struct {
	elems []findElem
}

type findElem interface{}

// pull-expr                  = ['pull' variable pattern]
// pattern                    = (input-name | pattern-data-literal)
// aggregate                  = [aggregate-fn-name fn-arg+]

type pullExpr struct {
	variable variable
	pattern  pullPattern
}

type pullPattern interface{}

type aggregate struct {
	name edn.Symbol
	args []fnArg
}

// fn-arg                     = (variable | constant | src-var)

type fnArg interface{}

// with-clause                = ':with' variable+
// where-clauses              = ':where' clause+

// inputs                     = ':in' (src-var | variable | pattern-var | rules-var)+

type input interface{}

// src-var                    = symbol starting with "$"
// variable                   = symbol starting with "?"
// rules-var                  = the symbol "%"
// plain-symbol               = symbol that does not begin with "$" or "?"

type srcVar edn.Symbol
type variable edn.Symbol
type plainSymbol edn.Symbol

var rulesVar = edn.Symbol{Namespace: "", Name: "%"}

// pattern-var                = plain-symbol
type patternVar plainSymbol

// and-clause                 = [ 'and' clause+ ]
// expression-clause          = (data-pattern | pred-expr | fn-expr | rule-expr)
// rule-expr                  = [ src-var? rule-name (variable | constant | '_')+]
// not-clause                 = [ src-var? 'not' clause+ ]
// not-join-clause            = [ src-var? 'not-join' [variable+] clause+ ]
// or-clause                  = [ src-var? 'or' (clause | and-clause)+]
// or-join-clause             = [ src-var? 'or-join' rule-vars (clause | and-clause)+ ]
// rule-vars                  = [variable+ | ([variable+] variable*)]
// clause                     = (not-clause | not-join-clause | or-clause | or-join-clause | expression-clause)

type andClause struct {
	clauses []clause
}

type expressionClause interface{}

// TODO: rule-expr
// TODO: not-clause
// TODO: not-join-clause
// TODO: or-clause
// TODO: or-join-clause

type clause interface{}

// data-pattern               = [ src-var? (variable | constant | '_')+ ]
// constant                   = any non-variable data literal
// pred-expr                  = [ [pred fn-arg+] ]
// fn-expr                    = [ [fn fn-arg+] binding]

type dataPattern struct {
	source  srcVar
	pattern []patternValue
}

type patternValue interface{}

type predExpr struct {
	pred edn.Symbol
	args []fnArg
}

type fnExpr struct {
	fn      edn.Symbol
	args    []fnArg
	binding binding
}

// binding                    = (bind-scalar | bind-tuple | bind-coll | bind-rel)
// bind-scalar                = variable
// bind-tuple                 = [ (variable | '_')+]
// bind-coll                  = [variable '...']
// bind-rel                   = [ [(variable | '_')+] ]

type binding interface{}

type bindScalar variable

type bindTuple struct {
	bindings []variable
}

type bindColl variable

type bindRel struct {
	bindings []variable
}

func main() {
	println("hi!")
}
