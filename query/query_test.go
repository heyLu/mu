package query

import (
	"fmt"
	tu "github.com/klingtnet/gol/util/testing"
	"testing"

	"github.com/heyLu/edn"
)

func TestExamples(t *testing.T) {
	attrs := map[variable]int{
		newVar("name"): 0,
		newVar("age"):  1,
	}

	tuples := []tuple{
		sliceTuple{"Jane", 13},
		sliceTuple{"Alice", 7},
		sliceTuple{"Fred", 3},
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
		sliceTuple{"Jane", "pancakes"},
		sliceTuple{"Alice", "the stars"},
		sliceTuple{"Fred", "Alice"},
		sliceTuple{"Fred", "Little Fred"},
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
			fmt.Printf("%v -> %v\n", attr, tuple.ValueAt(idx))
		}
	}

	fmt.Println()
	fmt.Println("3 years old")
	age3 := pattern{newVar("name"), 3, newVar("likes")}
	threeYearOlds := lookupPatternColl(joined.tuples, age3)
	for _, tuple := range threeYearOlds.tuples {
		for attr, idx := range threeYearOlds.attrs {
			fmt.Printf("%v -> %v\t\t", attr, tuple.ValueAt(idx))
		}
		fmt.Println()
	}

	fmt.Println()
	fmt.Println("13 years old, likes pancakes")
	ctx := context{
		sources: map[variable]source{
			newVar("$"): []tuple{
				sliceTuple{"Jane", 13},
				sliceTuple{"Alice", 7},
				sliceTuple{"Fred", 3},
				sliceTuple{"Jane", "pancakes"},
				sliceTuple{"Alice", "the stars"},
				sliceTuple{"Fred", "Alice"},
				sliceTuple{"Fred", "Little Fred"},
			},
		},
	}
	clauses := []clause{
		patternClause{
			source:  newVar("$"),
			pattern: pattern{newVar("name"), 13},
		},
		patternClause{
			source:  newVar("$"),
			pattern: pattern{newVar("name"), "pancakes"},
		},
	}
	newCtx := runQuery(ctx, clauses)
	vars := []variable{newVar("name")}
	fmt.Println(vars)
	res := collect(newCtx, vars)
	for vals, _ := range res {
		fmt.Println(vals)
	}

	ctx = context{
		sources: map[variable]source{
			newVar("$"): []tuple{
				sliceTuple{"Jane", 13},
				sliceTuple{"Alice", 7},
				sliceTuple{"Fred", 3},
				sliceTuple{"Little Fred", 1},
				sliceTuple{"Judy", 4},
				sliceTuple{"Jane", "Alice"},
				sliceTuple{"Jane", "Fred"},
				sliceTuple{"Little Fred", "Alice"},
			},
		},
	}
	clauses = []clause{
		patternClause{
			source:  newVar("$"),
			pattern: pattern{"Jane", newVar("friend")},
		},
		patternClause{
			source:  newVar("$"),
			pattern: pattern{newVar("friend"), newVar("age")},
		},
	}
	newCtx = runQuery(ctx, clauses)
	vars = []variable{newVar("age"), newVar("friend")}
	fmt.Println(vars)
	res = collect(newCtx, vars)
	for vals, _ := range res {
		fmt.Println(vals)
	}
}

func TestQ(t *testing.T) {
	data := []tuple{
		sliceTuple{"Jane", 13},
		sliceTuple{"Alice", 7},
		sliceTuple{"Fred", 3},
		sliceTuple{"Little Fred", 1},
		sliceTuple{"Judy", 4},
		sliceTuple{"Jane", "Alice"},
		sliceTuple{"Jane", "Fred"},
		sliceTuple{"Little Fred", "Alice"},
	}

	query, err := edn.DecodeString(`
{:find [?age ?friend]
 :where [["Jane" ?friend]
         [?friend ?age]]}
`)
	tu.ExpectNil(t, err)

	res, err := Q(query, data)
	tu.ExpectNil(t, err)

	for res, _ := range res.(map[Indexed]bool) {
		fmt.Println(res)
	}
}
