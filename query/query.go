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

type Query struct{}

func Q(query Query, inputs ...interface{}) (interface{}, error) {
	return nil, nil
}
