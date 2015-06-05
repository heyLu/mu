package main

import (
	"fmt"
)

type Datom struct{}
type IndexRoot struct{}
type Log struct{}
type Db struct{}

// The in-memory index is supposed to merge the log with the data
// in the persisted index.  The Datomic docs state that it's
// "dictated by the transactor" and that it holds data that is "not
// yet committed to a disk index".
//
// Is it possible to realize this index such that it only results
// in "additional" segments, and does not interfere with queries by
// having to merge it with the persisted segments?
//
// More stuff from the docs:
//
//   - built from the log
//   - updated on transactions
//   - parts are dropped when indexing is done
//
// - new datoms are not a problem (just new segments which contain the
//     new datoms), however, they still need to be "in the right order",
//     so that the queries can find them.  so the in-memory index will
//     likely "rewrite" segments so that the new datoms are found.
// - it will likely contain "new roots", because otherwise the new
//     datoms wouldn't be found either.
// - a naive implementation could be to *always* look into the in-memory
//     index first, before doing anything else
// - it could also "shadow" existing segments, so that new datoms that
//     should be in them are present.  (it doesn't need to do that,
//     however, because segments are immutable.)
//
// Is that a plan?
//
//   - accumulate new datoms from the log
//   - build a new index root and supporting segments with them
//   - check the memory index first, or at least support looking for
//       dbs/datoms in it
//
// Not yet.
//
// Let's think about just creating an index for the latest db,
// nothing else:
//
//   - again, accumulate new datoms from the log (i.e. handle
//       retractions, but this may mean that we need to create
//       segments right away)
//   - for newly created entities, simply write them to separate
//       segments, and update the index root and directories
//   - for updated entities, it's a little bit trickier.  we
//       could "splice" them into separate segments as well,
//       but it's probably easier to simply put them into
//       existing segments, creating new ones and updating the
//       references in the root and directories
//
// That... doesn't seem very smart.  In fact, it sounds as if
// the memory index has to do everything the real indexer has
// to do later anyway.  That could be it, but it seems rather
// strange, and one would hope that there is a more efficient
// way to do it.
//
// There's this type in datomic, `->MergeIter`, which might be
// just the thing we're looking for:  If we store all datoms in
// the memory index in a btset (or multiple, on per index) and
// then *merge* the iterators from the segments and the memory
// index, then don't have to update any segments, because
// merging is possible.
// Retractions are still something to think about, but this is
// a good start.
type InMemoryIndex struct{}

type SegmentedIndex interface {
	// "in which segment should a datom be?" (if it exists)
	WhereIs(datom Datom) (segmentId string)

	// "construct a new index from added/retracted datoms"
	//
	// - constructs a new segmented index
	// - shares already existing parts
	// - creates segments
	// - specifies "where" new segments go (i.e. changes root and directories
	//     to point to/include them)
	Index(newDatoms []Datom) (newRoot IndexRoot)

	// "create a database from the log and the persisted index"
	//
	// - should probably only create a new IndexRoot, with some in-memory segments?
	// - only needs to provide access to the most recent db
	MergeLog(log Log) (database Db)
}

func main() {
	fmt.Println(".")
}
