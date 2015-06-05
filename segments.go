package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
)

type Datom struct{}
type IndexRoot struct{}
type Log struct{}

//type Db struct{}

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

type Root struct {
	tData       TransposedData
	directories []string
}

type Directory struct {
	tData    TransposedData
	segments []string
	mystery1 []int
	mystery2 []int
}

type TransposedData struct {
	values       []interface{}
	entities     []int
	attributes   []int
	transactions []int
	addeds       []bool
}

// The (global) object cache
//
// Individual stores check the cache before retrieving an object from
// the cache.  When it is missing they get it from storage and put it
// in the cache, returning *the same value* that is returned when it
// is returned by the cache.
//
// The cache is a LRU cache, which removes old objects.
// (http://godoc.org/github.com/golang/groupcache/lru seems suitable.)
//
// cache.Cache is the global cache instance in the `cache` package.
//
// Code using either the storage *or* the cache should not need to
// cast values retrieved from it.  The cache should probably do the
// casting, that way each store does not have to do it.
var cache Cache

type Cache interface {
	// This seems non-ideal, because the cache shouldn't know too
	// much about the data, and because we'd need to do basically
	// the same action for `Root`s and `Directory`s, just with a
	// different cast.
	//
	// However, where *should* we do it?
	GetTransposedData(store Store, id string) *TransposedData
}

type globalCache struct {
	cache map[string]interface{}
}

func (c *globalCache) GetTransposedData(store Store, id string) *TransposedData {
	if val, ok := c.cache[id]; ok {
		// This... doesn't seem right.  Should we store these in different caches?
		// But that would only make this *one place* nicer, and make restricting
		// the cache size more difficult.
		return val.(*TransposedData)
	}

	log.Printf("[cache] get tdata segment %s from store\n", id)
	_, err := store.Get(id)
	if err != nil {
		log.Fatal("[cache] get from store: ", err)
		return nil
	}

	// FIXME: parse data
	var tData *TransposedData = nil
	c.cache[id] = tData

	return tData
}

type Store interface {
	Get(id string) ([]byte, error)
	Put(id string, data []byte) error
	Delete(id string) error
}

type memoryStore struct {
	store map[string][]byte
}

func (s *memoryStore) Get(id string) ([]byte, error) {
	if data, ok := s.store[id]; ok {
		return data, nil
	}

	return nil, fmt.Errorf("No such object: %s", id)
}

func (s *memoryStore) Put(id string, data []byte) error {
	s.store[id] = data
	return nil
}

func (s *memoryStore) Delete(id string) error {
	delete(s.store, id)
	return nil
}

type fileStore struct {
	path string
}

func (s fileStore) blobPath(id string) string {
	return path.Join(s.path, id[len(id)-2:], id)
}

func (s fileStore) Get(id string) ([]byte, error) {
	return ioutil.ReadFile(s.blobPath(id))
}

func (s fileStore) Put(id string, data []byte) error {
	return ioutil.WriteFile(s.blobPath(id), data, 0644)
}

func (s fileStore) Delete(id string) error {
	return os.Remove(s.blobPath(id))
}

type Db struct {
	eavt Index
	aevt Index
	avet Index
	vaet Index
}

// Could be purely in-memory or segmented, or an index
// that mixes these.  (I.e. when there are new datoms that are
// not yet indexed.
type Index interface {
	DatomsAt(start, end Datom) Iterator
}

type Iterator interface {
	Next() *Datom
}

// DatomsAt returns an iterator for the datoms between start and end
//
// - figures out the segment where the start datom should be
// - when it hits the end of the segment, it either goes on to the next
// - or it goes one level up and down again

// only one type of connections?
type Connection struct{}

func (c Connection) TransactDatoms(datoms []Datom) error {
	// write datoms to log (maybe assign ids here?  or not, not sure...)
	//   this means writing a new root node to storage
	//   do we want to support non-segmented storage?  (single-file is still possible via sqlite, kv dbs or something custom, but fressian-in-a-file would still be useful for debugging)
	// possibly trigger indexing (if enough datoms in log)
	//   (for now, do it immediately if necessary, but log it)
	// update in-memory index to provide recent db
	return nil
}

func main() {
	fmt.Println(".")
}
