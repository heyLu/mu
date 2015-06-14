package index

import (
	"bytes"
	"compress/gzip"
	"github.com/heyLu/fressian"
	"log"

	"github.com/heyLu/mu/store"
)

type Cache interface {
	Get(id string) (interface{}, bool)
	Put(id string, val interface{})
}

type memoryCache struct {
	cache map[string]interface{}
}

func (c *memoryCache) Get(id string) (interface{}, bool) {
	val, ok := c.cache[id]
	return val, ok
}

func (c *memoryCache) Put(id string, val interface{}) {
	c.cache[id] = val
}

// FIXME: use "github.com/golang/groupcache/lru instead
var cache Cache = &memoryCache{make(map[string]interface{}, 100)}

func GetFromCache(store store.Store, id string) interface{} {
	if val, ok := cache.Get(id); ok {
		return val
	}

	//log.Println("[cache] get:", id)
	data, err := store.Get(id)
	if err != nil {
		log.Fatal("[cache] store.Get: ", err)
		return nil
	}

	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		log.Fatal("[cache] gzip.NewReader: ", err)
		return nil
	}

	r := fressian.NewReader(gz, SegmentReadHandlers)
	val, err := r.ReadValue()
	if err != nil {
		log.Fatal("[cache] r.ReadValue: ", err)
		return nil
	}

	cache.Put(id, val)
	return val
}

// TODO: make all of these public?
func GetRoot(store store.Store, id string) Root           { return GetFromCache(store, id).(Root) }
func getDirectory(store store.Store, id string) Directory { return GetFromCache(store, id).(Directory) }
func getSegment(store store.Store, id string) TransposedData {
	return GetFromCache(store, id).(TransposedData)
}
