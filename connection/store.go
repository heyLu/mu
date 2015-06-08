package connection

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/heyLu/fressian"
	"net/url"

	"../database"
	"../index"
	"../store"
	_ "../store/file"
)

type storeConnection struct {
	store  store.Store
	rootId string
	db     *database.Database
	//log    *Log
}

func (c *storeConnection) Db() (*database.Database, error) { return c.db, nil }

func (c *storeConnection) TransactDatoms(datoms []index.Datom) error {
	return fmt.Errorf("storeConnection#TransactDatoms: not implemented")
}

func connectToStore(u *url.URL) (Connection, error) {
	// get store from url scheme
	store, err := store.Get(u)
	if err != nil {
		return nil, err
	}

	rootId := u.Query().Get("root")
	if rootId == "" {
		return nil, fmt.Errorf("must specify a ?root=<root> parameter")
	}

	// get database root from store (by db name = last path component?)
	root, err := getDbRoot(store, rootId)
	if err != nil {
		return nil, err
	}

	// get index roots from store
	// create segment indexes
	eavt := getIndex(root, "eavt-main", store, index.CompareEavtIndex)
	aevt := getIndex(root, "aevt-main", store, index.CompareAevtIndex)
	avet := getIndex(root, "avet-main", store, index.CompareAvetIndex)
	vaet := getIndex(root, "raet-main", store, index.CompareVaetIndex)

	// get log from store
	// create in-memory indexes
	// create merged indexes

	db := database.New(eavt, aevt, avet, vaet)
	conn := &storeConnection{
		store:  store,
		rootId: rootId,
		db:     db,
		//log:    nil,
	}

	return conn, nil
}

func getIndex(root map[interface{}]interface{}, id string, store store.Store, compare index.CompareFn) *index.SegmentedIndex {
	indexRootId := root[fressian.Keyword{"", id}].(string)
	indexRoot := index.GetRoot(store, indexRootId)
	return index.NewSegmentedIndex(&indexRoot, store, compare)
}

func getDbRoot(store store.Store, id string) (map[interface{}]interface{}, error) {
	data, err := store.Get(id)
	if err != nil {
		return nil, err
	}

	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	r := fressian.NewReader(gz, nil)
	val, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	return val.(map[interface{}]interface{}), nil
}
