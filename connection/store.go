package connection

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"github.com/heyLu/fressian"
	"net/url"

	"../database"
	"../index"
	log "../log"
	"../store"
	_ "../store/file"
)

type storeConnection struct {
	store  store.Store
	rootId string
	db     *database.Database
	log    *log.Log
}

func (c *storeConnection) Db() *database.Database { return c.db }

func (c *storeConnection) Log() *log.Log { return c.log }

func (c *storeConnection) TransactDatoms(datoms []index.Datom) error {
	return fmt.Errorf("storeConnection#TransactDatoms: not implemented")
}

func connectToStore(u *url.URL) (Connection, error) {
	// get store from url scheme
	store, err := store.Get(u)
	if err != nil {
		return nil, err
	}

	dbName := u.Query().Get("name")
	if dbName == "" {
		return nil, fmt.Errorf("must specify a ?name=<name> parameter")
	}
	rootId := DbNameToId(dbName)

	root := index.GetFromCache(store, rootId).(map[interface{}]interface{})
	indexRootId := root[fressian.Keyword{"index", "root-id"}].(string)
	//logRootId := root[fressian.Keyword{"log", "root-id"}].(string)
	//logTail := root[fressian.Keyword{"log", "tail"}].(string)

	db, log := CurrentDb(store, indexRootId, "", []byte{})

	conn := &storeConnection{
		store:  store,
		rootId: rootId,
		db:     db,
		log:    log,
	}

	return conn, nil
}

func DbNameToId(dbName string) string {
	sum := md5.Sum([]byte(dbName))
	return fressian.NewUUIDFromBytes(sum[:]).String()
}

func CurrentDb(store store.Store, indexRootId, logRootId string, logTail []byte) (*database.Database, *log.Log) {
	indexRoot := index.GetFromCache(store, indexRootId).(map[interface{}]interface{})

	// get index roots from store
	// create segment indexes
	eavt := getIndex(indexRoot, "eavt-main", store, index.CompareEavtIndex)
	aevt := getIndex(indexRoot, "aevt-main", store, index.CompareAevtIndex)
	avet := getIndex(indexRoot, "avet-main", store, index.CompareAvetIndex)
	vaet := getIndex(indexRoot, "raet-main", store, index.CompareVaetIndex)

	// get log from store
	// create in-memory indexes
	// create merged indexes
	l := log.FromStore(store, logRootId, logTail)
	if len(l.Tail) > 0 {
		memoryEavt := index.NewMemoryIndex(index.CompareEavt)
		memoryAevt := index.NewMemoryIndex(index.CompareAevt)
		memoryAvet := index.NewMemoryIndex(index.CompareAvet)
		memoryVaet := index.NewMemoryIndex(index.CompareVaet)

		makeDb := func() *database.Database {
			return database.New(
				index.NewMergedIndex(memoryEavt, eavt, index.CompareEavt),
				index.NewMergedIndex(memoryAevt, aevt, index.CompareAevt),
				index.NewMergedIndex(memoryAvet, avet, index.CompareAvet),
				index.NewMergedIndex(memoryVaet, vaet, index.CompareVaet))
		}
		db := makeDb()

		for _, tx := range l.Tail {
			fmt.Printf("adding %d datoms from tx %d\n", len(tx.Datoms), tx.T)
			/*for _, datom := range tx.Datoms {
				fmt.Println(datom)
			}*/
			memoryEavt = memoryEavt.AddDatoms(tx.Datoms)
			memoryAevt = memoryAevt.AddDatoms(tx.Datoms)
			avetDatoms, vaetDatoms := FilterAvetAndVaet(db, tx.Datoms)
			memoryAvet = memoryAvet.AddDatoms(avetDatoms)
			memoryVaet = memoryVaet.AddDatoms(vaetDatoms)
			db = makeDb()
		}
		return db, l
	} else {
		return database.New(eavt, aevt, avet, vaet), l
	}
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
