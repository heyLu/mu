package connection

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"github.com/heyLu/fressian"
	"net/url"
	"sync"

	"../database"
	"../index"
	log "../log"
	"../store"
	_ "../store/file"
	"../transactor"
)

type storeConnection struct {
	store  store.Store
	rootId string
	db     *database.Db
	log    *log.Log

	// Used to protect against dirty reads of db and log.
	lock *sync.RWMutex
	// Used to ensure that transaction are serialized.
	txLock *sync.Mutex
}

func (c *storeConnection) Db() *database.Db {
	c.lock.RLock()
	db := c.db
	c.lock.RUnlock()
	return db
}

func (c *storeConnection) Log() *log.Log {
	c.lock.RLock()
	log := c.log
	c.lock.RUnlock()
	return log
}

func (c *storeConnection) Index(datoms []index.Datom) error {
	// TODO: implement this
	return fmt.Errorf("storeConnection#TransactDatoms: not implemented")
}

func (c *storeConnection) Transact(datoms []index.Datom) (*transactor.TxResult, error) {
	c.txLock.Lock()
	defer c.txLock.Unlock()
	newLog, txResult, err := transactor.Transact(c.db, c.log, datoms)
	if err != nil {
		return nil, err
	}
	// TODO: write new root with datoms/LogTx to store
	c.lock.Lock()
	c.db = txResult.DbAfter
	c.log = newLog
	c.lock.Unlock()
	return txResult, nil
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

	// TODO: read log root and log tail from the segment (and don't cache it)
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

func CurrentDb(store store.Store, indexRootId, logRootId string, logTail []byte) (*database.Db, *log.Log) {
	indexRoot := index.GetFromCache(store, indexRootId).(map[interface{}]interface{})

	// get index roots from store
	// create segment indexes
	eavt := getIndex(indexRoot, "eavt-main", store, index.CompareEavtIndex)
	aevt := getIndex(indexRoot, "aevt-main", store, index.CompareAevtIndex)
	avet := getIndex(indexRoot, "avet-main", store, index.CompareAvetIndex)
	vaet := getIndex(indexRoot, "raet-main", store, index.CompareVaetIndex)

	memoryEavt := index.NewMemoryIndex(index.CompareEavt)
	memoryAevt := index.NewMemoryIndex(index.CompareAevt)
	memoryAvet := index.NewMemoryIndex(index.CompareAvet)
	memoryVaet := index.NewMemoryIndex(index.CompareVaet)

	db := database.New(
		index.NewMergedIndex(memoryEavt, eavt, index.CompareEavt),
		index.NewMergedIndex(memoryAevt, aevt, index.CompareAevt),
		index.NewMergedIndex(memoryAvet, avet, index.CompareAvet),
		index.NewMergedIndex(memoryVaet, vaet, index.CompareVaet))

	// get log from store
	// create in-memory indexes
	// create merged indexes
	l := log.FromStore(store, logRootId, logTail)
	if len(l.Tail) > 0 {
		for _, tx := range l.Tail {
			fmt.Printf("adding %d datoms from tx %d\n", len(tx.Datoms), tx.T)
			/*for _, datom := range tx.Datoms {
				fmt.Println(datom)
			}*/
			db = db.WithDatoms(tx.Datoms)
		}
	}

	return db, l
}

func getIndex(root map[interface{}]interface{}, id string, store store.Store, compare index.CompareFn) *index.SegmentedIndex {
	indexRootId := root[fressian.Keyword{"", id}].(fressian.UUID).String()
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
