package connection

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"github.com/heyLu/fressian"
	"net/url"
	"sync"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	"github.com/heyLu/mu/log"
	"github.com/heyLu/mu/store"
	_ "github.com/heyLu/mu/store/file"
	"github.com/heyLu/mu/transactor"
)

type storeConnection struct {
	store       store.Store
	indexRootId string
	db          *database.Db
	log         *log.Log

	// Used to protect against dirty reads of db and log.
	lock sync.RWMutex
	// Used to ensure that transaction are serialized.
	txLock sync.Mutex
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

func (c *storeConnection) Transact(datoms []transactor.TxDatum) (*transactor.TxResult, error) {
	c.txLock.Lock()
	defer c.txLock.Unlock()
	tx, txResult, err := transactor.Transact(c.db, datoms)
	if err != nil {
		return nil, err
	}

	// write new root with datoms/LogTx to store
	newLog := c.log.WithTx(tx)
	dbRoot, err := newDbRoot(c.indexRootId, newLog.RootId, newLog.Tail)
	if err != nil {
		return nil, err
	}
	newIndexRootId := log.Squuid().String()
	err = writeToStore(c.store, nil, newIndexRootId, dbRoot)
	if err != nil {
		return nil, err
	}

	c.lock.Lock()
	c.indexRootId = newIndexRootId
	c.db = txResult.DbAfter
	c.log = newLog
	c.lock.Unlock()
	return txResult, nil
}

func newDbRoot(indexRootId, logRootId string, logTail []log.LogTx) (map[interface{}]interface{}, error) {
	dbRoot := map[interface{}]interface{}{}
	dbRoot[fressian.Keyword{"index", "root-id"}] = indexRootId
	dbRoot[fressian.Keyword{"log", "root-id"}] = logRootId

	buf := new(bytes.Buffer)
	w := fressian.NewWriter(buf, log.WriteHandler)
	err := w.WriteValue(logTail)
	if err != nil {
		return nil, err
	}
	w.Flush()
	dbRoot[fressian.Keyword{"log", "tail"}] = buf.Bytes()

	return dbRoot, nil
}

func writeToStore(store store.Store, handler fressian.WriteHandler, id string, val interface{}) error {
	fmt.Printf("writeToStore: %s -> %v\n", id, val)
	buf := new(bytes.Buffer)
	w := fressian.NewGzipWriter(buf, handler)
	err := w.WriteValue(val)
	if err != nil {
		return err
	}
	w.Flush()
	return store.Put(id, buf.Bytes())
}

func connectToStore(u *url.URL) (Connection, error) {
	// get store from url scheme
	store, err := store.Open(u)
	if err != nil {
		return nil, err
	}

	dbName := u.Query().Get("name")
	if dbName == "" {
		return nil, fmt.Errorf("must specify a ?name=<name> parameter")
	}
	rootId := DbNameToId(dbName)

	// TODO: read log root and log tail from the segment (and don't cache it)
	root, err := getDbRoot(store, rootId)
	if err != nil { // new db (not really, could be anything...)
		fmt.Printf("new db: %s (%v): %s\n", rootId, root, err)
		indexRootId := log.Squuid().String()
		root, err = newDbRoot(indexRootId, "", []log.LogTx{})
		if err != nil {
			return nil, err
		}

		err = writeToStore(store, nil, rootId, root)
		if err != nil {
			return nil, err
		}

		indexRoot := make(map[interface{}]interface{})
		eavtRootId := log.Squuid()
		indexRoot[fressian.Keyword{"", "eavt-main"}] = eavtRootId
		aevtRootId := log.Squuid()
		indexRoot[fressian.Keyword{"", "aevt-main"}] = aevtRootId
		avetRootId := log.Squuid()
		indexRoot[fressian.Keyword{"", "avet-main"}] = avetRootId
		vaetRootId := log.Squuid()
		indexRoot[fressian.Keyword{"", "raet-main"}] = vaetRootId
		indexRoot[fressian.Keyword{"", "nextT"}] = 0

		err = writeToStore(store, nil, indexRootId, indexRoot)
		if err != nil {
			return nil, err
		}

		emptyRoot := new(index.Root)
		for _, rootId := range []fressian.UUID{eavtRootId, aevtRootId, avetRootId, vaetRootId} {
			err = writeToStore(store, index.SegmentWriteHandler, rootId.String(), *emptyRoot)
			if err != nil {
				return nil, err
			}
		}

	}
	indexRootId := root[fressian.Keyword{"index", "root-id"}].(string)
	logRootId := root[fressian.Keyword{"log", "root-id"}].(string)
	logTail := root[fressian.Keyword{"log", "tail"}].([]byte)

	db, log := CurrentDb(store, indexRootId, logRootId, logTail)

	conn := &storeConnection{
		store:       store,
		indexRootId: rootId,
		db:          db,
		log:         log,
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
	fmt.Println("get index", id, indexRootId)
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
	if val == nil && err != nil {
		return nil, err
	}

	return val.(map[interface{}]interface{}), nil
}
