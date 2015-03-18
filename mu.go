package main

import (
	"fmt"
	"github.com/heyLu/fressian"
	"log"
	"net/url"
	"os"

	"./index"
	"./storage"
)

type Connection struct {
	store *storage.Store
}

func Connect(u *url.URL) (*Connection, error) {
	store, err := storage.Open(u)
	if err != nil {
		return nil, err
	}

	return &Connection{store}, nil
}

type Database struct {
	eavt index.Index
	aevt index.Index
	avet index.Index
	vaet index.Index
}

func (c *Connection) Db() (*Database, error) {
	indexRootRaw, err := storage.Get(c.store, c.store.IndexRootId(), nil)
	if err != nil {
		return nil, err
	}
	indexRoot := indexRootRaw.(map[interface{}]interface{})
	eavtId := indexRoot[fressian.Key{"", "eavt-main"}].(string)
	eavt, err := index.New(c.store, eavtId)
	if err != nil {
		return nil, err
	}
	aevtId := indexRoot[fressian.Key{"", "aevt-main"}].(string)
	aevt, err := index.New(c.store, aevtId)
	if err != nil {
		return nil, err
	}
	avetId := indexRoot[fressian.Key{"", "avet-main"}].(string)
	avet, err := index.New(c.store, avetId)
	if err != nil {
		return nil, err
	}
	vaetId := indexRoot[fressian.Key{"", "raet-main"}].(string)
	vaet, err := index.New(c.store, vaetId)
	if err != nil {
		return nil, err
	}
	return &Database{eavt, aevt, avet, vaet}, nil
}

func main() {
	u, err := url.Parse(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	conn, err := Connect(u)
	if err != nil {
		log.Fatal(err)
	}

	db, err := conn.Db()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(db)

	for _, datom := range db.eavt.Datoms() {
		fmt.Println(datom)
	}
}
