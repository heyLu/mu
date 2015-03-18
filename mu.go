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
	return &Database{eavt}, nil
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
