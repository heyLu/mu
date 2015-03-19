package main

import (
	"fmt"
	"github.com/heyLu/fressian"
	"log"
	"net/url"
	"os"

	"./connection"
)

func Connect(u *url.URL) (*connection.Connection, error) {
	return connection.New(u)
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

	datoms := db.Eavt().Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		fmt.Println(datom)
	}

	dbIdent := fressian.Key{"db", "ident"}
	fmt.Printf("%#v -> %d\n", dbIdent, db.Entid(dbIdent))
	fmt.Printf("%d -> %#v\n", 10, db.Ident(10))

	dbIdentEntity := db.Entity(10)
	dbCardinality := fressian.Key{"db", "cardinality"}
	fmt.Printf("(:db/cardinality (entity db %d)) ;=> %#v\n", 10, dbIdentEntity.Get(dbCardinality))
}
