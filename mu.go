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
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <dir>\n", os.Args[0])
		os.Exit(1)
	}

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

	cmd := "eavt"
	if len(os.Args) == 3 {
		cmd = os.Args[2]
	}

	switch cmd {
	case "eavt":
		datoms := db.Eavt().Datoms()
		for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
			fmt.Println(datom)
		}

	case "example":
		dbIdent := fressian.Key{"db", "ident"}
		fmt.Printf("%#v -> %d\n", dbIdent, db.Entid(dbIdent))
		fmt.Printf("%d -> %#v\n", 10, db.Ident(10))

		dbIdentEntity := db.Entity(10)
		dbCardinality := fressian.Key{"db", "cardinality"}
		fmt.Printf("(:db/cardinality (entity db %d)) ;=> %#v\n", 10, dbIdentEntity.Get(dbCardinality))

	default:
		fmt.Println("unknown command:", cmd)
		os.Exit(1)
	}
}
