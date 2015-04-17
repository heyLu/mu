package main

import (
	"fmt"
	"github.com/heyLu/fressian"
	"log"
	"net/url"
	"os"
	"strconv"

	"./connection"
	_ "./connection/backup"
	_ "./connection/memory"
	"./database"
	"./index"
)

func Connect(u *url.URL) (connection.Connection, error) {
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
	if len(os.Args) >= 3 {
		cmd = os.Args[2]
	}

	switch cmd {
	case "eavt", "aevt", "avet", "vaet":
		datoms := getIndex(db, cmd).Datoms()
		for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
			fmt.Println(datom)
		}

	case "seek":
		indexName := "eavt"
		component := 0
		if len(os.Args) >= 4 {
			indexName = os.Args[3]
			if len(os.Args) >= 5 {
				component, _ = strconv.Atoi(os.Args[4])
			}
		}
		datoms := getIndex(db, indexName).SeekDatoms(component)
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

	case "transact-to":
		rawUrl := "memory://test"
		if len(os.Args) >= 4 {
			rawUrl = os.Args[3]
		}
		toUrl, err := url.Parse(rawUrl)
		if err != nil {
			log.Fatal(err)
		}

		toConn, err := Connect(toUrl)
		if err != nil {
			log.Fatal(err)
		}

		allDatoms := make([]index.Datom, 0, 1000)
		datoms := db.Eavt().Datoms()
		for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
			allDatoms = append(allDatoms, *datom)
		}

		toConn.TransactDatoms(allDatoms)
		toDb, _ := toConn.Db()
		datoms = toDb.Eavt().Datoms()
		for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
			fmt.Println(datom)
		}

	default:
		fmt.Println("unknown command:", cmd)
		os.Exit(1)
	}
}

func getIndex(db *database.Database, indexName string) index.Index {
	switch index.Type(indexName) {
	case index.Eavt:
		return db.Eavt()
	case index.Aevt:
		return db.Aevt()
	case index.Avet:
		return db.Avet()
	case index.Vaet:
		return db.Vaet()
	default:
		log.Fatal("unknown index:", indexName)
		return db.Eavt()
	}
}
