package main

import (
	"fmt"
	"github.com/heyLu/fressian"
	"log"
	"net/url"
	"os"

	"../../database"
	"../../index"

	mu "../.."
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <url>\n", os.Args[0])
		os.Exit(1)
	}

	u, err := url.Parse(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	conn, err := mu.Connect(u)
	if err != nil {
		log.Fatal(err)
	}

	db := conn.Db()
	fmt.Println(db)

	cmd := "eavt"
	if len(os.Args) >= 3 {
		cmd = os.Args[2]
	}

	switch cmd {
	case "eavt", "aevt", "avet", "vaet":
		printDatoms(getIndex(db, cmd).Datoms())

	case "example":
		dbIdent := fressian.Keyword{"db", "ident"}
		fmt.Printf("%#v -> %d\n", dbIdent, db.Entid(dbIdent))
		fmt.Printf("%d -> %#v\n", 10, db.Ident(10))

		dbIdentEntity := db.Entity(10)
		dbCardinality := fressian.Keyword{"db", "cardinality"}
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

		toConn, err := mu.Connect(toUrl)
		if err != nil {
			log.Fatal(err)
		}

		allDatoms := make([]index.Datom, 0, 1000)
		datoms := db.Eavt().Datoms()
		for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
			allDatoms = append(allDatoms, *datom)
		}

		err = toConn.Index(allDatoms)
		if err != nil {
			log.Fatal(err)
		}
		toDb := toConn.Db()
		printDatoms(toDb.Eavt().Datoms())

	case "test-transact":
		fmt.Println("transact(conn, [[0 1 \"Jane\" 0 true]])")
		nameIsJane := mu.RawDatum(0, 1, "Jane")
		_, err := mu.Transact(conn, mu.Datums(nameIsJane))
		if err != nil {
			log.Fatal(err)
		}
		newDb := conn.Db()
		printDatoms(newDb.Eavt().Datoms())

		fmt.Println("transact(conn, [[0 1 \"Jane Lane\" 0 true]])")
		nameIsJane = mu.RawDatum(0, 1, "Jane Lane")
		_, err = mu.Transact(conn, mu.Datums(nameIsJane))
		if err != nil {
			log.Fatal(err)
		}
		newDb = conn.Db()
		printDatoms(newDb.Eavt().Datoms())

		fmt.Println("transact(conn, [[0 1 \"Jane Lane\" 0 false]])")
		_, err = mu.Transact(conn, mu.Datums(nameIsJane.Retraction()))
		if err != nil {
			log.Fatal(err)
		}
		newDb = conn.Db()
		printDatoms(newDb.Eavt().Datoms())

	default:
		fmt.Println("unknown command:", cmd)
		os.Exit(1)
	}
}

func getIndex(db *database.Db, indexName string) index.Index {
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

func printDatoms(iter index.Iterator) {
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		fmt.Println(datom)
	}
}
