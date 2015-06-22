package main

import (
	"fmt"
	"log"
	"os"

	"github.com/heyLu/mu"
	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	"github.com/heyLu/mu/transactor"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <url>\n", os.Args[0])
		os.Exit(1)
	}

	conn, err := mu.Connect(os.Args[1])
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
		dbIdent := mu.Keyword("db", "ident")
		fmt.Printf("%#v -> %d\n", dbIdent, db.Entid(dbIdent))
		fmt.Printf("%d -> %#v\n", 10, db.Ident(10))

		dbIdentEntity := db.Entity(10)
		dbCardinality := mu.Keyword("db", "cardinality")
		fmt.Printf("(:db/cardinality (entity db %d)) ;=> %#v\n", 10, dbIdentEntity.Get(dbCardinality))

	case "create-database":
		isNew, err := mu.CreateDatabase(os.Args[1])
		if err != nil {
			log.Fatal("create-database: ", err)
		}

		if isNew {
			fmt.Println("created new database")
		} else {
			fmt.Println("database already exists")
		}

	case "info":
		fmt.Println("basisT:", db.BasisT())
		fmt.Println("nextT:", db.NextT())

	case "datoms":
		if len(os.Args) < 4 {
			log.Fatal("missing datoms pattern")
		}

		iter, err := mu.DatomsString(db, os.Args[3])
		if err != nil {
			log.Fatal(err)
		}

		for datom := iter.Next(); datom != nil; datom = iter.Next() {
			fmt.Println(datom)
		}

	case "entity":
		if len(os.Args) < 4 {
			log.Fatal("missing entity id")
		}

		lookup, err := transactor.HasLookupFromEDN(os.Args[3])
		if err != nil {
			log.Fatal(err)
		}

		eid := db.Entid(lookup)
		if eid == -1 {
			log.Fatal("no such entity")
		}

		entity := db.Entity(eid)
		for _, k := range entity.Keys() {
			fmt.Printf("%-20v%v\n", k, entity.Get(k))
		}

	case "transact":
		if len(os.Args) < 4 {
			log.Fatal("Missing tx data")
		}

		for _, arg := range os.Args[3:] {
			txRes, err := mu.TransactString(conn, arg)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("datoms from tx:")
			for _, datom := range txRes.Datoms {
				fmt.Println(datom)
			}
		}

	case "log":
		for _, tx := range conn.Log().Tail {
			fmt.Println(tx.T)
			for _, datom := range tx.Datoms {
				fmt.Println(" ", datom)
			}
			fmt.Println()
		}

	case "transact-to":
		rawUrl := "memory://test"
		if len(os.Args) >= 4 {
			rawUrl = os.Args[3]
		}

		toConn, err := mu.Connect(rawUrl)
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
