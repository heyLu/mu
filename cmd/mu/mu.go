package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/heyLu/mu"
	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	"github.com/heyLu/mu/transactor"
)

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Printf("Usage: %s <url>\n", os.Args[0])
		os.Exit(1)
	}

	conn, err := mu.Connect(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	db := conn.Db()

	cmd := "eavt"
	if flag.NArg() >= 2 {
		cmd = flag.Arg(1)
	}

	switch cmd {
	case "eavt", "aevt", "avet", "vaet":
		printDatoms(getIndex(db, cmd).Datoms())

	case "create-database":
		isNew, err := mu.CreateDatabase(flag.Arg(0))
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
		if flag.NArg() < 3 {
			log.Fatal("missing datoms pattern")
		}

		iter, err := mu.DatomsString(db, flag.Arg(2))
		if err != nil {
			log.Fatal(err)
		}

		for datom := iter.Next(); datom != nil; datom = iter.Next() {
			fmt.Println(datom)
		}

	case "entity":
		if flag.NArg() < 3 {
			log.Fatal("missing entity id")
		}

		lookup, err := transactor.HasLookupFromEDN(flag.Arg(2))
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
		if flag.NArg() < 3 {
			log.Fatal("Missing tx data")
		}

		for _, arg := range flag.Args()[2:] {
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
