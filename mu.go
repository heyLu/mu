package main

import (
	"fmt"
	"github.com/heyLu/fressian"
	"log"
	"net/url"
	"os"

	"./connection"
	_ "./connection/backup"
	_ "./connection/file"
	_ "./connection/memory"
	"./database"
	"./index"
)

func Connect(u *url.URL) (connection.Connection, error) {
	return connection.New(u)
}

func Transact(conn connection.Connection, origDatoms []index.Datom) error {
	db, err := conn.Db()
	if err != nil {
		return err
	}

	datoms := make([]index.Datom, 0, len(origDatoms))
	for _, datom := range origDatoms {
		// if db already contains a value for the attribute, retract it before adding the new value.
		// (assumes the attribute has cardinality one.)
		if prev, ok := previousValue(db, datom); ok {
			//fmt.Println("retracting", prev)
			datoms = append(datoms, prev.Retraction())
		}
		//fmt.Println("adding", datom)
		datoms = append(datoms, datom)
	}

	return conn.TransactDatoms(datoms)
}

func previousValue(db *database.Database, datom index.Datom) (*index.Datom, bool) {
	iter := db.Eavt().DatomsAt(index.NewDatom(datom.E(), datom.A(), "", -1, true), index.MaxDatom)
	prev := iter.Next()
	if prev == nil {
		return nil, false
	} else {
		return prev, true
	}
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

		toConn, err := Connect(toUrl)
		if err != nil {
			log.Fatal(err)
		}

		allDatoms := make([]index.Datom, 0, 1000)
		datoms := db.Eavt().Datoms()
		for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
			allDatoms = append(allDatoms, *datom)
		}

		err = toConn.TransactDatoms(allDatoms)
		if err != nil {
			log.Fatal(err)
		}
		toDb, _ := toConn.Db()
		datoms = toDb.Eavt().Datoms()
		for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
			fmt.Println(datom)
		}

	case "test-transact":
		fmt.Println("transact(conn, [[0 1 \"Jane\" 0 true]])")
		nameIsJane := index.NewDatom(0, 1, "Jane", 0, true)
		err := Transact(conn, []index.Datom{nameIsJane})
		if err != nil {
			log.Fatal(err)
		}
		newDb, _ := conn.Db()
		printDatoms(newDb.Eavt().Datoms())

		fmt.Println("transact(conn, [[0 1 \"Jane Lane\" 0 true]])")
		nameIsJane = index.NewDatom(0, 1, "Jane Lane", 0, true)
		err = Transact(conn, []index.Datom{nameIsJane})
		if err != nil {
			log.Fatal(err)
		}
		newDb, _ = conn.Db()
		printDatoms(newDb.Eavt().Datoms())

		fmt.Println("transact(conn, [[0 1 \"Jane Lane\" 0 false]])")
		err = Transact(conn, []index.Datom{nameIsJane.Retraction()})
		if err != nil {
			log.Fatal(err)
		}
		newDb, _ = conn.Db()
		printDatoms(newDb.Eavt().Datoms())

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

func printDatoms(iter index.Iterator) {
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		fmt.Println(datom)
	}
}
