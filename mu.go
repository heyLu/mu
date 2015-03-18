package main

import (
	"fmt"
	"log"
	"net/url"
	"os"

	"./connection"
)

func Connect(u *url.URL) (*connection.Connection, error) {
	return connection.New(u)
}

func Db(conn *connection.Connection) (*connection.Database, error) {
	return conn.Db()
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

	for _, datom := range db.Eavt().Datoms() {
		fmt.Println(datom)
	}
}
