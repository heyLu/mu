/*

Taking notes.

notes new <title> < note.txt
  create a new note called `title` with the content from `note.txt`

notes edit <id or title> < note.txt
  change the notes content

*/
package main

import (
	"flag"
	"fmt"
	"github.com/heyLu/fressian"
	"log"
	"net/url"
	"os"

	mu "../.."
)

var dbUrl = flag.String("db", "file://notes.db", "The database to store the notes in")

func main() {
	flag.Parse()

	if flag.NArg() != 2 {
		printUsage()
	}

	u, err := url.Parse(*dbUrl)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := mu.Connect(u)
	if err != nil {
		log.Fatal(err)
	}
	//defer conn.Disconnect()

	db, err := conn.Db()
	if err != nil {
		log.Fatal(err)
	}

	cmd := flag.Arg(0)
	title := flag.Arg(1)
	fmt.Println(cmd, title)

	switch cmd {
	case "new":
		// create a new note
		nameAttr := db.Entid(fressian.Keyword{"", "name"})
		if nameAttr == -1 {
			log.Fatal(":name attribute not present")
		}
		err = mu.Transact(conn, mu.Datoms(mu.Datum(-1, nameAttr, title)))
		if err != nil {
			log.Fatal(err)
		}
	case "edit":
		// find note id, edit it
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Printf("Usage: %s <cmd> <title>\n", os.Args[0])
	fmt.Println("  (Where <cmd> is one of `new` or `edit`.)")
	os.Exit(1)
}
