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
	"golang.org/x/crypto/ssh/terminal"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"

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

	switch cmd {
	case "init":
		if db.Entid(mu.Keyword("", "name")) != -1 {
			fmt.Println("already initialized")
			os.Exit(1)
		}

		nameId := mu.Tempid(mu.DbPartDb, -1)
		contentId := mu.Tempid(mu.DbPartDb, -2)
		err = mu.Transact(conn,
			mu.Datoms(
				// :name attribute (type string, cardinality one)
				mu.Datum(nameId, mu.DbIdent, mu.Keyword("", "name")),
				mu.Datum(nameId, mu.DbType, mu.DbTypeString),
				mu.Datum(nameId, mu.DbCardinality, mu.DbCardinalityOne),
				// :content attribute (type string, cardinality one)
				mu.Datum(contentId, mu.DbIdent, mu.Keyword("", "content")),
				mu.Datum(contentId, mu.DbType, mu.DbTypeString),
				mu.Datum(contentId, mu.DbCardinality, mu.DbCardinalityOne),
			))
		if err != nil {
			log.Fatal("could not initialize database: ", err)
		}
	case "new":
		nameAttr := db.Entid(mu.Keyword("", "name"))
		contentAttr := db.Entid(mu.Keyword("", "content"))
		if nameAttr == -1 || contentAttr == -1 {
			log.Fatalf("db not initialized, run `%s init _` first")
		}

		content, err := getContent()
		if err != nil {
			log.Fatal(err)
		}

		err = mu.Transact(conn,
			mu.Datoms(
				mu.Datum(-1, nameAttr, title),
				mu.Datum(-1, contentAttr, content),
			))
		if err != nil {
			log.Fatal(err)
		}
	case "edit":
		// find note id, edit it
	default:
		printUsage()
	}
}

func getContent() (string, error) {
	if terminal.IsTerminal(int(os.Stdin.Fd())) {
		f, err := ioutil.TempFile("", "note-")
		if err != nil {
			return "", err
		}
		defer os.Remove(f.Name())

		editor := getEnv("EDITOR", "vi")
		cmd := exec.Command(editor, f.Name())
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		err = cmd.Run()
		if err != nil {
			return "", err
		}

		content, err := ioutil.ReadAll(f)
		return string(content), nil
	} else {
		content, err := ioutil.ReadAll(os.Stdin)
		return string(content), err
	}
}

func getEnv(key, defaultValue string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	} else {
		return val
	}
}

func printUsage() {
	fmt.Printf("Usage: %s <cmd> <title>\n", os.Args[0])
	fmt.Println("  (Where <cmd> is one of `new` or `edit`.)")
	os.Exit(1)
}
