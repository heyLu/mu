package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"strconv"

	mu "../.."
	"../../connection"
	"../../database"
)

func main() {
	var dbUrl string
	var db *database.Database
	var conn connection.Connection

	cli := &cobra.Command{
		Use:   os.Args[0],
		Short: " simple note taking application",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			u, err := url.Parse(dbUrl)
			if err != nil {
				log.Fatal(err)
			}

			conn, err = mu.Connect(u)
			if err != nil {
				log.Fatal(err)
			}
			//defer conn.Disconnect()

			db, err = conn.Db()
			if err != nil {
				log.Fatal(err)
			}
		},
	}
	cli.PersistentFlags().StringVar(&dbUrl, "db", "file://notes.db", "the database to connect to")

	initCommand := &cobra.Command{
		Use:   "init",
		Short: "initialize the database",
		Run: func(cmd *cobra.Command, args []string) {
			if db.Entid(mu.Keyword("", "name")) != -1 {
				fmt.Println("already initialized")
				os.Exit(1)
			}

			nameId := mu.Tempid(mu.DbPartDb, -1)
			contentId := mu.Tempid(mu.DbPartDb, -2)
			err := mu.Transact(conn,
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
		},
	}

	newCommand := &cobra.Command{
		Use:   "new <title> [<content>]",
		Short: "create a new note",
		Run: func(cmd *cobra.Command, args []string) {
			requireArgs(cmd, args, 1)

			nameAttr := db.Entid(mu.Keyword("", "name"))
			contentAttr := db.Entid(mu.Keyword("", "content"))
			if nameAttr == -1 || contentAttr == -1 {
				log.Fatalf("db not initialized, run `%s init _` first")
			}

			content := getContentFrom(args, 1, "")
			err := mu.Transact(conn,
				mu.Datoms(
					mu.Datum(-1, nameAttr, args[0]),
					mu.Datum(-1, contentAttr, content),
				))
			if err != nil {
				log.Fatal(err)
			}
		},
	}

	editCommand := &cobra.Command{
		Use:   "edit <id or title> [<content>]",
		Short: "edit a note",
		Run: func(cmd *cobra.Command, args []string) {
			requireArgs(cmd, args, 1)

			contentAttr := db.Entid(mu.Keyword("", "content"))
			if contentAttr == -1 {
				log.Fatalf("db not initialized, run `%s init _` first")
			}

			noteId := findNote(db, args[0])
			note := db.Entity(noteId)
			prevContent := note.Get(mu.Keyword("", "content")).(string)
			content := getContentFrom(args, 1, prevContent)

			if prevContent == content {
				fmt.Println("no changes")
			} else {
				err := mu.Transact(conn, mu.Datoms(mu.Datum(noteId, contentAttr, content)))
				if err != nil {
					log.Fatal(err)
				}
			}
		},
	}

	listCommand := &cobra.Command{
		Use:   "list",
		Short: "list all notes",
		Run: func(cmd *cobra.Command, args []string) {
			nameAttr := db.Entid(mu.Keyword("", "name"))
			if nameAttr == -1 {
				log.Fatalf("db not initialized, run `%s init _` first")
			}

			iter := db.Aevt().DatomsAt(mu.Datum(-1, nameAttr, ""), mu.Datum(10000, nameAttr, ""))
			for datom := iter.Next(); datom != nil; datom = iter.Next() {
				fmt.Printf("%d: %s\n", datom.Entity(), datom.Value().Val())
			}
		},
	}

	showCommand := &cobra.Command{
		Use:   "show <id or title>",
		Short: "display a note",
		Run: func(cmd *cobra.Command, args []string) {
			requireArgs(cmd, args, 1)

			noteId := findNote(db, args[0])
			note := db.Entity(noteId) // should check if it exists!
			title := note.Get(mu.Keyword("", "name"))
			content := note.Get(mu.Keyword("", "content"))
			fmt.Printf("# %s (%d)\n\n%s", title, noteId, content)
		},
	}

	cli.AddCommand(initCommand)
	cli.AddCommand(newCommand)
	cli.AddCommand(editCommand)
	cli.AddCommand(listCommand)
	cli.AddCommand(showCommand)
	cli.Execute()
}

func requireArgs(cmd *cobra.Command, args []string, num int) {
	if len(args) < num {
		cmd.Usage()
		os.Exit(1)
	}
}

func findNote(db *database.Database, idOrTitle string) int {
	nameAttr := db.Entid(mu.Keyword("", "name"))
	if nameAttr == -1 {
		log.Fatalf("db not initialized, run `%s init _` first")
	}

	entity, err := strconv.Atoi(idOrTitle)
	if err != nil {
		iter := db.Aevt().DatomsAt(mu.Datum(-1, nameAttr, ""), mu.Datum(10000, nameAttr, ""))
		for datom := iter.Next(); datom != nil; datom = iter.Next() {
			if datom.Value().Val() == idOrTitle {
				return datom.Entity()
			}
		}

		fmt.Println("no such note:", idOrTitle)
		os.Exit(1)
	} else {
		iter := db.Eavt().SeekDatoms(mu.Datum(entity, nameAttr, ""))
		datom := iter.Next()
		if datom == nil || datom.Entity() != entity || datom.Attribute() != nameAttr {
			fmt.Println("no such note:", idOrTitle)
			os.Exit(1)
		}
		return entity
	}

	return -1
}

func getContentFrom(args []string, pos int, defaultContent string) string {
	if len(args) > pos {
		rawContent, err := ioutil.ReadFile(args[pos])
		if err != nil {
			log.Fatal(err)
		}
		return string(rawContent)
	} else {
		content, err := getContent("")
		if err != nil {
			log.Fatal(err)
		}
		return content
	}
}

func getContent(content string) (string, error) {
	if terminal.IsTerminal(int(os.Stdin.Fd())) {
		f, err := ioutil.TempFile("", "note-")
		if err != nil {
			return "", err
		}
		defer os.Remove(f.Name())

		if content != "" {
			_, err = f.WriteString(content)
			if err != nil {
				return "", err
			}

			_, err = f.Seek(0, 0)
			if err != nil {
				return "", err
			}
		}

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
