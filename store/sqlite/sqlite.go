package sqlite

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"net/url"
	"os"

	"github.com/heyLu/mu/store"
)

func init() {
	store.Register("sqlite", create, open)
}

func create(u *url.URL) (bool, error) {
	path := u.Host + u.Path

	isNew := false
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			isNew = true
		} else {
			return false, err
		}
	}
	f.Close()

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return false, err
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS mu_kvs (
	id TEXT NOT NULL PRIMARY KEY,
	data BLOB
)`)
	if err != nil {
		return false, err
	}

	return isNew, nil
}

func open(u *url.URL) (store.Store, error) {
	path := u.Host + u.Path

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	f.Close()

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	return &sqliteStore{db}, nil
}

type sqliteStore struct {
	db *sql.DB
}

func (s *sqliteStore) Get(id string) ([]byte, error) {
	var data []byte
	err := s.db.QueryRow("SELECT data FROM mu_kvs WHERE id = ?", id).Scan(&data)
	switch {
	case err == sql.ErrNoRows:
		return nil, fmt.Errorf("key does not exist")
	case err != nil:
		return nil, err
	default:
		return data, nil
	}
}

func (s *sqliteStore) Put(id string, data []byte) error {
	_, err := s.db.Exec("INSERT OR REPLACE INTO mu_kvs VALUES (?, ?)",
		id, data, id)
	return err
}

func (s *sqliteStore) Delete(id string) error {
	_, err := s.db.Exec("DELETE FROM mu_kvs WHERE id = ?", id)
	return err
}

func (s *sqliteStore) Close() error {
	return s.db.Close()
}
