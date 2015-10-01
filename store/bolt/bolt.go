package bolt

import (
	"fmt"
	"github.com/boltdb/bolt"
	"net/url"
	"os"
	"time"

	"github.com/heyLu/mu/store"
)

func init() {
	store.Register("bolt", create, open)
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

	db, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return false, err
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("mu_kvs"))
		return err
	})
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

	db, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &boltStore{db}, nil
}

type boltStore struct {
	db *bolt.DB
}

func (s *boltStore) Get(id string) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		data = tx.Bucket([]byte("mu_kvs")).Get([]byte(id))
		if data == nil {
			return fmt.Errorf("key does not exist")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *boltStore) Put(id string, data []byte) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("mu_kvs")).Put([]byte(id), data)
	})
	return err
}

func (s *boltStore) Delete(id string) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("mu_kvs")).Delete([]byte(id))
	})
	return err
}

func (s *boltStore) Close() error {
	return s.db.Close()
}
