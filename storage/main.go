package storage

import (
	"errors"
	"github.com/heyLu/fressian"
	"io"
	"net/url"
	"os"
	"path"
)

type Store struct {
	baseDir     string
	indexRootId string
}

func (s *Store) IndexRootId() string { return s.indexRootId }

func (s *Store) Get(id string) (io.ReadCloser, error) {
	l := len(id)
	p := path.Join(s.baseDir, "values", id[l-2:l], id)
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func Open(u *url.URL) (*Store, error) {
	baseDir := u.Path
	rootId := u.Query().Get("root")
	if rootId == "" {
		return nil, errors.New("must specify a ?root=<root> parameter")
	}
	p := path.Join(baseDir, "roots", rootId)
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	rootRaw, err := fressian.NewReader(f, nil).ReadObject()
	if err != nil {
		return nil, err
	}
	root := rootRaw.(map[interface{}]interface{})
	indexRootId := root[fressian.Key{"index", "root-id"}].(string)
	return &Store{baseDir, indexRootId}, nil
}
