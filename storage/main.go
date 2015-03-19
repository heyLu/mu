package storage

import (
	"compress/gzip"
	"errors"
	"github.com/heyLu/fressian"
	"io"
	"net/url"
	"os"
	"path"
)

var objectCache = make(map[string]interface{}, 1000)

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
		roots, err := listDir(path.Join(baseDir, "roots"))
		if err != nil {
			return nil, err
		}

		if len(roots) == 0 {
			return nil, errors.New("invalid dir, no roots")
		} else if len(roots) > 1 {
			return nil, errors.New("multiple roots, must specify a ?root=<root> parameter")
		}
		rootId = roots[0]
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

func listDir(path string) ([]string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return dir.Readdirnames(-1)
}

func Get(s *Store, id string, handlers map[string]fressian.ReadHandler) (interface{}, error) {
	if val, ok := objectCache[id]; ok {
		return val, nil
	}

	r, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	g, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}

	obj, err := fressian.NewReader(g, handlers).ReadObject()
	if err != nil {
		return nil, err
	}
	objectCache[id] = obj
	return obj, nil
}
