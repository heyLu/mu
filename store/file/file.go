package file

import (
	"io/ioutil"
	"net/url"
	"os"
	"path"

	"github.com/heyLu/mu/store"
)

func init() {
	store.Register("files", open)
}

func open(u *url.URL) (store.Store, error) {
	path := u.Host + u.Path

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	f.Close()

	return &fileStore{path}, nil
}

type fileStore struct {
	path string
}

func (s fileStore) blobPath(id string) string {
	return path.Join(s.path, id[len(id)-2:], id)
}

func (s fileStore) Get(id string) ([]byte, error) {
	return ioutil.ReadFile(s.blobPath(id))
}

func (s fileStore) Put(id string, data []byte) error {
	return ioutil.WriteFile(s.blobPath(id), data, 0644)
}

func (s fileStore) Delete(id string) error {
	return os.Remove(s.blobPath(id))
}
