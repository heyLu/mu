package file

import (
	"io/ioutil"
	"os"
	"path"
)

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
