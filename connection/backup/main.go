package backup

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"

	connection ".."
)

func init() {
	connection.Register("backup", New)
}

func New(u *url.URL) (connection.Connection, error) {
	baseDir := u.Host + u.Path
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

	log.Fatal("reading the root not implemented")

	indexRootId := ""
	newU, _ := url.Parse(fmt.Sprintf("files://%s/values?root=%s", baseDir, indexRootId))
	return connection.New(newU)
}

func listDir(path string) ([]string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return dir.Readdirnames(-1)
}
