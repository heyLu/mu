package backup

import (
	"errors"
	"fmt"
	"github.com/heyLu/fressian"
	"net/url"
	"os"
	"path"

	"github.com/heyLu/mu/connection"
	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	"github.com/heyLu/mu/log"
	"github.com/heyLu/mu/store"
	"github.com/heyLu/mu/transactor"
)

func init() {
	connection.Register("backup", New)
}

type Connection struct {
	db  *database.Db
	log *log.Log
}

func (c *Connection) Db() *database.Db { return c.db }
func (c *Connection) Log() *log.Log    { return c.log }

func (c *Connection) Index([]index.Datom) error {
	return fmt.Errorf(".Index is not supported on backups")
}

func (c *Connection) Transact([]transactor.TxDatum) (*transactor.TxResult, error) {
	return nil, fmt.Errorf(".Transact is not supported on backups")
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

	// read root which contains index/root-id, log/root-id and log/tail
	// construct a connection from that (log -> memory index, index/root-id -> segmented index
	root, err := getRoot(path.Join(baseDir, "roots", rootId))
	if err != nil {
		return nil, err
	}

	indexRootId := root[fressian.Keyword{"index", "root-id"}].(string)
	logRootId := root[fressian.Keyword{"log", "root-id"}].(string)
	logTail := root[fressian.Keyword{"log", "tail"}].([]byte)
	storeUrl, _ := url.Parse(fmt.Sprintf("files://%s/values", u.Host+u.Path))
	store, err := store.Open(storeUrl)
	if err != nil {
		return nil, err
	}
	db, log := connection.CurrentDb(store, indexRootId, logRootId, logTail)
	return &Connection{db, log}, nil
}

func listDir(path string) ([]string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return dir.Readdirnames(-1)
}

func getRoot(path string) (map[interface{}]interface{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := fressian.NewReader(f, nil)
	rootRaw, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	return rootRaw.(map[interface{}]interface{}), nil
}
