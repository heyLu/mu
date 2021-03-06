package file

import (
	"github.com/heyLu/fressian"
	"net/url"
	"os"

	"github.com/heyLu/mu/connection"
	memoryConn "github.com/heyLu/mu/connection/memory"
	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	"github.com/heyLu/mu/log"
	"github.com/heyLu/mu/transactor"
)

func init() {
	connection.Register("file", New)
}

type Connection struct {
	path string
	conn connection.Connection
}

func New(u *url.URL) (connection.Connection, error) {
	path := u.Host + u.Path

	// does not exist, create an empty db
	_, err := os.Stat(path)
	if err != nil {
		memConn, _ := memoryConn.New(u)
		conn := &Connection{path, memConn}
		err = conn.Index(nil) // writes empty db to `path`
		if err != nil {
			return nil, err
		}
		return conn, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := fressian.NewReader(f, ReadHandlers)
	dbRaw, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	db := dbRaw.(*database.Db)
	conn := memoryConn.NewFromDb(db)
	return &Connection{path, conn}, nil
}

func (c *Connection) Db() *database.Db {
	return c.conn.Db()
}

func (c *Connection) Log() *log.Log { return nil }

func (c *Connection) Index(datoms []index.Datom) error {
	// FIXME: write using memory index, then write to file
	//        (don't modify if an io error occurs)
	err := c.conn.Index(datoms)
	if err != nil {
		return err
	}

	f, err := os.Create(c.path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := fressian.NewWriter(f, WriteHandler)
	err = w.WriteValue(c.conn.Db())
	if err != nil {
		return err
	}
	err = w.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) Transact(datoms []transactor.TxDatum) (*transactor.TxResult, error) {
	txResult, err := c.conn.Transact(datoms)
	if err != nil {
		return nil, err
	}

	err = c.Index(nil)
	if err != nil {
		return nil, err
	}

	return txResult, nil
}
