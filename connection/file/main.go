package file

import (
	"compress/gzip"
	"github.com/heyLu/fressian"
	"net/url"
	"os"

	connection ".."
	"../../database"
	"../../index/"
	memoryConn "../memory"
)

func init() {
	connection.Register("file", New)
}

type Connection struct {
	path     string
	conn     connection.Connection
	compress bool
}

func New(u *url.URL) (connection.Connection, error) {
	path := u.Host + u.Path

	compress := true
	if u.Query().Get("compress") == "false" {
		compress = false
	}

	// does not exist, create an empty db
	_, err := os.Stat(path)
	if err != nil {
		memConn, _ := memoryConn.New(u)
		conn := &Connection{path, memConn, compress}
		err = conn.TransactDatoms(nil) // writes empty db to `path`
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

	var r *fressian.Reader
	if compress {
		r = fressian.NewGzipReader(f, ReadHandlers)
	} else {
		r = fressian.NewReader(f, ReadHandlers)
	}
	dbRaw, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	db := dbRaw.(*database.Database)
	conn := memoryConn.NewFromDb(db)
	return &Connection{path, conn, compress}, nil
}

func (c *Connection) Db() (*database.Database, error) {
	return c.conn.Db()
}

func (c *Connection) TransactDatoms(datoms []index.Datom) error {
	// FIXME: write using memory index, then write to file
	//        (don't modify if an io error occurs)
	err := c.conn.TransactDatoms(datoms)
	if err != nil {
		return err
	}

	f, err := os.Create(c.path)
	if err != nil {
		return err
	}
	defer f.Close()

	var w *fressian.Writer
	var gz *gzip.Writer
	if c.compress {
		gz = gzip.NewWriter(f)
		w = fressian.NewWriter(gz, WriteHandler)
	} else {
		w = fressian.NewWriter(f, WriteHandler)
	}

	db, _ := c.conn.Db()
	err = w.WriteValue(db)
	if err != nil {
		return err
	}

	err = w.Flush()
	if err != nil {
		return err
	}

	if c.compress {
		return gz.Flush()
	}

	return nil
}
