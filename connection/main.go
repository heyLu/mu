package connection

import (
	"errors"
	"fmt"
	"log"
	"net/url"

	"../database"
	"../index"
	dbLog "../log"
	"../transactor"
)

type Connector func(u *url.URL) (Connection, error)

type Connection interface {
	Db() *database.Db
	Log() *dbLog.Log
	Index(datoms []index.Datom) error
	Transact(datoms []transactor.TxDatum) (*transactor.TxResult, error)
}

var registeredConnectors = map[string]Connector{}

func Register(name string, connector Connector) {
	if _, ok := registeredConnectors[name]; ok {
		log.Fatal("duplicate connector for ", name)
	}

	registeredConnectors[name] = connector
}

func New(u *url.URL) (Connection, error) {
	if u.Scheme == "file" || u.Scheme == "backup" {
		connector, ok := registeredConnectors[u.Scheme]
		if !ok {
			return nil, errors.New(fmt.Sprint("no such connector: ", u.Scheme))
		}

		return connector(u)
	}

	return connectToStore(u)
}
