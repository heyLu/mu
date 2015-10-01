package connection

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	txlog "github.com/heyLu/mu/log"
	"github.com/heyLu/mu/transactor"
)

type Connector func(u *url.URL) (Connection, error)

type Connection interface {
	Db() *database.Db
	Log() *txlog.Log
	Index(datoms []index.Datom) error
	Transact(datoms []transactor.TxDatum) (*transactor.TxResult, error)
}

var registeredConnectors = map[string]Connector{}

func Register(name string, connector Connector) {
	if _, ok := registeredConnectors[name]; ok {
		panic(fmt.Sprint("duplicate connector for ", name))
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
