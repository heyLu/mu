package mu

import (
	"net/url"

	"./connection"
	_ "./connection/backup"
	_ "./connection/file"
	_ "./connection/memory"
	"./database"
	"./index"
)

func Connect(u *url.URL) (connection.Connection, error) {
	return connection.New(u)
}

func Transact(conn connection.Connection, origDatoms []index.Datom) error {
	db, err := conn.Db()
	if err != nil {
		return err
	}

	datoms := make([]index.Datom, 0, len(origDatoms))
	for _, datom := range origDatoms {
		// if db already contains a value for the attribute, retract it before adding the new value.
		// (assumes the attribute has cardinality one.)
		if prev, ok := previousValue(db, datom); ok {
			//fmt.Println("retracting", prev)
			datoms = append(datoms, prev.Retraction())
		}
		//fmt.Println("adding", datom)
		datoms = append(datoms, datom)
	}

	return conn.TransactDatoms(datoms)
}

func previousValue(db *database.Database, datom index.Datom) (*index.Datom, bool) {
	iter := db.Eavt().DatomsAt(index.NewDatom(datom.E(), datom.A(), "", -1, true), index.MaxDatom)
	prev := iter.Next()
	if prev == nil {
		return nil, false
	} else {
		return prev, true
	}
}
