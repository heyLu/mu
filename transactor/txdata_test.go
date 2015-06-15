package transactor

import (
	"fmt"
	"github.com/heyLu/fressian"
	tu "github.com/klingtnet/gol/util/testing"
	"testing"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
)

const (
	ident          = 10
	valueType      = 40
	cardinality    = 41
	cardinalityOne = 35
)

var (
	attrName = database.Keyword{fressian.Keyword{"", "name"}}
)

var db = database.Empty.WithDatoms(
	[]index.Datom{
		index.NewDatom(0, ident, attrName.Keyword, 0, true),
		index.NewDatom(0, valueType, int(index.String), 0, true),
		index.NewDatom(0, cardinality, cardinalityOne, 0, true),
	})

func TestResolve(t *testing.T) {
	datum := Datum{
		E: database.Id(-1),
		A: database.Keyword(attrName),
		V: NewValue("Jane"),
	}

	_, err := datum.Resolve(db)
	tu.ExpectNil(t, err)

	datum.V = NewValue(10)
	_, err = datum.Resolve(db)
	fmt.Println(err)
	tu.ExpectNotNil(t, err)
}
