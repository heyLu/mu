package mu

import (
	tu "github.com/klingtnet/gol/util/testing"
	"testing"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
)

var (
	attrName    = Attribute("", "name")
	attrNameRaw = 1
	attrAge     = Attribute("", "age")
	attrAgeRaw  = 2
)

var db = database.Empty.WithDatoms(
	[]index.Datom{
		index.NewDatom(attrNameRaw, DbIdent, attrName.Keyword, 0, true),
		index.NewDatom(attrAgeRaw, DbIdent, attrAge.Keyword, 0, true),

		index.NewDatom(100, attrNameRaw, "Jane", 1, true),
		index.NewDatom(100, attrAgeRaw, 3, 1, true),

		index.NewDatom(101, attrNameRaw, "Judy", 2, true),
		index.NewDatom(101, attrAgeRaw, 7, 2, true),

		index.NewDatom(102, attrNameRaw, "Fred", 3, true),
		index.NewDatom(102, attrAgeRaw, 2, 3, true),
	})

func TestE(t *testing.T) {
	iter, err := Datoms(db, E(Id(100)))
	tu.RequireNil(t, err)
	datom := iter.Next()
	tu.RequireNotNil(t, datom)
	tu.ExpectEqual(t, datom.E(), 100)
	tu.ExpectEqual(t, datom.A(), attrNameRaw)
	tu.ExpectEqual(t, datom.V().Val(), "Jane")
}

func TestEa(t *testing.T) {
	iter, err := Datoms(db, Ea(Id(100), attrName))
	tu.RequireNil(t, err)

	datom := iter.Next()
	tu.RequireNotNil(t, datom)
	tu.ExpectEqual(t, datom.E(), 100)
	tu.ExpectEqual(t, datom.A(), attrNameRaw)
	tu.ExpectEqual(t, datom.V().Val(), "Jane")
}

func TestEav(t *testing.T) {
	iter, err := Datoms(db, Eav(Id(101), attrName, "Judy"))
	tu.RequireNil(t, err)

	datom := iter.Next()
	tu.RequireNotNil(t, datom)
	tu.ExpectEqual(t, datom.E(), 101)
	tu.ExpectEqual(t, datom.A(), attrNameRaw)
	tu.ExpectEqual(t, datom.V().Val(), "Judy")

	datom = iter.Next()
	tu.ExpectNil(t, datom)
}

func TestEav_NoMatch(t *testing.T) {
	iter, err := Datoms(db, Eav(Id(100), attrName, "Judy"))
	tu.RequireNil(t, err)
	tu.ExpectNil(t, iter.Next())
}

func TestA(t *testing.T) {
	iter, err := Datoms(db, A(attrName))
	tu.RequireNil(t, err)
	expectIterCount(t, iter, 3)
}

func TestAe(t *testing.T) {
	iter, err := Datoms(db, Ae(attrName, Id(102)))
	tu.RequireNil(t, err)

	datom := iter.Next()
	tu.RequireNotNil(t, datom)
	tu.ExpectEqual(t, datom.E(), 102)
	tu.ExpectEqual(t, datom.A(), attrNameRaw)
	tu.ExpectEqual(t, datom.V().Val(), "Fred")
}

func expectIterCount(t *testing.T, iter index.Iterator, count int) {
	n := 0
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		n += 1
	}
	tu.ExpectEqual(t, n, count)
}
