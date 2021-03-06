package database

import (
	"fmt"
	"github.com/heyLu/fressian"
	"time"

	"github.com/heyLu/mu/index"
)

type Db struct {
	eavt           *index.MergedIndex
	aevt           *index.MergedIndex
	avet           *index.MergedIndex
	vaet           *index.MergedIndex
	basisT         int
	nextT          int
	useHistory     bool
	asOf           int
	since          int
	filter         Filter
	attributeCache map[int]Attribute
}

type Filter func(db *Db, datom *index.Datom) bool

var Empty = NewInMemory(
	index.NewMemoryIndex(index.CompareEavt),
	index.NewMemoryIndex(index.CompareAevt),
	index.NewMemoryIndex(index.CompareAvet),
	index.NewMemoryIndex(index.CompareVaet))

func New(eavt, aevt, avet, vaet *index.MergedIndex) *Db {
	return &Db{
		eavt:           eavt,
		aevt:           aevt,
		avet:           avet,
		vaet:           vaet,
		basisT:         0,
		nextT:          1000,
		useHistory:     false,
		asOf:           -1,
		since:          -1,
		filter:         nil,
		attributeCache: make(map[int]Attribute, 100)}
}

func NewInMemory(eavt, aevt, avet, vaet *index.MemoryIndex) *Db {
	empty := index.NewSegmentedIndex(&index.Root{}, nil, index.CompareEavtIndex)
	return New(
		index.NewMergedIndex(eavt, empty, index.CompareEavt),
		index.NewMergedIndex(aevt, empty, index.CompareAevt),
		index.NewMergedIndex(avet, empty, index.CompareAvet),
		index.NewMergedIndex(vaet, empty, index.CompareVaet))
}

func (db *Db) Eavt() EavtIndex { return EavtIndex{db.index(db.eavt)} }
func (db *Db) Aevt() AevtIndex { return AevtIndex{db.index(db.aevt)} }
func (db *Db) Avet() AvetIndex { return AvetIndex{db.index(db.avet)} }
func (db *Db) Vaet() VaetIndex { return VaetIndex{db.index(db.vaet)} }

func (db *Db) index(index index.Index) *dbIndex {
	return &dbIndex{
		db:    db,
		index: index,
	}
}

type dbIndex struct {
	db    *Db
	index index.Index
}

func (i *dbIndex) Datoms() index.Iterator {
	return i.DatomsAt(index.MinDatom, index.MaxDatom)
}

func (i *dbIndex) SeekDatoms(start index.Datom) index.Iterator {
	return i.DatomsAt(start, index.MaxDatom)
}

func (i *dbIndex) DatomsAt(start, end index.Datom) index.Iterator {
	iter := i.index.DatomsAt(start, end)
	if i.db.since > 0 {
		minTx := 3*(1<<42) + i.db.since
		iter = index.FilterIterator(iter, func(datom *index.Datom) bool {
			return datom.Tx() >= minTx
		})
	}
	if i.db.asOf >= 0 {
		maxTx := 3*(1<<42) + i.db.asOf
		iter = index.FilterIterator(iter, func(datom *index.Datom) bool {
			return datom.Tx() <= maxTx
		})
	}
	if !i.db.useHistory {
		iter = withoutRetractions(iter)
	}
	if i.db.filter != nil {
		rawDb := *i.db
		rawDb.filter = nil
		filter := func(datom *index.Datom) bool {
			return i.db.filter(&rawDb, datom)
		}
		iter = index.FilterIterator(iter, filter)
	}
	return iter
}

func (i *dbIndex) Datoms2(entity HasLookup, attribute HasLookup, value interface{}) index.Iterator {
	minE, maxE := index.MinDatom.E(), index.MaxDatom.E()
	if entity != nil {
		e := i.db.Entid(entity)
		minE, maxE = e, e
	}

	minA, maxA := index.MinDatom.A(), index.MaxDatom.A()
	if attribute != nil {
		a := i.db.Entid(attribute)
		minA, maxA = a, a
	}

	minV, maxV := index.MinDatom.V(), index.MaxDatom.V()
	if value != nil {
		v := index.NewValue(value)
		minV, maxV = v, v
	}

	return i.DatomsAt(
		index.NewDatom(minE, minA, minV, index.MaxDatom.Tx(), index.MinDatom.Added()),
		index.NewDatom(maxE, maxA, maxV, index.MinDatom.Tx(), index.MaxDatom.Added()))
}

type EavtIndex struct {
	*dbIndex
}

func (i EavtIndex) Datoms2(entity HasLookup, attribute HasLookup, value interface{}) index.Iterator {
	return i.dbIndex.Datoms2(entity, attribute, value)
}

type AevtIndex struct {
	*dbIndex
}

func (i AevtIndex) Datoms2(attribute HasLookup, entity HasLookup, value interface{}) index.Iterator {
	return i.dbIndex.Datoms2(entity, attribute, value)
}

type AvetIndex struct {
	*dbIndex
}

func (i AvetIndex) Datoms2(attribute HasLookup, value interface{}, entity HasLookup) index.Iterator {
	return i.dbIndex.Datoms2(entity, attribute, value)
}

type VaetIndex struct {
	*dbIndex
}

func (i VaetIndex) Datoms2(value HasLookup, attribute HasLookup, entity HasLookup) index.Iterator {
	var v interface{} = value
	if value != nil {
		v = index.NewValue(i.db.Entid(value))
	}

	return i.dbIndex.Datoms2(entity, attribute, v)
}

func (db *Db) History() *Db {
	newDb := *db
	newDb.useHistory = true
	return &newDb
}

func (db *Db) AsOf(t int) *Db {
	newDb := *db
	newDb.asOf = t % (3 * (1 << 42))
	return &newDb
}

func (db *Db) AsOfTime(t time.Time) *Db {
	return db.AsOf(db.tAtTime(t))
}

func (db *Db) AsOfT() int {
	return db.asOf
}

func (db *Db) Since(t int) *Db {
	newDb := *db
	newDb.since = t % (3 * (1 << 42))
	return &newDb
}

func (db *Db) SinceTime(t time.Time) *Db {
	return db.Since(db.tAtTime(t))
}

func (db *Db) SinceT() int {
	return db.since
}

func (db *Db) Filter(filter Filter) *Db {
	newDb := *db
	if newDb.filter == nil {
		newDb.filter = filter
	} else {
		prevFilter := db.filter
		newDb.filter = func(db *Db, datom *index.Datom) bool {
			return prevFilter(db, datom) && filter(db, datom)
		}
	}
	return &newDb
}

func (db *Db) IsFiltered() bool {
	return db.filter != nil
}

func (db *Db) EntidAt(part HasLookup, t int) int {
	partId, err := part.Lookup(db)
	if err != nil {
		// FIXME: panic instead?
		return -1
	}

	// FIXME: disallow values which are neither t values nor tx ids
	return partId*(1<<42) + (t % (3 * (1 << 42)))
}

func (db *Db) EntidAtTime(part HasLookup, t time.Time) int {
	return db.EntidAt(part, db.tAtTime(t))
}

const dbTxInstant = 50

// tAtTime returns the t of the transaction whose txInstant is "closest"
// to the time.
//
// If the time is smaller than the txInstant of the first transaction,
// it returns the t of the first transaction.  If the time is between
// two transactions, it returns the later one.  If the time after any
// transaction, it returns db.NextT().
func (db *Db) tAtTime(t time.Time) int {
	iter := db.Avet().DatomsAt(
		index.NewDatom(index.MinDatom.E(), dbTxInstant, t, index.MaxDatom.Tx(), false),
		index.NewDatom(index.MaxDatom.E(), dbTxInstant, index.MaxValue, index.MinDatom.Tx(), true))
	datom := iter.Next()
	if datom == nil {
		return db.NextT()
	} else {
		return datom.Tx() % (3 * (1 << 42))
	}
}

func (db *Db) BasisT() int { return db.basisT }
func (db *Db) NextT() int  { return db.nextT }

func (db *Db) WithDatoms(datoms []index.Datom) *Db {
	return db.WithDatomsT(db.basisT, db.nextT, datoms)
}

func (db *Db) WithDatomsT(basisT, nextT int, datoms []index.Datom) *Db {
	eavt := db.eavt.AddDatoms(datoms)
	aevt := db.aevt.AddDatoms(datoms)
	avetDatoms, vaetDatoms := FilterAvetAndVaet(db, datoms)
	avet := db.avet.AddDatoms(avetDatoms)
	vaet := db.vaet.AddDatoms(vaetDatoms)
	newDb := New(eavt, aevt, avet, vaet)
	newDb.basisT = basisT
	newDb.nextT = nextT
	return newDb
}

func (db *Db) Entid(lookup HasLookup) int {
	eid, err := lookup.Lookup(db)
	if err != nil {
		return -1
	}

	return eid
}

func (db *Db) Ident(entity int) *Keyword {
	// FIXME [perf]: use `.DatomsAt` and/or caching (datomic does this on `connect`)
	datoms := db.Aevt().Datoms()
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		if datom.Entity() == entity && datom.Attribute() == 10 {
			key := datom.Value().Val().(fressian.Keyword)
			return &Keyword{key}
		}
	}

	return nil
}

type Entity struct {
	db             *Db
	id             int
	attributeCache map[Keyword]interface{}
}

// Entity constructs a lazy, cached "view" of all datoms with a given
// entity id.
func (db *Db) Entity(id int) Entity {
	return Entity{db, id, map[Keyword]interface{}{}}
}

// Datoms returns an iterator over all datoms for this entity.
func (e Entity) Datoms() index.Iterator {
	return e.db.Eavt().DatomsAt(
		index.NewDatom(e.id, index.MinDatom.A(), index.MinValue, index.MaxDatom.Tx(), false),
		index.NewDatom(e.id, index.MaxDatom.A(), index.MaxValue, index.MinDatom.Tx(), true))
}

// Keys returns a slice of all attributes of this entity.
func (e Entity) Keys() []Keyword {
	// TODO: cache attributes here as well?
	keys := []Keyword{}
	prevKeys := map[Keyword]bool{}

	iter := e.Datoms()
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		kw := e.db.Ident(datom.Attribute())
		if kw == nil {
			panic(fmt.Sprint("attribute has no `:db/ident`:", datom.Attribute()))
		}
		if _, ok := prevKeys[*kw]; ok {
			continue
		}
		prevKeys[*kw] = true
		keys = append(keys, *kw)
	}
	return keys
}

// Get retrieves the value for the attribute.
//
// The resulting value is cached.  If no value is found, `nil` is returned.
func (e Entity) Get(key Keyword) interface{} {
	if val, ok := e.attributeCache[key]; ok {
		return val
	}

	attrId := e.db.Entid(key)
	if attrId == -1 {
		return nil
	}
	hasMany := e.db.Attribute(attrId).Cardinality() == CardinalityMany
	isRef := e.db.Attribute(attrId).Type() == index.Ref
	vals := []interface{}{}

	min, max := index.MinDatom, index.MaxDatom
	datoms := e.db.Eavt().DatomsAt(
		index.NewDatom(e.id, attrId, min.V(), max.Tx(), min.Added()),
		index.NewDatom(e.id, attrId, max.V(), min.Tx(), max.Added()))
	for datom := datoms.Next(); datom != nil; datom = datoms.Next() {
		var val interface{}
		if isRef {
			val = e.db.Entity(datom.Value().Val().(int))
		} else {
			val = datom.Value().Val()
		}

		if hasMany {
			vals = append(vals, val)
		} else {
			e.attributeCache[key] = val
			return val
		}
	}

	if hasMany {
		return vals
	}

	return nil
}

// Touch caches all attributes of this entity.
func (e Entity) Touch() {
	iter := e.Datoms()
	for datom := iter.Next(); datom != nil; datom = iter.Next() {
		kw := e.db.Ident(datom.Attribute())
		if kw == nil {
			panic(fmt.Sprint("attribute has no `:db/ident`:", datom.Attribute()))
		}
		e.attributeCache[*kw] = datom.Value().Val()
	}
}

// AsMap returns a view of this entity as a map.
//
// The returned map will only contain cached attributes.  To get a map of
// all attributes call `.Touch` first.
func (e Entity) AsMap() map[Keyword]interface{} {
	return e.attributeCache
}

type Attribute struct {
	id          int
	ident       fressian.Keyword
	cardinality Cardinality
	valueType   index.ValueType
	unique      Unique
	indexed     bool
	noHistory   bool
}

type Unique int

const (
	UniqueNil      Unique = 0
	UniqueValue    Unique = 37
	UniqueIdentity Unique = 38
)

func (u Unique) String() string {
	switch u {
	case UniqueNil:
		return "Unique(nil)"
	case UniqueValue:
		return "Unique(:db.unique/value)"
	case UniqueIdentity:
		return "Unique(:db.unique/identity)"
	default:
		return "Unique(invalid)"
	}
}

func (u Unique) IsValid() bool {
	return u == UniqueValue || u == UniqueIdentity
}

type Cardinality int

const (
	CardinalityOne  = 35
	CardinalityMany = 36
)

func (c Cardinality) String() string {
	switch c {
	case CardinalityOne:
		return "CardinalityOne"
	case CardinalityMany:
		return "CardinalityMany"
	default:
		return "CardinalityInvalid"
	}
}

func (c Cardinality) IsValid() bool {
	return c == CardinalityOne || c == CardinalityMany
}

func (db *Db) Attribute(id int) *Attribute {
	attr, ok := db.attributeCache[id]
	if ok {
		//log.Println("attribute from cache:", attr)
		return &attr
	} else {
		iter := db.Eavt().DatomsAt(
			index.NewDatom(id, 0, index.MinValue, index.MaxDatom.Tx(), false),
			index.NewDatom(id, index.MaxDatom.A(), index.MaxValue, index.MinDatom.Tx(), true))
		found := false
		attr = Attribute{
			id: id,
		}

		for datom := iter.Next(); datom != nil; datom = iter.Next() {
			found = true

			switch datom.Attribute() {
			case 10: // :db/ident
				attr.ident = datom.Value().Val().(fressian.Keyword)
			case 40: // :db/valueType
				attr.valueType = index.ValueType(datom.Value().Val().(int))
			case 41: // :db/cardinality
				attr.cardinality = Cardinality(datom.Value().Val().(int))
			case 42: // :db/unique
				attr.unique = Unique(datom.Value().Val().(int))
			case 44: // :db/index
				attr.indexed = datom.Value().Val().(bool)
			case 45: // :db/noHistory
				attr.noHistory = datom.Value().Val().(bool)
			}
		}

		if !found {
			return nil
		}

		db.attributeCache[id] = attr
		//log.Println("attribute from db:", attr)
		return &attr
	}
}

func (a Attribute) Id() int                  { return a.id }
func (a Attribute) Ident() fressian.Keyword  { return a.ident }
func (a Attribute) Cardinality() Cardinality { return a.cardinality }
func (a Attribute) Type() index.ValueType    { return a.valueType }
func (a Attribute) Unique() Unique           { return a.unique }
func (a Attribute) Indexed() bool            { return a.indexed }
func (a Attribute) NoHistory() bool          { return a.noHistory }
