package transactor

import (
	"fmt"
	"github.com/heyLu/edn"
	"github.com/heyLu/fressian"
	"math/big"
	"time"

	"github.com/heyLu/mu/database"
)

func TxDataFromEDN(s string) ([]TxDatum, error) {
	val, err := edn.DecodeString(s)
	if err != nil {
		return nil, err
	}

	vals, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("tx data must be a list of values")
	}

	txData := make([]TxDatum, 0, len(vals))
	for _, val := range vals {
		txDatum, err := txDatumFromValue(val)
		if err != nil {
			return nil, err
		}

		txData = append(txData, txDatum)
	}

	return txData, nil
}

func HasLookupFromEDN(s string) (database.HasLookup, error) {
	val, err := edn.DecodeString(s)
	if err != nil {
		return nil, err
	}

	lookup, err := EntityFromValue(val)
	if err != nil {
		return nil, err
	}

	return lookup, err
}

func txDatumFromValue(val interface{}) (TxDatum, error) {
	switch val := val.(type) {
	case []interface{}:
		return datumFromValue(val)
	case map[interface{}]interface{}:
		return txMapFromValue(val)
	default:
		return nil, fmt.Errorf("don't know how to convert %v to a tx datum", val)
	}
}

var (
	opAdd     = edn.Keyword{Namespace: "db", Name: "add"}
	opRetract = edn.Keyword{Namespace: "db", Name: "retract"}
)

func datumFromValue(val []interface{}) (*Datum, error) {
	if len(val) != 4 {
		return nil, fmt.Errorf("datum must be of the form [op e a v], but was %v", val)
	}

	opRaw, ok := val[0].(edn.Keyword)
	if !ok || (opRaw != opAdd && opRaw != opRetract) {
		return nil, fmt.Errorf("op must be :db/add or :db/retract, but was %v", val[0])
	}
	op := Assert
	if opRaw == opRetract {
		op = Retract
	}

	entity, err := EntityFromValue(val[1])
	if err != nil {
		return nil, err
	}

	attribute, err := attributeFromValue(val[2])
	if err != nil {
		return nil, err
	}

	value, err := datumValueFromValue(val[3])
	if err != nil {
		return nil, err
	}

	datum := Datum{
		Op: op,
		E:  entity,
		A:  attribute,
		V:  *value,
	}
	return &datum, nil
}

var dbId = edn.Keyword{Namespace: "db", Name: "id"}
var dbIdSym = edn.Symbol{Namespace: "db", Name: "id"}

func txMapFromValue(val map[interface{}]interface{}) (*TxMap, error) {
	idRaw, ok := val[dbId]
	if !ok {
		return nil, fmt.Errorf("tx map needs a :db/id")
	}

	id, err := EntityFromValue(idRaw)
	if err != nil {
		return nil, err
	}

	attributes := map[database.Keyword][]Value{}
	for kRaw, v := range val {
		k, ok := kRaw.(edn.Keyword)
		if !ok {
			return nil, fmt.Errorf("attribute key must be a keyword, but was %v", kRaw)
		}

		if k == dbId {
			continue
		}

		kw := toKeyword(k)

		vsRaw, ok := v.(map[interface{}]bool)
		if ok {
			vs := make([]Value, 0, len(vsRaw))
			for v, _ := range vsRaw {
				v, err := datumValueFromValue(v)
				if err != nil {
					return nil, err
				}

				vs = append(vs, *v)
			}
			attributes[kw] = vs
		} else {
			v, err := datumValueFromValue(v)
			if err != nil {
				return nil, err
			}

			attributes[kw] = []Value{*v}
		}
	}

	txMap := TxMap{
		Id:         id,
		Attributes: attributes,
	}
	return &txMap, nil
}

func EntityFromValue(val interface{}) (database.HasLookup, error) {
	switch val := val.(type) {
	case int64:
		return database.Id(val), nil
	case edn.Keyword:
		return toKeyword(val), nil
	case []interface{}:
		lookup, err := lookupRefFromValue(val)
		if err != nil {
			return nil, err
		}

		return lookup, nil
	default:
		if tagged, ok := val.(edn.Tagged); ok && tagged.Tag == dbIdSym {
			id, err := idFromValue(tagged)
			if err != nil {
				return nil, err
			}

			return id, nil
		} else {
			return nil, fmt.Errorf("entity id must be an integer, a lookup ref or a #db/id[part id], but was %v", val)
		}
	}
}

func lookupRefFromValue(val []interface{}) (database.HasLookup, error) {
	if len(val) != 2 {
		return nil, fmt.Errorf("lookup ref must be of the form [kw val], but was %v", val)
	}

	kw, ok := val[0].(edn.Keyword)
	if !ok {
		return nil, fmt.Errorf("lookup ref must be of the form [kw val], but was %v", val)
	}

	vv, err := datumValueFromValue(val[1])
	if err != nil {
		return nil, err
	}

	v, err := vv.Get(nil, false)
	if err != nil {
		return nil, err
	}

	lookupRef := database.LookupRef{
		Attribute: toKeyword(kw),
		Value:     *v,
	}

	return lookupRef, nil
}

func attributeFromValue(val interface{}) (database.HasLookup, error) {
	switch val := val.(type) {
	case int64:
		return database.Id(val), nil
	case edn.Keyword:
		return toKeyword(val), nil
	default:
		return nil, fmt.Errorf("invalid attribute %v", val)
	}
}

func datumValueFromValue(val interface{}) (*Value, error) {
	switch val := val.(type) {
	case bool, int64, float64, string, time.Time, *big.Int:
		v := NewValue(val)
		return &v, nil
	case edn.Keyword:
		v := NewValue(toKeyword(val))
		return &v, nil
	case edn.UUID:
		v := NewValue(fressian.UUID{Msb: val.Msb, Lsb: val.Lsb})
		return &v, nil
	default:
		return nil, fmt.Errorf("invalid value %v", val)
	}
}

func idFromValue(id edn.Tagged) (database.HasLookup, error) {
	val, ok := id.Value.([]interface{})
	if !ok || len(val) != 2 {
		return nil, fmt.Errorf("db id must be of the form #db/id [part id], but was #db/id %v", id.Value)
	}

	partKw, ok := val[0].(edn.Keyword)
	if !ok {
		return nil, fmt.Errorf("db id partition must be a keyword, but was %v", val[0])
	}

	var part int
	switch partKw {
	case edn.Keyword{Namespace: "db.part", Name: "db"}:
		part = 0
	case edn.Keyword{Namespace: "db.part", Name: "tx"}:
		part = 3
	case edn.Keyword{Namespace: "db.part", Name: "user"}:
		part = 4
	default:
		return nil, fmt.Errorf("unknown partition %v", partKw)
	}

	eid, ok := val[1].(int64)
	if !ok || eid >= 0 {
		return nil, fmt.Errorf("db id value must be an negative integer, but was %v", val[1])
	}

	return database.Id(-(part*(1<<42) - int(eid))), nil
}

func toKeyword(kw edn.Keyword) database.Keyword {
	return database.Keyword{
		fressian.Keyword{
			Namespace: kw.Namespace,
			Name:      kw.Name,
		},
	}
}
