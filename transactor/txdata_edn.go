package transactor

import (
	"fmt"
	"github.com/heyLu/edn"
	"github.com/heyLu/fressian"
	"time"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
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

func txDatumFromValue(val interface{}) (TxDatum, error) {
	switch val := val.(type) {
	case []interface{}:
		return datumFromValue(val)
	/*case map[interface{}]interface{}:
	return txMapFromValue(val)*/
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

	entity, err := entityFromValue(val[1])
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

func entityFromValue(val interface{}) (database.HasLookup, error) {
	switch val := val.(type) {
	case int64:
		return database.Id(val), nil
	case edn.Keyword:
		return toKeyword(val), nil
	case []interface{}:
		if len(val) != 2 {
			return nil, fmt.Errorf("lookup ref must be of the form [kw val], but was %v", val)
		}

		kw, ok := val[0].(edn.Keyword)
		if !ok {
			return nil, fmt.Errorf("lookup ref must be of the form [kw val], but was %v", val)
		}

		lookupRef := database.LookupRef{
			Attribute: toKeyword(kw),
			Value:     index.NewValue(val[1]),
		}

		return lookupRef, nil
	default:
		return nil, fmt.Errorf("invalid entity %v", val)
	}
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
	case int64, string, time.Time:
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

func toKeyword(kw edn.Keyword) database.Keyword {
	return database.Keyword{
		fressian.Keyword{
			Namespace: kw.Namespace,
			Name:      kw.Name,
		},
	}
}
