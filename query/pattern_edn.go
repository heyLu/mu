package query

import (
	"fmt"
	"github.com/heyLu/edn"
	"github.com/heyLu/fressian"
	"strings"
	"time"

	"github.com/heyLu/mu/database"
	"github.com/heyLu/mu/index"
	"github.com/heyLu/mu/transactor"
)

func PatternFromEDN(s string) (*Pattern, error) {
	val, err := edn.DecodeString(s)
	if err != nil {
		return nil, err
	}

	vals, ok := val.([]interface{})
	if !ok || len(vals) != 3 {
		return nil, fmt.Errorf("pattern must be of the form [e a v], but was %v", val)
	}

	e := isVariable(vals[0])
	a := isVariable(vals[1])
	v := isVariable(vals[2])

	var entity database.HasLookup
	var attribute database.Keyword
	var value index.Value

	if !e {
		var err error
		entity, err = transactor.EntityFromValue(vals[0])
		if err != nil {
			return nil, err
		}
	}

	if !a {
		attrRaw, ok := vals[1].(edn.Keyword)
		if !ok {
			return nil, fmt.Errorf("invalid attribute %v", vals[1])
		}

		attribute = toKeyword(attrRaw)
	}

	if !v {
		val, err := toValue(vals[2])
		if err != nil {
			return nil, err
		}

		value = *val
	}

	if !e && a && v {
		pattern := E(entity)
		return &pattern, nil
	} else if !e && !a && v {
		pattern := Ea(entity, attribute)
		return &pattern, nil
	} else if !e && !a && !v {
		pattern := Eav(entity, attribute, value)
		return &pattern, nil
	} else if e && !a && v {
		pattern := A(attribute)
		return &pattern, nil
	}

	return nil, fmt.Errorf("not implemented")
}

func isVariable(val interface{}) bool {
	sym, ok := val.(edn.Symbol)
	return ok && sym.Namespace == "" && strings.Index(sym.Name, "?") == 0
}

func toValue(val interface{}) (*index.Value, error) {
	switch val := val.(type) {
	case int64, string, time.Time:
		v := index.NewValue(val)
		return &v, nil
	case edn.Keyword:
		v := index.NewValue(toKeyword(val))
		return &v, nil
	case edn.UUID:
		v := index.NewValue(fressian.UUID{Msb: val.Msb, Lsb: val.Lsb})
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
