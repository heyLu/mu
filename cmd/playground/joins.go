package main

type indexed interface {
	valueAt(idx int) value
}

type tuple []value

func (t tuple) valueAt(idx int) value { return t[idx] }

type value interface{}

type variable interface{}

func getterFn(attrs map[variable]int, attr variable) func(tuple) value {
	idx := attrs[attr]
	return func(tuple tuple) value {
		return tuple.valueAt(idx)
	}
}

func hashKeyFn(getters ...func(tuple) value) func(tuple) indexed {
	return func(tuple tuple) indexed {
		vals := make([]value, len(getters))
		for i, getter := range getters {
			vals[i] = getter(tuple)
		}
		return newHashKey(vals)
	}
}

func main() {
	var val value
	val = 10
	println("hi!")
	println(val)

	println()
	m := map[indexed][]value{}
	println(m)
}

func newHashKey(vals []value) indexed {
	switch len(vals) {
	case 1:
		return key1{val1: vals[0]}
	case 2:
		return key2{val1: vals[0], val2: vals[1]}
	case 3:
		return key3{val1: vals[0], val2: vals[1],
			val3: vals[2]}
	default:
		panic("unsupported join arity")
	}
}

type key1 struct{ val1 value }

func (j key1) valueAt(idx int) value {
	switch idx {
	case 1:
		return j.val1
	default:
		panic("invalid index")
	}
}

type key2 struct{ val1, val2 value }

func (j key2) valueAt(idx int) value {
	switch idx {
	case 1:
		return j.val1
	case 2:
		return j.val2
	default:
		panic("invalid index")
	}
}

type key3 struct{ val1, val2, val3 value }

func (j key3) valueAt(idx int) value {
	switch idx {
	case 1:
		return j.val1
	case 2:
		return j.val2
	case 3:
		return j.val3
	default:
		panic("invalid index")
	}
}
