package query

import (
	"fmt"
)

// newHashKey returns a value implementing indexed that contains
// the given values.
//
// For now the maximum number of values supported is three.  This limit
// is arbitrary and can easily be changed.  We will change it based on
// how much joins queries need in practice.
func newHashKey(vals []value) Indexed {
	switch len(vals) {
	case 1:
		return key1{val1: vals[0]}
	case 2:
		return key2{val1: vals[0], val2: vals[1]}
	case 3:
		return key3{val1: vals[0], val2: vals[1], val3: vals[2]}
	case 4:
		return key4{val1: vals[0], val2: vals[1], val3: vals[2], val4: vals[3]}
	default:
		panic("unsupported join arity")
	}
}

type key1 struct{ val1 value }

func (k key1) ValueAt(idx int) value {
	switch idx {
	case 0:
		return k.val1
	default:
		panic("invalid index")
	}
}

func (k key1) Length() int { return 1 }

func (k key1) String() string {
	return fmt.Sprintf("[%v]", k.val1)
}

type key2 struct{ val1, val2 value }

func (k key2) ValueAt(idx int) value {
	switch idx {
	case 0:
		return k.val1
	case 1:
		return k.val2
	default:
		panic("invalid index")
	}
}

func (k key2) Length() int { return 2 }

func (k key2) String() string {
	return fmt.Sprintf("[%v %v]", k.val1, k.val2)
}

type key3 struct{ val1, val2, val3 value }

func (k key3) ValueAt(idx int) value {
	switch idx {
	case 0:
		return k.val1
	case 1:
		return k.val2
	case 2:
		return k.val3
	default:
		panic("invalid index")
	}
}

func (k key3) String() string {
	return fmt.Sprintf("[%v %v %v]", k.val1, k.val2, k.val3)
}

func (k key3) Length() int { return 3 }

type key4 struct{ val1, val2, val3, val4 value }

func (k key4) ValueAt(idx int) value {
	switch idx {
	case 0:
		return k.val1
	case 1:
		return k.val2
	case 2:
		return k.val3
	case 3:
		return k.val4
	default:
		panic("invalid index")
	}
}

func (k key4) Length() int { return 4 }

func (k key4) String() string {
	return fmt.Sprintf("[%v %v %v %v]", k.val1, k.val2, k.val3, k.val4)
}
