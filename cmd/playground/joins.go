package main

type indexed interface {
	valueAt(idx int) value
}

type value interface{}

func main() {
	var val value
	val = 10
	println("hi!")
	println(val)

	println()
	m := map[join3][]value{}
	println(m)
}

type join1 struct{ val1 value }

func (j join1) valueAt(idx int) value {
	switch idx {
	case 1:
		return j.val1
	default:
		panic("invalid index")
	}
}

type join2 struct{ val1, val2 value }

func (j join2) valueAt(idx int) value {
	switch idx {
	case 1:
		return j.val1
	case 2:
		return j.val2
	default:
		panic("invalid index")
	}
}

type join3 struct{ val1, val2, val3 value }

func (j join3) valueAt(idx int) value {
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
