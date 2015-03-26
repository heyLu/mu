package Comparable

type Comparable interface {
	Compare(Comparable) int
}

func Lt(a, b Comparable) bool  { return a.Compare(b) < 0 }
func Lte(a, b Comparable) bool { return a.Compare(b) <= 0 }
func Gt(a, b Comparable) bool  { return a.Compare(b) > 0 }
func Gte(a, b Comparable) bool { return a.Compare(b) >= 0 }
func Eq(a, b Comparable) bool  { return a.Compare(b) == 0 }
func Neq(a, b Comparable) bool { return a.Compare(b) != 0 }

type Int int

func (i Int) Compare(c Comparable) int {
	return int(i - c.(Int))
}

type String string

func (s String) Compare(c Comparable) int {
	if s == c.(String) {
		return 0
	} else if s < c.(String) {
		return -1
	} else {
		return 1
	}
}
