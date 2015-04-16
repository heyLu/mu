package index

import (
	"github.com/heyLu/fressian"
	"testing"
)

func TestValueCompare(t *testing.T) {
	expectLt(t, NewValue(false), NewValue(true))
	expectEq(t, NewValue(true), NewValue(true))
	expectGt(t, NewValue(true), NewValue(false))

	expectLt(t, NewValue(3), NewValue(4))
	expectEq(t, NewValue(4), NewValue(4))
	expectGt(t, NewValue(5), NewValue(4))

	expectLt(t, NewValue("a"), NewValue("b"))
	expectEq(t, NewValue("b"), NewValue("b"))
	expectGt(t, NewValue("c"), NewValue("b"))

	expectLt(t, NewValue(0), NewValue(""))
	expectLt(t, NewValue(0), NewValue(fressian.Key{"", ""}))
	expectLt(t, NewValue(fressian.Key{"", ""}), NewValue(""))
}

func expectLt(t *testing.T, v1, v2 Value) {
	if v1.Compare(v2) >= 0 {
		t.Errorf("expected %#v < %#v", v1, v2)
	}
}

func expectEq(t *testing.T, v1, v2 Value) {
	if v1.Compare(v2) != 0 {
		t.Errorf("expected %#v == %#v", v1, v2)
	}
}

func expectGt(t *testing.T, v1, v2 Value) {
	if v1.Compare(v2) <= 0 {
		t.Errorf("expected %#v > %#v", v1, v2)
	}
}
