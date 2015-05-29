package index

import (
	"github.com/heyLu/fressian"
	tu "github.com/klingtnet/gol/util/testing"
	"testing"
)

var (
	d1 = &Datom{0, 1, NewValue("hey"), 2, true}
	d2 = &Datom{1, 1, NewValue("ho"), 2, true}
	d3 = &Datom{2, 0, NewValue("huh"), 2, true}
)

func TestCompareEavt(t *testing.T) {
	tu.ExpectEqual(t, CompareEavt(d1, d2), -1)
	tu.ExpectEqual(t, CompareEavt(d2, d1), 1)
	tu.ExpectEqual(t, CompareEavt(d1, d1), 0)
}

func TestCompareAevt(t *testing.T) {
	tu.ExpectEqual(t, CompareAevt(d1, d2), -1)
	tu.ExpectEqual(t, CompareAevt(d3, d1), -1)
}

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
	expectLt(t, NewValue(0), NewValue(fressian.Keyword{"", ""}))
	expectLt(t, NewValue(fressian.Keyword{"", ""}), NewValue(""))
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
