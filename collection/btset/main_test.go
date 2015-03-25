package btset

import (
	"fmt"
	tu "github.com/klingtnet/gol/util/testing"
	"testing"
)

func TestBinarySearchL(t *testing.T) {
	xs := []int{1, 14, 37, 109, 110, 385, 583}
	tu.ExpectEqual(t, binarySearchL(xs, 0, len(xs), 10), 1)
}

func TestConj(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj(i)
		fmt.Println(set.lookup(i))
	}
}
