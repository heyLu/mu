package btset

import (
	tu "github.com/klingtnet/gol/util/testing"
	"math/rand"
	"testing"
	"time"

	c "../../comparable"
)

func TestCompare(t *testing.T) {
	tu.ExpectEqual(t, c.Eq(c.Int(1), c.Int(1)), true)
}

func TestBinarySearchL(t *testing.T) {
	xs := []c.Comparable{c.Int(1), c.Int(14), c.Int(37), c.Int(109), c.Int(110), c.Int(385), c.Int(583)}
	tu.ExpectEqual(t, binarySearchL(xs, 0, len(xs), c.Int(10)), 1)
}

func TestConj(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj(c.Int(i))
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.lookup(c.Int(i)), c.Int(i))
	}

	for i := 0; i < 1000; i++ {
		tu.ExpectEqual(t, set.lookup(c.Int(i)), c.Int(i))
	}
}

func TestConjImmutable(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		v := c.Int(i)
		newSet := set.conj(v)
		tu.ExpectEqual(t, newSet.cnt, i+1)
		expectEqual(t, newSet.lookup(v), v)
		tu.ExpectEqual(t, set.lookup(v), nil)
		set = newSet
	}
}

func TestConjRandom(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		n := c.Int(rand.Int())
		set = set.conj(n)
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.lookup(n), n)
	}
}

func TestDisj(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj(c.Int(i))
	}

	for i := 0; i < 1000; i++ {
		v := c.Int(i)
		tu.RequireEqual(t, set.lookup(v), v)
		set = set.disj(v)
		tu.ExpectEqual(t, set.cnt, 1000-i-1)
		tu.RequireEqual(t, set.lookup(v), nil)
	}
}

func TestIter(t *testing.T) {
	rand.Seed(time.Now().Unix())

	num := 1000
	ns := make([]c.Comparable, num)
	set := New()
	for i := 0; i < num; i++ {
		ns[i] = c.Int(rand.Intn(num * 1000))
		set = set.conj(ns[i])
	}

	iter := set.iter()
	i := 0
	var last c.Comparable = c.Int(-1)
	for iter != nil {
		i += 1
		tu.ExpectEqual(t, c.Lt(last, iter.first()), true)
		last = iter.first()
		iter = iter.next()
	}
	tu.ExpectEqual(t, i, set.cnt)
}

func TestIterReverse(t *testing.T) {
	rand.Seed(time.Now().Unix())

	num := 1000
	ns := make([]c.Comparable, num)
	set := New()
	for i := 0; i < num; i++ {
		ns[i] = c.Int(rand.Intn(num * 1000))
		set = set.conj(ns[i])
	}

	iter := set.iter().reverse()
	i := 0
	var last c.Comparable = c.Int(num * 1000)
	for iter != nil {
		i += 1
		tu.ExpectEqual(t, c.Gt(last, iter.first()), true)
		last = iter.first()
		iter = iter.next()
	}
	tu.ExpectEqual(t, i, set.cnt)
}

func TestIterReverseTwice(t *testing.T) {
	num := 1000
	set := New()
	for i := 0; i < num; i++ {
		set = set.conj(c.Int(rand.Intn(num * 1000)))
	}

	iter1 := set.iter()
	iter2 := iter1.reverse().reverse()
	for iter1 != nil {
		tu.ExpectNotNil(t, iter2)
		tu.ExpectEqual(t, iter1.first(), iter2.first())
		iter1 = iter1.next()
		iter2 = iter2.next()
	}
}

func TestSlice(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj(c.Int(rand.Intn(5000)))
	}

	iter := slice(set, c.Int(300), c.Int(500))
	for iter != nil {
		tu.ExpectEqual(t, c.Int(300).Compare(iter.first()) <= 0 && iter.first().Compare(c.Int(500)) <= 0, true)
		iter = iter.next()
	}
}

func BenchmarkConj(b *testing.B) {
	set := New()
	for i := 0; i < b.N; i++ {
		set = set.conj(c.Int(i))
	}
}

func BenchmarkConjRandom(b *testing.B) {
	set := New()
	for i := 0; i < b.N; i++ {
		set = set.conj(c.Int(rand.Intn(b.N * 1000)))
	}
}

func BenchmarkLookup(b *testing.B) {
	set := New()
	for i := 0; i < 100000; i++ {
		set = set.conj(c.Int(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		set.lookup(c.Int(i))
	}
}

func expectEqual(t *testing.T, actual, expected c.Comparable) {
	if c.Neq(actual, expected) {
		t.Errorf("%#v != %#v", actual, expected)
	}
}
