package btset

import (
	tu "github.com/klingtnet/gol/util/testing"
	"math/rand"
	"testing"
	"time"
)

func TestCompare(t *testing.T) {
	tu.ExpectEqual(t, eq(Int(1), Int(1)), true)
}

func TestBinarySearchL(t *testing.T) {
	xs := []comparable{Int(1), Int(14), Int(37), Int(109), Int(110), Int(385), Int(583)}
	tu.ExpectEqual(t, binarySearchL(xs, 0, len(xs), Int(10)), 1)
}

func TestConj(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj(Int(i))
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.lookup(Int(i)), Int(i))
	}

	for i := 0; i < 1000; i++ {
		tu.ExpectEqual(t, set.lookup(Int(i)), Int(i))
	}
}

func TestConjImmutable(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		v := Int(i)
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
		n := Int(rand.Int())
		set = set.conj(n)
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.lookup(n), n)
	}
}

func TestDisj(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj(Int(i))
	}

	for i := 0; i < 1000; i++ {
		v := Int(i)
		tu.RequireEqual(t, set.lookup(v), v)
		set = set.disj(v)
		tu.ExpectEqual(t, set.cnt, 1000-i-1)
		tu.RequireEqual(t, set.lookup(v), nil)
	}
}

func TestIter(t *testing.T) {
	rand.Seed(time.Now().Unix())

	num := 1000
	ns := make([]comparable, num)
	set := New()
	for i := 0; i < num; i++ {
		ns[i] = Int(rand.Intn(num * 1000))
		set = set.conj(ns[i])
	}

	iter := set.iter()
	i := 0
	var last comparable = Int(-1)
	for iter != nil {
		i += 1
		tu.ExpectEqual(t, lt(last, iter.first()), true)
		last = iter.first()
		iter = iter.next()
	}
	tu.ExpectEqual(t, i, set.cnt)
}

func TestIterReverse(t *testing.T) {
	rand.Seed(time.Now().Unix())

	num := 1000
	ns := make([]comparable, num)
	set := New()
	for i := 0; i < num; i++ {
		ns[i] = Int(rand.Intn(num * 1000))
		set = set.conj(ns[i])
	}

	iter := set.iter().reverse()
	i := 0
	var last comparable = Int(num * 1000)
	for iter != nil {
		i += 1
		tu.ExpectEqual(t, gt(last, iter.first()), true)
		last = iter.first()
		iter = iter.next()
	}
	tu.ExpectEqual(t, i, set.cnt)
}

func TestIterReverseTwice(t *testing.T) {
	num := 1000
	set := New()
	for i := 0; i < num; i++ {
		set = set.conj(Int(rand.Intn(num * 1000)))
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
		set = set.conj(Int(rand.Intn(5000)))
	}

	iter := slice(set, Int(300), Int(500))
	for iter != nil {
		tu.ExpectEqual(t, Int(300).compare(iter.first()) <= 0 && iter.first().compare(Int(500)) <= 0, true)
		iter = iter.next()
	}
}

func BenchmarkConj(b *testing.B) {
	set := New()
	for i := 0; i < b.N; i++ {
		set = set.conj(Int(i))
	}
}

func BenchmarkConjRandom(b *testing.B) {
	set := New()
	for i := 0; i < b.N; i++ {
		set = set.conj(Int(rand.Intn(b.N * 1000)))
	}
}

func BenchmarkLookup(b *testing.B) {
	set := New()
	for i := 0; i < 100000; i++ {
		set = set.conj(Int(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		set.lookup(Int(i))
	}
}

func expectEqual(t *testing.T, actual, expected comparable) {
	if neq(actual, expected) {
		t.Errorf("%#v != %#v", actual, expected)
	}
}
