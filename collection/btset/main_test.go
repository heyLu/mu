package btset

import (
	tu "github.com/klingtnet/gol/util/testing"
	"math/rand"
	"testing"
	"time"
)

func TestBinarySearchL(t *testing.T) {
	xs := []int{1, 14, 37, 109, 110, 385, 583}
	tu.ExpectEqual(t, binarySearchL(xs, 0, len(xs), 10), 1)
}

func TestConj(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj(i)
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.lookup(i), i)
	}

	for i := 0; i < 1000; i++ {
		tu.ExpectEqual(t, set.lookup(i), i)
	}
}

func TestConjImmutable(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		newSet := set.conj(i)
		tu.ExpectEqual(t, newSet.cnt, i+1)
		tu.ExpectEqual(t, newSet.lookup(i), i)
		tu.ExpectEqual(t, set.lookup(i), -1)
		set = newSet
	}
}

func TestConjRandom(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		n := rand.Int()
		set = set.conj(n)
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.lookup(n), n)
	}
}

func TestDisj(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj(i)
	}

	for i := 0; i < 1000; i++ {
		tu.RequireEqual(t, set.lookup(i), i)
		set = set.disj(i)
		tu.ExpectEqual(t, set.cnt, 1000-i-1)
		tu.RequireEqual(t, set.lookup(i), -1)
	}
}

func TestIter(t *testing.T) {
	rand.Seed(time.Now().Unix())

	num := 1000
	ns := make([]int, num)
	set := New()
	for i := 0; i < num; i++ {
		ns[i] = rand.Intn(num * 1000)
		set = set.conj(ns[i])
	}

	iter := set.iter()
	i := 0
	for iter != nil {
		i += 1
		iter = iter.next()
	}
	tu.ExpectEqual(t, i, set.cnt)
}

func TestSlice(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj(rand.Intn(5000))
	}

	iter := slice(set, 300, 500)
	for iter != nil {
		tu.ExpectEqual(t, 300 <= iter.first() && iter.first() <= 500, true)
		iter = iter.next()
	}
}

func BenchmarkConj(b *testing.B) {
	set := New()
	for i := 0; i < b.N; i++ {
		set = set.conj(i)
	}
}

func BenchmarkLookup(b *testing.B) {
	set := New()
	for i := 0; i < 100000; i++ {
		set = set.conj(i)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		set.lookup(i)
	}
}
