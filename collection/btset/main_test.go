package btset

import (
	crypto_rand "crypto/rand"
	"encoding/hex"
	tu "github.com/klingtnet/gol/util/testing"
	"math/rand"
	"testing"
	"time"

	c "github.com/heyLu/mu/comparable"
)

func TestCompare(t *testing.T) {
	tu.ExpectEqual(t, c.Eq(c.Int(1), c.Int(1)), true)
}

func TestBinarySearchL(t *testing.T) {
	xs := []interface{}{c.Int(1), c.Int(14), c.Int(37), c.Int(109), c.Int(110), c.Int(385), c.Int(583)}
	cmp := func(a, b interface{}) int {
		return a.(c.Int).Compare(b.(c.Int))
	}
	tu.ExpectEqual(t, binarySearchL(xs, 0, len(xs), c.Int(10), cmp), 1)
}

func TestConj(t *testing.T) {
	set := NewComparable()
	for i := 0; i < 1000; i++ {
		set = set.Conj(c.Int(i))
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.Lookup(c.Int(i)), c.Int(i))
	}

	for i := 0; i < 1000; i++ {
		tu.ExpectEqual(t, set.Lookup(c.Int(i)), c.Int(i))
	}
}

func TestConjStrings(t *testing.T) {
	set := NewComparable()
	num := 1000
	buf := make([]byte, 10)
	for i := 0; i < num; i++ {
		crypto_rand.Read(buf)
		s := hex.EncodeToString(buf)
		set = set.Conj(c.String(s))
	}

	tu.ExpectEqual(t, set.cnt, num)
}

func TestConjImmutable(t *testing.T) {
	set := NewComparable()
	for i := 0; i < 1000; i++ {
		v := c.Int(i)
		newSet := set.Conj(v)
		tu.ExpectEqual(t, newSet.cnt, i+1)
		expectEqual(t, newSet.Lookup(v).(c.Comparable), v)
		tu.ExpectEqual(t, set.Lookup(v), nil)
		set = newSet
	}
}

func TestConjRandom(t *testing.T) {
	set := NewComparable()
	for i := 0; i < 1000; i++ {
		n := c.Int(rand.Int())
		set = set.Conj(n)
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.Lookup(n), n)
	}
}

func TestDisj(t *testing.T) {
	set := NewComparable()
	for i := 0; i < 1000; i++ {
		set = set.Conj(c.Int(i))
	}

	for i := 0; i < 1000; i++ {
		v := c.Int(i)
		tu.RequireEqual(t, set.Lookup(v), v)
		set = set.Disj(v)
		tu.ExpectEqual(t, set.cnt, 1000-i-1)
		tu.RequireEqual(t, set.Lookup(v), nil)
	}
}

func TestIter(t *testing.T) {
	rand.Seed(time.Now().Unix())

	num := 1000
	ns := make([]c.Comparable, num)
	set := NewComparable()
	for i := 0; i < num; i++ {
		ns[i] = c.Int(rand.Intn(num * 1000))
		set = set.Conj(ns[i])
	}

	iter := set.Iter()
	i := 0
	var last c.Comparable = c.Int(-1)
	for iter != nil {
		i += 1
		tu.ExpectEqual(t, c.Lt(last, iter.First().(c.Comparable)), true)
		last = iter.First().(c.Comparable)
		iter = iter.Next()
	}
	tu.ExpectEqual(t, i, set.cnt)
}

func TestIterReverse(t *testing.T) {
	rand.Seed(time.Now().Unix())

	num := 1000
	ns := make([]c.Comparable, num)
	set := NewComparable()
	for i := 0; i < num; i++ {
		ns[i] = c.Int(rand.Intn(num * 1000))
		set = set.Conj(ns[i])
	}

	iter := set.Iter().Reverse()
	i := 0
	var last c.Comparable = c.Int(num * 1000)
	for iter != nil {
		i += 1
		tu.ExpectEqual(t, c.Gt(last, iter.First().(c.Comparable)), true)
		last = iter.First().(c.Comparable)
		iter = iter.Next()
	}
	tu.ExpectEqual(t, i, set.cnt)
}

func TestIterReverseTwice(t *testing.T) {
	num := 1000
	set := NewComparable()
	for i := 0; i < num; i++ {
		set = set.Conj(c.Int(rand.Intn(num * 1000)))
	}

	iter1 := set.Iter()
	iter2 := iter1.Reverse().Reverse()
	for iter1 != nil {
		tu.ExpectNotNil(t, iter2)
		tu.ExpectEqual(t, iter1.First(), iter2.First())
		iter1 = iter1.Next()
		iter2 = iter2.Next()
	}
}

func TestSlice(t *testing.T) {
	set := NewComparable()
	for i := 0; i < 1000; i++ {
		set = set.Conj(c.Int(rand.Intn(5000)))
	}

	iter := Slice(set, c.Int(300), c.Int(500))
	for iter != nil {
		tu.ExpectEqual(t, c.Int(300).Compare(iter.First().(c.Comparable)) <= 0 &&
			iter.First().(c.Comparable).Compare(c.Int(500)) <= 0, true)
		iter = iter.Next()
	}
}

func BenchmarkConj(b *testing.B) {
	set := NewComparable()
	for i := 0; i < b.N; i++ {
		set = set.Conj(c.Int(i))
	}
}

func BenchmarkConjRandom(b *testing.B) {
	set := NewComparable()
	for i := 0; i < b.N; i++ {
		set = set.Conj(c.Int(rand.Intn(b.N * 1000)))
	}
}

func BenchmarkLookup(b *testing.B) {
	set := NewComparable()
	for i := 0; i < 100000; i++ {
		set = set.Conj(c.Int(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		set.Lookup(c.Int(i))
	}
}

func expectEqual(t *testing.T, actual, expected c.Comparable) {
	if c.Neq(actual, expected) {
		t.Errorf("%#v != %#v", actual, expected)
	}
}
