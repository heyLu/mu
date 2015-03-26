package btset

import (
	//crypto_rand "crypto/rand"
	//"encoding/hex"
	tu "github.com/klingtnet/gol/util/testing"
	"math/rand"
	"testing"
	"time"
)

func TestCompare(t *testing.T) {
	tu.ExpectEqual(t, eq((1), (1)), true)
}

func TestBinarySearchL(t *testing.T) {
	xs := []interface{}{(1), (14), (37), (109), (110), (385), (583)}
	tu.ExpectEqual(t, binarySearchL(xs, 0, len(xs), (10)), 1)
}

func TestConj(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj((i))
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.lookup((i)), (i))
	}

	for i := 0; i < 1000; i++ {
		tu.ExpectEqual(t, set.lookup((i)), (i))
	}
}

/*func TestConjStrings(t *testing.T) {
	set := New()
	num := 1000
	buf := make([]byte, 10)
	for i := 0; i < num; i++ {
		crypto_rand.Read(buf)
		s := hex.EncodeToString(buf)
		set = set.conj((s))
	}

	tu.ExpectEqual(t, set.cnt, num)
}*/

func TestConjImmutable(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		v := (i)
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
		n := (rand.Int())
		set = set.conj(n)
		tu.ExpectEqual(t, set.cnt, i+1)
		tu.ExpectEqual(t, set.lookup(n), n)
	}
}

func TestDisj(t *testing.T) {
	set := New()
	for i := 0; i < 1000; i++ {
		set = set.conj((i))
	}

	for i := 0; i < 1000; i++ {
		v := (i)
		tu.RequireEqual(t, set.lookup(v), v)
		set = set.disj(v)
		tu.ExpectEqual(t, set.cnt, 1000-i-1)
		tu.RequireEqual(t, set.lookup(v), nil)
	}
}

func TestIter(t *testing.T) {
	rand.Seed(time.Now().Unix())

	num := 1000
	ns := make([]interface{}, num)
	set := New()
	for i := 0; i < num; i++ {
		ns[i] = (rand.Intn(num * 1000))
		set = set.conj(ns[i])
	}

	iter := set.iter()
	i := 0
	var last interface{} = (-1)
	for iter != nil {
		i += 1
		tu.ExpectEqual(t, compare(last, iter.first()) < 0, true)
		last = iter.first()
		iter = iter.next()
	}
	tu.ExpectEqual(t, i, set.cnt)
}

func TestIterReverse(t *testing.T) {
	rand.Seed(time.Now().Unix())

	num := 1000
	ns := make([]interface{}, num)
	set := New()
	for i := 0; i < num; i++ {
		ns[i] = (rand.Intn(num * 1000))
		set = set.conj(ns[i])
	}

	iter := set.iter().reverse()
	i := 0
	var last interface{} = (num * 1000)
	for iter != nil {
		i += 1
		tu.ExpectEqual(t, compare(last, iter.first()) > 0, true)
		last = iter.first()
		iter = iter.next()
	}
	tu.ExpectEqual(t, i, set.cnt)
}

func TestIterReverseTwice(t *testing.T) {
	num := 1000
	set := New()
	for i := 0; i < num; i++ {
		set = set.conj((rand.Intn(num * 1000)))
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
		set = set.conj((rand.Intn(5000)))
	}

	iter := slice(set, (300), (500))
	for iter != nil {
		tu.ExpectEqual(t, compare(300, iter.first()) <= 0 && compare(iter.first(), 500) <= 0, true)
		iter = iter.next()
	}
}

func BenchmarkConj(b *testing.B) {
	set := New()
	for i := 0; i < b.N; i++ {
		set = set.conj((i))
	}
}

func BenchmarkConjRandom(b *testing.B) {
	set := New()
	for i := 0; i < b.N; i++ {
		set = set.conj((rand.Intn(b.N * 1000)))
	}
}

func BenchmarkLookup(b *testing.B) {
	set := New()
	for i := 0; i < 100000; i++ {
		set = set.conj((i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		set.lookup((i))
	}
}

func expectEqual(t *testing.T, actual, expected interface{}) {
	if compare(actual, expected) != 0 {
		t.Errorf("%#v != %#v", actual, expected)
	}
}
