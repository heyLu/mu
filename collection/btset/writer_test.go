package btset

import (
	"bytes"
	"github.com/heyLu/fressian"
	tu "github.com/klingtnet/gol/util/testing"
	"io"
	"os"
	"testing"
)

func TestWriteFile(t *testing.T) {
	set := New(func(a, b interface{}) int { return a.(int) - b.(int) })
	for i := 0; i < 1000; i++ {
		set = set.Conj(i)
	}

	f, err := os.Create("test.btset.fsn")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer os.Remove("test.btset.fsn")

	err = writeSet(f, set)
	if err != nil {
		t.Error(err)
	}
}

func TestWriting(t *testing.T) {
	buf := new(bytes.Buffer)

	cmp := func(a, b interface{}) int { return a.(int) - b.(int) }
	set := New(cmp)
	for i := 0; i < 10000; i++ {
		set = set.Conj(i)
	}

	err := writeSet(buf, set)
	tu.ExpectNil(t, err)

	set2, err := readSet(buf)
	tu.ExpectNil(t, err)

	tu.ExpectEqual(t, set.Count(), set2.Count())
	iter1 := set.Iter()
	iter2 := set.Iter()
	for iter1 != nil {
		tu.ExpectNotNil(t, iter2)
		tu.ExpectEqual(t, iter1.First(), iter2.First())
		iter1 = iter1.Next()
		iter2 = iter2.Next()
	}
}

func writeSet(iow io.Writer, set *Set) error {
	w := fressian.NewWriter(iow, WriteHandler)
	defer w.Flush()
	return w.WriteValue(set)
}

func readSet(ior io.Reader) (*Set, error) {
	r := fressian.NewReader(ior, ReadHandlers)
	val, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	return val.(*Set), nil
}
