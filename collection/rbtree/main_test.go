package rbtree

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
)

type num int

func (n num) Less(o Ord) bool  { return n < o.(num) }
func (n num) Equal(o Ord) bool { return n == o.(num) }

func TestEmpty(t *testing.T) {
	tree := Empty()
	if tree != nil {
		t.Error("t must be nil")
	}
}

func TestInsert(t *testing.T) {
	tree := Empty().Add(num(3)).Add(num(4)).Add(num(5)).Add(num(42))
	ns := []num{1, 2, 3, 4, 5, 40, 42}
	for _, n := range ns {
		fmt.Println(n, tree.Contains(n))
	}
	fmt.Println()

	printTree("", tree)
}

type str string

func (s str) Less(o Ord) bool  { return s < o.(str) }
func (s str) Equal(o Ord) bool { return s == o.(str) }

func TestInsertStrings(t *testing.T) {
	tree := Empty()
	buf := make([]byte, 5)

	for i := 0; i < 17; i++ {
		rand.Read(buf)
		tree = tree.Add(str(hex.EncodeToString(buf)))
	}

	printTree("", tree)

	fmt.Println()
	keys := tree.Keys()
	for v := keys.Next(); v != nil; v = keys.Next() {
		fmt.Println(v)
	}
}

func printTree(indent string, t *Tree) {
	if t == nil {
		return
	}

	printTree(indent+"  ", t.l)
	fmt.Printf("%s%t %v\n", indent, t.c, t.v)
	printTree(indent+"  ", t.r)
}
