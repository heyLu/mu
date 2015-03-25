// Package btset implements an immutable B+-tree.
package btset

import (
	"log"
)

func half(x int) int { return x >> 1 }

const (
	minLen     = 64
	maxLen     = 128
	avgLen     = (maxLen + minLen) >> 1
	levelShift = 8
	pathMask   = (1 << levelShift) - 1
	emptyPath  = 0
)

// TODO: make it customizable, à la *cmp*
// likely a Comparable interface, with a Less or Compare method
func compare(a, b int) int {
	return a - b
}

func pathGet(path, level int) int {
	return pathMask & (path >> uint(level))
}

func pathSet(path, level, idx int) int {
	return path | (idx << uint(level))
}

func eq(a, b int) bool {
	return 0 == compare(a, b)
}

func binarySearchL(arr []int, l, r, k int) int {
	for {
		if l <= r {
			m := half(l + r)
			mk := arr[m]
			cmp := compare(mk, k)
			if cmp < 0 {
				l = m + 1
			} else {
				r = m - 1
			}
		} else {
			return l
		}
	}
}

func binarySearchR(arr []int, l, r, k int) int {
	for {
		if l <= r {
			m := half(l + r)
			mk := arr[m]
			cmp := compare(mk, k)
			if cmp > 0 {
				r = m - 1
			} else {
				l = m + 1
			}
		} else {
			return l
		}
	}
}

func lookupExact(arr []int, key int) int {
	arrL := len(arr)
	idx := binarySearchL(arr, 0, arrL-1, key)
	if idx < arrL && eq(arr[idx], key) {
		return idx
	} else {
		return -1
	}
}

func lookupRange(arr []int, key int) int {
	arrL := len(arr)
	idx := binarySearchL(arr, 0, arrL-1, key)
	if idx == arrL {
		return -1
	} else {
		return idx
	}
}

// Array operations

func aLast(arr []int) int {
	return arr[len(arr)-1]
}

func aConcat(a1, a2 []int) []int {
	return append(a1, a2...)
}

func cutNSplice(arr []int, cutFrom, cutTo, spliceFrom, spliceTo int, xs []int) []int {
	var (
		xsL    = len(xs)
		l1     = spliceFrom - cutFrom
		l2     = cutTo - spliceTo
		l1xs   = l1 + xsL
		newArr = make([]int, l1+xsL+l2)
	)

	for i := 0; i < l1; i++ {
		newArr[i] = arr[cutFrom+i]
	}
	for i := 0; i < xsL; i++ {
		newArr[i+l1] = xs[i]
	}
	for i := 0; i < l2; i++ {
		newArr[i+l1xs] = arr[spliceTo+i]
	}

	return newArr
}

func cutAll(arr []int, cutFrom int) []int {
	return arr[cutFrom:]
}

func cut(arr []int, cutFrom, cutTo int) []int {
	return arr[cutFrom:cutTo]
}

func splice(arr []int, spliceFrom, spliceTo int, xs []int) []int {
	return cutNSplice(arr, 0, len(arr), spliceFrom, spliceTo, xs)
}

func insert(arr []int, idx int, xs []int) []int {
	return cutNSplice(arr, 0, len(arr), idx, idx, xs)
}

func mergeNSplit(a1, a2 []int) *[2][]int {
	var (
		a1L            = len(a1)
		a2L            = len(a2)
		totalL         = a1L + a2L
		r1L            = half(totalL)
		r2L            = totalL - r1L
		r1             = make([]int, r1L)
		r2             = make([]int, r2L)
		toA, fromA     []int
		toIdx, fromIdx int
	)

	for i := 0; i < totalL; i++ {
		if i < r1L {
			toA = r1
			toIdx = i
		} else {
			toA = r2
			toIdx = i - r1L
		}

		if i < a1L {
			fromA = a1
			fromIdx = i
		} else {
			fromA = a2
			fromIdx = i - a1L
		}

		toA[toIdx] = fromA[fromIdx]
	}

	return &[2][]int{r1, r2}
}

func aConcatNodes(a1, a2 []anyNode) []anyNode {
	return append(a1, a2...)
}

func cutNSpliceNodes(arr []anyNode, cutFrom, cutTo, spliceFrom, spliceTo int, xs []anyNode) []anyNode {
	var (
		xsL    = len(xs)
		l1     = spliceFrom - cutFrom
		l2     = cutTo - spliceTo
		l1xs   = l1 + xsL
		newArr = make([]anyNode, l1+xsL+l2)
	)

	for i := 0; i < l1; i++ {
		newArr[i] = arr[cutFrom+i]
	}
	for i := 0; i < xsL; i++ {
		newArr[i+l1] = xs[i]
	}
	for i := 0; i < l2; i++ {
		newArr[i+l1xs] = arr[spliceTo+i]
	}

	return newArr
}

func cutAllNodes(arr []anyNode, cutFrom int) []anyNode {
	return arr[cutFrom:]
}

func cutNodes(arr []anyNode, cutFrom, cutTo int) []anyNode {
	return arr[cutFrom:cutTo]
}

func spliceNodes(arr []anyNode, spliceFrom, spliceTo int, xs []anyNode) []anyNode {
	return cutNSpliceNodes(arr, 0, len(arr), spliceFrom, spliceTo, xs)
}

func insertNodes(arr []anyNode, idx int, xs []anyNode) []anyNode {
	return cutNSpliceNodes(arr, 0, len(arr), idx, idx, xs)
}

func mergeNSplitNodes(a1, a2 []anyNode) *[2][]anyNode {
	var (
		a1L            = len(a1)
		a2L            = len(a2)
		totalL         = a1L + a2L
		r1L            = half(totalL)
		r2L            = totalL - r1L
		r1             = make([]anyNode, r1L)
		r2             = make([]anyNode, r2L)
		toA, fromA     []anyNode
		toIdx, fromIdx int
	)

	for i := 0; i < totalL; i++ {
		if i < r1L {
			toA = r1
			toIdx = i
		} else {
			toA = r2
			toIdx = i - r1L
		}

		if i < a1L {
			fromA = a1
			fromIdx = i
		} else {
			fromA = a2
			fromIdx = i - a1L
		}

		toA[toIdx] = fromA[fromIdx]
	}

	return &[2][]anyNode{r1, r2}
}

func eqArr(a1 []int, a1From, a1To int, a2 []int, a2From, a2To int, eq func(a, b int) bool) bool {
	l := a1To - a1From

	if l != a2To-a2From {
		return false
	}

	i := 0
	for {
		if i == l {
			return true
		} else if !eq(a1[a1From+i], a2[a2From+i]) {
			return false
		} else {
			i += 1
		}
	}

	return false
}

func checkNSplice(arr []int, from, to int, newArr []int) []int {
	if eqArr(arr, from, to, newArr, 0, len(newArr), eq) {
		return arr
	} else {
		return splice(arr, from, to, newArr)
	}
}

func arrMapNodes(f func(anyNode) int, arr []anyNode) []int {
	l := len(arr)
	newArr := make([]int, l)
	for i := 0; i < l; i++ {
		newArr[i] = f(arr[i])
	}
	return newArr
}

func arrMapInplace(f func(int) int, arr []int) []int {
	l := len(arr)
	for i := 0; i < l; i++ {
		arr[i] = f(arr[i])
	}
	return arr
}

func arrPartitionApprox(minLen, maxLen int, arr []int) [][]int {
	var (
		chunkLen = avgLen
		l        = len(arr)
		acc      = make([][]int, 0)
	)

	if l == 0 {
		return acc
	}

	pos := 0
	for {
		rest := l - pos

		if rest <= maxLen {
			acc = append(acc, cutAll(arr, pos))
			break
		} else if rest >= chunkLen+maxLen {
			acc = append(acc, cut(arr, pos, pos+chunkLen))
			pos += chunkLen
		} else {
			pieceLen := half(rest)
			acc = append(acc, cut(arr, pos, pos+pieceLen))
			pos += pieceLen
		}
	}

	return acc
}

func arrDistinct(arr []int, cmp func(a, b int) int) []int {
	i := 0
	for {
		if i >= len(arr) {
			break
		}
		if i > 0 && 0 == cmp(arr[i], arr[i-1]) {
			arr = append(arr[:i], arr[i+1:]...)
		} else {
			i += 1
		}
	}
	return arr
}

//

func limKey(node anyNode) int { return aLast(node.getkeys()) }

func returnArray(a1, a2, a3 anyNode) []anyNode {
	if a1 != nil {
		if a2 != nil {
			if a3 != nil {
				return []anyNode{a1, a2, a3}
			} else {
				return []anyNode{a1, a2}
			}
		} else {
			if a3 != nil {
				return []anyNode{a1, a3}
			} else {
				return []anyNode{a1}
			}
		}
	} else {
		if a2 != nil {
			if a3 != nil {
				return []anyNode{a2, a3}
			} else {
				return []anyNode{a2}
			}
		} else {
			return []anyNode{a3}
			/*if a3 != nil {
				return []anyNode{a3}
			} else {
				return []anyNode{}
			}*/
		}
	}
}

func rotate(node anyNode, isRoot bool, left, right anyNode) []anyNode {
	if isRoot { // root never merges
		return []anyNode{node}
	} else if node.length() > minLen { // enough keys, nothing to merge
		return returnArray(left, node, right)
	} else if left != nil && left.length() <= minLen { // left and this can be merged into one
		return returnArray(nil, left.merge(node), right)
	} else if right != nil && right.length() <= minLen { // right and this can be merged into one
		return returnArray(left, node.merge(right), nil)
	} else if left != nil && (right == nil || left.length() < right.length()) { // left has fewer nodes, redistribute with it
		nodes := left.mergeNSplit(node)
		return returnArray(nodes[0], nodes[1], right)
	} else { // right has fewer nodes, redistribute with it
		nodes := node.mergeNSplit(right)
		return returnArray(left, nodes[0], nodes[1])
	}
}

type anyNode interface {
	length() int
	getkeys() []int
	merge(node anyNode) anyNode
	mergeNSplit(node anyNode) []anyNode
	lookup(key int) int
	conj(key int) []anyNode
	disj(key int, isRoot bool, left, right anyNode) []anyNode
}

type pointerNode struct {
	keys     []int
	pointers []anyNode
}

func (n *pointerNode) length() int    { return len(n.keys) }
func (n *pointerNode) getkeys() []int { return n.keys }

func (n *pointerNode) lookup(key int) int {
	idx := lookupRange(n.keys, key)
	if idx != -1 {
		return n.pointers[idx].lookup(key)
	} else {
		return -1
	}
}

func (n *pointerNode) conj(key int) []anyNode {
	idx := binarySearchL(n.keys, 0, len(n.keys)-2, key)
	nodes := n.pointers[idx].conj(key)

	if nodes == nil {
		return nil
	}

	newKeys := checkNSplice(n.keys, idx, idx+1, arrMapNodes(limKey, nodes))
	newPointers := spliceNodes(n.pointers, idx, idx+1, nodes)

	if len(newPointers) <= maxLen {
		return []anyNode{&pointerNode{newKeys, newPointers}}
	} else {
		middle := half(len(newPointers))
		return []anyNode{
			&pointerNode{cut(newKeys, 0, middle), cutNodes(newPointers, 0, middle)},
			&pointerNode{cutAll(newKeys, middle), cutAllNodes(newPointers, middle)},
		}
	}

	log.Fatal("unreachable")
	return nil
}

func (n *pointerNode) merge(next anyNode) anyNode {
	return &pointerNode{
		aConcat(n.keys, next.(*pointerNode).keys),
		aConcatNodes(n.pointers, next.(*pointerNode).pointers)}
}

func (n *pointerNode) mergeNSplit(next anyNode) []anyNode {
	ks := mergeNSplit(n.keys, next.(*pointerNode).keys)
	ps := mergeNSplitNodes(n.pointers, next.(*pointerNode).pointers)
	return []anyNode{
		&pointerNode{ks[0], ps[0]},
		&pointerNode{ks[1], ps[1]}}
}

func (n *pointerNode) disj(key int, isRoot bool, left, right anyNode) []anyNode {
	idx := lookupRange(n.keys, key)

	if -1 == idx { // short-circuit, key not here
		return nil
	}

	child := n.pointers[idx]
	var (
		leftChild  anyNode = nil
		rightChild anyNode = nil
	)
	if idx-1 >= 0 {
		leftChild = n.pointers[idx-1]
	}
	if idx+1 < len(n.pointers) {
		rightChild = n.pointers[idx+1]
	}
	disjned := child.disj(key, false, leftChild, rightChild)

	if disjned == nil {
		return nil
	}

	leftIdx := idx
	if leftChild != nil {
		leftIdx = idx - 1
	}
	rightIdx := idx + 1
	if rightChild != nil {
		rightIdx = idx + 2
	}

	newKeys := checkNSplice(n.keys, leftIdx, rightIdx, arrMapNodes(limKey, disjned))
	newPointers := spliceNodes(n.pointers, leftIdx, rightIdx, disjned)

	return rotate(&pointerNode{newKeys, newPointers}, isRoot, left, right)
}

type leafNode struct {
	keys []int // actually values
}

func (n *leafNode) length() int    { return len(n.keys) }
func (n *leafNode) getkeys() []int { return n.keys }

func (n *leafNode) lookup(key int) int {
	idx := lookupExact(n.keys, key)

	if -1 == idx {
		return -1
	}

	return n.keys[idx]
}

func (n *leafNode) conj(key int) []anyNode {
	idx := binarySearchL(n.keys, 0, len(n.keys)-1, key)
	keysL := len(n.keys)

	if idx < keysL && eq(key, n.keys[idx]) { // already there
		return nil
	} else if keysL == maxLen { // splitting
		middle := half(keysL + 1)
		if idx > middle { // new key goes to second half
			return []anyNode{
				&leafNode{cut(n.keys, 0, middle)},
				&leafNode{cutNSplice(n.keys, middle, keysL, idx, idx, []int{key})},
			}
		} else { // new key goes to first half
			return []anyNode{
				&leafNode{cutNSplice(n.keys, 0, middle, idx, idx, []int{key})},
				&leafNode{cut(n.keys, middle, keysL)},
			}
		}
	} else { // ok as is
		return []anyNode{&leafNode{splice(n.keys, idx, idx, []int{key})}}
	}

	log.Fatal("unreachable")
	return nil
}

func (n *leafNode) merge(next anyNode) anyNode {
	return &leafNode{aConcat(n.keys, next.(*leafNode).keys)}
}

func (n *leafNode) mergeNSplit(next anyNode) []anyNode {
	ks := mergeNSplit(n.keys, next.(*leafNode).keys)
	return returnArray(&leafNode{ks[0]}, &leafNode{ks[1]}, nil)
}

func (n *leafNode) disj(key int, isRoot bool, left, right anyNode) []anyNode {
	idx := lookupExact(n.keys, key)

	if -1 == idx {
		return nil
	}

	newKeys := splice(n.keys, idx, idx+1, []int{})
	return rotate(&leafNode{newKeys}, isRoot, left, right)
}

func btsetConj(set *Set, key int) *Set {
	roots := set.root.conj(key)

	if roots == nil { // nothing changed
		return set
	} else if len(roots) == 1 { // keeping single root
		return alterSet(set, roots[0], set.shift, set.cnt+1)
	} else { // introducing new root
		return alterSet(set, &pointerNode{arrMapNodes(limKey, roots), roots}, set.shift+levelShift, set.cnt+1)
	}
}

func btsetDisj(set *Set, key int) *Set {
	newRoots := set.root.disj(key, true, nil, nil)

	if newRoots == nil { // nothing changed, key wasn't in set
		return set
	}

	newRoot := newRoots[0]
	if nr, ok := newRoot.(*pointerNode); ok && len(nr.pointers) == 1 { // root has one child, make it the new root
		return alterSet(set, nr.pointers[0], set.shift-levelShift, set.cnt-1)
	} else { // keeping root level
		return alterSet(set, newRoot, set.shift, set.cnt-1)
	}
}

// iteration

func keysFor(set *Set, path int) []int {
	level := set.shift
	node := set.root
	for {
		if level > 0 {
			node = node.(*pointerNode).pointers[pathGet(path, level)]
			level -= levelShift
		} else {
			return node.(*leafNode).keys
		}
	}
}

func nextPathNode(node anyNode, path int, level int) int {
	idx := pathGet(path, level)
	if level > 0 { // inner node
		subPath := nextPathNode(node.(*pointerNode).pointers[idx], path, level-levelShift)
		if -1 == subPath { // nested node overflow
			if idx+1 < len(node.(*pointerNode).pointers) { // advance current node idx, reset subsequent indexes
				return pathSet(emptyPath, level, idx+1)
			} else { // current node overflow
				return -1
			}
		} else { // keep current idx
			return pathSet(subPath, level, idx)
		}
	} else { // leaf
		if idx+1 < len(node.(*leafNode).keys) { // advance leaf idx
			return pathSet(emptyPath, 0, idx+1)
		} else { // leaf overflow
			return -1
		}
	}

	log.Fatal("unreachable")
	return -1
}

func nextPath(set *Set, path int) int {
	return nextPathNode(set.root, path, set.shift)
}

func rpath(node anyNode, level int) int {
	path := emptyPath
	for {
		if level > 0 {
			ps := node.(*pointerNode).pointers
			node = ps[len(ps)-1]
			path = pathSet(path, level, len(ps)-1)
			level -= levelShift
		} else {
			return pathSet(path, 0, len(node.(*leafNode).keys)-1)
		}
	}
}

type setIter struct {
	set         *Set
	left, right int
	keys        []int
	idx         int
}

func (i *setIter) first() int {
	if i.keys != nil {
		return i.keys[i.idx]
	} else {
		return -1
	}
}

func (i *setIter) next() *setIter {
	if i.keys != nil {
		if i.idx+1 < len(i.keys) { // can use cached array to move forward
			if i.left+1 < i.right {
				return &setIter{i.set, i.left + 1, i.right, i.keys, i.idx + 1}
			} else {
				return nil
			}
		} else {
			left := nextPath(i.set, i.left)
			if left != -1 && left < i.right {
				return btsetIter(i.set, left, i.right)
			} else {
				return nil
			}
		}
	} else {
		return nil
	}
}

func btsetIter(set *Set, left, right int) *setIter {
	return &setIter{set, left, right, keysFor(set, left), pathGet(left, 0)}
}

func fullBtsetIter(set *Set) *setIter {
	if len(set.root.getkeys()) > 0 {
		left := emptyPath
		right := rpath(set.root, set.shift) + 1
		return btsetIter(set, left, right)
	} else {
		return nil
	}
}

func internalSeek(set *Set, key int) int {
	node := set.root
	path := emptyPath
	level := set.shift
	for {
		keys := node.getkeys()
		keysL := len(keys)
		if 0 == level {
			idx := binarySearchL(keys, 0, keysL-1, key)
			if keysL == idx {
				return -1
			} else {
				return pathSet(path, 0, idx)
			}
		} else {
			idx := binarySearchL(keys, 0, keysL-2, key)
			node = node.(*pointerNode).pointers[idx]
			path = pathSet(path, level, idx)
			level -= levelShift
		}
	}
}

func internalRseek(set *Set, key int) int {
	node := set.root
	path := emptyPath
	level := set.shift
	for {
		keys := node.getkeys()
		keysL := len(keys)
		if 0 == level {
			idx := binarySearchR(keys, 0, keysL-1, key)
			return pathSet(path, 0, idx)
		} else {
			idx := binarySearchR(keys, 0, keysL-2, key)
			node = node.(*pointerNode).pointers[idx]
			path = pathSet(path, level, idx)
			level -= levelShift
		}
	}
}

func internalSlice(set *Set, keyFrom, keyTo int) *setIter {
	path := internalSeek(set, keyFrom)
	if path >= 0 {
		tillPath := internalRseek(set, keyTo)
		if tillPath > path {
			return &setIter{set, path, tillPath, keysFor(set, path), pathGet(path, 0)}
		} else {
			return nil
		}
	} else {
		return nil
	}
}

func slice(set *Set, keys ...int) *setIter {
	switch len(keys) {
	case 1:
		return slice(set, keys[0], keys[0])
	case 2:
		return internalSlice(set, keys[0], keys[1])
	default:
		log.Fatal("keys must be one or two integers")
		return nil
	}
}

// public interface

func alterSet(set *Set, root anyNode, shift, cnt int) *Set {
	return &Set{root, shift, cnt, set.comparator}
}

type Set struct {
	root       anyNode
	shift      int
	cnt        int
	comparator func(a, b int) int
}

func New() *Set {
	return &Set{
		&leafNode{make([]int, 0)},
		0,
		0,
		compare,
	}
}

func (s *Set) conj(key int) *Set {
	return btsetConj(s, key)
}

func (s *Set) disj(key int) *Set {
	return btsetDisj(s, key)
}

func (s *Set) lookup(key int) int {
	return s.root.lookup(key)
}

func (s *Set) iter() *setIter {
	return fullBtsetIter(s)
}
