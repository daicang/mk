package mk

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

const (
	// Minimum keys per page is 2
	minKeys = 2

	// Maximum key size is 1MB.
	maxKeySize = 1 << 20

	// Maximum value size is 1GB.
	maxValueSize = 1 << 30
)

// KeyType represents type for key
type KeyType []byte

// ValueType represents type for value
type ValueType []byte

func (k KeyType) lessThan(other KeyType) bool {
	return bytes.Compare(k, other) == -1
}

func (k KeyType) equalTo(other KeyType) bool {
	return bytes.Equal(k, other)
}

// node represents the b+tree node,
// it holds the same number of keys and values
// for both leaf and internal node.
type node struct {
	// pgid is the id of mapped page.
	pgid pgid

	tx *Tx

	// isLeaf marks leaf nodes.
	isLeaf bool

	// balanced node skips rebalance.
	balanced bool

	// spilled node skips spill.
	//
	spilled bool

	// key in parent node.
	key KeyType

	// parent is pointer to parent node.
	parent *node

	// keys in this node.
	keys []KeyType

	// values represent values, only for leaf node.
	values []ValueType

	// childPgids holds children pgids.
	childPgids []pgid

	// childPtrs holds children pointers.
	childPtrs []*node
}

// String returns string representation of node.
func (n *node) String() string {
	return fmt.Sprintf("node[%d] len=%d leaf=%t", n.pgid, len(n.keys), n.isLeaf)
}

// root returns root node from current node.
func (n *node) root() *node {
	r := n
	for r.parent != nil {
		r = r.parent
	}

	return r
}

// read initialize a node from page.
func (n *node) read(p *page) {
	n.pgid = p.id
	n.isLeaf = p.isLeaf()

	for i := 0; i < p.numKeys; i++ {
		n.keys = append(n.keys, p.getKey(i))

		if n.isLeaf {
			n.values = append(n.values, p.getValue(i))
		} else {
			n.childPgids = append(n.childPgids, p.getChildPgid(i))
		}
	}

	if len(n.keys) > 0 {
		n.key = n.keys[0]
	}
}

// write writes node to given page
func (n *node) write(p *page) {
	p.numKeys = len(n.keys)
	offset := len(n.keys) * pairSize
	buf := (*[maxMmSize]byte)(unsafe.Pointer(&p.pairs))[offset:]

	if n.isLeaf {
		p.flags |= leafPageFlag

		for i := 0; i < len(n.keys); i++ {
			meta := p.getPair(i)
			meta.offset = offset
			meta.keySize = len(n.keys[i])
			meta.valueSize = len(n.values[i])

			copy(buf, n.keys[i])
			buf = buf[meta.keySize:]

			copy(buf, n.values[i])
			buf = buf[meta.valueSize:]

			offset += meta.keySize
			offset += meta.valueSize
		}
	} else {
		p.flags |= internalPageFlag

		for i := 0; i < len(n.keys); i++ {
			meta := p.getPair(i)
			meta.offset = offset
			meta.keySize = len(n.keys[i])
			meta.childID = n.childPgids[i]

			copy(buf, n.keys[i])
			buf = buf[meta.keySize:]

			offset += meta.keySize
		}
	}
}

// search searches key in index, returns (found, first eq/larger index)
// when all indexes are smaller, returned index is len(index)
func (n *node) search(key KeyType) (bool, int) {
	larger := sort.Search(len(n.keys), func(i int) bool {
		return key.lessThan(n.keys[i])
	})
	if larger > 0 && key.equalTo(n.keys[larger-1]) {
		return true, larger - 1
	}
	return false, larger
}

// insertKeyAt inserts key at given index.
func (n *node) insertKeyAt(i int, key KeyType) {
	n.keys = append(n.keys, KeyType{})
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key
}

// removeKeyAt removes key at given index.
func (n *node) removeKeyAt(i int) KeyType {
	if i >= len(n.keys) {
		panic("Key index out of bound")
	}

	removed := n.keys[i]

	copy(n.keys[i:], n.keys[i+1:])
	n.keys = n.keys[:len(n.keys)-1]

	return removed
}

// insertValueAt places value at given index.
func (n *node) insertValueAt(i int, value ValueType) {
	if !n.isLeaf {
		panic("Leaf-only operation")
	}

	if i > len(n.values) {
		panic("Invalid index")
	}

	n.values = append(n.values, ValueType{})
	copy(n.values[i+1:], n.values[i:])
	n.values[i] = value
}

// removeValueAt removes value at given index.
func (n *node) removeValueAt(i int) ValueType {
	if !n.isLeaf {
		panic("Leaf-only operation")
	}

	if i >= len(n.values) {
		panic("Invalid index")
	}

	oldValue := n.values[i]

	copy(n.values[i:], n.values[i+1:])
	n.values = n.values[:len(n.values)-1]

	return oldValue
}

// insertChildAt inserts child at given index.
func (n *node) insertChildAt(i int, child *node) {
	if n.isLeaf {
		panic("Internal-node-only operation")
	}

	if i >= len(n.childPtrs) {
		panic("Index out of bound")
	}

	n.childPtrs = append(n.childPtrs, nil)
	copy(n.childPtrs[i+1:], n.childPtrs[i:])
	n.childPtrs[i] = child

	n.childPgids = append(n.childPgids, 0)
	copy(n.childPgids[i+1:], n.childPgids[i:])
	n.childPgids[i] = child.pgid
}

// removeChildAt removes child at given index.
func (n *node) removeChildAt(i int) {
	if n.isLeaf {
		panic("Internal-node-only operation")
	}

	if i >= len(n.childPgids) {
		panic("Index out of bound")
	}

	copy(n.childPgids[i:], n.childPgids[i+1:])
	n.childPgids = n.childPgids[:len(n.childPgids)-1]

	copy(n.childPtrs[i:], n.childPtrs[i+1:])
	n.childPtrs = n.childPtrs[:len(n.childPtrs)-1]
}

// size returns page size after mmap
func (n *node) size() int {
	sz := pageHeaderSize + len(n.keys)*pairSize

	if n.isLeaf {
		for i, key := range n.keys {
			sz += len(key) + len(n.values[i])
		}
	} else {
		for _, key := range n.keys {
			sz += len(key)
		}
	}

	return sz
}

// Overfill returns true when size > pageSize and have more than 2x minKeys.
func (n *node) overfill() bool {
	return len(n.keys) > 2*minKeys && n.size() > pageSize
}

func (n *node) underfill() bool {
	return len(n.keys) < minKeys || n.size() < pageSize/4
}

func (n *node) split() []*node {
	nodes := []*node{}
	node := n

	for {
		_, next := node.splitTwo()

		if next == nil {
			break
		}

		nodes = append(nodes, node)
		node = next
	}

	return nodes
}

func (n *node) splitTwo() (*node, *node) {
	fillPercent := 0.5

	if !n.overfill() {
		return n, nil
	}

	// Here we have oversized one page and have more than 2x minKeys
	// Where to split page: keys >= minKeys and size >= fillPercent * pageSize
	size := pageHeaderSize
	splitIndex := 0

	for i, key := range n.keys {
		size += pairSize
		size += len(key)

		if n.isLeaf {
			size += len(n.values[i])
		}

		if i >= minKeys && size >= int(float64(pageSize)*fillPercent) {
			splitIndex = i

			break
		}
	}

	if n.parent == nil {
		n.parent = &node{
			childPtrs: []*node{n},
		}
	}

	next := node{
		isLeaf: n.isLeaf,
		parent: n.parent,
	}

	n.parent.childPtrs = append(n.parent.childPtrs, &next)

	next.keys = n.keys[splitIndex:]
	n.keys = n.keys[:splitIndex]

	if n.isLeaf {
		next.values = n.values[splitIndex:]
		n.values = n.values[:splitIndex]
	}

	return n, &next
}

// spill writes node to dirty pages.
func (n *node) spill() error {
	if n.spilled {
		return nil
	}

	for _, child := range n.childPtrs {
		err := child.spill()
		if err != nil {
			return err
		}
	}

	nodes := n.split()

	for _, node := range nodes {
		if node.pgid > 0 {
			node.tx.db.freelist.free(node.tx.id, node.tx.page(node.pgid))
			node.pgid = 0
		}

		// TODO: use tx.allocate
		p, err := node.tx.db.allocate((n.size() / pageSize) + 1)
		if err != nil {
			return err
		}

		node.pgid = p.id
		node.write(p)
		node.spilled = true

		if node.key == nil {
			node.key = node.keys[0]
		}

		if n.parent != nil {
			found, i := n.parent.search(node.key)
			if found {
				n.parent.childPgids[i] = node.pgid
			} else {
				n.parent.insertChildAt(i, node)
			}
		}
	}

	// TODO: why we need to call spill for root?
	if n.parent != nil && n.parent.pgid == 0 {
		// n.children = nil
		return n.parent.spill()
	}

	return nil
}

// rebalance merges underfill nodes
func (n *node) rebalance() {
	if n.balanced {
		return
	}

	n.balanced = true

	if !n.underfill() {
		return
	}

	// Root case.
	if n.parent == nil {

		// Bring up the only child.
		if !n.isLeaf && len(n.keys) == 1 {
			child := n.childPtrs[0]

			n.isLeaf = child.isLeaf
			n.keys = child.keys[:]
			n.values = child.values[:]
			n.childPtrs = child.childPtrs[:]
			n.childPgids = child.childPgids[:]

			// Reparent grand children
			for _, ch := range n.childPtrs {
				ch.parent = n
			}

			child.free()
		}

		return
	}

	// Remove empty node.
	if len(n.keys) == 0 {
		found, i := n.parent.search(n.key)
		if !found {
			panic("Key not found in parent")
		}

		n.parent.removeKeyAt(i)
		n.parent.removeChildAt(i)

		n.free()

		n.parent.rebalance()

		return
	}

	var from *node
	var to *node
	var fromIndex int

	// Merge with left sibling, if already leftmost, right sibling
	if n == n.parent.childPtrs[0] {
		fromIndex = 1
		from = n.parent.childPtrs[1]
		to = n
	} else {
		_, i := n.parent.search(n.key)
		fromIndex = i
		from = n
		to = n.parent.childPtrs[i-1]
	}

	// We can ensure nodes on the same level are both leaf or internal
	if from.isLeaf != to.isLeaf {
		panic("Internal node and leaf node should not be on the same level")
	}

	// Move children, empty for leaf node.
	for i, ch := range from.childPtrs {
		to.childPgids = append(to.childPgids, from.childPgids[i])
		ch.parent = to
		to.childPtrs = append(to.childPtrs, ch)
		from.removeChildAt(i)
	}

	to.keys = append(to.keys, from.keys...)
	to.values = append(to.values, from.values...)

	n.parent.removeKeyAt(fromIndex)
	n.parent.removeChildAt(fromIndex)

	n.parent.rebalance()
}

// TODO: add freeList logic
func (n *node) free() {

}
