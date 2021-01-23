package mk

import (
	"fmt"
	"sort"
	"unsafe"
)

const (
	// Maximum key size is 1MB.
	maxKeySize = 1 << 20

	// Maximum value size is 1GB.
	maxValueSize = 1 << 30
)

// node represents b+tree node for indexing.
// node holds the same number of keys and values(or children).
type node struct {
	// pgid is the id of mapped page.
	pgid pgid

	// pointer to transaction this node is created at.
	tx *Tx

	// isLeaf marks leaf nodes.
	isLeaf bool

	// balanced node skips tryMerge.
	balanced bool

	// spilled node skips spill.
	spilled bool

	// key of the node, would be node.keys[0].
	key KeyType

	// parent is pointer to parent node.
	parent *node

	// keys in this node.
	// [child-0] key-0 | [child-1] key-1 | .. | [child-last] key-last
	// So, key-i >= child-i.key
	keys []KeyType

	// values represent values, only for leaf node.
	values []ValueType

	// childPgids holds children pgids.
	childPgids []pgid
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

// isRoot returns whether it is root node.
func (n *node) isRoot() bool {
	return n.parent == nil
}

// read initialize a node from page.
func (n *node) read(p *page) {
	n.pgid = p.id
	n.isLeaf = p.isLeaf()

	for i := 0; i < p.numKeys; i++ {
		n.keys = append(n.keys, p.getKeyAt(i))

		if n.isLeaf {
			n.values = append(n.values, p.getValueAt(i))
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
	offset := len(n.keys) * pairHeaderSize
	buf := (*[maxMmapSize]byte)(unsafe.Pointer(&p.pairs))[offset:]

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

// search searches key in index, returns (found, first equal-or-larger index)
// when all indexes are smaller, returned index is len(index)
func (n *node) search(key KeyType) (bool, int) {
	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i].greaterEqual(key)
	})

	if i < len(n.keys) && key.equalTo(n.getKeyAt(i)) {
		return true, i
	}

	return false, i
}

// insertKeyValueAt inserts key/value pair into leaf node.
func (n *node) insertKeyValueAt(i int, key KeyType, value ValueType) {
	if !n.isLeaf {
		panic("Leaf-only operation")
	}

	if i > n.keyCount() {
		panic("Index out of bound")
	}

	n.keys = append(n.keys, KeyType{})
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key

	n.values = append(n.values, ValueType{})
	copy(n.values[i+1:], n.values[i:])
	n.values[i] = value
}

// insertKeyChildAt inserts key/pgid into internal node.
func (n *node) insertKeyChildAt(i int, key KeyType, pid pgid) {
	if n.isLeaf {
		panic("Internal-only operation")
	}

	if i > n.keyCount() {
		panic("Index out of bound")
	}

	n.keys = append(n.keys, KeyType{})
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key

	n.childPgids = append(n.childPgids, 0)
	copy(n.childPgids[i+1:], n.childPgids[i:])
	n.childPgids[i] = pid
}

func (n *node) getKeyAt(i int) KeyType {
	return n.keys[i]
}

func (n *node) getValueAt(i int) ValueType {
	return n.values[i]
}

// getChildAt returns one child node.
func (n *node) getChildAt(i int) *node {
	if i < 0 || i >= n.keyCount() {
		panic(fmt.Sprintf("Invalid child index: %d out of %d", i, n.keyCount()))
	}

	return n.tx.getNode(n.childPgids[i], n)
}

// removeKeyValueAt removes key/value at given index.
func (n *node) removeKeyValueAt(i int) (KeyType, ValueType) {
	if !n.isLeaf {
		panic("Leaf-only operation")
	}

	if i >= len(n.values) {
		panic("Invalid index")
	}

	removedKey := n.keys[i]
	removedValue := n.values[i]

	copy(n.keys[i:], n.keys[i+1:])
	n.keys = n.keys[:len(n.keys)-1]

	copy(n.values[i:], n.values[i+1:])
	n.values = n.values[:len(n.values)-1]

	return removedKey, removedValue
}

// removeKeyChildAt removes key/child at given index.
func (n *node) removeKeyChildAt(i int) (KeyType, pgid) {
	if n.isLeaf {
		panic("Internal-node-only operation")
	}

	if i >= len(n.childPgids) {
		panic("Index out of bound")
	}

	removedKey := n.keys[i]
	removedChild := n.childPgids[i]

	copy(n.keys[i:], n.keys[i+1:])
	n.keys = n.keys[:len(n.keys)-1]

	copy(n.childPgids[i:], n.childPgids[i+1:])
	n.childPgids = n.childPgids[:len(n.childPgids)-1]

	return removedKey, removedChild
}

// size returns page size after mmap
func (n *node) mapSize() int {
	size := pageHeaderSize + pairHeaderSize*n.keyCount()

	for i := range n.keys {
		size += len(n.getKeyAt(i))

		if n.isLeaf {
			size += len(n.getValueAt(i))
		}
	}

	return size
}

func (n *node) keyCount() int {
	return len(n.keys)
}

// split splits node into multiple siblings according to size and keys.
// split sets parent for new node, but will not update new nodes to parent node.
func (n *node) split() []*node {
	nodes := []*node{}
	node := n

	for {
		next := node.splitTwo()

		if next == nil {
			break
		}

		nodes = append(nodes, node)
		node = next
	}

	return nodes
}

// splitTwo splits node into two if:
// 1. node map size > pageSize, and
// 2. node has more than splitKeyCount
// splitTwo will not update new node to parent node.
func (n *node) splitTwo() *node {
	if n.keyCount() <= splitKeyCount {
		return nil
	}

	if n.mapSize() <= pageSize {
		return nil
	}

	// Split oversized page with > splitKeyCount keys
	size := pageHeaderSize
	splitIndex := 0

	for i, key := range n.keys {
		size += pairHeaderSize
		size += len(key)

		if n.isLeaf {
			size += len(n.values[i])
		}

		// Split at key >= minKeyCount and size >= splitPagePercent * pageSize
		if i >= minKeyCount && size >= int(float64(pageSize)*splitPagePercent) {
			splitIndex = i

			break
		}
	}

	// Split root node
	if n.isRoot() {
		n.parent = &node{
			keys:       []KeyType{n.key},
			childPgids: []pgid{n.pgid},
		}
	}

	next := node{
		isLeaf: n.isLeaf,
		parent: n.parent,
	}

	next.keys = n.keys[splitIndex:]
	n.keys = n.keys[:splitIndex]

	if n.isLeaf {
		next.values = n.values[splitIndex:]
		n.values = n.values[:splitIndex]
	}

	return &next
}

// spill recursively splits node and writes to pages(not to disk).
func (n *node) spill() bool {
	if n.spilled {
		return true
	}

	// Spill children first
	for i := 0; i < n.keyCount(); i++ {
		ok := n.getChildAt(i).spill()
		if !ok {
			return false
		}
	}

	for _, node := range n.split() {
		// Return node's page to freelist
		if node.pgid > 0 {
			node.tx.db.freelist.free(node.tx.id, node.tx.getPage(node.pgid))
			node.pgid = 0
		}

		// Then allocate page for node.
		// For simplicity, allocate one more page
		p, ok := node.tx.allocate((n.mapSize() / pageSize) + 1)
		if !ok {
			return false
		}

		// Allocate page for new nodes
		node.pgid = p.id

		// Write to memory page
		node.write(p)
		node.spilled = true

		if node.key == nil {
			node.key = node.keys[0]
		}

		if !node.isRoot() {
			_, i := node.parent.search(node.key)
			node.parent.insertKeyChildAt(i, node.key, node.pgid)
		}
	}

	return true
}

// tryMerge merges underfill nodes.
func (n *node) tryMerge() {
	if n.balanced {
		return
	}

	n.balanced = true

	if n.keyCount() < minKeyCount {
		return
	}

	if n.mapSize() < int(float64(pageSize)*mergePagePercent) {
		return
	}

	// Root node, bring up the if have only one child
	if n.isRoot() {
		if !n.isLeaf && n.keyCount() == 1 {
			child := n.getChildAt(0)

			n.isLeaf = child.isLeaf

			n.keys = child.keys[:]
			n.values = child.values[:]

			n.childPgids = child.childPgids[:]

			// Reparent grand children
			for i := 0; i < n.keyCount(); i++ {
				n.getChildAt(i).parent = n
			}

			child.free()
		}

		return
	}

	// Remove empty node
	if n.keyCount() == 0 {
		// n.key could be different to parent index key
		_, i := n.parent.search(n.key)

		n.parent.removeKeyChildAt(i)
		n.free()

		n.parent.tryMerge()

		return
	}

	if n.parent.keyCount() < 2 {
		panic("Parent should have at least one child")
	}

	var from *node
	var to *node
	var fromIdx int

	if n.pgid == n.parent.childPgids[0] {
		// Merge node[i=0] <- node[1]
		fromIdx = 1
		from = n.parent.getChildAt(1)
		to = n
	} else {
		// Merge node[i-1] <- node[i]
		_, i := n.parent.search(n.key)

		fromIdx = i
		from = n
		to = n.parent.getChildAt(i - 1)
	}

	// Check node type
	if from.isLeaf != to.isLeaf {
		panic("Sibling nodes should have same type")
	}

	for i := 0; i < from.keyCount(); i++ {
		from.getChildAt(i).parent = to
	}

	to.keys = append(to.keys, from.keys...)
	to.values = append(to.values, from.values...)
	to.childPgids = append(to.childPgids, from.childPgids...)

	n.parent.removeKeyChildAt(fromIdx)

	from.free()

	n.parent.tryMerge()
}

// free returns page to freelist.
func (n *node) free() {
	delete(n.tx.nodes, n.pgid)
	delete(n.tx.pages, n.pgid)

	if n.pgid != 0 {
		n.tx.db.freelist.free(n.tx.id, n.tx.getPage(n.pgid))
	}
}
