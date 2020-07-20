package mk

import (
	"bytes"
	"fmt"
	"sort"
)

type keyType []byte
type valueType []byte

func (k keyType) lessThan(other keyType) bool {
	return bytes.Compare(k, other) == -1
}

func (k keyType) equalTo(other keyType) bool {
	return bytes.Equal(k, other)
}

type pair struct {
	key   keyType
	value valueType
}

// node represents the b+tree node
// len(values) = 0 or len(index)
// len(children) = len(index) + 1
type node struct {
	// pgid is the id of mapped page
	pgid pgid

	// isLeaf marks leaf nodes
	isLeaf bool

	// balanced node skips rebalance
	balanced bool

	// parent is pointer to parent node
	parent *node

	// index represent sequence of keys.
	index []keyType

	// values represent values, empty for inner node
	values []valueType

	// children represent pointers to child nodes
	children []*node
}

// tree is the in-memory b+tree index
type tree struct {
	// order is the maxium number of keys in one node, the minium number of keys is order/2
	order int

	// pointer to root node
	root *node
}

// newTree creates a new b+tree
func newTree() *tree {
	return &tree{
		// TODO: change order, set root
		order: 3,
		root:  nil,
	}
}

// insert returns (found, oldValue)
func (t *tree) insert(key keyType, value valueType) (bool, valueType) {
	return t.root.insert(key, value)
}

// get returns (found, value)
func (t *tree) get(key keyType) (bool, valueType) {
	return t.root.get(key)
}

// remove returns (found, oldValue)
func (t *tree) remove(key keyType) (bool, valueType) {
	return t.root.remove(key)
}

// print prints the tree to stdout
func (t *tree) print() {

}

// Same as node.minKeys in boltdb
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

// String returns string representation of node
func (n *node) String() string {
	return fmt.Sprintf("node%d", n.pgid)
}

func newNode() *node {
	return &node{
		pgid:     0,
		isLeaf:   false,
		balanced: false,
		parent:   nil,
		index:    []keyType{},
		values:   []valueType{},
		children: []*node{},
	}
}

func (n *node) read(p *page) {
	nodeType := p.getType()

	n.pgid = p.ID
	if nodeType == InternalPage {
		n.isLeaf = false
		for i := 0; i < p.numKeys; i++ {
			im := p.getIndexMeta(i)
			n.index = append(n.index, im.getKey())
			// TODO: how read handles children? by pointer or pgid?
			// n.children = append(n.children, im.getChildPgid())
		}

	} else {
		n.isLeaf = true
		for i := 0; i < p.numKeys; i++ {
			kvm := p.getKvMeta(i)
			n.index = append(n.index, kvm.getKey())
			n.values = append(n.values, kvm.getValue())
		}

	}
}

func (n *node) write(p *page) {
	if n.isLeaf {
		p.flags |= leafFlag
		p.numKeys = len(n.index)
		offset := len(n.index) * kvMetaSize

		for i := 0; i < len(n.index); i++ {
			kvm := p.getKvMeta(i)
			kvm.offset = offset
			kvm.keySize = len(n.index[i])
			kvm.valueSize = len(n.values[i])

			offset -= kvMetaSize
			offset += kvm.keySize + kvm.valueSize
		}

	} else {
		p.flags |= internalFlag
		p.numKeys = len(n.index)
		offset := len(n.index) * kvMetaSize

		for i := 0; i < len(n.index); i++ {
			im := p.getIndexMeta(i)
			im.offset = offset
			im.keySize = len(n.index[i])
			im.childID = n.children[i].pgid

			offset -= indexMetaSize
			offset += im.keySize
		}
	}
}

// get searches given key from subtree, returns (found, value)
func (n *node) get(key keyType) (bool, valueType) {
	curr := n
	for curr.isLeaf == false {
		_, i := curr.search(key)
		curr = curr.children[i]
	}
	found, i := curr.search(key)
	if found {
		return true, n.values[i]
	}
	return false, valueType{}
}

// insert inserts key and value into node, returns (found, oldValue)
// TODO: add split logic
func (n *node) insert(key keyType, value valueType) (bool, valueType) {
	found, i := n.search(key)
	if n.isLeaf {
		if found {
			oldValue := n.values[i]
			n.values[i] = value
			return true, oldValue
		}
		n.balanced = false
		n.insertValueAt(i, value)
		return false, valueType{}
	}
	// Now n must be inner node
	return n.children[i].insert(key, value)
}

// remove removes given key from node recursively, returns (found, oldValue)
// TODO: add split logic
func (n *node) remove(key keyType) (bool, valueType) {
	found, i := n.search(key)
	if n.isLeaf {
		if !found {
			return false, nil
		}
		n.balanced = false
		return true, n.removeValueAt(i)
	}
	return n.children[i].remove(key)
}

// search searches key in index, returns (found, first eq/larger index)
// when all indexes are smaller, returned index is len(index)
func (n *node) search(key keyType) (bool, int) {
	larger := sort.Search(len(n.index), func(i int) bool {
		return key.lessThan(n.index[i])
	})
	if larger > 0 && key.equalTo(n.index[larger-1]) {
		return true, larger - 1
	}
	return false, larger
}

// insertValueAt places value at given position of node.values
func (n *node) insertValueAt(i int, value valueType) {
	assert(n.isLeaf, "insert value for non-leaf node")
	assert(i < len(n.values), "insertValueAt: invalid i=%s, len=%s", i, len(n.values))
	n.values = append(n.values, valueType{})
	copy(n.values[i+1:], n.values[i:])
	n.values[i] = value
}

func (n *node) removeValueAt(i int) valueType {
	assert(n.isLeaf, "remove value for non-leaf node")
	assert(i < len(n.values), "removeValueAt: invalid i=%s, len=%s", i, len(n.values))
	oldValue := n.values[i]
	copy(n.values[i:], n.values[i+1:])
	n.values = n.values[:len(n.values)-1]
	return oldValue
}
