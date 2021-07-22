package mk

import (
	"bytes"
	"fmt"
	"sort"
)

const (
	// Node key count is [4, 8]
	// Except root node, which can hold [1, 8]
	// Notice for B+tree,
	minKeys   = 4
	MaxKeys   = 2 * minKeys
	MaxValues = MaxKeys + 1
	MaxPairs  = MaxValues
)

var (
	// splitSize is node split threshold
	splitSize          = PageSize / 2
	underfillThreshold = PageSize / 4
)

// NodeInterface represents b+tree node
type NodeInterface interface {
	String() string

	ReadPage(PageInterface)
	WritePage(PageInterface)
	Dereference()

	IsRoot() bool
	IsLeaf() bool
	IsBalanced() bool

	GetRoot() NodeInterface

	PersistencySize() int

	Search(key []byte) (bool, int)

	InsertKeyValueAt(i int, key, value []byte)
	InsertKeyChildAt(i int, key []byte, cid int)

	Split() []NodeInterface
	// Merge()
}

// Node implements NodeInterface as B+tree node.
type Node struct {
	// id is page map index.
	// index=0 marks node as not mapped to page.
	id int
	// isLeaf marks leaf nodes.
	isLeaf bool
	// balanced node can skip merge.
	balanced bool
	// Spilled node can skip spill.
	// Initially, every node has spilled=false
	spilled bool
	// parent pointer.
	parent *Node
	// keys, or indexes for internal nodes.
	keys [][]byte
	// values for leaf node.
	values [][]byte
	// child pgids.
	cids []int
}

func NewNode() NodeInterface {
	return &Node{}
}

// String returns string representation of node.
func (n Node) String() string {
	typ := "internal"
	if n.isLeaf {
		typ = "leaf"
	}
	return fmt.Sprintf("node[%d] %s index=%d", n.id, typ, len(n.keys))
}

// GetRoot returns root node from current node.
func (n *Node) GetRoot() NodeInterface {
	r := n
	for !n.IsRoot() {
		r = r.parent
	}
	return r
}

func (n *Node) IsBalanced() bool {
	return n.balanced
}

func (n *Node) IsLeaf() bool {
	return n.isLeaf
}

func (n *Node) IsRoot() bool {
	return n.parent == nil
}

func (n *Node) getChildCount() int {
	if n.isLeaf {
		return 0
	}
	return len(n.cids)
}

// ReadPage initiate a node from page.
func (n *Node) ReadPage(p PageInterface) {
	n.id = p.GetIndex()
	n.isLeaf = p.IsLeaf()

	for i := 0; i < p.GetKeyCount(); i++ {
		n.keys = append(n.keys, p.GetKeyAt(i))
	}

	if n.isLeaf {
		for i := 0; i < p.GetChildCount(); i++ {
			n.values = append(n.values, p.GetValueAt(i))
		}
	} else {
		for i := 0; i < p.GetChildCount(); i++ {
			n.cids = append(n.cids, p.GetChildIDAt(i))
		}
	}
}

// WritePage writes node to given page.
func (n *Node) WritePage(p PageInterface) {
	keyOffset := (len(n.keys) + 1) * KvMetaSize
	p.SetKeyCount(len(n.keys))

	if n.isLeaf {
		p.SetFlag(LeafPage)
		for i := 0; i < len(n.keys); i++ {
			p.WriteKeyValueAt(i, keyOffset, n.keys[i], n.values[i])
			keyOffset += len(n.keys[i]) + len(n.values[i])
		}
	} else {
		p.SetFlag(InternalPage)
		for i := 0; i < len(n.keys); i++ {
			p.WriteKeyChildAt(i, keyOffset, n.keys[i], n.cids[i])
			keyOffset += len(n.keys[i])
		}
		lastIdx := len(n.keys)
		p.WriteKeyChildAt(lastIdx, keyOffset, []byte{}, n.cids[lastIdx])
	}
}

// Dereference moves key and value to heap.
func (n *Node) Dereference() {
	for i, key := range n.keys {
		buf := make([]byte, len(key))
		copy(buf, key)
		n.keys[i] = buf
	}
	if n.isLeaf {
		for i, val := range n.values {
			buf := make([]byte, len(val))
			copy(buf, val)
			n.values[i] = buf
		}
	}
}

// Search searches key in index,
// When found, return [true, index]
// When not found, return [false, first-greater-index]
func (n *Node) Search(key []byte) (bool, int) {
	i := sort.Search(len(n.keys), func(i int) bool {
		// return first greater or equal
		return bytes.Compare(key, n.keys[i]) >= 0
	})
	if i < len(n.keys) && bytes.Equal(key, n.keys[i]) {
		return true, i
	}
	return false, i
}

// InsertKeyValueAt inserts key/value pair into leaf node.
func (n *Node) InsertKeyValueAt(i int, key, value []byte) {
	if !n.isLeaf {
		panic("Leaf-only operation")
	}

	n.keys = append(n.keys, []byte{})
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key

	n.values = append(n.values, []byte{})
	copy(n.values[i+1:], n.values[i:])
	n.values[i] = value
}

// InsertKeyChildAt inserts key/int into internal node.
// TODO: internal node layout changed
func (n *Node) InsertKeyChildAt(i int, key []byte, cid int) {
	if n.isLeaf {
		panic("Internal-only operation")
	}

	n.keys = append(n.keys, []byte{})
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key

	n.cids = append(n.cids, 0)
	copy(n.cids[i+1:], n.cids[i:])
	n.cids[i] = cid
}

func (n *Node) GetKeyAt(i int) []byte {
	return n.keys[i]
}

func (n *Node) GetValueAt(i int) []byte {
	if !n.isLeaf {
		panic("get value in internal node")
	}
	return n.values[i]
}

func (n *Node) SetValueAt(i int, v []byte) {
	if !n.isLeaf {
		panic("set value in internal node")
	}
	n.values[i] = v
}

func (n *Node) GetChildID(i int) int {
	if n.isLeaf {
		panic("get child at leaf node")
	}
	return n.cids[i]
}

func (n *Node) SetChildID(i int, cid int) {
	if n.isLeaf {
		panic("set child at leaf node")
	}
	n.cids[i] = cid
}

// RemoveKeyValueAt removes key/value at given index.
func (n *Node) RemoveKeyValueAt(i int) ([]byte, []byte) {
	if !n.isLeaf {
		panic("Leaf-only operation")
	}

	removedKey := n.keys[i]
	removedValue := n.values[i]

	copy(n.keys[i:], n.keys[i+1:])
	n.keys = n.keys[:len(n.keys)-1]

	copy(n.values[i:], n.values[i+1:])
	n.values = n.values[:len(n.values)-1]

	return removedKey, removedValue
}

// RemoveKeyChildAt removes key/child at given index.
func (n *Node) RemoveKeyChildAt(i int) ([]byte, int) {
	if n.isLeaf {
		panic("Internal-node-only operation")
	}

	removedKey := n.keys[i]
	removedChild := n.cids[i]

	copy(n.keys[i:], n.keys[i+1:])
	n.keys = n.keys[:len(n.keys)-1]

	copy(n.cids[i:], n.cids[i+1:])
	n.cids = n.cids[:len(n.cids)-1]

	return removedKey, removedChild
}

// size returns size to write to page buffer.
func (n *Node) size() int {
	dataSize := 0
	for i := range n.keys {
		dataSize += len(n.keys[i])
		if n.isLeaf {
			dataSize += len(n.values[i])
		}
	}
	return HeaderSize + KvMetaSize*n.KeyCount() + dataSize
}

func (n *Node) KeyCount() int {
	return len(n.keys)
}

// Split splits node into multiple siblings according to size
// and keys.
// split sets Parent for new node, but not update for parent-side,
// and not allocate page for new node.
func (n *Node) Split() []NodeInterface {
	nodes := []*Node{}
	node := n
	for {
		nodes = append(nodes, node)
		next := node.splitTwo()
		if next == nil {
			break
		}
		node = next
	}

	return nodes
}

// Underfill returns whether node should be merged.
func (n *Node) Underfill() bool {
	return n.KeyCount() < minKeys || n.size() <= underfillThreshold
}

func (n *Node) getFirstSplitIndex() int {
	size := HeaderSize
	for i, key := range n.keys {
		size += KvMetaSize
		size += len(key)
		if n.isLeaf {
			size += len(n.values[i])
		}
		if i >= minKeys && size >= splitSize {
			return i
		}
	}
	panic("Failed to get split index")
}

// splitTwo splits overfilled nodes, will not
// update new node to Parent node, will not
// allocate page for new node.
func (n *Node) splitTwo() *Node {
	if n.KeyCount() <= MaxKeys || n.size() <= PageSize {
		return nil
	}

	splitIndex := n.getFirstSplitIndex()
	splitKey := n.keys[splitIndex]

	next := &Node{}
	next.isLeaf = n.isLeaf

	next.keys = n.keys[splitIndex:]
	n.keys = n.keys[:splitIndex]
	if n.isLeaf {
		next.values = n.values[splitIndex:]
		n.values = n.values[:splitIndex]
	} else {
		next.cids = n.cids[splitIndex:]
		n.cids = n.cids[:splitIndex]
	}

	if n.IsRoot() {
		// Split root, create a new root
		n.parent = &Node{}
		n.parent.isLeaf = false
		n.parent.keys = [][]byte{splitKey}
		// TODO: add children array.
		// Seems we have to trace new nodes from children array during split.
		n.parent.cids = []int{n.id, 0}
	}

	next.parent = n.parent

	return next
}

// Merge merges underfilled nodes with sibliings.
// Merge runs bottom-up
func (n *Node) Merge() {
	if n.balanced {
		return
	}
	n.balanced = true

	if !n.Underfill() {
		return
	}

	if n.IsRoot() {
		if n.getChildCount() == 1 {
			// Merge with only child
			child := tx.getChildAt(n, 0)

			n.IsLeaf = child.IsLeaf
			n.Keys = child.Keys
			n.Values = child.Values
			n.Cids = child.Cids
			// Reparent grand children
			for i := 0; i < n.KeyCount(); i++ {
				tx.getChildAt(n, i).Parent = n
			}
			tx.freeNode(child)
		}
		return
	}

	if n.KeyCount() == 0 {
		// Remove empty node, also remove inode from parent
		// n.key could be different to Parent index key
		_, i := n.Parent.Search(n.Key)
		n.Parent.RemoveKeyChildAt(i)
		tx.freeNode(n)
		// check parent merge
		tx.merge(n.Parent)
		return
	}

	if n.Parent.KeyCount() < 2 {
		panic("Parent should have at least one child")
	}

	var from *Node
	var to *Node
	var fromIdx int

	if n.Index == n.Parent.Cids[0] {
		// Leftmost node, merge right sibling with it
		fromIdx = 1
		from = tx.getChildAt(n.Parent, 1)
		to = n
	} else {
		// merge current node with left sibling
		_, i := n.Parent.Search(n.Key)
		fromIdx = i
		from = n
		to = tx.getChildAt(n.Parent, i-1)
	}

	// Check node type
	if from.IsLeaf != to.IsLeaf {
		panic("Sibling nodes should have same type")
	}
	// Reparent from node child
	for i := 0; i < from.KeyCount(); i++ {
		tx.getChildAt(from, i).Parent = to
	}

	to.Keys = append(to.Keys, from.Keys...)
	to.Values = append(to.Values, from.Values...)
	to.Cids = append(to.Cids, from.Cids...)

	n.Parent.RemoveKeyChildAt(fromIdx)
	tx.freeNode(from)
	tx.merge(n.Parent)
}
