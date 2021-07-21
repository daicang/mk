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
	splitPagePercent = 0.5
	underfillPercent = 0.25

	splitThreshold     = int(float64(PageSize) * splitPagePercent)
	underfillThreshold = int(float64(PageSize) * underfillPercent)
)

// NodeInterface represents b+tree node
type NodeInterface interface {
	String() string

	ReadPage(PageInterface)
	WritePage(PageInterface)
	Dereference()

	IsRoot() bool
	GetRoot() NodeInterface

	PersistencySize() int

	Search(key []byte) (bool, int)

	InsertKeyValueAt(i int, key, value []byte)
	InsertKeyChildAt(i int, key []byte, cid int)

	// Split()
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
	// keys in this node.
	keys [][]byte
	// child pointers
	// empty for leaf nodes
	children []*Node
	// values for leaf node.
	// empty for internal nodes.
	values [][]byte
	// cids holds children ints.
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

// IsRoot returns whether it is root node.
func (n *Node) IsRoot() bool {
	return n.parent == nil
}

// ReadPage initiate a node from page.
func (n *Node) ReadPage(p PageInterface) {
	n.id = p.Getint()
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
	if i > n.KeyCount() {
		panic("Index out of bound")
	}

	n.keys = append(n.keys, []byte{})
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key

	n.values = append(n.values, []byte{})
	copy(n.values[i+1:], n.values[i:])
	n.values[i] = value
}

// InsertKeyChildAt inserts key/int into internal node.
func (n *Node) InsertKeyChildAt(i int, key []byte, cid int) {
	if n.isLeaf {
		panic("Internal-only operation")
	}
	if i > n.KeyCount() {
		panic("Index out of bound")
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

// RemoveKeyChildAt removes key/child at given index.
func (n *Node) RemoveKeyChildAt(i int) ([]byte, int) {
	if n.isLeaf {
		panic("Internal-node-only operation")
	}
	if i >= len(n.cids) {
		panic("Index out of bound")
	}

	removedKey := n.keys[i]
	removedChild := n.cids[i]

	copy(n.keys[i:], n.keys[i+1:])
	n.keys = n.keys[:len(n.keys)-1]

	copy(n.cids[i:], n.cids[i+1:])
	n.cids = n.cids[:len(n.cids)-1]

	return removedKey, removedChild
}

// PersistenncySize returns required size to write to memory page.
func (n *Node) PersistencySize() int {
	dataSize := 0
	for i := range n.keys {
		dataSize += len(n.keys[i])
		if n.isLeaf {
			dataSize += len(n.values[i])
		}
	}
	return
}

func (n *Node) KeyCount() int {
	return len(n.keys)
}

// Split splits node into multiple siblings according to size
// and keys.
// split sets Parent for new node, but not update for parent-side,
// and not allocate page for new node.
func (n *Node) Split() []*Node {
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
	return n.KeyCount() < minKeys || n.Size() < underfillThreshold
}

// Overfill returns node size > pageSize and key > maxKeys.
func (n *Node) Overfill() bool {
	return n.KeyCount() > maxKeys && n.Size() > PageSize
}

func isSplitPoint(i, size int) bool {
	return i >= minKeys && size >= splitThreshold
}

// splitTwo splits overfilled nodes, will not
// update new node to Parent node, will not
// allocate page for new node.
func (n *Node) splitTwo() *Node {
	if !n.Overfill() {
		return nil
	}
	size := HeaderSize
	splitIndex := 0
	// Search split point
	for i, key := range n.keys {
		size += PairInfoSize
		size += len(key)
		if n.isLeaf {
			size += len(n.values[i])
		}
		if isSplitPoint(i, size) {
			splitIndex = i
			break
		}
	}
	// If it's root, prepare a new parent
	if n.IsRoot() {
		n.Parent = &Node{
			Keys: []Key{n.Key},
			Cids: []int{n.Index},
		}
	}
	next := Node{
		IsLeaf: n.isLeaf,
		Parent: n.Parent,
	}
	// Split key, value, children
	next.keys = n.keys[splitIndex:]
	n.keys = n.keys[:splitIndex]
	if n.isLeaf {
		next.values = n.values[splitIndex:]
		n.values = n.values[:splitIndex]
	} else {
		next.cids = n.cids[splitIndex:]
		n.cids = n.cids[:splitIndex]
	}

	return &next
}
