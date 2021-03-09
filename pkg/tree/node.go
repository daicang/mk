package tree

import (
	"fmt"
	"sort"
	"unsafe"

	"github.com/daicang/mk/pkg/common"
	"github.com/daicang/mk/pkg/kv"
	"github.com/daicang/mk/pkg/page"
)

const (
	// Minimal keys per node
	minKeys = 2
	// Maxium keys per node
	maxKeys = 4
)

var (
	splitPagePercent = 0.5
	underfillPercent = 0.25

	splitThreshold     = int(float64(page.PageSize) * splitPagePercent)
	underfillThreshold = int(float64(page.PageSize) * underfillPercent)
)

// Node represents b+tree node for indexing.
// node holds the same number of keys and values(or children).
type Node struct {
	// Index is page map index. Index=0 marks node as not mapped to page.
	Index common.Pgid
	// IsLeaf marks leaf nodes.
	IsLeaf bool
	// Balanced node can skip merge.
	Balanced bool
	// Spilled node can skip spill.
	Spilled bool
	// key of the node, would be node.Keys[0].
	Key kv.Key
	// Parent is pointer to Parent node.
	Parent *Node
	// keys in this node.
	// [child-0] key-0 | [child-1] key-1 | .. | [child-last] key-last
	// So, key-i >= child-i.key
	Keys []kv.Key
	// values represent values, only for leaf node.
	Values []kv.Value
	// cids holds children pgids.
	Cids []common.Pgid
}

// String returns string representation of node.
func (n *Node) String() string {
	typ := "internal"
	if n.IsLeaf {
		typ = "leaf"
	}
	return fmt.Sprintf("node[%d] %s keys=%d", n.Index, typ, len(n.Keys))
}

// Root returns root node from current node.
func (n *Node) Root() *Node {
	r := n
	for !n.IsRoot() {
		r = r.Parent
	}
	return r
}

// IsRoot returns whether it is root node.
func (n *Node) IsRoot() bool {
	return n.Parent == nil
}

// ReadPage initiate a node from page.
func (n *Node) ReadPage(p *page.Page) {
	n.Index = p.Index
	n.IsLeaf = p.IsLeaf()

	for i := 0; i < p.Count; i++ {
		n.Keys = append(n.Keys, p.GetKeyAt(i))
		if n.IsLeaf {
			n.Values = append(n.Values, p.GetValueAt(i))
		} else {
			n.Cids = append(n.Cids, p.GetChildPgid(i))
		}
	}

	if len(n.Keys) > 0 {
		n.Key = n.Keys[0]
	}
}

// WritePage writes node to given page
func (n *Node) WritePage(p *page.Page) {
	offset := uint32(len(n.Keys) * page.PairInfoSize)
	buf := (*[common.MmapMaxSize]byte)(unsafe.Pointer(&p.Data))[offset:]
	p.Count = len(n.Keys)

	if n.IsLeaf {
		p.SetFlag(page.FlagLeaf)
		for i := 0; i < len(n.Keys); i++ {
			keySize := uint32(len(n.Keys[i]))
			valueSize := uint32(len(n.Values[i]))
			p.SetPairInfo(i, keySize, valueSize, 0, offset)

			copy(buf, n.Keys[i])
			buf = buf[keySize:]

			copy(buf, n.Values[i])
			buf = buf[valueSize:]

			offset += keySize + valueSize
		}
	} else {
		p.SetFlag(page.FlagInternal)
		for i := 0; i < len(n.Keys); i++ {
			keySize := uint32(len(n.Keys[i]))
			p.SetPairInfo(i, keySize, 0, n.GetChildID(i), offset)

			copy(buf, n.Keys[i])
			buf = buf[keySize:]

			offset += keySize
		}
	}
}

// Search searches key in index, returns (found, first equal-or-larger index)
// when all indexes are smaller, returned index is len(index)
func (n *Node) Search(key kv.Key) (bool, int) {
	i := sort.Search(len(n.Keys), func(i int) bool {
		return n.Keys[i].GreaterEqual(key)
	})
	// Found
	if i < len(n.Keys) && key.EqualTo(n.GetKeyAt(i)) {
		return true, i
	}
	return false, i
}

// InsertKeyValueAt inserts key/value pair into leaf node.
func (n *Node) InsertKeyValueAt(i int, key kv.Key, value kv.Value) {
	if !n.IsLeaf {
		panic("Leaf-only operation")
	}
	if i > n.KeyCount() {
		panic("Index out of bound")
	}

	n.Keys = append(n.Keys, kv.Key{})
	copy(n.Keys[i+1:], n.Keys[i:])
	n.Keys[i] = key

	n.Values = append(n.Values, kv.Value{})
	copy(n.Values[i+1:], n.Values[i:])
	n.Values[i] = value
}

// InsertKeyChildAt inserts key/pgid into internal node.
func (n *Node) InsertKeyChildAt(i int, key kv.Key, pid common.Pgid) {
	if n.IsLeaf {
		panic("Internal-only operation")
	}
	if i > n.KeyCount() {
		panic("Index out of bound")
	}

	n.Keys = append(n.Keys, kv.Key{})
	copy(n.Keys[i+1:], n.Keys[i:])
	n.Keys[i] = key

	n.Cids = append(n.Cids, 0)
	copy(n.Cids[i+1:], n.Cids[i:])
	n.Cids[i] = pid
}

func (n *Node) GetKeyAt(i int) kv.Key {
	return n.Keys[i]
}

func (n *Node) GetValueAt(i int) kv.Value {
	if !n.IsLeaf {
		panic("get value in internal node")
	}
	return n.Values[i]
}

func (n *Node) SetValueAt(i int, v kv.Value) {
	if !n.IsLeaf {
		panic("set value in internal node")
	}
	n.Values[i] = v
}

func (n *Node) GetChildID(i int) common.Pgid {
	if n.IsLeaf {
		panic("get child at leaf node")
	}
	return n.Cids[i]
}

func (n *Node) SetChildID(i int, cid common.Pgid) {
	if n.IsLeaf {
		panic("set child at leaf node")
	}
	n.Cids[i] = cid
}

// RemoveKeyValueAt removes key/value at given index.
func (n *Node) RemoveKeyValueAt(i int) (kv.Key, kv.Value) {
	if !n.IsLeaf {
		panic("Leaf-only operation")
	}
	if i >= len(n.Values) {
		panic("Invalid index")
	}

	removedKey := n.Keys[i]
	removedValue := n.Values[i]

	copy(n.Keys[i:], n.Keys[i+1:])
	n.Keys = n.Keys[:len(n.Keys)-1]

	copy(n.Values[i:], n.Values[i+1:])
	n.Values = n.Values[:len(n.Values)-1]

	return removedKey, removedValue
}

// RemoveKeyChildAt removes key/child at given index.
func (n *Node) RemoveKeyChildAt(i int) (kv.Key, common.Pgid) {
	if n.IsLeaf {
		panic("Internal-node-only operation")
	}
	if i >= len(n.Cids) {
		panic("Index out of bound")
	}

	removedKey := n.Keys[i]
	removedChild := n.Cids[i]

	copy(n.Keys[i:], n.Keys[i+1:])
	n.Keys = n.Keys[:len(n.Keys)-1]

	copy(n.Cids[i:], n.Cids[i+1:])
	n.Cids = n.Cids[:len(n.Cids)-1]

	return removedKey, removedChild
}

// Size returns size when write to memory page.
func (n *Node) Size() int {
	size := page.HeaderSize + page.PairInfoSize*n.KeyCount()
	for i := range n.Keys {
		size += len(n.GetKeyAt(i))
		if n.IsLeaf {
			size += len(n.GetValueAt(i))
		}
	}
	return size
}

func (n *Node) KeyCount() int {
	return len(n.Keys)
}

// Split splits node into multiple siblings according to size and keys.
// split sets Parent for new node, but will not update new nodes to Parent node.
func (n *Node) Split() []*Node {
	nodes := []*Node{}
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

// Underfill returns whether node should be merged.
func (n *Node) Underfill() bool {
	return n.KeyCount() < minKeys || n.Size() < underfillThreshold
}

// Overfill returns node size > pageSize and key > maxKeys.
func (n *Node) Overfill() bool {
	return n.KeyCount() > maxKeys && n.Size() > page.PageSize
}

func isSplitPoint(i, size int) bool {
	return i >= minKeys && size >= splitThreshold
}

// splitTwo splits overfilled nodes.
// splitTwo will not update new node to Parent node.
func (n *Node) splitTwo() *Node {
	if !n.Overfill() {
		return nil
	}
	size := page.HeaderSize
	splitIndex := 0
	// Search split point
	for i, key := range n.Keys {
		size += page.PairInfoSize
		size += len(key)
		if n.IsLeaf {
			size += len(n.Values[i])
		}
		if isSplitPoint(i, size) {
			splitIndex = i
			break
		}
	}
	// If it's root, prepare a new parent
	if n.IsRoot() {
		n.Parent = &Node{
			Keys: []kv.Key{n.Key},
			Cids: []common.Pgid{n.Index},
		}
	}
	next := Node{
		IsLeaf: n.IsLeaf,
		Parent: n.Parent,
	}
	// Split key, value, children
	next.Keys = n.Keys[splitIndex:]
	n.Keys = n.Keys[:splitIndex]
	if n.IsLeaf {
		next.Values = n.Values[splitIndex:]
		n.Values = n.Values[:splitIndex]
	} else {
		next.Cids = n.Cids[splitIndex:]
		n.Cids = n.Cids[:splitIndex]
	}

	return &next
}
