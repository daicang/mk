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
	minKeyCount = 2
	// Maxium keys per node
	maxKeyCount = 4
	// splitPagePercent marks first split point.
	splitPagePercent = 0.5
	// mergePagePercent
	mergePagePercent = 0.25
)

// Node represents b+tree node for indexing.
// node holds the same number of keys and values(or children).
type Node struct {
	// pgid is the id of mapped page.
	pgid common.Pgid
	// IsLeaf marks leaf nodes.
	IsLeaf bool
	// balanced node skips tryMerge.
	balanced bool
	// spilled node skips spill.
	Spilled bool
	// key of the node, would be node.keys[0].
	key kv.Key
	// Parent is pointer to Parent node.
	Parent *Node
	// keys in this node.
	// [child-0] key-0 | [child-1] key-1 | .. | [child-last] key-last
	// So, key-i >= child-i.key
	keys []kv.Key
	// values represent values, only for leaf node.
	values []kv.Value
	// childPgids holds children pgids.
	childPgids []common.Pgid
}

// String returns string representation of node.
func (n *Node) String() string {
	return fmt.Sprintf("node[%d] len=%d leaf=%t", n.pgid, len(n.keys), n.IsLeaf)
}

// root returns root node from current node.
func (n *Node) root() *Node {
	r := n
	for !n.isRoot() {
		r = r.Parent
	}
	return r
}

// isRoot returns whether it is root node.
func (n *Node) isRoot() bool {
	return n.Parent == nil
}

// ReadPage initialize a node from page.
func (n *Node) ReadPage(p *page.Page) {
	n.pgid = p.Index
	n.IsLeaf = p.IsLeaf()

	for i := 0; i < p.Count; i++ {
		n.keys = append(n.keys, p.GetKeyAt(i))
		if n.IsLeaf {
			n.values = append(n.values, p.GetValueAt(i))
		} else {
			n.childPgids = append(n.childPgids, p.GetChildPgid(i))
		}
	}

	if len(n.keys) > 0 {
		n.key = n.keys[0]
	}
}

// WritePage writes node to given page
func (n *Node) WritePage(p *page.Page) {
	p.Count = len(n.keys)

	offset := uint32(len(n.keys) * page.PairInfoSize)
	buf := (*[common.MmapMaxSize]byte)(unsafe.Pointer(&p.Data))[offset:]

	if n.IsLeaf {
		p.SetFlag(page.FlagLeaf)

		for i := 0; i < len(n.keys); i++ {
			keySize := uint32(len(n.keys[i]))
			valueSize := uint32(len(n.values[i]))
			p.SetPairInfo(i, keySize, valueSize, 0, offset)

			copy(buf, n.keys[i])
			buf = buf[keySize:]

			copy(buf, n.values[i])
			buf = buf[valueSize:]

			offset = offset + keySize + valueSize
		}
	} else {
		p.SetFlag(page.FlagInternal)

		for i := 0; i < len(n.keys); i++ {
			keySize := uint32(len(n.keys[i]))

			p.SetPairInfo(i, keySize, 0, n.GetChildID(i), offset)

			copy(buf, n.keys[i])
			buf = buf[keySize:]

			offset = offset + keySize
		}
	}
}

// search searches key in index, returns (found, first equal-or-larger index)
// when all indexes are smaller, returned index is len(index)
func (n *Node) search(key kv.Key) (bool, int) {
	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i].GreaterEqual(key)
	})

	if i < len(n.keys) && key.EqualTo(n.getKeyAt(i)) {
		return true, i
	}

	return false, i
}

// insertKeyValueAt inserts key/value pair into leaf node.
func (n *Node) insertKeyValueAt(i int, key kv.Key, value kv.Value) {
	if !n.IsLeaf {
		panic("Leaf-only operation")
	}

	if i > n.keyCount() {
		panic("Index out of bound")
	}

	n.keys = append(n.keys, kv.Key{})
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key

	n.values = append(n.values, kv.Value{})
	copy(n.values[i+1:], n.values[i:])
	n.values[i] = value
}

// insertKeyChildAt inserts key/pgid into internal node.
func (n *Node) insertKeyChildAt(i int, key kv.Key, pid common.Pgid) {
	if n.IsLeaf {
		panic("Internal-only operation")
	}

	if i > n.keyCount() {
		panic("Index out of bound")
	}

	n.keys = append(n.keys, kv.Key{})
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key

	n.childPgids = append(n.childPgids, 0)
	copy(n.childPgids[i+1:], n.childPgids[i:])
	n.childPgids[i] = pid
}

func (n *Node) getKeyAt(i int) kv.Key {
	return n.keys[i]
}

func (n *Node) getValueAt(i int) kv.Value {
	return n.values[i]
}

func (n *Node) GetChildID(i int) common.Pgid {
	if n.IsLeaf {
		panic("get child at leaf node")
	}
	return n.childPgids[i]
}

func (n *Node) setChildID(i int, cid common.Pgid) {
	if n.IsLeaf {
		panic("set child at leaf node")
	}
	n.childPgids[i] = cid
}

// removeKeyValueAt removes key/value at given index.
func (n *Node) removeKeyValueAt(i int) (kv.Key, kv.Value) {
	if !n.IsLeaf {
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
func (n *Node) removeKeyChildAt(i int) (kv.Key, pgid) {
	if n.IsLeaf {
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
func (n *Node) mapSize() int {
	size := page.HeaderSize + pairHeaderSize*n.keyCount()

	for i := range n.keys {
		size += len(n.getKeyAt(i))

		if n.IsLeaf {
			size += len(n.getValueAt(i))
		}
	}

	return size
}

func (n *Node) keyCount() int {
	return len(n.keys)
}

// split splits node into multiple siblings according to size and keys.
// split sets Parent for new node, but will not update new nodes to Parent node.
func (n *Node) split() []*Node {
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

// splitTwo splits node into two if:
// 1. node map size > pageSize, and
// 2. node has more than splitKeyCount
// splitTwo will not update new node to Parent node.
func (n *Node) splitTwo() *Node {
	if n.keyCount() <= splitKeyCount {
		return nil
	}

	if n.mapSize() <= pageSize {
		return nil
	}

	// Split oversized page with > splitKeyCount keys
	size := page.HeaderSize
	splitIndex := 0

	for i, key := range n.keys {
		size += pairHeaderSize
		size += len(key)

		if n.IsLeaf {
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
		n.Parent = &Node{
			keys:       []kv.Key{n.key},
			childPgids: []common.Pgid{n.pgid},
		}
	}

	next := Node{
		IsLeaf: n.IsLeaf,
		Parent: n.Parent,
	}

	next.keys = n.keys[splitIndex:]
	n.keys = n.keys[:splitIndex]

	if n.IsLeaf {
		next.values = n.values[splitIndex:]
		n.values = n.values[:splitIndex]
	}

	return &next
}

// free returns page to freelist.
func (n *Node) free() {
	delete(n.tx.nodes, n.pgid)
	delete(n.tx.pages, n.pgid)

	if n.pgid != 0 {
		n.tx.db.freelist.free(n.tx.id, n.tx.getPage(n.pgid))
	}
}
