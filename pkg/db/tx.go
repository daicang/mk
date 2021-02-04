package db

import (
	"fmt"
	"sort"
	"unsafe"

	"github.com/daicang/mk/pkg/common"
	"github.com/daicang/mk/pkg/kv"
	"github.com/daicang/mk/pkg/page"
	"github.com/daicang/mk/pkg/tree"
)

// Tx represents transaction.
type Tx struct {
	db *DB
	// Transaction ID
	id uint32
	// Read-only mark
	writable bool
	// Pointer to mata struct
	meta *Meta
	// root points to the b+tree root
	root *tree.Node
	// All accessed nodes in this transaction.
	nodes map[uint32]*tree.Node
	// All accessed pages in this transaction.
	pages map[int32]*page.Page
}

// NewWritableTx creates new writable transaction.
func NewWritableTx(db *DB) (*Tx, bool) {
	if db.writableTx != nil {
		fmt.Println("Cannot create multiple writable tx")

		return nil, false
	}

	rootPage := db.getPage(db.meta.rootPage)
	root := &tree.Node{
		Parent: nil,
	}

	fmt.Printf("rootid=%d\n", db.meta.rootPage)

	fmt.Printf("Page: %s\n", rootPage)

	root.ReadPage(rootPage)

	fmt.Printf("Node: %s\n", root)

	// TESTING
	if !root.IsLeaf {
		panic("root should be leaf")
	}

	t := Tx{
		db:       db,
		id:       1,
		writable: true,
		meta:     db.meta.copy(),
		root:     root,
		nodes:    map[pgid]*node{},
		pages:    map[pgid]*page{},
	}

	t.nodes[db.meta.root] = root
	t.pages[db.meta.root] = rootPage

	db.txs = append(db.txs, &t)
	db.writableTx = &t

	return &t, true
}

// NewReadOnlyTx returns new read-only transaction.
func NewReadOnlyTx(db *DB) (*Tx, bool) {
	rootPage := db.getPage(db.meta.root)
	root := &node{
		parent: nil,
	}
	root.read(rootPage)

	t := Tx{
		db:       db,
		id:       1,
		writable: false,
		meta:     db.meta.copy(),
		root:     root,
		nodes:    map[pgid]*node{},
		pages:    map[pgid]*page{},
	}

	t.nodes[db.meta.root] = root
	t.pages[db.meta.root] = rootPage

	db.txs = append(db.txs, &t)

	return &t, true
}

// allocate returns contiguous pages.
func (t *Tx) allocate(count int) (*page, bool) {
	if !t.writable {
		panic("Read only tx can't allocate")
	}

	return t.db.allocate(count)
}

func (t *Tx) close() {

}

// Commit balance b+tree, write changes to disk, and close transaction.
func (t *Tx) Commit() bool {
	if !t.writable {
		fmt.Println("Commit on read only tx")

		return false
	}

	// Merge underfill nodes
	for _, node := range t.nodes {
		tree.tryMerge()
	}

	// Split nodes and write to memory page
	ok := t.spillNode(t.root)
	if !ok {
		fmt.Println("Failed to spill")

		t.rollback()

		return false
	}

	// Root may be changed after spill
	t.root = t.root.root()

	// Write to disk
	ok = t.write()
	if !ok {
		fmt.Println("Failed to write transaction")

		t.rollback()

		return false
	}

	t.close()

	return true
}

// write writes all pages hold by this transaction.
func (t *Tx) write() bool {
	pages := pages{}

	for _, p := range t.pages {
		pages = append(pages, p)
	}

	sort.Sort(pages)

	// Write pages to disk
	for _, p := range pages {
		pos := int64(p.id) * int64(pageSize)
		size := (p.overflow + 1) * pageSize
		buf := (*[maxMmapSize]byte)(unsafe.Pointer(p))

		_, err := t.db.file.WriteAt(buf[:size], pos)
		if err != nil {
			fmt.Printf("Failed to write page: %v\n", err)

			return false
		}
	}

	// Return single pages to page pool
	for _, p := range pages {
		if p.overflow == 0 {
			buf := (*[maxMmapSize]byte)(unsafe.Pointer(p))[:pageSize]
			for i := range buf {
				buf[i] = 0
			}

			t.db.pagePool.Put(buf)
		}
	}

	return true
}

// TODO:
func (t *Tx) rollback() {
	// delete()
}

// getPage returns page from pgid.
func (t *Tx) getPage(id pgid) *page {
	// Check page buffer first
	p, exist := t.pages[id]
	if exist {
		return p
	}

	// If not found, return page from memory map
	p = t.db.getPage(id)
	t.pages[id] = p

	return p
}

// getNode returns node from pgid.
func (t *Tx) getNode(id common.Pgid, parent *node) *node {
	n, exist := t.nodes[id]
	if exist {
		return n
	}

	p := t.getPage(id)

	n = &node{
		parent: parent,
	}

	n.read(p)
	t.nodes[id] = n

	return n
}

// Get searches given key, returns (found, value)
func (t *Tx) Get(key kv.Key) (bool, kv.Value) {
	curr := t.root

	for !curr.isLeaf {
		_, i := curr.search(key)
		curr = curr.getChildAt(i)
	}

	found, i := curr.search(key)
	if found {
		return true, curr.values[i]
	}

	return false, kv.Value{}
}

// Set sets key with value, returns (found, oldValue)
func (t *Tx) Set(key kv.Key, value kv.Value) (bool, kv.Value) {
	if !t.writable {
		panic("Readonly transaction")
	}

	curr := t.root

	for {
		found, i := curr.search(key)

		if curr.isLeaf {
			if found {
				oldValue := curr.values[i]
				curr.values[i] = value

				return true, oldValue
			}

			curr.balanced = false
			curr.insertKeyValueAt(i, key, value)

			return false, kv.Value{}
		}

		curr = curr.getChildAt(i)
	}
}

// Remove removes given key from node recursively, returns (found, oldValue).
func (t *Tx) Remove(key kv.Key) (bool, kv.Value) {
	if !t.writable {
		panic("Readonly transaction")
	}

	curr := t.root

	for {
		found, i := curr.search(key)

		if curr.isLeaf {
			if !found {
				return false, nil
			}

			curr.balanced = false
			_, value := curr.removeKeyValueAt(i)

			return true, value
		}

		curr = curr.getChildAt(i)
	}
}

// getChildAt returns one child node.
func (tx *Tx) GetChildAt(n *tree.Node, i int) *tree.Node {
	if i < 0 || i >= n.KeyCount() {
		panic(fmt.Sprintf("Invalid child index: %d out of %d", i, n.keyCount()))
	}

	return Tx.getNode(n.GetChildID(i), n)
}

// spill recursively splits node and writes to pages(not to disk).
func (tx *Tx) spillNode(n *tree.Node) bool {
	if n.Spilled {
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
func (n *Node) tryMerge() {
	if n.balanced {
		return
	}

	n.balanced = true

	if n.keyCount() < minKeyCount {
		return
	}

	if n.mapSize() < int(float64(page.PageSize)*mergePagePercent) {
		return
	}

	// Root node, bring up the if have only one child
	if n.isRoot() {
		if !n.IsLeaf && n.keyCount() == 1 {
			child := n.getChildAt(0)

			n.IsLeaf = child.IsLeaf

			n.keys = child.keys[:]
			n.values = child.values[:]

			n.childPgids = child.childPgids[:]

			// ReParent grand children
			for i := 0; i < n.keyCount(); i++ {
				n.getChildAt(i).Parent = n
			}

			child.free()
		}

		return
	}

	// Remove empty node
	if n.keyCount() == 0 {
		// n.key could be different to Parent index key
		_, i := n.Parent.search(n.key)

		n.Parent.removeKeyChildAt(i)
		n.free()

		n.Parent.tryMerge()

		return
	}

	if n.Parent.keyCount() < 2 {
		panic("Parent should have at least one child")
	}

	var from *Node
	var to *Node
	var fromIdx int

	if n.pgid == n.Parent.childPgids[0] {
		// Merge node[i=0] <- node[1]
		fromIdx = 1
		from = n.Parent.getChildAt(1)
		to = n
	} else {
		// Merge node[i-1] <- node[i]
		_, i := n.Parent.search(n.key)

		fromIdx = i
		from = n
		to = n.Parent.getChildAt(i - 1)
	}

	// Check node type
	if from.IsLeaf != to.IsLeaf {
		panic("Sibling nodes should have same type")
	}

	for i := 0; i < from.keyCount(); i++ {
		from.getChildAt(i).Parent = to
	}

	to.keys = append(to.keys, from.keys...)
	to.values = append(to.values, from.values...)
	to.childPgids = append(to.childPgids, from.childPgids...)

	n.Parent.removeKeyChildAt(fromIdx)

	from.free()

	n.Parent.tryMerge()
}
