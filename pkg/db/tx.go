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
	nodes map[common.Pgid]*tree.Node
	// All accessed pages in this transaction.
	pages map[common.Pgid]*page.Page
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
		nodes:    map[common.Pgid]*tree.Node{},
		pages:    map[common.Pgid]*page.Page{},
	}

	t.nodes[db.meta.rootPage] = root
	t.pages[db.meta.rootPage] = rootPage

	db.txs = append(db.txs, &t)
	db.writableTx = &t

	return &t, true
}

// NewReadOnlyTx returns new read-only transaction.
func NewReadOnlyTx(db *DB) (*Tx, bool) {
	rootPage := db.getPage(db.meta.rootPage)
	root := &tree.Node{
		Parent: nil,
	}
	root.ReadPage(rootPage)

	t := Tx{
		db:       db,
		id:       1,
		writable: false,
		meta:     db.meta.copy(),
		root:     root,
		nodes:    map[common.Pgid]*tree.Node{},
		pages:    map[common.Pgid]*page.Page{},
	}

	t.nodes[db.meta.rootPage] = root
	t.pages[db.meta.rootPage] = rootPage
	db.txs = append(db.txs, &t)

	return &t, true
}

// allocate returns contiguous pages.
func (t *Tx) allocate(count int) (*page.Page, bool) {
	if !t.writable {
		panic("Read only tx can't allocate")
	}
	return t.db.allocate(count)
}

func (t *Tx) close() {

}

// Commit balance b+tree, write changes to disk, and close transaction.
func (tx *Tx) Commit() bool {
	if !tx.writable {
		panic("commit read-only tx")
	}
	// Merge underfill nodes
	for _, node := range tx.nodes {
		tx.merge(node)
	}

	// Split nodes and write to memory page
	ok := tx.spillNode(tx.root)
	if !ok {
		fmt.Println("Failed to spill")
		tx.rollback()
		return false
	}

	// Root may be changed after spill
	tx.root = tx.root.Root()

	// Write to disk
	ok = tx.write()
	if !ok {
		fmt.Println("Failed to write transaction")
		tx.rollback()
		return false
	}

	tx.close()
	return true
}

// write writes all pages hold by this transaction.
func (t *Tx) write() bool {
	pages := page.Pages{}
	for _, p := range t.pages {
		pages = append(pages, p)
	}
	sort.Sort(pages)

	// Write pages to disk
	for _, p := range pages {
		pos := int64(p.Index) * int64(page.PageSize)
		size := (p.Overflow + 1) * page.PageSize
		buf := (*[common.MmapMaxSize]byte)(unsafe.Pointer(p))
		_, err := t.db.file.WriteAt(buf[:size], pos)
		if err != nil {
			fmt.Printf("Failed to write page: %v\n", err)
			return false
		}
	}

	// Return single pages to page pool
	for _, p := range pages {
		if p.Overflow == 0 {
			buf := (*[common.MmapMaxSize]byte)(unsafe.Pointer(p))[:page.PageSize]
			for i := range buf {
				buf[i] = 0
			}
			t.db.singlePages.Put(buf)
		}
	}

	return true
}

// TODO:
func (t *Tx) rollback() {
	// delete()
}

// getPage returns page from pgid.
func (t *Tx) getPage(id common.Pgid) *page.Page {
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
func (t *Tx) getNode(id common.Pgid, parent *tree.Node) *tree.Node {
	n, exist := t.nodes[id]
	if exist {
		return n
	}

	p := t.getPage(id)
	n = &tree.Node{
		Parent: parent,
	}

	n.ReadPage(p)
	t.nodes[id] = n

	return n
}

// Get searches given key, returns (found, value)
func (t *Tx) Get(key kv.Key) (bool, kv.Value) {
	curr := t.root
	for !curr.IsLeaf {
		_, i := curr.Search(key)
		curr = t.getChildAt(curr, i)
	}
	found, i := curr.Search(key)
	if found {
		return true, curr.GetValueAt(i)
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
		found, i := curr.Search(key)

		if curr.IsLeaf {
			if found {
				oldValue := curr.GetValueAt(i)
				curr.SetValueAt(i, value)
				return true, oldValue
			}
			curr.Balanced = false
			curr.InsertKeyValueAt(i, key, value)

			return false, kv.Value{}
		}

		curr = t.getChildAt(curr, i)
	}
}

// Remove removes given key from node recursively, returns (found, oldValue).
func (t *Tx) Remove(key kv.Key) (bool, kv.Value) {
	if !t.writable {
		panic("Readonly transaction")
	}

	curr := t.root

	for {
		found, i := curr.Search(key)

		if curr.IsLeaf {
			if !found {
				return false, nil
			}

			curr.Balanced = false
			_, value := curr.RemoveKeyValueAt(i)

			return true, value
		}

		curr = t.getChildAt(curr, i)
	}
}

// getChildAt returns one child node.
func (tx *Tx) getChildAt(n *tree.Node, i int) *tree.Node {
	if i < 0 || i >= n.KeyCount() {
		panic(fmt.Sprintf("Invalid child index: %d out of %d", i, n.KeyCount()))
	}
	return tx.getNode(n.GetChildID(i), n)
}

// spill recursively splits node and writes to pages(not to disk).
func (tx *Tx) spillNode(n *tree.Node) bool {
	if n.Spilled {
		return true
	}
	// Spill children first
	for i := 0; i < n.KeyCount(); i++ {
		ch := tx.getChildAt(n, i)
		ok := tx.spillNode(ch)
		if !ok {
			return false
		}
	}
	// Split self
	for _, node := range n.Split() {
		// Ensure page for each node.
		// Only the first node could have associated page,
		// free this page first.
		if node.Index != 0 {
			tx.db.freelist.Add(tx.getPage(node.Index))
			// Mark node as page-freed
			node.Index = 0
		}
		// Then allocate page for node.
		// For simplicity, allocate one more page
		p, ok := tx.allocate((n.Size() / page.PageSize) + 1)
		if !ok {
			return false
		}
		node.Index = p.Index
		// Write to page
		node.WritePage(p)
		node.Spilled = true
		if node.Key == nil {
			node.Key = node.Keys[0]
		}

		// Insert new node to parent.
		if !node.IsRoot() {
			_, i := node.Parent.Search(node.Key)
			node.Parent.InsertKeyChildAt(i, node.Key, node.Index)
		}
	}
	return true
}

// merge merges underfill nodes.
// merge runs a bottom-up way.
func (tx *Tx) merge(n *tree.Node) {
	if n.Balanced {
		return
	}
	n.Balanced = true
	if !n.Underfill() {
		return
	}

	if n.IsRoot() {
		// When root has only one child, merge with it
		if !n.IsLeaf && n.KeyCount() == 1 {
			child := tx.getChildAt(n, 0)

			n.IsLeaf = child.IsLeaf
			n.Keys = child.Keys[:]
			n.Values = child.Values[:]
			n.Cids = child.Cids[:]
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

	var from *tree.Node
	var to *tree.Node
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

// freeNode returns page to freelist.
func (tx *Tx) freeNode(n *tree.Node) {
	delete(tx.nodes, n.Index)
	delete(tx.pages, n.Index)
	if n.Index != 0 {
		tx.db.freelist.Add(tx.getPage(n.Index))
	}
}
