package mk

import (
	"fmt"
	"sort"
	"unsafe"
)

// Tx represents transaction,
// the MVCC layer above storage structure.
type Tx struct {
	db *DB
	// Transaction ID
	id uint32
	// Read-only mark
	writable bool
	// Pointer to mata struct
	meta *DBMeta
	// root points to the b+tree root
	root *NodeInterface
	// All accessed nodes in this transaction.
	nodes map[pgid]*NodeInterface
	// Dirty pages in this tx, nil for read-only tx.
	dirtyPages map[pgid]*PageInterface
}

// NewWritable creates new writable transaction.
func NewWritable(db *DB) (*Tx, bool) {
	if db.wtx != nil {
		fmt.Println("Cannot create multiple writable tx")
		return nil, false
	}
	db.lastTxID++
	tx := Tx{
		db:         db,
		id:         db.lastTxID,
		writable:   true,
		meta:       db.meta.copy(),
		nodes:      map[pgid]*Node{},
		dirtyPages: map[pgid]*Page{},
	}

	rootPage := db.getPage(db.meta.rootPage)
	tx.root = &Node{
		Parent: nil,
	}
	// fmt.Printf("rootid=%d\n", db.meta.rootPage)
	// fmt.Printf("Page: %s\n", rootPage)
	tx.root.ReadPage(rootPage)
	// fmt.Printf("Root: %s\n", root)
	tx.nodes[db.meta.rootPage] = tx.root

	db.txs = append(db.txs, &tx)
	db.wtx = &tx

	return &tx, true
}

// NewReadOnlyTx returns new read-only transaction.
func NewReadOnlyTx(db *DB) (*Tx, bool) {
	db.lastTxID++
	tx := Tx{
		db:         db,
		id:         db.lastTxID,
		writable:   false,
		meta:       db.meta.copy(),
		nodes:      map[pgid]*Node{},
		dirtyPages: nil,
	}
	rootPage := db.getPage(db.meta.rootPage)
	tx.root = &Node{
		Parent: nil,
	}
	tx.root.ReadPage(rootPage)
	tx.nodes[db.meta.rootPage] = tx.root

	db.txs = append(db.txs, &tx)

	return &tx, true
}

// allocate allocates contiguous pages.
func (tx *Tx) allocate(count int) (*Page, bool) {
	if !tx.writable {
		panic("Read only tx can't allocate")
	}
	pg, ok := tx.db.allocate(count)
	if ok {
		// Put new page in dirtyPages
		tx.dirtyPages[pg.Index] = pg
	}
	return pg, ok
}

func (tx *Tx) close() {}

// Commit balance b+tree, write changes to disk, and close transaction.
func (tx *Tx) Commit() bool {
	if !tx.writable {
		panic("commit read-only tx")
	}
	// Merge underfill nodes
	for _, n := range tx.nodes {
		tx.merge(n)
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

	// Free and reallocate freelist page
	tx.db.freelist.Add(tx.db.getPage(tx.meta.freelistPage))
	p, ok := tx.allocate(tx.db.freelist.Size())
	if !ok {
		return false
	}
	tx.db.freelist.WritePage(p)
	tx.meta.freelistPage = p.Index

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

type Pages []*PageHeader

func (pgs Pages) Len() int {
	return len(pgs)
}

func (pgs Pages) Less(i, j int) bool {
	return pgs[i].Index < pgs[j].Index
}

func (pgs Pages) Swap(i, j int) {
	pgs[i], pgs[j] = pgs[j], pgs[i]
}

// write writes all pages hold by this transaction.
func (tx *Tx) write() bool {
	// Write pages to disk in order
	pages := Pages{}
	for _, p := range tx.dirtyPages {
		pages = append(pages, p)
	}
	sort.Sort(pages)
	for _, p := range pages {
		pos := int64(p.Index) * int64(PageSize)
		size := (p.Overflow + 1) * PageSize
		buf := (*[MaxMapBytes]byte)(unsafe.Pointer(p))
		_, err := tx.db.file.WriteAt(buf[:size], pos)
		if err != nil {
			fmt.Printf("Failed to write page: %v\n", err)
			return false
		}
	}

	for _, p := range pages {
		if p.Overflow == 0 {
			// Return single pages to page pool
			buf := (*[PageSize]byte)(unsafe.Pointer(p))
			for i := range buf {
				buf[i] = 0
			}
			tx.db.singlePages.Put(buf) // nolint: staticcheck
		}
	}

	return true
}

func (tx *Tx) rollback() {
	if tx.writable {
		tx.db.freelist.Rollback()
		// TODO: freelist.reload()
	}
	tx.close()
}

// getPage returns page from pgid.
func (tx *Tx) getPage(id pgid) *Page {
	p, exist := tx.dirtyPages[id]
	if exist {
		return p
	}
	return tx.db.getPage(id)
}

// getNode returns node from pgid.
func (tx *Tx) getNode(id pgid, parent *Node) *Node {
	n, exist := tx.nodes[id]
	if exist {
		return n
	}

	p := tx.getPage(id)
	n = &Node{
		Parent: parent,
	}

	n.ReadPage(p)
	tx.nodes[id] = n

	return n
}

// Get searches given key, returns (found, value)
func (tx *Tx) Get(key Key) (bool, Value) {
	curr := tx.root
	for !curr.IsLeaf {
		_, i := curr.Search(key)
		curr = tx.getChildAt(curr, i)
	}
	found, i := curr.Search(key)
	if found {
		return true, curr.GetValueAt(i)
	}
	return false, Value{}
}

// Set sets key with value, returns (found, oldValue)
func (tx *Tx) Set(key Key, value Value) (bool, Value) {
	if !tx.writable {
		panic("Readonly transaction")
	}

	curr := tx.root
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

			return false, Value{}
		}

		curr = tx.getChildAt(curr, i)
	}
}

// Remove removes given key from node recursively, returns (found, oldValue).
func (tx *Tx) Remove(key Key) (bool, Value) {
	if !tx.writable {
		panic("Readonly transaction")
	}

	curr := tx.root

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

		curr = tx.getChildAt(curr, i)
	}
}

// getChildAt returns one child node.
func (tx *Tx) getChildAt(n *Node, i int) *Node {
	if i < 0 || i >= n.KeyCount() {
		panic(fmt.Sprintf("Invalid child index: %d out of %d", i, n.KeyCount()))
	}
	return tx.getNode(n.GetChildID(i), n)
}

// spill splits node and writes to pages(not to disk).
// spill run top-down
func (tx *Tx) spillNode(n *Node) bool {
	if n.Spilled {
		return true
	}
	// Spill children first
	if !n.IsLeaf {
		for i := 0; i < n.KeyCount(); i++ {
			ch := tx.getChildAt(n, i)
			ok := tx.spillNode(ch)
			if !ok {
				return false
			}
		}
	}
	// Split self
	for _, n := range n.Split() {
		// Since node is changed after split, we need
		// to allocate new pages for each node.
		if n.Index != 0 {
			// When node has an old page, return to
			// freelist
			tx.db.freelist.Add(tx.getPage(n.Index))
			n.Index = 0
		}
		// Allocate new page
		// For simplicity, allocate one more page
		p, ok := tx.allocate((n.Size() / PageSize) + 1)
		if !ok {
			return false
		}
		n.Index = p.Index
		// Write to page
		n.WritePage(p)
		// Spilled is only set to true here
		n.Spilled = true
		if n.Key == nil {
			n.Key = n.Keys[0]
		}
		// Insert new node to parent.
		if !n.IsRoot() {
			_, i := n.Parent.Search(n.Key)
			n.Parent.InsertKeyChildAt(i, n.Key, n.Index)
		}
	}
	return true
}

// merge merges underfilled nodes with sibliings.
// merge runs bottom-up
func (tx *Tx) merge(n *Node) {
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

// freeNode returns page to freelistx.
func (tx *Tx) freeNode(n *Node) {
	delete(tx.nodes, n.Index)
	delete(tx.dirtyPages, n.Index)
	if n.Index != 0 {
		tx.db.freelist.Add(tx.getPage(n.Index))
	}
}
