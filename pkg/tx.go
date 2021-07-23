package mk

import (
	"fmt"
	"sort"
)

// Tx represents transaction,
// the MVCC layer above storage structure.
type Tx struct {
	db *DB
	// Transaction ID
	id int
	// Read-only mark
	writable bool
	// Pointer to mata struct
	meta *DBMeta
	// root points to the b+tree root
	root NodeInterface
	// nodes stores all accessed nodes in this transaction.
	nodes map[int]NodeInterface
	// Dirty pages in this tx, nil for read-only tx.
	dirtyPages map[int]PageInterface
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
		nodes:      map[int]NodeInterface{},
		dirtyPages: map[int]PageInterface{},
	}

	rootPage := db.getPage(db.meta.rootPage)
	tx.root = NewNode()
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
		nodes:      map[int]NodeInterface{},
		dirtyPages: nil,
	}
	rootPage := db.getPage(db.meta.rootPage)
	tx.root = NewNode()
	tx.root.ReadPage(rootPage)
	tx.nodes[db.meta.rootPage] = tx.root

	db.txs = append(db.txs, &tx)

	return &tx, true
}

// allocate allocates contiguous pages.
func (tx *Tx) allocate(count int) (PageInterface, bool) {
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
	ok := tx.split(tx.root)
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

// write writes all pages hold by this transaction.
func (tx *Tx) write() bool {
	// Write pages to disk in order
	pages := []PageInterface{}
	for _, p := range tx.dirtyPages {
		pages = append(pages, p)
	}
	sort.Slice(pages, func(i, j int) bool {
		return pages[i].GetIndex() < pages[j].GetIndex()
	})
	for _, p := range pages {
		offset := p.GetIndex() * PageSize
		buf := p.GetBuffer()

		_, err := tx.db.file.WriteAt(buf, int64(offset))
		if err != nil {
			fmt.Printf("Failed to write page: %v\n", err)
			return false
		}
	}

	// Return single page buffer to pool
	for _, p := range pages {
		if p.GetPageCount() == 1 {
			buf := p.GetBuffer()
			for i := range buf {
				buf[i] = 0
			}
			tx.db.singlePages.Put(buf)
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

// getPage returns page from int.
func (tx *Tx) getPage(id int) PageInterface {
	p, exist := tx.dirtyPages[id]
	if exist {
		return p
	}
	return PageFromBuffer(*tx.db.mmBuf, id)
}

func (tx *Tx) getNode(id int, parent NodeInterface) NodeInterface {
	n, exist := tx.nodes[id]
	if exist {
		return n
	}

	p := tx.getPage(id)
	n = NewNode()
	n.ReadPage(p)
	n.SetParent(parent)

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
func (tx *Tx) Set(key, value []byte) (bool, []byte) {
	if !tx.writable {
		panic("Readonly transaction")
	}

	curr := tx.root
	for {
		found, i := curr.Search(key)
		if curr.IsLeaf() {
			if found {
				old := curr.GetValueAt(i)
				curr.SetValueAt(i, value)
				return true, old
			}

			curr.Balanced = false
			curr.InsertKeyValueAt(i, key, value)

			return false, []byte{}
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

// split splits node from top-down and writes to page buffer(not to disk).
func (tx *Tx) split(n NodeInterface) bool {
	if n.Spilled {
		return true
	}
	if n.IsInternal() {
		// Spill child nodes first
		for i := 0; i < n.GetChildCount(); i++ {
			child := tx.getNode(n.GetCIDAt(i), n)
			ok := tx.split(child)
			if !ok {
				return false
			}
		}
	}

	for _, n := range n.Split() {
		// Remember we're in a writable transaction,
		// so for every node in the access path, whether
		// it's splited or node, we need to allocate a new
		// page.
		if n.GetIndex() != 0 {
			// Return the old page
			tx.db.freelist.Add(tx.id, tx.getPage(n.GetIndex()))
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

// // merge merges underfilled nodes with sibliings.
// // merge runs bottom-up
// func (tx *Tx) merge(n *Node) {
// 	if n.IsBalanced() {
// 		return
// 	}
// 	n.Balanced = true
// 	if !n.Underfill() {
// 		return
// 	}

// 	if n.IsRoot() {
// 		// When root has only one child, merge with it
// 		if !n.IsLeaf && n.KeyCount() == 1 {
// 			child := tx.getChildAt(n, 0)

// 			n.IsLeaf = child.IsLeaf
// 			n.Keys = child.Keys
// 			n.Values = child.Values
// 			n.Cids = child.Cids
// 			// Reparent grand children
// 			for i := 0; i < n.KeyCount(); i++ {
// 				tx.getChildAt(n, i).Parent = n
// 			}
// 			tx.freeNode(child)
// 		}
// 		return
// 	}

// 	if n.KeyCount() == 0 {
// 		// Remove empty node, also remove inode from parent
// 		// n.key could be different to Parent index key
// 		_, i := n.Parent.Search(n.Key)
// 		n.Parent.RemoveKeyChildAt(i)
// 		tx.freeNode(n)
// 		// check parent merge
// 		tx.merge(n.Parent)
// 		return
// 	}

// 	if n.Parent.KeyCount() < 2 {
// 		panic("Parent should have at least one child")
// 	}

// 	var from *Node
// 	var to *Node
// 	var fromIdx int

// 	if n.Index == n.Parent.Cids[0] {
// 		// Leftmost node, merge right sibling with it
// 		fromIdx = 1
// 		from = tx.getChildAt(n.Parent, 1)
// 		to = n
// 	} else {
// 		// merge current node with left sibling
// 		_, i := n.Parent.Search(n.Key)
// 		fromIdx = i
// 		from = n
// 		to = tx.getChildAt(n.Parent, i-1)
// 	}

// 	// Check node type
// 	if from.IsLeaf != to.IsLeaf {
// 		panic("Sibling nodes should have same type")
// 	}
// 	// Reparent from node child
// 	for i := 0; i < from.KeyCount(); i++ {
// 		tx.getChildAt(from, i).Parent = to
// 	}

// 	to.Keys = append(to.Keys, from.Keys...)
// 	to.Values = append(to.Values, from.Values...)
// 	to.Cids = append(to.Cids, from.Cids...)

// 	n.Parent.RemoveKeyChildAt(fromIdx)
// 	tx.freeNode(from)
// 	tx.merge(n.Parent)
// }

// freeNode returns page to freelistx.
func (tx *Tx) freeNode(n *Node) {
	delete(tx.nodes, n.Index)
	delete(tx.dirtyPages, n.Index)
	if n.Index != 0 {
		tx.db.freelist.Add(tx.getPage(n.Index))
	}
}
