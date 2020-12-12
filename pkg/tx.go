package mk

import (
	"sort"
	"unsafe"
)

type txid int

// Tx represents transaction.
type Tx struct {
	db *DB

	// Transaction ID
	id txid

	// Read-only mark
	writable bool

	meta *Meta

	// All accessed nodes in this transaction.
	nodes map[pgid]*node

	// root points to the b+tree root
	root *node

	// All accessed pages in this transaction.
	pages map[pgid]*page
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
		log.Info("Commit on read only tx")

		return false
	}

	// Merge underfill nodes
	for _, node := range t.nodes {
		node.tryMerge()
	}

	// Split nodes and write to memory page
	ok := t.root.spill()
	if !ok {
		log.Info("Failed to spill")

		t.rollback()

		return false
	}

	// Root may be changed after spill
	t.root = t.root.root()

	// Write to disk
	ok = t.write()
	if !ok {
		log.Info("Failed to write transaction")

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
			log.Error(err, "Failed to write page")

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

// getPage returns page for this transaction.
func (t *Tx) getPage(id pgid) *page {
	// Check page buffer first
	p, exist := t.pages[id]
	if exist {
		return p
	}

	// If not found, return page from memory map
	p = t.db.pageFromMmap(id)
	t.pages[id] = p

	return p
}

// getNode returns node from given page
func (t *Tx) getNode(p *page, parent *node) *node {
	n, exist := t.nodes[p.id]
	if exist {
		return n
	}

	n = &node{
		parent: parent,
	}
	n.read(p)

	t.nodes[p.id] = n

	return n
}

// Get searches given key, returns (found, value)
func (t *Tx) Get(key KeyType) (bool, ValueType) {
	curr := t.root

	for curr.isLeaf == false {
		_, i := curr.search(key)
		curr = curr.childPtrs[i]
	}

	found, i := curr.search(key)
	if found {
		return true, curr.values[i]
	}

	return false, ValueType{}
}

// Set sets key with value, returns (found, oldValue)
func (t *Tx) Set(key KeyType, value ValueType) (bool, ValueType) {
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
			curr.insertValueAt(i, value)

			return false, ValueType{}
		}

		curr = curr.childPtrs[i]
	}
}

// Remove removes given key from node recursively, returns (found, oldValue).
func (t *Tx) Remove(key KeyType) (bool, ValueType) {
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

			return true, curr.removeValueAt(i)
		}

		curr = curr.childPtrs[i]
	}
}
