package mk

import (
	"sort"
	"unsafe"
)

type txid int

// Tx represents transaction
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

func (t *Tx) close() {

}

// Commit rebalances b+tree and write changes to disk,
// then closes the transaction.
func (t *Tx) Commit() error {
	if !t.writable {
		return errReadOnly
	}

	for _, node := range t.nodes {
		node.rebalance()
	}

	err := t.root.spill()
	if err != nil {
		return err
	}

	t.root = t.root.root()

	err = t.write()
	if err != nil {
		t.rollback()
	}

	t.close()

	return nil
}

// write writes all pages hold by this transaction.
func (t *Tx) write() error {
	pages := pages{}

	for _, p := range t.pages {
		pages = append(pages, p)
	}

	sort.Sort(pages)

	// Write pages to disk
	for _, p := range pages {
		pos := int64(p.id) * int64(pageSize)
		size := (p.overflow + 1) * pageSize
		buf := (*[maxArrSize]byte)(unsafe.Pointer(p))

		_, err := t.db.file.WriteAt(buf[:size], pos)
		if err != nil {
			log.Info("Failed to write page")

			return err
		}
	}

	// Return single pages to page pool
	for _, p := range pages {
		if p.overflow == 0 {
			buf := (*[maxArrSize]byte)(unsafe.Pointer(p))[:pageSize]
			for i := range buf {
				buf[i] = 0
			}

			t.db.pagePool.Put(buf)
		}
	}

	return nil
}

// TODO:
func (t *Tx) rollback() {
	// delete()
}

// page checks page from buffer first, when not found,
// return page from memory map.
func (t *Tx) getPage(id pgid) *page {
	p, exist := t.pages[id]
	if exist {
		return p
	}

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
