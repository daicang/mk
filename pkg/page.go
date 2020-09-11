package mk

import (
	"fmt"
	"os"
	"unsafe"
)

const (
	metaPageFlag     = 1
	freelistPageFlag = 1 << 1
	internalPageFlag = 1 << 2
	leafPageFlag     = 1 << 3

	MetaPage     = "MetaPage"
	FreelistPage = "FreelistPage"
	InternalPage = "InternalPage"
	LeafPage     = "LeafPage"
	UnknownPage  = "UnknownPage"

	pageHeaderSize = int(unsafe.Sizeof(page{}))
	pairSize       = int(unsafe.Sizeof(pair{}))
)

var (
	// Default page size on Linux/Mac OS is 4KB
	pageSize = os.Getpagesize()
)

type pgid uint32

// page is the basic mmap storage block
// internal page: page struct | pairs | key | key | ..
// leaf page: page struct | pairs | key | value | key | ..
type page struct {
	// Each page has its index
	id pgid

	tx *tx

	// flag marks page type
	flags uint16

	// overflow is number of following overflow pages, 0 for single page
	overflow int

	// numKeys is key count
	numKeys int

	// pairs marks the starting addr of pairs.
	pairs uintptr
}

type pages []*page

func (pgs pages) Len() int {
	return len(pgs)
}

func (pgs pages) Less(i, j int) bool {
	return pgs[i].id < pgs[j].id
}

func (pgs pages) Swap(i, j int) {
	pgs[i], pgs[j] = pgs[j], pgs[i]
}

// pair stores metadata for one KV pair or one index
type pair struct {
	// offset is the offset from pairs to starting addr of the key
	// page.pairs + offset = key addr
	// key addr + len(key) = value addr
	offset int

	// Keysz is the length of the key
	keySize int

	// Valuesz is the length of the value
	// 0 for internal node with no value
	valueSize int

	// childID is the child pgid
	// empty for leaf node with no child
	childID pgid
}

// String returns string for print.
func (p *page) String() string {
	return fmt.Sprintf("%s[%d] %d keys, overflow=%d", p.getType(), p.id, p.numKeys, p.overflow)
}

func (p *page) isMeta() bool {
	return (p.flags & metaPageFlag) != 0
}

func (p *page) isFreelist() bool {
	return (p.flags & freelistPageFlag) != 0
}

func (p *page) isLeaf() bool {
	return (p.flags & leafPageFlag) != 0
}

func (p *page) isInternal() bool {
	return (p.flags & internalPageFlag) != 0
}

// getType returns page type as string
func (p *page) getType() string {
	if (p.flags & metaPageFlag) != 0 {
		return MetaPage
	}

	if (p.flags & freelistPageFlag) != 0 {
		return FreelistPage
	}

	if (p.flags & internalPageFlag) != 0 {
		return InternalPage
	}

	if (p.flags & leafPageFlag) != 0 {
		return LeafPage
	}

	log.Info("Unknown page")

	return UnknownPage
}

// toMeta converts page to DB meta struct
func (p *page) toMeta() *Meta {
	if !p.isMeta() {
		panic("Calling toMeta with non-meta page")
	}

	return (*Meta)(unsafe.Pointer(&p.pairs))
}

func (p *page) free() {

}

func (p *page) getPair(i int) *pair {
	return &(*[maxArrSize]pair)(unsafe.Pointer(&p.pairs))[i]
}

func (p *page) getKey(i int) KeyType {
	pair := p.getPair(i)
	buf := (*[maxMmSize]byte)(unsafe.Pointer(&p.pairs))[pair.offset:]

	return buf[:pair.keySize]
}

func (p *page) getValue(i int) ValueType {
	if !p.isLeaf() {
		panic("Must call getValue on leaf node")
	}

	pair := p.getPair(i)
	valueOffset := pair.offset + pair.keySize
	buf := (*[maxMmSize]byte)(unsafe.Pointer(&p.pairs))[valueOffset:]

	return buf[:pair.valueSize]
}

func (p *page) getChildPgid(i int) pgid {
	if !p.isInternal() {
		panic("Must call getChildPgid on internal node")
	}

	return p.getPair(i).childID
}

// bufferPage returns page in given buffer, starting from 0
func bufferPage(buf []byte, i int) *page {
	return (*page)(unsafe.Pointer(&buf[i*pageSize]))
}
