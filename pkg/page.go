package mk

import (
	"fmt"
	"unsafe"
)

const (
	// Meta page flag
	MetaPage = 1
	// Freelist page flag
	FreelistPage = 1 << 1
	// Internal page flag
	InternalPage = 1 << 2
	// Leaf page flag
	LeafPage = 1 << 3
)

const (
	// HeaderSize is page header size
	HeaderSize = int(unsafe.Sizeof(PageHeader{}))
	// KvMetaSize is key-value pair size
	KvMetaSize = int(unsafe.Sizeof(kvMeta{}))
)

const (
	// PairInfoSize is size for each pair info
	// PairInfoSize = int(unsafe.Sizeof(pairInfo{}))
	// PageSize should be OS page size, we use fixed 4KB for simplicity
	PageSize = 4096
)

// DBMeta holds database metadata.
type DBMeta struct {
	// magic should be mkMagic, to distinguish DB file
	magic uint32
	// number of allocated pages, also id of next new page
	// in headroom
	totalPages int
	// page id of first freelist page
	freelistPage int
	// page id of root page
	rootPage int
}

type PageInterface interface {
	String() string

	IsMeta() bool
	IsLeaf() bool
	IsFreelist() bool
	IsInternal() bool

	GetDBMeta() *DBMeta

	GetKeyCount() int
	GetChildCount() int
	Getint() int

	GetKeyAt(int) []byte
	GetValueAt(int) []byte
	GetChildIDAt(int) int

	CalcSize(int, int) int

	SetKeyCount(int)
	SetFlag(uint16)

	WriteKeyValueAt(i, keyOffset int, key, value []byte)
	WriteKeyChildAt(i, keyOffset int, key []byte, cid int)
}

// Page implements PageInterface
// Leaf page layout:
// pageHeader | [count]kvMeta | <key data> | <value data>
//
// Internal page layout:
// pageHeader | [count]kvMeta | <key data>
// Count = len(value) = len(key)+1, last meta key info is empty
type PageHeader struct {
	// overflow counter, 0 for single page
	overflow int
	// key count, childCount = keyCount + 1 for internal page
	// childCount = keyCount for leaf page
	keyCount int
	// mmap index
	// starting page (index 0) should never be freed
	index int
	// type mark
	flag uint16
	// starting point of metadata.
	anchor uintptr
}

// PageFromBuffer returns page with given index in a buffer.
// Go slices are metadata to underlying structure, but
// arrays are values. So never pass arrays.
func PageFromBuffer(buf []byte, i int) PageInterface {
	return (*PageHeader)(unsafe.Pointer(&buf[i*int(PageSize)]))
}

type kvMeta struct {
	// offset from anchor to key
	keyOffset int
	keySize   int
	valueSize int
	// child page id
	cid int
}

// String returns string for print.
func (p PageHeader) String() string {
	return fmt.Sprintf(
		"page[%d] %s keys=%d, overflow=%d",
		p.index,
		p.getType(),
		p.keyCount,
		p.overflow,
	)
}

func (p *PageHeader) SetKeyCount(count int) {
	p.keyCount = count
}

func (p *PageHeader) GetKeyCount() int {
	return p.keyCount
}

func (p *PageHeader) GetChildCount() int {
	if p.IsLeaf() {
		return p.keyCount
	}
	return p.keyCount + 1
}

func (p *PageHeader) Getint() int {
	return p.index
}

func (p *PageHeader) GetDBMeta() *DBMeta {
	if !p.IsMeta() {
		panic("not meta page")
	}
	return (*DBMeta)(unsafe.Pointer(&p.anchor))
}

// Note: key could be in mmap region, therefore immutable
func (p *PageHeader) GetKeyAt(i int) []byte {
	meta := (*[MaxPairs]kvMeta)(unsafe.Pointer(&p.anchor))[i]
	buf := (*[MaxMapBytes]byte)(unsafe.Pointer(&p.anchor))
	return buf[meta.keyOffset : meta.keyOffset+meta.keySize]
}

// Note: value could be in mmap region, therefore immutable
func (p *PageHeader) GetValueAt(i int) []byte {
	if p.IsInternal() {
		panic("not leaf page")
	}
	meta := (*[MaxKeys]kvMeta)(unsafe.Pointer(&p.anchor))[i]
	buf := (*[MaxMapBytes]byte)(unsafe.Pointer(&p.anchor))
	begin := meta.keyOffset + meta.keySize
	end := begin + meta.valueSize
	return buf[begin:end]
}

func (p *PageHeader) GetChildIDAt(i int) int {
	if p.IsLeaf() {
		panic("not internal page")
	}
	meta := (*[MaxKeys]kvMeta)(unsafe.Pointer(&p.anchor))[i]
	return meta.cid
}

func (p *PageHeader) CalcSize(slotCount int, dataSize int) int {
	return HeaderSize + KvMetaSize*slotCount + dataSize
}

// header | [count]kvMeta | key | value | key | value | ..
func (p *PageHeader) WriteKeyValueAt(i, keyOffset int, key, value []byte) {
	km := (*[MaxKeys]kvMeta)(unsafe.Pointer(&p.anchor))[i]
	km.keySize = len(key)
	km.valueSize = len(value)
	km.keyOffset = keyOffset

	buf := (*[MaxMapBytes]byte)(unsafe.Pointer(&p.anchor))[keyOffset:]
	copy(buf, key)
	copy(buf[len(key):], value)
}

// The last key shoud be empty since it's for internal page.
func (p *PageHeader) WriteKeyChildAt(i, keyOffset int, key []byte, cid int) {
	km := (*[MaxKeys]kvMeta)(unsafe.Pointer(&p.anchor))[i]
	km.keySize = len(key)
	km.cid = cid
	km.keyOffset = keyOffset

	buf := (*[MaxMapBytes]byte)(unsafe.Pointer(&p.anchor))[keyOffset:]
	copy(buf, key)
}

func (p *PageHeader) SetFlag(flag uint16) {
	p.flag |= flag
}

func (p *PageHeader) IsMeta() bool {
	return (p.flag & MetaPage) != 0
}

func (p *PageHeader) IsFreelist() bool {
	return (p.flag & FreelistPage) != 0
}

func (p *PageHeader) IsLeaf() bool {
	return (p.flag & LeafPage) != 0
}

func (p *PageHeader) IsInternal() bool {
	return (p.flag & InternalPage) != 0
}

// getType returns page type as string
func (p PageHeader) getType() string {
	if p.IsMeta() {
		return "meta"
	}
	if p.IsFreelist() {
		return "freelist"
	}
	if p.IsInternal() {
		return "internal"
	}
	if p.IsLeaf() {
		return "leaf"
	}
	panic("Unknown page type")
}
