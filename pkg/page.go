package mk

import (
	"unsafe"

	"github.com/heketi/heketi/pkg/db"
)

const (
	// InternalFlag marks page as internal node
	metaFlag = 0x01

	// internalFlag marks page as leaf
	internalFlag = 0x02

	// leafFlag
	leafFlag = 0x04

	MetaPage     = "MetaPage"
	InternalPage = "InternalPage"
	LeafPage     = "LeafPage"
	UnknownPage  = "UnknownPage"

	MaxAllocSize = 0xFFFFFFF
	maxPageIndex = 0x7FFFFFF
	KVMetaSize   = int(unsafe.Sizeof(KVMeta{}))
)

// page is the mmap storage block
// layout:
// page struct | list of KVMeta or IndexMeta | (*DataPtr) keys or keys with values
type page struct {
	// Each page has its index
	ID uint64

	// Flag marks page type
	Flags uint16

	// Overflow is 0 for single page
	Overflow uint32

	// NumKeys is key count
	NumKeys uint16

	// DataPrt points to starting address of metadata
	DataPtr uintptr
}

// KVMeta stores offset and size of one KV pair
type kvMeta struct {
	// Offset represents offset between KV content and this Meta struct
	// in bytes
	Offset uint32

	// Keysz is the length of the key
	KeySize uint32

	// Valuesz is the length of the value
	ValueSize uint32
}

type indexMeta struct {
	Offset uint32

	KeySize uint32

	ChildID uint32
}

func (p *page) getType() string {
	if (p.Flags & metaFlag) != 0 {
		return MetaPage
	}
	if (p.Flags & internalFlag) != 0 {
		return InternalPage
	}
	if (p.Flags & leafFlag) != 0 {
		return LeafPage
	}
	return UnknownPage
}

// GetChildPgid returns child pgid for given index
func (p *Page) GetChildPgid(index uint16) Pgid {
	return (*[maxPageIndex]Pgid)(unsafe.Pointer(&p.ChildPtr))[index]
}

// GetKVMeta returns KVMeta for given index
func (p *Page) GetKVMeta(index uint16) *KVMeta {
	return &((*[maxPageIndex]KVMeta)(unsafe.Pointer(&p.MetaPtr)))[index]
}

// Key returns the content of the key
func (m *KVMeta) Key() []byte {
	buf := (*[MaxAllocSize]byte)(unsafe.Pointer(&m))
	return buf[m.Offset : m.Offset+m.Keysz]
}

// Value returns the content of the value
func (m *KVMeta) Value() []byte {
	buf := (*[MaxAllocSize]byte)(unsafe.Pointer(&m))
	begin := m.Offset + m.Keysz
	return buf[begin : begin+m.Valuesz]
}

type Pager struct {
	db    *db.DB
	pager map[Pgid]*Page
}

func (p *Pager) page(id Pgid) *Page {
	if p, ok := p.pager[id]; ok {
		return p
	}
	return p.db.GetPage(id)
}
