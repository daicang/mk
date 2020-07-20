package mk

import (
	"unsafe"

	"github.com/heketi/heketi/pkg/db"
)

const (
	// InternalFlag marks page as internal node
	InternalFlag = 0x01

	// LeafFlag marks page as leaf
	LeafFlag = 0x02

	MaxAllocSize = 0xFFFFFFF
	maxPageIndex = 0x7FFFFFF
	KVMetaSize   = int(unsafe.Sizeof(KVMeta{}))
)

// Pgid is page id
type pgid uint64

// Page represents one mmap page
// layout:
// page struct | child pgid | child pgid | .. | (p.Data)meta | meta | .. | key | value | key | value | ..
type page struct {
	// Each page has its index
	ID pgid
	// Flag tell the page is leaf or not
	Flags uint16
	// Count is inode count
	Count uint16
	// ChildPtr points to starting address of children pgid array
	// Empty when page is leaf
	ChildPtr uintptr
	// Meta points to starting address of KV metadata
	MetaPtr uintptr
}

// KVMeta stores offset and size of one KV pair
type KVMeta struct {
	// Offset represents offset between KV content and this Meta struct
	// in bytes
	Offset uint32
	// Keysz is the length of the key
	Keysz uint32
	// Valuesz is the length of the value
	Valuesz uint32
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
