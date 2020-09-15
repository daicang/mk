package mk

import (
	"os"
	"unsafe"
)

const (
	metaFlag     = 0x01
	internalFlag = 0x02
	leafFlag     = 0x04

	MetaPage     = "MetaPage"
	InternalPage = "InternalPage"
	LeafPage     = "LeafPage"
	UnknownPage  = "UnknownPage"

	// maxArraySize is uint64 max, must be constant
	maxArraySize = 0xFFFFFFFF

	// maxPageIndex should be at least maxAllocSize / pageSize
	// must be constant
	maxPageIndex = 0xFFFFFFF

	metaSize = int(unsafe.Sizeof(meta{}))
)

var (
	// Linux page size by default is 4kB, 2^12 Bytes
	pageSize = os.Getpagesize()
)

type pgid uint32

// page is the basic mmap storage block
// page layout:
// page struct | (metaAddr)meta structs | (dataAddr) keys or kvs
type page struct {
	// Each page has its index
	ID pgid

	// lag marks page type
	flags uint16

	// overflow is 0 for single page
	overflow int

	// numKeys is key count
	numKeys int

	// metaAddr marks the starting address of metadata
	metaAddr uintptr
}

// meta stores metadata for one KV pair or one index
type meta struct {
	// offset is the shift from dataAddr
	offset int

	// Keysz is the length of the key
	keySize int

	// Valuesz is the length of the value (for leaf node)
	valueSize int

	// childID is the child pgid (for internal node)
	childID pgid
}

func (p *page) getType() string {
	if (p.flags & metaFlag) != 0 {
		return MetaPage
	}
	if (p.flags & internalFlag) != 0 {
		return InternalPage
	}
	if (p.flags & leafFlag) != 0 {
		return LeafPage
	}
	debug("Unknown page")
	return UnknownPage
}

func (p *page) getMeta(i int) *meta {
	return &(*[maxPageIndex]meta)(unsafe.Pointer(&p.metaAddr))[i]
}

func (p *page) getDataptr()

func (p *page) getKey(i int) keyType {
	meta := p.getMeta(i)
	buf := (*[maxArraySize]byte)(unsafe.Pointer(kvm))
	return buf[kvm.offset : kvm.offset+kvm.keySize]
}

func (p *page) getValue(i int) valueType {
	buf := (*[maxArraySize]byte)(unsafe.Pointer(kvm))
	return buf[kvm.offset+kvm.keySize : kvm.offset+kvm.keySize+kvm.valueSize]
}

func (p *page) getChildPgid(i int) pgid {
	return im.childID
}
