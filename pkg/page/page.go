package page

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/daicang/mk/pkg/common"
	"github.com/daicang/mk/pkg/kv"
)

const (
	// FlagMeta is meta page flag
	FlagMeta = 1
	// FlagFreelist is freelist page flag
	FlagFreelist = 1 << 1
	// FlagInternal is internal page flag
	FlagInternal = 1 << 2
	// FlagLeaf is leaf page flag
	FlagLeaf = 1 << 3
	// HeaderSize is page header size
	HeaderSize = int(unsafe.Sizeof(Page{}))
)

const (
	// PairInfoSize is size for each pair info
	PairInfoSize = int(unsafe.Sizeof(pairInfo{}))
)

var (
	// PageSize is OS page size, normally 4KB
	PageSize = os.Getpagesize()
)

// Page is the basic mmap block
// internal page: page struct | data | key | key | ..
// leaf     page: page struct | data | key | value | key | ..
type Page struct {
	// overflow counter, 0 for single page
	Overflow int
	// key/freeslot count
	Count int
	// index at mmap file
	Index common.Pgid
	// type mark
	Flags uint16
	// starting addr of data.
	Data uintptr
}

// pairInfo stores metadata for:
// - key-value pair (for leaf node)
// - b+tree index   (for internal node)
type pairInfo struct {
	// &page.data + offset = &key
	offset uint32
	// key length
	// &key + keySize = &value
	keySize uint32
	// value length, 0 for internal node
	valueSize uint32
	// child pgid, 0 for leaf node
	childID common.Pgid
}

type Pages []*Page

func (pgs Pages) Len() int {
	return len(pgs)
}

func (pgs Pages) Less(i, j int) bool {
	return pgs[i].Index < pgs[j].Index
}

func (pgs Pages) Swap(i, j int) {
	pgs[i], pgs[j] = pgs[j], pgs[i]
}

// String returns string for print.
func (p *Page) String() string {
	return fmt.Sprintf(
		"page[%d] %s keys=%d, overflow=%d",
		p.Index,
		p.getType(),
		p.Count,
		p.Overflow,
	)
}

func (p *Page) SetFlag(flag uint16) {
	p.Flags |= flag
}

func (p *Page) IsMeta() bool {
	return (p.Flags & FlagMeta) != 0
}

func (p *Page) IsFreelist() bool {
	return (p.Flags & FlagFreelist) != 0
}

func (p *Page) IsLeaf() bool {
	return (p.Flags & FlagLeaf) != 0
}

func (p *Page) IsInternal() bool {
	return (p.Flags & FlagInternal) != 0
}

// getType returns page type as string
func (p *Page) getType() string {
	if (p.Flags & FlagMeta) != 0 {
		return "meta"
	}
	if (p.Flags & FlagFreelist) != 0 {
		return "freelist"
	}
	if (p.Flags & FlagInternal) != 0 {
		return "internal"
	}
	if (p.Flags & FlagLeaf) != 0 {
		return "leaf"
	}
	panic("Unknown page type")
}

func (p *Page) getPairInfo(i int) *pairInfo {
	return &(*[common.MmapMaxSize]pairInfo)(unsafe.Pointer(&p.Data))[i]
}

func (p *Page) SetPairInfo(i int, ks, vs uint32, cid common.Pgid, offset uint32) {
	pi := p.getPairInfo(i)
	pi.offset = offset
	pi.keySize = ks

	if p.IsLeaf() {
		pi.valueSize = vs
	} else {
		pi.childID = cid
	}
}

// GetKeyAt returns key with given index.
// note: the key is in mmap buffer, not heap
func (p *Page) GetKeyAt(i int) kv.Key {
	pair := p.getPairInfo(i)
	buf := (*[common.MmapMaxSize]byte)(unsafe.Pointer(&p.Data))[pair.offset:]
	return buf[:pair.keySize]
}

// GetValueAt returns value with given index.
// note: the value is in mmap buffer, not heap
func (p *Page) GetValueAt(i int) kv.Value {
	if p.IsInternal() {
		panic("error: get value at internal page")
	}
	pair := p.getPairInfo(i)
	valueOffset := pair.offset + pair.keySize
	buf := (*[common.MmapMaxSize]byte)(unsafe.Pointer(&p.Data))[valueOffset:]
	return buf[:pair.valueSize]
}

func (p *Page) GetChildPgid(i int) common.Pgid {
	if p.IsLeaf() {
		panic("error: get child at leaf page")
	}
	return p.getPairInfo(i).childID
}

// FromBuffer returns page with given index in a buffer.
// Go slices are metadata to underlying structure, but
// arrays are values. So never pass arrays.
func FromBuffer(buf []byte, i common.Pgid) *Page {
	return (*Page)(unsafe.Pointer(&buf[i*common.Pgid(PageSize)]))
}
