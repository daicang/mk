package mk

import (
	"sort"
	"unsafe"
)

const (
	maxFreeSlot = 1 << 34
)

type ints []int

func (p ints) Len() int           { return len(p) }
func (p ints) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p ints) Less(i, j int) bool { return p[i] < p[j] }

func merge(a, b ints) ints {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(ints, len(a)+len(b))
	ia, ib := 0, 0
	for ia < len(a) && ib < len(b) {
		if a[ia] < b[ib] {
			merged[ia+ib] = a[ia]
			ia++
		} else {
			merged[ia+ib] = b[ib]
			ib++
		}
	}
	if ia == len(a) {
		copy(merged[ia+ib:], b[ib:])
	} else {
		copy(merged[ia+ib:], a[ia:])
	}

	return merged
}

// Freelist tracks unused page slots in mmap.
type Freelist struct {
	// free slot ids
	slots []int
	// pages to freed by writable transaction
	txFreed map[int][]int
}

// NewFreelist returns empty freelist.
func NewFreelist() *Freelist {
	return &Freelist{
		slots:   []int{},
		txFreed: map[int][]int{},
	}
}

// Allocate find n contiguous pages slots from freelist,
// returns (start int, succeed)
func (f *Freelist) Allocate(n int) (int, bool) {
	startID := int(0)
	lastID := int(0)

	for i, currentID := range f.slots {
		// for first page and discontinuous page, recount
		if i == 0 || currentID != lastID+1 {
			startID = currentID
		}

		if int(currentID-startID+1) == n {
			// Found n continuous pages, take out from ints
			copy(f.slots[i+1-n:], f.slots[i+1:])
			f.slots = f.slots[:len(f.slots)-n]

			return startID, true
		}

		lastID = currentID
	}

	return 0, false
}

// Add adds page to freelist tx cache.
func (f *Freelist) Add(tid int, p PageInterface) {
	if p.GetIndex() == 0 {
		panic("Page already freed")
	}
	if _, exist := f.txFreed[tid]; !exist {
		f.txFreed[tid] = []int{}
	}
	for i := 0; i < p.GetPageCount(); i++ {
		f.txFreed[tid] = append(f.txFreed[tid], p.GetIndex()+i)
	}
}

// Release put tx cache pages to freelist.
func (f *Freelist) Release() {
	sort.Sort(f.txFreed)
	f.ids = merge(f.ids, f.txFreed)
	f.txFreed = ints{}
}

// Rollback clears transaction freed pages.
func (f *Freelist) Rollback() {
	f.txFreed = []int{}
}

// Size returns size when write to memory page.
func (f *Freelist) Size() int {
	return HeaderSize + int(unsafe.Sizeof(uint32(0)))*len(f.slots)
}

// ReadPage reads freelist from page.
func (f *Freelist) ReadPage(p PageInterface) {
	if !p.IsFreelist() {
		panic("page type mismatch")
	}
	buf := (*[maxFreeSlot]int)(unsafe.Pointer(&p.Data))
	for i := 0; i < p.Count; i++ {
		f.ids = append(f.ids, buf[i])
	}
}

// WritePage write freelist to page.
// page header | int 1 | int 2 | ..
func (f *Freelist) WritePage(p PageInterface) {
	p.SetFlag(FreelistPage)
	p.Count = len(f.ids)
	buf := (*[maxFreeSlot]int)(unsafe.Pointer(&p.Data))
	for i, id := range f.ids {
		buf[i] = id
	}
}
