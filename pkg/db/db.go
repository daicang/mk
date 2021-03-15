package db

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/daicang/mk/pkg/common"
	"github.com/daicang/mk/pkg/freelist"
	"github.com/daicang/mk/pkg/page"
)

const (
	// Magic indentifies DB file
	Magic = 0x20202021
)

const (
	// Memory map grows by 1GB.
	MmapStep = 1 << 30
)

// Options holds info to start DB.
type Options struct {
	// DB mmap file path
	Path string
}

// DB represents one database.
type DB struct {
	// Path to memory mapping file
	path string
	// Meta block
	meta *Meta
	// Memory map file pointer
	file *os.File
	// pointer to memory map array, without size limit
	mmBuf *[]byte
	// pointer to memory map array, with size limit
	mmSizedBuf *[common.MmapMaxSize]byte
	// All current transaction
	txs []*Tx
	// There can only be one writable transaction
	wtx *Tx
	// mmapSize is the mmaped file size
	mmapSize int
	// single page pool
	singlePages sync.Pool
	// mmap empty page slots
	freelist *freelist.Freelist
}

// Meta holds database metadata.
type Meta struct {
	// magic should be mkMagic
	magic uint32
	// number of allocated pages, also id of next new page
	totalPages common.Pgid
	// freelist page id
	freelistPage common.Pgid
	// root page id
	rootPage common.Pgid
}

func (m *Meta) copy() *Meta {
	return &Meta{
		magic:        m.magic,
		rootPage:     m.rootPage,
		freelistPage: m.freelistPage,
		totalPages:   m.totalPages,
	}
}

// pageMeta retrieves meta struct from page.
func pageMeta(p *page.Page) *Meta {
	if !p.IsMeta() {
		panic("not meta page")
	}
	return (*Meta)(unsafe.Pointer(&p.Data))
}

// Open returns (DB, succeed)
func Open(opts Options) (*DB, bool) {
	db := &DB{
		path: opts.Path,
	}
	_, err := os.Stat(db.path)
	// Create DB file if unexist
	if os.IsNotExist(err) {
		ok := db.initFile()
		if !ok {
			fmt.Println("Failed to create new DB")
			return nil, false
		}
	}
	// Open DB file
	db.file, err = os.OpenFile(db.path, os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Failed to open DB file: %v\n", err)
		return nil, false
	}
	// Read DB file
	buf := make([]byte, 2*page.PageSize)
	_, err = db.file.Read(buf)
	if err != nil {
		fmt.Printf("Failed to read DB file: %v\n", err)
		return nil, false
	}
	// Load meta info
	mt := pageMeta(page.FromBuffer(buf, 0))
	if mt.magic != Magic {
		fmt.Println("magic not match")
		return nil, false
	}
	db.meta = mt
	// Start mmap
	ok := db.mmap(common.MmapMinSize)
	if !ok {
		fmt.Println("failed to mmap")
		return nil, false
	}
	// Load freelist
	db.freelist = freelist.NewFreelist()
	pgFreelist := db.getPage(db.meta.freelistPage)
	db.freelist.ReadPage(pgFreelist)
	// Init single page pool
	db.singlePages = sync.Pool{
		New: func() interface{} { return make([]byte, page.PageSize) },
	}

	return db, true
}

// initFile initiates new DB file.
func (db *DB) initFile() bool {
	fd, err := os.Create(db.path)
	if err != nil {
		fmt.Printf("Failed to create new DB file: %v\n", err)
		return false
	}
	db.file = fd

	buf := make([]byte, 3*page.PageSize)
	// First page is meta page
	p0 := page.FromBuffer(buf, 0)
	p0.Index = 0
	p0.SetFlag(page.FlagMeta)
	p0.Overflow = 0

	mt := pageMeta(p0)
	mt.magic = Magic
	mt.freelistPage = 1
	mt.rootPage = 2
	mt.totalPages = 3

	// Second page is for freelist
	p1 := page.FromBuffer(buf, 1)
	p1.Index = 1
	p1.SetFlag(page.FlagFreelist)

	// Third page is for root node
	p2 := page.FromBuffer(buf, 2)
	p2.Index = 2
	p2.SetFlag(page.FlagLeaf)

	// Write and sync
	_, err = db.file.WriteAt(buf, 0)
	if err != nil {
		fmt.Printf("Failed to write new DB file: %v\n", err)
		return false
	}
	err = db.file.Sync()
	if err != nil {
		fmt.Printf("Failed to sync new DB file: %v\n", err)
		return false
	}

	return true
}

// allocate allocates contiguous pages, returns (*page, succeed).
func (db *DB) allocate(count int) (*page.Page, bool) {
	// Allocate memory buffer to hold new page
	var buf []byte
	if count == 1 {
		buf = db.singlePages.Get().([]byte)
	} else {
		buf = make([]byte, count*page.PageSize)
	}
	// New page struct
	p := page.FromBuffer(buf, 0)
	p.Overflow = count - 1

	// Check freelist for memory-map free slot
	id, ok := db.freelist.Allocate(count)
	if ok {
		p.Index = id
		return p, true
	}

	// When no proper "hole", enlarge memory mapping
	p.Index = db.wtx.meta.totalPages
	db.wtx.meta.totalPages += common.Pgid(count)
	mmapSize := int(db.wtx.meta.totalPages * common.Pgid(page.PageSize))

	// Enlarge mmap
	if mmapSize > db.mmapSize {
		ok := db.mmap(mmapSize)
		if !ok {
			return nil, false
		}
	}

	return p, true
}

// roundMmapSize doubles mmap size to 1GB,
// then grows by 1GB up to maxMmapSize
func roundMmapSize(size int) int {
	if size < 1<<30 {
		for i := 1; i <= 30; i++ {
			if size < 1<<i {
				size = 1 << i
				break
			}
		}
		return size
	}

	// Align by step
	size += MmapStep
	size -= size % MmapStep

	if size > common.MmapMaxSize {
		fmt.Println("Exceed max mmap size, round up")
		size = common.MmapMaxSize
	}

	return size
}

// mmap create mmap for at least given size.
func (db *DB) mmap(sz int) bool {
	fInfo, err := db.file.Stat()
	if err != nil {
		fmt.Printf("Failed to stat mmap file: %v\n", err)
		return false
	}

	mapFileSize := int(fInfo.Size())
	if mapFileSize > sz {
		sz = mapFileSize
	}

	sz = roundMmapSize(sz)
	if db.wtx != nil {
		db.wtx.root.Dereference()
	}

	buf, err := syscall.Mmap(
		int(db.file.Fd()),
		0,
		sz,
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		fmt.Printf("mmap failed: %v\n", err)
		return false
	}

	db.mmBuf = &buf
	// buf is []byte slice, so &buf != &buf[0]
	db.mmSizedBuf = (*[common.MmapMaxSize]byte)(unsafe.Pointer(&buf[0]))
	db.mmapSize = sz
	page0 := page.FromBuffer(*db.mmBuf, 0)
	db.meta = pageMeta(page0)

	return true
}

// getPage returns page from memory map
func (db *DB) getPage(index common.Pgid) *page.Page {
	offset := index * common.Pgid(page.PageSize)
	return (*page.Page)(unsafe.Pointer(&db.mmSizedBuf[offset]))
}
