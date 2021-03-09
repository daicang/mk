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
	// DB file magic
	Magic = 0xDCDB2020
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
	writableTx *Tx
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
		magic:      m.magic,
		rootPage:   m.rootPage,
		totalPages: m.totalPages,
	}
}

// pageMeta retrieves meta struct from page.
func pageMeta(p *page.Page) *Meta {
	if !p.IsMeta() {
		panic("not meta page")
	}
	return (*Meta)(unsafe.Pointer(&p.Data))
}

// OpenDB returns (DB, succeed)
func OpenDB(opts Options) (*DB, bool) {
	db := DB{
		path: opts.Path,
	}

	_, err := os.Stat(db.path)
	if os.IsNotExist(err) {
		ok := db.createNew()
		if !ok {
			fmt.Println("Failed to create new DB")

			return nil, false
		}
	}

	ok := db.load()
	if !ok {
		fmt.Println("Failed to load DB")
		return nil, false
	}

	db.singlePages = sync.Pool{
		New: func() interface{} { return make([]byte, page.PageSize) },
	}

	ok = db.doMmap(common.MmapMinSize)
	if !ok {
		return nil, false
	}

	// TODO: init freelist

	return &db, true
}

// createNew creates file and a new DB.
func (db *DB) createNew() bool {
	var err error
	db.file, err = os.Create(db.path)
	if err != nil {
		fmt.Printf("Failed to create new DB file: %v\n", err)

		return false
	}

	buf := make([]byte, 3*page.PageSize)
	p := page.FromBuffer(buf, 0)

	// First page is meta page
	p.Index = 0
	p.SetFlag(page.FlagMeta)
	p.Overflow = 0

	mt := pageMeta(p)
	mt.magic = Magic
	// The third page is root page
	mt.rootPage = 2
	// New DB has 3 pages
	mt.totalPages = 3

	// Second page is empty freelist page
	p = page.FromBuffer(buf, 1)
	p.Index = 1
	p.SetFlag(page.FlagFreelist)

	// Third page is empty leaf page
	p = page.FromBuffer(buf, 2)
	p.Index = 2
	p.SetFlag(page.FlagLeaf)

	// Write buf to db file
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

// load initiates DB from file
func (db *DB) load() bool {
	var err error

	db.file, err = os.OpenFile(db.path, os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Failed to open DB file: %v\n", err)

		return false
	}

	buf := make([]byte, page.PageSize*3)
	_, err = db.file.Read(buf)
	if err != nil {
		fmt.Printf("Failed to read DB file: %v\n", err)

		return false
	}

	mt := pageMeta(page.FromBuffer(buf, 0))
	if mt.magic != Magic {
		fmt.Println("File magic not match")

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
	p.Index = db.writableTx.meta.totalPages
	db.writableTx.meta.totalPages += common.Pgid(count)
	mmapSize := int(db.writableTx.meta.totalPages * common.Pgid(page.PageSize))

	// Enlarge mmap
	if mmapSize > db.mmapSize {
		ok := db.doMmap(mmapSize)
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

// doMmap starts memory map for at least minsz.
func (db *DB) doMmap(requiredSize int) bool {
	fInfo, err := db.file.Stat()
	if err != nil {
		fmt.Printf("Failed to stat mmap file: %v\n", err)
		return false
	}

	mapFileSize := int(fInfo.Size())
	if mapFileSize > requiredSize {
		requiredSize = mapFileSize
	}

	requiredSize = roundMmapSize(requiredSize)

	// TODO: dereference before unmapping

	buf, err := syscall.Mmap(
		int(db.file.Fd()),
		0,
		requiredSize,
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		fmt.Printf("mmap failed: %v\n", err)
		return false
	}

	db.mmBuf = &buf
	db.mmSizedBuf = (*[common.MmapMaxSize]byte)(unsafe.Pointer(&buf))
	db.mmapSize = requiredSize
	page0 := page.FromBuffer(*db.mmBuf, 0)
	db.meta = pageMeta(page0)

	return true
}

// getPage returns page from memory map
func (db *DB) getPage(index common.Pgid) *page.Page {
	offset := index * common.Pgid(page.PageSize)
	return (*page.Page)(unsafe.Pointer(&db.mmSizedBuf[offset]))
}
