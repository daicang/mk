package db

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"

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
	mmSizedBuf *[MmapMaxSize]byte
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
	// freelist page id
	freelistPage uint32
	// root page id
	rootPage uint32
	// number of allocated pages, also id of next new page
	totalPages uint32
}

func (m *Meta) copy() *Meta {
	return &Meta{
		magic:      m.magic,
		root:       m.root,
		totalPages: m.totalPages,
	}
}

// pageMeta retrieves meta struct from page.
func pageMeta(p *page.Page) *Meta {
	if !p.IsMeta() {
		panic("Calling getMeta with non-meta page")
	}

	return (*Meta)(unsafe.Pointer(&p.Data))
}

// OpenDB returns (DB, succeed)
func OpenDB(opts Options) (*DB, bool) {
	db := DB{
		path:     opts.Path,
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
		New: func() interface{} {return make([]byte, page.PageSize)},
	}

	ok = db.doMmap(MmapMinSize)
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
	p.SetPgid(0)
	p.SetFlag(page.FlagMeta)
	p.Overflow = 0

	mt := pageMeta(p)
	mt.magic = Magic
	// The third page is root page
	mt.root = 2
	// New DB has 3 pages
	mt.totalPages = 3

	// Second page is empty freelist page
	p = page.FromBuffer(buf, 1)
	p.SetPgid(1)
	p.SetFlag(page.FlagFreelist)

	// Third page is empty leaf page
	p = page.FromBuffer(buf, 2)
	p.SetPgid(2)
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
	// Allocate memory from pool/heap to hold new page
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*page.PageSize)
	}

	p := page.FromBuffer(buf, 0)
	p.Overflow = count - 1

	// Check freelist for "hole" in mmap file to save the newly allocated pages
	id, err := db.freelist.allocate(count)
	if err == nil {
		p.SetID(id)

		return p, true
	}

	// When no proper "hole", we need to enlarge memory mapping
	p.SetPgid(db.writableTx.meta.)

	db.writableTx.meta.totalPages += pgid(count)
	mmapSize := int(db.writableTx.meta.totalPages * pgid(pageSize))

	// Resize mmap if exceed
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
	size += mmapStep
	size -= size % mmapStep

	if size > maxMmapSize {
		Log.Info("Exceed max mmap size, round up")

		size = maxMmapSize
	}

	return size
}

// doMmap starts memory map for at least minsz.
func (db *DB) doMmap(requiredSize int) bool {
	fInfo, err := db.file.Stat()
	if err != nil {
		Log.Error(err, "Failed to stat mmap file")

		return false
	}

	mapFileSize := int(fInfo.Size())
	if mapFileSize > requiredSize {
		requiredSize = mapFileSize
	}

	requiredSize = roundMmapSize(requiredSize)

	// TODO: dereference before unmapping

	buf, err := syscall.Mmap(int(db.file.Fd()), 0, requiredSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		Log.Error(err, "mmap failed")

		return false
	}

	db.mmBuf = &buf
	db.mmSizedBuf = (*[maxMmapSize]byte)(unsafe.Pointer(&buf))
	db.mmapSize = requiredSize

	db.meta = bufferPage(*db.mmBuf, 0).getMeta()

	return true
}

// getPage returns page from memory map
func (db *DB) getPage(id pgid) *page {
	offset := id * pgid(pageSize)
	return (*page)(unsafe.Pointer(&db.mmSizedBuf[offset]))
}
