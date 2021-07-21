package mk

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
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
	// DB info
	// Meta block
	meta *DBMeta

	// Mmap info
	// Path to memory mapping file
	path string
	// Memory map file pointer
	file *os.File
	// mmapSize is the mmaped file size
	mmapSize int
	// pointer to memory map array, without size limit
	mmBuf *[]byte
	// pointer to memory map array, with size limit
	mmSizedBuf *[MaxMapBytes]byte

	// Transactions
	// Last transaction ID
	lastTxID uint32
	// All current transaction
	txs []*Tx
	// There can only be one writable transaction
	wtx *Tx

	// Pages
	// single page pool
	singlePages sync.Pool
	// mmap empty page slots
	freelist *Freelist
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
	db.file, err = os.OpenFile(db.path, os.O_RDWR, 0644)
	if err != nil {
		fmt.Printf("Failed to open DB file: %v\n", err)
		return nil, false
	}
	// Read DB file
	buf := make([]byte, 2*PageSize)
	_, err = db.file.Read(buf)
	if err != nil {
		fmt.Printf("Failed to read DB file: %v\n", err)
		return nil, false
	}
	// Load meta info
	metaPage := FromBuffer(buf, 0)
	dbMeta := metaPage.GetDBMeta()
	if dbMeta.magic != Magic {
		fmt.Println("magic not match")
		return nil, false
	}
	db.meta = dbMeta
	// Start mmap
	ok := db.mmap(MinMapBytes)
	if !ok {
		fmt.Println("failed to mmap")
		return nil, false
	}
	// Load freelist
	db.freelist = NewFreelist()
	pgFreelist := db.getPage(db.meta.freelistPage)
	db.freelist.ReadPage(pgFreelist)
	// Init single page pool
	db.singlePages = sync.Pool{
		New: func() interface{} { return make([]byte, PageSize) },
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

	buf := make([]byte, 3*PageSize)
	// First page is meta page
	p0 := FromBuffer(buf, 0)
	p0.Index = 0
	p0.SetFlag(FlagMeta)
	p0.Overflow = 0

	mt := pageMeta(p0)
	mt.magic = Magic
	mt.freelistPage = 1
	mt.rootPage = 2
	mt.totalPages = 3

	// Second page is for freelist
	p1 := FromBuffer(buf, 1)
	p1.Index = 1
	p1.SetFlag(FlagFreelist)

	// Third page is for root node
	p2 := FromBuffer(buf, 2)
	p2.Index = 2
	p2.SetFlag(FlagLeaf)

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
func (db *DB) allocate(count int) (*page, bool) {
	// Always allocate memory buffer for new page
	var buf []byte
	if count == 1 {
		buf = db.singlePages.Get().([]byte)
	} else {
		buf = make([]byte, count*PageSize)
	}
	p := FromBuffer(buf, 0)
	p.Overflow = count - 1
	// Check if new page can be mapped into slot in freelist
	id, ok := db.freelist.Allocate(count)
	if ok {
		p.Index = id
		return p, true
	}
	// No such slot, can map to headroom or need to enlarge mmap
	p.Index = db.wtx.meta.totalPages
	db.wtx.meta.totalPages += int(count)
	mmapSize := int(db.wtx.meta.totalPages) * PageSize
	// Check if need to enlarge mmap
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

	if size > MaxMapBytes {
		fmt.Println("Exceed max mmap size, round up")
		size = MaxMapBytes
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
	db.mmSizedBuf = (*[MaxMapBytes]byte)(unsafe.Pointer(&buf[0]))
	db.mmapSize = sz
	page0 := FromBuffer(*db.mmBuf, 0)
	db.meta = pageMeta(page0)

	return true
}

// getPage returns immutable page from memory map.
func (db *DB) getPage(index int) *Page {
	offset := index * int(PageSize)
	return (*Page)(unsafe.Pointer(&db.mmSizedBuf[offset]))
}
