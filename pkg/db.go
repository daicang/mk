package mk

import (
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// DB represents one database.
type DB struct {
	// Path to memory mapping file
	path string

	// Readonly mark
	readOnly bool

	// Meta block
	meta *Meta

	// Memory map file pointer
	file *os.File

	// pointer to memory map array, without size limit
	mmBuf *[]byte

	// pointer to memory map array, with size limit
	mmSizedBuf *[maxMmapSize]byte

	// All current transaction
	txs []*Tx

	// There can only be one writable transaction
	writableTx *Tx

	// mmapSize is the mmaped file size
	mmapSize int

	// pagePool stores single page byte array
	pagePool sync.Pool

	// freelist keeps freed pages
	freelist *pager
}

// Meta holds database metadata.
type Meta struct {
	// magic should be mkMagic
	magic uint32

	// root page id
	root pgid

	// totalPages is number of allocated pages,
	// also be pgid of next new page.
	totalPages pgid
}

func (m *Meta) copy() *Meta {
	return &Meta{
		magic:      m.magic,
		root:       m.root,
		totalPages: m.totalPages,
	}
}

// OpenDB returns (DB, succeed)
func OpenDB(opts Options) (*DB, bool) {
	db := DB{
		path:     opts.Path,
		readOnly: opts.ReadOnly,
	}

	_, err := os.Stat(db.path)

	if os.IsNotExist(err) {
		ok := db.createNew()
		if !ok {
			log.Info("Failed to create new DB")

			return nil, false
		}
	}

	ok := db.load()
	if !ok {
		log.Info("Failed to load DB")

		return nil, false
	}

	db.pagePool = sync.Pool{New: func() interface{} {
		return make([]byte, pageSize)
	}}

	ok = db.doMmap(minMmapSize)
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
		log.Error(err, "Failed to create new DB file")

		return false
	}

	buf := make([]byte, 4*pageSize)

	// First page is meta page
	p := bufferPage(buf, 0)

	p.SetPgid(0)
	p.SetFlag(metaPageFlag)
	p.overflow = 0

	m := p.toMeta()

	m.magic = Magic

	// The third page is root page
	m.root = 2

	// Newly initialized DB has 3 pages
	m.totalPages = 3

	// Second page is empty freelist page
	p = bufferPage(buf, 1)

	p.SetPgid(1)
	p.SetFlag(freelistPageFlag)

	// Third page is empty leaf page
	p = bufferPage(buf, 2)

	p.SetPgid(2)
	p.SetFlag(leafPageFlag)

	if !p.isLeaf() {
		panic("Root page should be leaf")
	}

	// Write buf to db file
	_, err = db.file.WriteAt(buf, 0)
	if err != nil {
		log.Error(err, "Failed to write new DB file")

		return false
	}

	err = db.file.Sync()
	if err != nil {
		log.Error(err, "Failed to sync new DB file")

		return false
	}

	return true
}

// load initiates DB from file
func (db *DB) load() bool {
	var err error

	db.file, err = os.OpenFile(db.path, os.O_CREATE, 0644)
	if err != nil {
		log.Error(err, "Failed to open DB file")

		return false
	}

	buf := make([]byte, pageSize*3)

	_, err = db.file.Read(buf)
	if err != nil {
		log.Error(err, "Failed to read DB file")

		return false
	}

	mt := bufferPage(buf, 0).toMeta()

	if mt.magic != Magic {
		log.Info("File magic not match")

		return false
	}

	return true
}

// allocate allocates contiguous pages.
func (db *DB) allocate(count int) (*page, bool) {
	// The memory to hold new page
	var buf []byte

	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*pageSize)
	}

	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = count - 1

	// Check freelist for "hole" in mmap file to save the newly allocated pages
	id, err := db.freelist.allocate(count)
	if err == nil {
		p.id = id

		return p, true
	}

	// When no proper "hole", we need to enlarge memory mapping
	p.SetPgid(db.writableTx.meta.totalPages)

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
		log.Info("Exceed max mmap size, round up")

		size = maxMmapSize
	}

	return size
}

// doMmap starts memory map for at least minsz.
func (db *DB) doMmap(requiredSize int) bool {
	fInfo, err := db.file.Stat()
	if err != nil {
		log.Error(err, "Failed to stat mmap file")

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
		log.Error(err, "mmap failed")

		return false
	}

	db.mmBuf = &buf
	db.mmSizedBuf = (*[maxMmapSize]byte)(unsafe.Pointer(&buf))
	db.mmapSize = requiredSize

	db.meta = bufferPage(*db.mmBuf, 0).toMeta()

	return true
}

// getPage returns page from memory map
func (db *DB) getPage(id pgid) *page {
	offset := id * pgid(pageSize)
	return (*page)(unsafe.Pointer(&db.mmSizedBuf[offset]))
}
