package mk

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

var (
	errReadOnly = errors.New("DB is read only")
)

// DB is the main database structure.
type DB struct {
	// Path to memory mapping file
	path string

	//
	opened bool

	// Readonly mark
	readOnly bool

	// Database file begins with 2 meta blocks, block 0
	meta0 *Meta

	// Database file begins with 2 meta blocks, block 1
	meta1 *Meta

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

	//
	version uint32

	// pages is the number of current memory mapped pages
	// also used as pgid of first new page, since pgid
	// starts at 0
	pages pgid
}

func (m *Meta) copy() *Meta {
	new := Meta{
		magic:   m.magic,
		version: m.version,
	}

	return &new
}

// Open opens DB with given options.
func Open(opts Options) (*DB, bool) {
	db := DB{
		path:     opts.Path,
		readOnly: opts.ReadOnly,
	}

	_, err := os.Stat(db.path)

	if os.IsNotExist(err) {
		// Create new DB
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

	// First 2 pages are meta
	for i := 0; i < 2; i++ {
		p := bufferPage(buf, i)

		p.SetPgid(pgid(i))
		p.SetFlag(metaPageFlag)
		p.overflow = 0

		m := p.toMeta()

		m.version = DBVersion
		m.magic = Magic
	}

	// Third page is empty freelist page
	p := bufferPage(buf, 2)

	p.SetPgid(2)
	p.SetFlag(freelistPageFlag)

	// Fourth page is empty leaf page
	p = bufferPage(buf, 3)

	p.SetPgid(3)
	p.SetFlag(leafPageFlag)

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

// NewTransaction starts a new transaction, return err when
func (db *DB) NewTransaction(writable bool) (*Tx, error) {
	if writable {
		if db.readOnly || db.writableTx != nil {
			return nil, errReadOnly
		}
	}

	t := Tx{
		db:       db,
		writable: writable,
		meta:     db.meta0.copy(),
	}

	db.txs = append(db.txs, &t)

	if writable {
		db.writableTx = &t
	}

	return &t, nil
}

// load initiates DB from file
func (db *DB) load() bool {
	var err error

	db.file, err = os.OpenFile(db.path, os.O_CREATE, 0644)
	if err != nil {
		log.Error(err, "Failed to open DB file")

		return false
	}

	buf := make([]byte, pageSize*4)

	_, err = db.file.Read(buf)
	if err != nil {
		log.Error(err, "Failed to read DB file")

		return false
	}

	meta0 := bufferPage(buf, 0).toMeta()

	if meta0.magic != Magic {
		log.Info("File magic not match")

		return false
	}

	return true
}

// allocate allocates contiguous pages from freelist,
// when freelist is empty, call mmap() to extend memory mapping
func (db *DB) allocate(count int) (*page, bool) {
	var buf []byte

	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*pageSize)
	}

	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = count - 1

	// Check free page id from freelist first
	id, err := db.freelist.allocate(count)
	if err == nil {
		p.id = id

		return p, true
	}

	// TODO: why add 1?
	p.SetPgid(db.writableTx.meta.pages)
	newSize := (int(p.id) + count + 1) * pageSize

	// Resize mmap if exceed
	if newSize > db.mmapSize {
		ok := db.doMmap(newSize)
		if !ok {
			return nil, false
		}
	}

	db.writableTx.meta.pages += pgid(count)

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

	db.meta0 = bufferPage(*db.mmBuf, 0).toMeta()
	db.meta1 = bufferPage(*db.mmBuf, 1).toMeta()

	return true
}

// pageFromMmap returns page from memory map
func (db *DB) pageFromMmap(id pgid) *page {
	offset := id * pgid(pageSize)
	return (*page)(unsafe.Pointer(&db.mmSizedBuf[offset]))
}
