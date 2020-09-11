package mk

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	version = 1

	mkMagic = 0xDCDB2020
)

const (
	// Max memory mappping size is 64GB.
	maxMmSize = 1 << 36

	// Max size when converting pointer to byte array
	maxArrSize = maxMmSize

	// Initial memory map size, in bits
	initMmBits = 17

	// Memory map initial size is 128KB.
	initMmSize = 1 << initMmBits

	// After the first memory map step,
	// memory map grows by the step, 1GB.
	mmStep = 1 << 30
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
	mmSizedBuf *[maxMmSize]byte

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

// Open initiates DB from given data file, create new when unexists
func Open(opts Options) (*DB, error) {
	db := DB{
		path:     opts.Path,
		readOnly: opts.ReadOnly,
	}

	_, err := os.Stat(db.path)

	if os.IsNotExist(err) {
		err = db.create()
		if err != nil {
			log.Info("Failed to create new DB")

			return nil, err
		}
	}

	err = db.load()
	if err != nil {
		log.Info("Failed to load DB")

		return nil, err
	}

	db.pagePool = sync.Pool{New: func() interface{} {
		return make([]byte, pageSize)
	}}

	if err = db.mmap(initMmSize); err != nil {
		return nil, err
	}

	// TODO: init freelist

	return &db, nil
}

// create creates DB from given option.
func (db *DB) create() error {
	buf := make([]byte, 4*pageSize)

	// Page 0,1 are meta pages
	for i := 0; i < 2; i++ {
		p := bufferPage(buf, i)
		p.id = pgid(i)
		p.flags = metaPageFlag

		m := p.toMeta()
		m.version = version
		m.magic = mkMagic
	}

	// page 2 is an empty freelist page
	p := bufferPage(buf, 2)
	p.id = pgid(2)
	p.flags |= freelistPageFlag

	// page 3 is an empty leaf page
	p = bufferPage(buf, 3)
	p.id = pgid(3)
	p.flags |= leafPageFlag

	_, err := db.file.WriteAt(buf, 0)
	if err != nil {
		log.Info("Failed to write new DB file")

		return err
	}

	err = db.file.Sync()
	if err != nil {
		log.Info("Failed to sync new DB file")

		return err
	}

	return nil
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
func (db *DB) load() error {
	fd, err := os.OpenFile(db.path, os.O_CREATE, 0644)
	if err != nil {
		log.Info("Failed to open DB file")

		return err
	}

	db.file = fd

	buf := make([]byte, pageSize*4)

	_, err = db.file.Read(buf)
	if err != nil {
		log.Info("Failed to read DB file")

		return err
	}

	page0 := bufferPage(buf, 0).toMeta()

	if page0.magic != mkMagic {
		log.Info("File magic not match")

		return errors.New("Invalid DB file")
	}

	return nil
}

// allocate allocates contiguous pages from freelist,
// when freelist is empty, call mmap() to extend memory mapping
func (db *DB) allocate(count int) (*page, error) {
	// Allocate a temporary buffer for the page.
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*pageSize)
	}

	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = count - 1

	// Use pages from the freelist if they are available.
	id, err := db.freelist.allocate(count)
	if err == nil {
		p.id = id

		return p, nil
	}

	// Resize mmap() if we're at the end.
	p.id = db.writableTx.meta.pages

	// TODO: why add 1?
	newSize := (int(p.id) + count + 1) * pageSize

	if newSize > db.mmapSize {
		err := db.mmap(newSize)
		if err != nil {
			return nil, err
		}
	}

	db.writableTx.meta.pages += pgid(count)

	return p, nil
}

func (db *DB) roundMmapSize(size int) int {
	if size < 1<<30 {
		// Double size from initMapsz to 1GB
		for i := initMmBits; i <= 30; i++ {
			if size < 1<<i {
				size = 1 << i
				break
			}
		}

		return size
	}

	// Align by mmStep
	size += mmStep
	size -= size % mmStep

	if size > maxMmSize {
		log.Info("Reached max memory map size, round to maxMmSize")

		size = maxMmSize
	}

	return size
}

// mmap creates memory map for at least minsz.
func (db *DB) mmap(minsz int) error {
	fInfo, err := db.file.Stat()
	if err != nil {
		log.Info("Failed to call stat")

		return err
	}

	size := int(fInfo.Size())

	if size < minsz {
		size = minsz
	}

	size = db.roundMmapSize(size)

	// TODO: dereference before unmapping

	buf, err := syscall.Mmap(int(db.file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Info("Failed to mmap")

		return err
	}

	db.mmBuf = &buf
	db.mmSizedBuf = (*[maxMmSize]byte)(unsafe.Pointer(&buf))
	db.mmapSize = size

	db.meta0 = bufferPage(*db.mmBuf, 0).toMeta()
	db.meta1 = bufferPage(*db.mmBuf, 1).toMeta()

	return nil
}

// pageFromMmap returns page from memory map
func (db *DB) pageFromMmap(id pgid) *page {
	offset := id * pgid(pageSize)
	return (*page)(unsafe.Pointer(&db.mmSizedBuf[offset]))
}
