package mk

import (
	"os"
	"syscall"
	"unsafe"

	"github.com/daicang/mk/pkg/page"
)

const (
	maxMapSize = 0xFFFFFFFFFFFF
)

var (
	PageSize = os.Getpagesize()
)

type DB struct {
	// path is the mmap file path
	path string

	// file is the handler of mmap file
	file *os.File

	// dataref is the mmap byte array
	dataref []byte

	// data is byte array pointer to mmap byte array
	data *[maxMapSize]byte

	// mmapSize is the mmaped file size
	mmapSize int
}

// Init initiates DB from given mmap file
func (db *DB) Init(path string) error {
	db.path = path
	if db.file, err = os.OpenFile(db.path, os.O_CREATE, 0644); err != nil {
		return err
	}

	buf := make([]byte, config.PageSize*4)
	for i := 0; i < 2; i++ {
		p := (*page.Page)(unsafe.Pointer(&buf[i*config.PageSize]))
		p.ID = page.Pgid(i)
	}
	db.file.WriteAt()
}

func (db *DB) mmap(sz int) error {
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

// GetPage returns page from mmap array
func (db *DB) GetPage(id page.Pgid) *page.Page {
	return (*page.Page)(db.dataref[id*PageSize])
}
