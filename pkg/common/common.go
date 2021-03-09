// Package common holds common types
package common

// Pgid is memory map page ID
type Pgid uint32

const (
	// Memory map initial size is 128KB.
	MmapMinSize = 1 << 17
	// Max memory map size is 16GB.
	MmapMaxSize = 1 << 34
)

const (
	// Maximum key size is 1MB.
	maxKeySize = 1 << 20
	// Maximum value size is 1GB.
	maxValueSize = 1 << 30
)
