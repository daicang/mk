package mk

// pgid is memory map page ID
type pgid uint32

const (
	// Initial memory map size is 128KB.
	MinMapBytes = 1 << 17
	// Max memory map size is 16GB.
	MaxMapBytes = 1 << 34
	// Go max integer is 2^63-1 on darwin/amd64
	// This should be sufficient to hold key/value
	// size and offset
)

const (
	// Maximum key size is 1MB.
	MaxKeyBytes = 1 << 20 // nolint
	// Maximum value size is 1GB.
	MaxValueBytes = 1 << 30 // nolint
)
