package mk

const (
	// Initial memory map size is 128KB.
	MinMapBytes = 1 << 17
	// Max memory map size is 16GB.
	MaxMapBytes = 1 << 34
	// Go max integer is 2^63-1 on darwin/amd64
	// This should be sufficient to hold key/value
	// size and offset
	// Also integer should be big enough to hold a page ID
)

const (
	// Maximum key size is 1MB.
	MaxKeyBytes = 1 << 20 // nolint
	// Maximum value size is 1GB.
	MaxValueBytes = 1 << 30 // nolint
)