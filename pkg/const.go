package mk

// DB
const (
	// Database version
	DBVersion = 1

	// DB file magic
	Magic = 0xDCDB2020
)

// Memory mapping
const (
	// Memory map initial size is 128KB.
	minMmapSize = 1 << 17

	// Memory map grows by 1GB.
	mmapStep = 1 << 30

	// Max memory map size is 16GB.
	maxMmapSize = 1 << 34
)

// B+tree node
const (
	// fillPercent marks first split point.
	fillPercent = 0.5

	minKeyCount = 2

	splitKeyCount = 4
)
