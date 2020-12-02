package mk

import "bytes"

// KeyType represents type for key
type KeyType []byte

// ValueType represents type for value
type ValueType []byte

func (k KeyType) lessThan(other KeyType) bool {
	return bytes.Compare(k, other) == -1
}

func (k KeyType) lessEqual(other KeyType) bool {
	return bytes.Compare(k, other) < 1
}

func (k KeyType) greaterEqual(other KeyType) bool {
	return bytes.Compare(k, other) >= 0
}

func (k KeyType) equalTo(other KeyType) bool {
	return bytes.Equal(k, other)
}
