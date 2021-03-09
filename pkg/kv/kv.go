package kv

import "bytes"

// Key represents type for key
type Key []byte

// Value represents type for value
type Value []byte

func (k Key) lessThan(other Key) bool {
	return bytes.Compare(k, other) == -1
}

func (k Key) lessEqual(other Key) bool {
	return bytes.Compare(k, other) < 1
}

func (k Key) GreaterEqual(other Key) bool {
	return bytes.Compare(k, other) >= 0
}

func (k Key) EqualTo(other Key) bool {
	return bytes.Equal(k, other)
}
