package testutil

import (
	"crypto/rand"

	fuzz "github.com/google/gofuzz"
)

var (
	f = fuzz.New()
)

// RandomKV returns random string map with given size.
func RandomKV(size int) map[string]string {
	kvs := map[string]string{}

	counter := 0
	for counter < size {
		var key, value string

		f.Fuzz(&key)
		f.Fuzz(&value)

		_, exist := kvs[key]
		if exist {
			continue
		}

		kvs[key] = value
		counter++
	}

	return kvs
}

func RandomByteArray(size int) []byte {
	arr := make([]byte, size)
	rand.Read(arr)
	return arr
}
