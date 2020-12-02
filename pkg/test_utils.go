package mk

import (
	"math"

	fuzz "github.com/google/gofuzz"
)

var (
	f = fuzz.New()
)

// allocPage allocates page with at least size bytes.
func allocPage(size int) *page {
	count := int(math.Ceil(float64(size) / float64(pageSize)))
	buf := make([]byte, count*pageSize)

	p := bufferPage(buf, 0)
	p.overflow = count - 1

	return p
}

// randomKV returns random string map with given size.
func randomKV(size int) map[string]string {
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

func randomByteArray(size int) []byte {
	arr := make([]byte, size)
	f.Fuzz(arr)

	return arr
}

// randomNode returns node filled with random KV.
func randomNode(keys int) (map[string]string, *node) {
	kvs := randomKV(keys)
	n := node{
		isLeaf: true,
	}

	for key, value := range kvs {
		_, i := n.search([]byte(key))

		n.insertKeyAt(i, []byte(key))
		n.insertValueAt(i, []byte(value))
	}

	return kvs, &n
}

func sizedNode(keySize, valueSize, keys int) *node {
	n := node{}

	for i := 0; i < keys; i++ {
		for {
			key := randomByteArray(keySize)
			value := randomByteArray(valueSize)

			found, j := n.search(key)
			if found {
				continue
			}

			n.insertKeyAt(j, key)
			n.insertValueAt(j, value)

			break
		}
	}

	return &n
}
