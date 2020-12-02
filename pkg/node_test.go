package mk

import (
	"testing"
)

func TestNodeWrite(t *testing.T) {
	size := 500
	kvs, n := randomNode(size)
	p := allocPage(n.mapSize())

	n.write(p)

	if !p.isLeaf() {
		t.Errorf("page should be leaf: type=%s", p.getType())
	}

	if p.KeyCount() != size {
		t.Errorf("Incorrect page size: expect %d, get %d", size, p.KeyCount())
	}

	for i := 0; i < p.KeyCount(); i++ {
		pk := string(p.getKeyAt(i))
		pv := string(p.getValueAt(i))

		if kvs[pk] != pv {
			t.Errorf("Bad pair in page: key=%s, value=%s, value in page=%s", pk, kvs[pk], pv)
		}
	}
}

func TestNodeRead(t *testing.T) {
	size := 500
	kvs, n1 := randomNode(size)
	p := allocPage(n1.mapSize())

	n1.write(p)

	n2 := &node{}

	n2.read(p)

	if !n2.isLeaf {
		t.Errorf("Node should be leaf")
	}

	if n2.keyCount() != size {
		t.Errorf("Incorrect size: expect %d, get %d", size, n2.keyCount())
	}

	for i, key := range n2.keys {
		val, exist := kvs[string(key)]
		if !exist {
			t.Errorf("key %s not exist", key)
			continue
		}

		if val != string(n2.values[i]) {
			t.Errorf("Value mismatch, expect %s, get %s", val, n2.values[i])
		}
	}
}

func TestNodeSearch(t *testing.T) {
	size := 300
	kvs, n := randomNode(size)

	for key, value := range kvs {
		exist, i := n.search([]byte(key))
		if !exist {
			t.Errorf("Key %s not found", key)

			continue
		}

		val := n.getValueAt(i)
		if value != string(val) {
			t.Errorf("Expected %s, get %s", value, string(val))
		}
	}
}

func TestNodeSplitTwo(t *testing.T) {
	_, n1 := randomNode(splitKeyCount)

	if n1.splitTwo() != nil {
		t.Errorf("Should not split node")
	}

}
