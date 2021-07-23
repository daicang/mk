package mk

import (
	"math"
	"testing"

	"github.com/daicang/mk/pkg/test"
)

func allocPage(size int) *Page {
	count := int(math.Ceil(float64(size) / float64(PageSize)))
	buf := make([]byte, count*PageSize)
	p := FromBuffer(buf, 0)
	p.Overflow = count - 1

	return p
}

// randomNode returns node filled with random KV.
func randomNode(keys int) (*Node, map[string]string) {
	kvs := test.RandomKV(keys)
	n := Node{
		IsLeaf: true,
	}
	for key, value := range kvs {
		_, i := n.Search([]byte(key))
		n.InsertKeyValueAt(i, []byte(key), []byte(value))
	}

	return &n, kvs
}

// GenNode generates node with option.
func GenNode(keys, keySize, valueSize int) *Node {
	n := Node{
		IsLeaf: true,
	}

	for i := 0; i < keys; i++ {
		for {
			key := test.RandomByteArray(keySize)
			value := test.RandomByteArray(valueSize)
			found, j := n.Search(key)
			if !found {
				n.InsertKeyValueAt(j, key, value)
				break
			}
		}
	}

	return &n
}

func TestNodeWrite(t *testing.T) {
	size := 500
	n, kvs := randomNode(size)
	p := allocPage(n.Size())

	n.WritePage(p)

	if !p.IsLeaf() {
		t.Error("page should be leaf")
	}

	if p.Count != size {
		t.Errorf("Incorrect page size: expect %d, get %d", size, p.Count)
	}

	for i := 0; i < p.Count; i++ {
		pk := string(p.GetKeyAt(i))
		pv := string(p.GetValueAt(i))

		if kvs[pk] != pv {
			t.Errorf("Bad pair in page: key=%s, value=%s, value in page=%s", pk, kvs[pk], pv)
		}
	}
}

func TestNodeRead(t *testing.T) {
	size := 500
	n1, kvs := randomNode(size)
	p := allocPage(n1.Size())
	n1.WritePage(p)
	n2 := &Node{}
	n2.ReadPage(p)

	if !n2.IsLeaf {
		t.Errorf("Node should be leaf")
	}
	if n2.KeyCount() != size {
		t.Errorf("Incorrect size: expect %d, get %d", size, n2.KeyCount())
	}

	for i, key := range n2.Keys {
		val, exist := kvs[string(key)]
		if !exist {
			t.Errorf("key %s not exist", key)
			continue
		}

		if val != string(n2.Values[i]) {
			t.Errorf("Value mismatch, expect %s, get %s", val, n2.Values[i])
		}
	}
}

func TestNodeSearch(t *testing.T) {
	size := 300
	n, kvs := randomNode(size)

	for key, value := range kvs {
		exist, i := n.Search([]byte(key))
		if !exist {
			t.Errorf("Key %s not found", key)

			continue
		}

		val := n.GetValueAt(i)
		if value != string(val) {
			t.Errorf("Expected %s, get %s", value, string(val))
		}
	}
}

func TestSplit1(t *testing.T) {
	n, _ := randomNode(2)
	nodes := n.Split()
	if len(nodes) != 1 {
		t.Fatalf("Split should return 1 node, get %d", len(nodes))
	}
}

func TestSplit2(t *testing.T) {
	keys := 64
	kvSize := (2*PageSize-2*HeaderSize)/keys - PairInfoSize
	keySize := kvSize / 2
	valueSize := kvSize / 2

	// Create a node with 2x page size
	n := GenNode(keys, keySize, valueSize)
	expectedSize := 2*PageSize - HeaderSize
	if n.Size() != 2*PageSize {
		t.Fatalf("Size should be %d, get %d", expectedSize, n.Size())
	}

	nodes := n.Split()

	if len(nodes) != 2 {
		t.Fatalf("Split should return 2 node, get %d", len(nodes))
	}
}

func TestNodeSplitTwo(t *testing.T) {
	n1, _ := randomNode(2)

	if n1.splitTwo() != nil {
		t.Errorf("Should not split node")
	}

	keyCount := 64
	kvSize := (2*PageSize-HeaderSize)/keyCount - PairInfoSize
	keySize := kvSize / 2
	valueSize := kvSize / 2

	// Create a node with 2x page size
	n2 := GenNode(keyCount, keySize, valueSize)

	t.Logf("nodeSize=%d, kvSize=%d", n2.Size(), kvSize)
	t.Logf("keySize=%d, valueSize=%d", keySize, valueSize)

	n3 := n2.splitTwo()

	if n3 == nil {
		t.Errorf("Should split two")
	}

	i := (splitThreshold - HeaderSize) / (PairInfoSize + kvSize)

	if n2.KeyCount() != i {
		t.Errorf("Incorrect split point: expect %d, get %d", i, n2.KeyCount())
	}

	if n3.KeyCount() != keyCount-i {
		t.Errorf("Incorrect new node: expect %d keys, get %d", keyCount-i, n3.KeyCount())
	}
}
