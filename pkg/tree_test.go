package mk

import (
	"bytes"
	"testing"
)

func TestInsert(t *testing.T) {
	tree := newTree()
	tree.insert()
}

func TestInsertValueAt(t *testing.T) {
	node := node{}
	node.insertValueAt(0, []byte("val1"))
	if len(node.values) != 1 && bytes.Compare(node.values[0], []byte("val")) {
		t.Error("Error insert value")
	}
}
