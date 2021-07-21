package mk

import (
	"bytes"
	"fmt"
	"testing"
)

func TestPageFromBuffer(t *testing.T) {
	buf := make([]byte, 5*PageSize)
	p := PageFromBuffer(buf, 0)

	p.SetFlag(LeafPage)
	if !p.IsLeaf() {
		t.Errorf("LeafPage flag not set")
	}

	p = PageFromBuffer(buf, 0)
	if !p.IsLeaf() {
		t.Errorf("LeafPage flag not set")
	}

	p = PageFromBuffer(buf, 1)
	p.SetFlag(InternalPage)
	if !p.IsInternal() {
		t.Errorf("InternalPage flag not set")
	}

	p = PageFromBuffer(buf, 1)
	if !p.IsInternal() {
		t.Errorf("InternalPage flag not set")
	}
}

func TestPageCounter(t *testing.T) {
	buf := make([]byte, 5*PageSize)
	p := PageFromBuffer(buf, 0)

	p.SetFlag(LeafPage)
	p.SetKeyCount(10)

	if p.GetKeyCount() != 10 {
		t.Errorf("Incorrect KeyCount")
	}
	if p.GetChildCount() != 10 {
		t.Errorf("Incorrect ChildCount")
	}

	p = PageFromBuffer(buf, 1)

	p.SetFlag(InternalPage)
	p.SetKeyCount(10)

	if p.GetKeyCount() != 10 {
		t.Errorf("Incorrect KeyCount")
	}
	if p.GetChildCount() != 11 {
		t.Errorf("Incorrect ChildCount")
	}
}

func TestWriteKeyValue(t *testing.T) {
	buf := make([]byte, 5*PageSize)
	p := PageFromBuffer(buf, 0)
	count := 10

	p.SetFlag(LeafPage)
	p.SetKeyCount(count)

	offset := count * KvMetaSize

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))

		p.WriteKeyValueAt(i, offset, key, value)
		offset += len(key) + len(value)
	}

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))

		if !bytes.Equal(p.GetKeyAt(i), key) {
			t.Errorf("Incorrect key at %d", i)
		}
		if !bytes.Equal(p.GetValueAt(i), value) {
			t.Errorf("Incorrect value at %d", i)
		}
	}
}

func TestWriteChildPage(t *testing.T) {
	buf := make([]byte, 5*PageSize)
	p := PageFromBuffer(buf, 0)
	count := 10

	p.SetFlag(InternalPage)
	p.SetKeyCount(count)

	offset := count * KvMetaSize

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		p.WriteKeyChildAt(i, offset, key, i)
		offset += len(key)
	}

	for i := 0; i < count; i++ {
		if p.GetChildIDAt(i) != i {
			t.Errorf("Incorrect child at %d", i)
		}
	}
}
