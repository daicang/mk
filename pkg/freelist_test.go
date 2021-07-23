package mk

import (
	"reflect"
	"testing"
)

func TestMerge(t *testing.T) {
	a := ints{1, 2, 3}
	b := ints{}
	expect := ints{1, 2, 3}
	result := merge(a, b)

	if !reflect.DeepEqual(result, expect) {
		t.Errorf("expect %v get %v", expect, result)
	}

	result = merge(b, a)
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("expect %v get %v", expect, result)
	}

	a = ints{1, 2, 3}
	b = ints{4, 5}
	expect = ints{1, 2, 3, 4, 5}
	result = merge(b, a)
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("expect %v get %v", expect, result)
	}

	a = ints{1, 3, 5, 7, 9}
	b = ints{2, 4, 6, 8, 10, 11, 12}
	expect = ints{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	result = merge(b, a)
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("expect %v get %v", expect, result)
	}
}

func TestAllocate(t *testing.T) {
	f := Freelist{
		ids: ints{},
	}
	_, success := f.Allocate(1)
	if success {
		t.Errorf("allocate empty freelist should fail")
	}

	f.ids = ints{1, 3, 4, 5, 6, 7}
	pid, success := f.Allocate(1)
	if !success || pid != 1 {
		t.Errorf("allocate failed: success %v, pid %v", success, pid)
	}
	if !reflect.DeepEqual(f.ids, ints{3, 4, 5, 6, 7}) {
		t.Errorf("incorrect ids: %v", f.ids)
	}

	f.ids = ints{1, 3, 5, 6, 7}
	pid, success = f.Allocate(2)
	if !success || pid != 5 {
		t.Errorf("allocate failed: success %v, pid %v", success, pid)
	}
	if !reflect.DeepEqual(f.ids, ints{1, 3, 7}) {
		t.Errorf("incorrect ids: %v", f.ids)
	}

	f.ids = ints{1, 3, 5, 6, 8}
	_, success = f.Allocate(3)
	if success {
		t.Errorf("allocate should fail")
	}
}

func TestReadWrite(t *testing.T) {
	f := NewFreelist()
	size := 200

	for i := 0; i < size; i++ {
		f.ids = append(f.ids, int(i))
	}

	buf := make([]byte, f.Size())
	p := FromBuffer(buf, 0)

	f.WritePage(p)

	f1 := NewFreelist()
	f1.ReadPage(p)

	if !reflect.DeepEqual(f.ids, f1.ids) {
		t.Errorf("failed to read / write")
	}
}
