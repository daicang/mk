package freelist

import (
	"reflect"
	"testing"

	"github.com/daicang/mk/pkg/page"
)

func TestMerge(t *testing.T) {
	a := pgids{1, 2, 3}
	b := pgids{}
	expect := pgids{1, 2, 3}
	result := merge(a, b)

	if !reflect.DeepEqual(result, expect) {
		t.Errorf("expect %v get %v", expect, result)
	}

	result = merge(b, a)
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("expect %v get %v", expect, result)
	}

	a = pgids{1, 2, 3}
	b = pgids{4, 5}
	expect = pgids{1, 2, 3, 4, 5}

	if !reflect.DeepEqual(result, expect) {
		t.Errorf("expect %v get %v", expect, result)
	}

	a = pgids{1, 3, 4, 7, 9}
	b = pgids{2, 4, 6, 8, 10, 11, 12}
	expect = pgids{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	if !reflect.DeepEqual(result, expect) {
		t.Errorf("expect %v get %v", expect, result)
	}
}

func TestAllocate(t *testing.T) {
	f := Freelist{
		ids: pgids{},
	}
	_, success := f.Allocate(1)
	if success {
		t.Errorf("allocate empty freelist should fail")
	}

	f.ids = pgids{1, 3, 4, 5, 6, 7}
	pid, success := f.Allocate(1)
	if !success || pid != 1 {
		t.Errorf("allocate failed: success %v, pid %v", success, pid)
	}
	if !reflect.DeepEqual(f.ids, pgids{3, 4, 5, 6, 7}) {
		t.Errorf("incorrect ids: %v", f.ids)
	}

	f.ids = pgids{1, 3, 5, 6, 7}
	pid, success = f.Allocate(2)
	if !success || pid != 5 {
		t.Errorf("allocate failed: success %v, pid %v", success, pid)
	}
	if !reflect.DeepEqual(f.ids, pgids{1, 3, 7}) {
		t.Errorf("incorrect ids: %v", f.ids)
	}

	f.ids = pgids{1, 3, 5, 6, 7}
	pid, success = f.Allocate(3)
	if success {
		t.Errorf("allocate should fail")
	}
}

func TestReadWrite(t *testing.T) {
	f := NewFreelist()
	size := 200

	for i := 0; i < size; i++ {
		f.ids = append(f.ids, uint32(i))
	}

	buf := make([]byte, f.Size())
	p := page.FromBuffer(buf, 0)

	f.WritePage(p)

	f1 := NewFreelist()
	f1.ReadPage(p)

	if !reflect.DeepEqual(f.ids, f1.ids) {
		t.Errorf("failed to read / write")
	}
}
