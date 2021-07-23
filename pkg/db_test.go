package mk

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/daicang/mk/pkg/test"
)

func TestCreateNew(t *testing.T) {
	testingDir, err := ioutil.TempDir("", "testing_data")
	if err != nil {
		t.Fatalf("Failed to create testing dir")
	}
	defer os.Remove(testingDir)

	db := DB{
		path: filepath.Join(testingDir, "db"),
	}
	ok := db.initFile()
	if !ok {
		t.Fatalf("Failed to create new DB")
	}

	buf := make([]byte, 3*PageSize)
	fd, _ := os.OpenFile(db.path, os.O_RDONLY, 0644)
	_, err = fd.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read db file")
	}

	for i := 0; i < 3; i++ {
		p := FromBuffer(buf, int(i))

		if p.Index != int(i) {
			t.Fatalf("Incorrect page id: expect %d get %d", i, p.Index)
		}
		switch i {
		case 0:
			if !p.IsMeta() {
				t.Fatal("First page should be meta page")
			}
			mt := pageMeta(p)
			if mt.magic != Magic {
				t.Fatalf("Meta page magic value error")
			}
			if mt.rootPage != 2 {
				t.Fatalf("Meta page root int error")
			}
		case 1:
			if !p.IsFreelist() {
				t.Fatalf("Second page should be freelist page")
			}
		case 2:
			if !p.IsLeaf() {
				t.Fatalf("Root page should be leaf")
			}
		}
	}
}

func TestOpen(t *testing.T) {
	testingDir, err := ioutil.TempDir("", "testing_data")
	if err != nil {
		t.Fatalf("Failed to create testing dir")
	}
	defer os.Remove(testingDir)

	_, ok := Open(Options{
		Path: filepath.Join(testingDir, "db"),
	})
	if !ok {
		t.Fatal("Failed to open DB")
	}
}

func TestWriteTx(t *testing.T) {
	testingDir, err := ioutil.TempDir("", "testing_data")
	if err != nil {
		t.Fatalf("Failed to create testing dir")
	}
	defer os.Remove(testingDir)

	opt := Options{
		Path: filepath.Join(testingDir, "db"),
	}
	db, ok := Open(opt)
	if !ok {
		t.Fatal("Failed to open DB")
	}
	tx, ok := NewWritable(db)
	if !ok {
		t.Fatal("Failed to create tx")
	}

	kvs := test.RandomKV(1)
	for key, value := range kvs {
		found, old := tx.Set([]byte(key), []byte(value))
		if found {
			t.Fatalf("Found should be false: old=%s", old)
		}
	}

	ok = tx.Commit()
	if !ok {
		t.Fatalf("Failed to commit")
	}

	tx, ok = NewReadOnlyTx(db)
	if !ok {
		t.Fatal("Failed to create read-only tx")
	}

	for key, value := range kvs {
		found, getValue := tx.Get([]byte(key))
		if !found {
			t.Fatalf("Cannot get key %s", string(key))
		}

		if !bytes.Equal(getValue, []byte(value)) {
			t.Fatalf("Expected value %s, get %s", value, getValue)
		}
	}
}
