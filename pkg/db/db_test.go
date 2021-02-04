package db

import (
	"os"
	"testing"
)

var (
	dataPath = "../testing_data/data"
)

func TestCreateNew(t *testing.T) {
	opt := Options{
		ReadOnly: false,
		Path:     dataPath,
	}

	os.Remove(dataPath)

	db := DB{
		path:     opt.Path,
		readOnly: opt.ReadOnly,
	}

	ok := db.createNew()
	if !ok {
		t.Error("Failed to create new DB")
	}

	_, err := os.Stat(db.path)
	if err != nil {
		t.Errorf("Failed to check data file: %v", err)
	}

	buf := make([]byte, 3*pageSize)
	fd, _ := os.OpenFile(db.path, os.O_CREATE, 0644)

	fd.Read(buf)

	for i := 0; i < 3; i++ {
		p := bufferPage(buf, i)
		if p.id != pgid(i) {
			t.Errorf("Incorrect page id")
		}

		switch i {
		case 0:
			if !p.isMeta() {
				t.Error("First page should be meta page")
			}

			mt := p.getMeta()

			if mt.magic != Magic {
				t.Errorf("Meta page magic value error")
			}

			if mt.root != 2 {
				t.Errorf("Meta page root pgid error")
			}

		case 1:
			if !p.isFreelist() {
				t.Errorf("Second page should be freelist page")
			}

		case 2:
			if !p.isLeaf() {
				t.Errorf("Root page should be leaf")
			}
		}
	}
}

// func TestDB(t *testing.T) {
// 	opt := Options{
// 		ReadOnly: false,
// 		Path:     dataPath,
// 	}

// 	os.Remove(dataPath)

// 	db, ok := OpenDB(opt)
// 	if !ok {
// 		t.Fatal("Failed to open DB")
// 	}

// 	tx, ok := NewWritableTx(db)
// 	if !ok {
// 		t.Fatal("Failed to create tx")
// 	}

// 	kvs := randomKV(1000)

// 	for key, value := range kvs {
// 		found, old := tx.Set([]byte(key), []byte(value))
// 		if found {
// 			t.Errorf("Found should be false: old=%s", old)
// 		}
// 	}

// 	ok = tx.Commit()
// 	if !ok {
// 		t.Errorf("Failed to commit")
// 	}

// }
