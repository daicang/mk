package mk

import (
	"os"
	"testing"
)

func TestInitDB(t *testing.T) {
	dataPath := "./data"

	opt := Options{
		ReadOnly: false,
		Path:     dataPath,
	}

	db, ok := InitDB(opt)
	if !ok {
		t.Error("Failed to open DB")
	}

	if db.path != dataPath {
		t.Errorf("DB path error: expect %s, get %s", dataPath, db.path)
	}

	_, err := os.Stat(db.path)
	if err != nil {
		t.Errorf("Failed to check data file: %v", err)
	}

	buf := make([]byte, 4*pageSize)

	fd, _ := os.OpenFile(db.path, os.O_CREATE, 0644)

	fd.Read(buf)

	for i := 0; i < 4; i++ {
		p := bufferPage(buf, i)
		if p.id != pgid(i) {
			t.Errorf("Incorrect page id")
		}

		switch i {
		case 0:
			fallthrough
		case 1:
			if p.toMeta().magic != Magic {
				t.Errorf("File magic not match")
			}

		case 2:
			if !p.isFreelist() {
				t.Errorf("Incorrect freelist page type")
			}
		case 3:
			if !p.isLeaf() {
				t.Errorf("Incorrect leaf page type")
			}
		}
	}
}

// func TestDB(t *testing.T) {
// 	opt := Options{
// 		ReadOnly: false,
// 		Path:     dataPath,
// 	}

// 	db, ok := InitDB(opt)
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
