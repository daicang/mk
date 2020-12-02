package mk

// import (
// 	"testing"
// )

// var (
// 	dataPath = "/Users/cang.dai/go/src/github.com/daicang/mk/data"
// )

// func TestDB(t *testing.T) {
// 	kvs := randomKV(1000)
// 	opt := Options{
// 		ReadOnly: false,
// 		Path:     dataPath,
// 	}

// 	db, ok := Open(opt)
// 	if !ok {
// 		t.Errorf("Failed to open DB")
// 	}

// 	tx, err := db.NewTransaction(true)
// 	if err != nil {
// 		panic(err)
// 	}

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
