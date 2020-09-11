package mk_test

import (
	"testing"

	mk "github.com/daicang/mk/pkg"
	fuzz "github.com/google/gofuzz"
)

var (
	dataPath = "~/go/src/github.com/daicang/mk/data"
)

func DBTest(t *testing.T) {
	f := fuzz.New()

	kvs := map[string]string{}

	for i := 0; i < 1000; i++ {
		var key, value string

		f.Fuzz(&key)
		f.Fuzz(&value)

		kvs[key] = value
	}

	opt := mk.Options{
		ReadOnly: false,
		Path:     dataPath,
	}

	db, err := mk.Open(opt)
	if err != nil {
		panic(err)
	}

	tx, err := db.NewTransaction(true)
	if err != nil {
		panic(err)
	}

	for key, value := range kvs {
		found, old := tx.Set([]byte(key), []byte(value))
		if found {
			t.Errorf("Found should be false: old=%s", old)
		}
	}

	err = tx.Commit()
	if err != nil {
		t.Errorf("Failed to commit: %v", err)
	}

}
