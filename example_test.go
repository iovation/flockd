package flockd_test

import (
	"fmt"
	"github.com/theory/flockd"
	"log"
	"os"
	"time"
)

var timeout = 10 * time.Millisecond

func Example() {
	// Set up a new database.
	db, err := flockd.New("flockd.db", timeout)
	if err != nil {
		log.Fatal("flockd.New", err)
	}
	defer os.Remove("flockd.db")

	// Create a table.
	tbl, err := db.Table("foo")
	if err != nil {
		log.Fatal("flockd.Table", err)
	}

	// Add a key/value pair to the table.
	key := "greeting"
	if err := tbl.Add(key, []byte("Hello world!")); err != nil {
		log.Fatal("flockd.Add", err)
	}

	// Fetch the value.
	val, err := tbl.Get(key)
	if err != nil {
		log.Fatal("flockd.Get", err)
	}
	fmt.Println(string(val))

	// Set the value.
	if err := tbl.Set(key, []byte("Goodbye world!")); err != nil {
		log.Fatal("flockd.Set", err)
	}

	// Fetch the newq value.
	val, err = tbl.Get(key)
	if err != nil {
		log.Fatal("flockd.Get", err)
	}
	fmt.Println(string(val))

	// Delete the value.
	if err := tbl.Delete(key); err != nil {
		log.Fatal("flockd.Delete", err)
	}

	// No more value.
	val, err = tbl.Get(key)
	if err != os.ErrNotExist {
		log.Fatal("flockd.Get", err)
	}

	// Output:
	// Hello world!
	// Goodbye world!
}
