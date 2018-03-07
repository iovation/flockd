package flockd_test

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/iovation/flockd"
)

var timeout = 10 * time.Millisecond

func Example() {
	// Set up a new database.
	db, err := flockd.New("flockd.db", timeout)
	if err != nil {
		log.Fatal("flockd.New", err)
	}
	defer os.RemoveAll("flockd.db")

	// Create a table.
	tbl, err := db.Table("foo")
	if err != nil {
		log.Fatal("flockd.Table", err)
	}

	// Add a key/value pair to the table.
	key := "greeting"
	if err := tbl.Create(key, []byte("Hello world!")); err != nil {
		log.Fatal("flockd.Create", err)
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

	// Fetch the new value.
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
