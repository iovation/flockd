package flockd_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"source.iovation.com/iol/flockd"
)

func tmpdir(name string) string {
	dir, err := ioutil.TempDir("", name)
	if err != nil {
		log.Fatal("TempDir", err)
	}
	return dir
}

func ExampleNew() {
	// Open the database with an access timeout of 10 ms.
	db, err := flockd.New("my.db", 10*time.Millisecond)
	if err != nil {
		log.Fatal("flockd.New", err)
	}

	// Optionally remove the database.
	defer os.RemoveAll(db.Path())
}

func ExampleDB_ForEach() {
	// Open the database
	path := tmpdir("foreach.db")
	db, err := flockd.New(path, time.Millisecond)
	if err != nil {
		log.Fatal("flockd.New", err)
	}
	defer os.RemoveAll(path)

	// Load some data into the root table.
	for k, v := range map[string]string{
		"thing 1":  "thing 2",
		"wilma":    "fred",
		"michelle": "barrack",
	} {
		if err := db.Set(k, []byte(v)); err != nil {
			log.Fatal("flockd.Set", err)
		}
	}

	// Iterate over the records in the root table.
	db.ForEach(func(key string, val []byte) error {
		fmt.Printf("%v: %s\n", key, val)
		return nil
	})
	// Unordered output:
	// thing 1: thing 2
	// wilma: fred
	// michelle: barrack
}

func ExampleTable_ForEach() {
	// Open the database
	path := tmpdir("foreach.db")
	db, err := flockd.New(path, time.Millisecond)
	if err != nil {
		log.Fatal("flockd.New", err)
	}
	defer os.RemoveAll(path)

	// Create a table.
	tbl, err := db.Table("simpsons.db")
	if err != nil {
		log.Fatal("flockd.DB.Table", err)
	}

	// Add some data.
	for k, v := range map[string]string{
		"Marge":   "Homer",
		"Selma":   "Sideshow Bob",
		"Manjula": "Apu",
		"Maude":   "Ned",
	} {
		if err := tbl.Set(k, []byte(v)); err != nil {
			log.Fatal("flockd.Table.Set", err)
		}
	}

	// Iterate over the records in the root table.
	tbl.ForEach(func(key string, val []byte) error {
		fmt.Printf("%v ❤️ %s\n", key, val)
		return nil
	})
	// Unordered output:
	// Marge ❤️ Homer
	// Maude ❤️ Ned
	// Manjula ❤️ Apu
	// Selma ❤️ Sideshow Bob
}
