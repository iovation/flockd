package flockd

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

func makeDB(b *testing.B) *DB {
	dir, err := ioutil.TempDir("", "bench")
	if err != nil {
		b.Fatal("TempDir", err)
	}
	db, err := New(dir)
	if err != nil {
		b.Fatal("New", err)
	}
	return db
}

func fillDB(b *testing.B, db *DB, tableCount, keyCount int) map[string][]string {
	tables := make(map[string][]string, tableCount)
	for tc := 0; tc < tableCount; tc++ {
		tblName := stringOf(random(3, 10))
		tbl, _ := db.Table(tblName)
		keys := make([]string, keyCount)
		for kc := 0; kc < keyCount; kc++ {
			key := stringOf(random(32, 128))
			val := make([]byte, random(64, 4096))
			rand.Read(val)
			if err := tbl.Set(key, val); err != nil {
				b.Fatal("Set", err)
			}
			keys[kc] = key
		}
		tables[tblName] = keys
	}
	return tables
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func stringOf(n int) string {
	b := make([]rune, n)
	cnt := len(letters)
	for i := range b {
		b[i] = letters[i%cnt]
	}
	return string(b)
}

var globalResultChan chan []byte

func benchmarkReads(b *testing.B, workerCount, tableCount, keyCount int) {
	db := makeDB(b)
	defer os.RemoveAll(db.root.path)
	tables := fillDB(b, db, tableCount, keyCount)

	// Holds our final results, to prevent compiler optimizations.
	globalResultChan = make(chan []byte, workerCount)

	var wg sync.WaitGroup
	wg.Add(workerCount)
	b.ResetTimer()

	for wc := 0; wc < workerCount; wc++ {
		go func(n int) {
			currentResult := []byte{}
			for i := 0; i < n; i++ {
				for tblName, keys := range tables {
					tbl, _ := db.Table(tblName)
					currentResult, _ = tbl.Get(keys[rand.Intn(len(keys))])
				}
			}
			globalResultChan <- currentResult
			wg.Done()
		}(b.N)
	}

	wg.Wait()
}

func BenchmarkReads(b *testing.B) {
	for desc, spec := range map[string][]int{
		"small":  {1, 10},
		"medium": {3, 100},
		// "large":  {30, 1000},
	} {
		for _, wc := range []int{1, 2, 4, 8, 16, 32, 64} {
			b.Run(fmt.Sprintf("%v_reads-%v", desc, wc), func(b *testing.B) {
				benchmarkReads(b, wc, spec[0], spec[1])
			})
		}
	}
}
