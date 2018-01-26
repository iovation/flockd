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

type tableSpec struct {
	name string
	keys []string
}

func fillDB(b *testing.B, db *DB, tableCount, keyCount int) []*tableSpec {
	tables := make([]*tableSpec, tableCount)
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
		tables[tc] = &tableSpec{tblName, keys}
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
				spec := tables[rand.Intn(len(tables))]
				tbl, _ := db.Table(spec.name)
				currentResult, _ = tbl.Get(spec.keys[rand.Intn(len(spec.keys))])
			}
			globalResultChan <- currentResult
			wg.Done()
		}(b.N)
	}

	wg.Wait()
}

func BenchmarkReads(b *testing.B) {
	for _, spec := range []struct {
		size     string
		tblCount int
		keyCount int
	}{
		{"small", 1, 10},
		{"medium", 3, 100},
		{"large", 10, 500},
	} {
		for _, wc := range []int{1, 2, 4, 8, 16, 32, 64} {
			b.Run(fmt.Sprintf("%v_reads-%v", spec.size, wc), func(b *testing.B) {
				benchmarkReads(b, wc, spec.tblCount, spec.keyCount)
			})
		}
	}
}

func benchmarkMix(b *testing.B, readerCount, writerCount, tableCount, keyCount int) {
	db := makeDB(b)
	defer os.RemoveAll(db.root.path)
	tables := fillDB(b, db, tableCount, keyCount)

	// Holds our final results, to prevent compiler optimizations.
	globalResultChan = make(chan []byte, readerCount)

	var wg sync.WaitGroup
	wg.Add(readerCount + writerCount)
	b.ResetTimer()

	for rc := 0; rc < readerCount; rc++ {
		go func(n int) {
			var err error
			currentResult := []byte{}
			for i := 0; i < n; i++ {
				tblName := rand.Intn(len(tables))
				spec := tables[tblName]
				tbl, _ := db.Table(spec.name)
				key := spec.keys[rand.Intn(len(spec.keys))]
				currentResult, err = tbl.Get(key)
				if err != nil {
					b.Logf("Error reading: %v", err)
				}
			}
			globalResultChan <- currentResult
			wg.Done()
		}(b.N)
	}

	for wc := 0; wc < writerCount; wc++ {
		go func(n int) {
			for i := 0; i < n; i++ {
				spec := tables[rand.Intn(len(tables))]
				tbl, _ := db.Table(spec.name)
				val := make([]byte, random(64, 4096))
				rand.Read(val)
				key := spec.keys[rand.Intn(len(spec.keys))]
				if err := tbl.Set(key, val); err != nil {
					b.Logf("Error writing: %v", err)
				}
				// XXX flockd_bench_test.go:157: Error writing: rename /var/folders/yv/h0_940zx2j38gfnkrt3xcc5c0000gn/T/bench250886901/abc.tbl/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWX.kv.tmp13309 /var/folders/yv/h0_940zx2j38gfnkrt3xcc5c0000gn/T/bench250886901/abc.tbl/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWX.kv: no such file or directory

			}
			wg.Done()
		}(b.N)
	}

	wg.Wait()
}

func BenchmarkMix(b *testing.B) {
	for _, spec := range []struct {
		size     string
		tblCount int
		keyCount int
	}{
		{"tiny", 1, 2},
		{"small", 1, 10},
	} {
		for _, split := range [][]int{
			{1, 1},
			{2, 2},
			{3, 4},
			{4, 8},
			{5, 16},
			{6, 32},
			{7, 64},
			{1, 10},
			{1, 50},
			{0, 1},
			{0, 5},
			{0, 10},
			{0, 50},
		} {
			b.Run(fmt.Sprintf("%v_reads-%v/%v", spec.size, split[0], split[1]), func(b *testing.B) {
				benchmarkMix(b, split[1], split[0], spec.tblCount, spec.keyCount)
			})
		}
	}
}
