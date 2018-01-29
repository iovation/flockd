/*

Package flockd provides a simple file system-based key/value database that uses
file locking for concurrency safety. Keys correpond to files, values to their
contents, and tables to directories. Files are share-locked on read (Get) and
exclusive-locked on write (Set and Delete).

*/
package flockd

import (
	"context"
	"github.com/theckman/go-flock"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DB defines a file system directory as the root for a simple key/value
// database.
type DB struct {
	root   *Table
	tables *sync.Map
}

// Table represents a diretory into which keys and values can be written.
type Table struct {
	path string
}

// New creates a new key/value database, with the specified directory as the
// root table. If the directory does not exist, it will be created. Returns an
// error if the directory creation fails.
func New(dir string) (*DB, error) {
	root, err := newTable(dir)
	if err != nil {
		return nil, err
	}
	return &DB{root: root, tables: &sync.Map{}}, nil
}

// Table creates a table in the database. The table corresponds to a
// subdirectory of the database root directory. Its name will be the table name
// plus the extension ".tbl". Keys and values can be written directly to the
// table. Pass a path created by filepath.Join to create a deeper subdirectory.
// If the directory does not exist, it will be created. Returns an error if the
// directory creation fails. If the table has been created previously, it will
// be returned immediately without checking for the existence of the directory
// on the file system.
func (db *DB) Table(subdir string) (*Table, error) {
	if table, ok := db.tables.Load(subdir); ok {
		return table.(*Table), nil
	}

	table, err := newTable(filepath.Join(db.root.path, subdir+".tbl"))
	if err != nil {
		return nil, err
	}
	db.tables.Store(subdir, table)
	return table, nil
}

func newTable(path string) (*Table, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	return &Table{path: path}, nil
}

// Get returns the value for the key by reading the file named for the key, plus
// the extension ".kv", from the root directory.
func (db *DB) Get(key string) ([]byte, error) {
	return db.root.Get(key)
}

// Set sets the value for the key by writing it to the file named for the key,
// plus the extension ".kv", in the root directory.
func (db *DB) Set(key string, val []byte) error {
	return db.root.Set(key, val)
}

// Delete deletes the key and its value by deleting the file named for the key,
// plus the extension ".kv", // in the root directory.
func (db *DB) Delete(key string) error {
	return db.root.Delete(key)
}

// Get returns the value for the key by reading the file named for key, plus the
// extension ".kv", from the table directory. The key must not contain a path
// separator character; if it does, os.ErrInvalid will be returned. If the file
// does not exist, os.ErrNotExist will be returned. For concurrency safetey, Get
// acquires a shared file system lock on the file before reading its contents.
// If the file has an exclusive lock on it, Get will spend up to a millisecond
// waiting for the shared lock before returning a context.DeadlineExceeded
// error.
func (table *Table) Get(key string) ([]byte, error) {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return nil, os.ErrInvalid
	}

	// Open the file.
	file := filepath.Join(table.path, key+".kv")
	fh, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	defer fh.Close()

	// Take a shared lock.
	lock, err := lockFile(file, false)
	if err != nil {
		return nil, err
	}
	defer lock.Unlock()

	// Fetch the contents.
	val, err := ioutil.ReadAll(fh)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Set sets the value for the key by writing it to the file named for key, plus
// the extension ".kv", in the table directory. The key must not contain a path
// separator character; if it does, os.ErrInvalid will be returned.
//
// To set the value, Set first creates a temporary file in the table directory
// and tries to acquire an exclusive lock. If the temporary file already has
// exclusive lock, Set will wait up to a millisecond to acquire the lock before
// returning a context.DeadlineExceeded error. Once it has the lock, it writes
// the value to the temporary file.
//
// Next, it tries to acquire an exclusive lock on the file with the key name,
// again waiting up to a millisecond before returning a context.DeadlineExceeded
// error. Once it has the lock, it moves the temporary file to the new file.
func (table *Table) Set(key string, value []byte) error {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return os.ErrInvalid
	}

	// Create a temporary file to write to.
	fh, err := ioutil.TempFile(table.path, key+".kv")
	defer fh.Close()
	tmp := fh.Name()
	defer os.Remove(tmp)

	// Take an exclusive lock on the temp file.
	lock, err := lockFile(tmp, true)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	// Write to the file.
	if _, err := fh.Write(value); err != nil {
		return err
	}
	if err := fh.Sync(); err != nil {
		return err
	}

	// XXX Is it necessary to lock the destination file?
	// Open the key file.
	// Take an exclusive lock on the key file.
	file := filepath.Join(table.path, key+".kv")
	lock2, err := lockFile(file, true)
	if err != nil {
		return err
	}
	defer lock2.Unlock()

	// Move the file.
	return os.Rename(tmp, file)
}

// Delete deletes the key and its value by deleting the file named for key, plus
// the extension ".kv", in the table directory. The key must not contain a path
// separator character; if it does, os.ErrInvalid will be returned. Before
// deleting the file, Delete tries to acquire an exclusive lock. If the file
// already has exclusive lock, Delete will wait up to a millisecond to acquire
// the lock before returning a context.DeadlineExceeded error. Once it has
// acquired the lock, it deletes the file.
func (table *Table) Delete(key string) error {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return os.ErrInvalid
	}

	// Open the file.
	file := filepath.Join(table.path, key+".kv")
	fh, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			// Already gone.
			return nil
		}
		return err
	}
	defer fh.Close()

	// Make sure it's not a directory.
	if info, err := fh.Stat(); err == nil && info.IsDir() {
		return os.ErrInvalid
	}

	// Take an exclusive lock.
	lock, err := lockFile(file, true)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	// Remove the file.
	return os.Remove(file)
}

// lockFile tries to acquire a shared or exclusive lock on a file, waiting up to
// a millisecond for the lock, and returns the lock or an error.
func lockFile(path string, exclusive bool) (*flock.Flock, error) {
	flock := flock.NewFlock(path)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	try := flock.TryRLockContext
	if exclusive {
		try = flock.TryLockContext
	}

	// Try to get the lock up to 100 times.
	if _, err := try(ctx, time.Millisecond/100); err != nil {
		return nil, err
	}
	return flock, nil
}
