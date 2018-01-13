/*

Package dirdb provides a simple file system directory-based key/value store. It
treats subdirectories as key spaces, keys as file names, and values as file
contents. It uses shared and exclusive file system locks on the keys (files) to
ensure concurrcy safety.

*/
package dirdb

import (
	"context"
	"github.com/theory/go-flock"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DB defines a file system directory as a simple key/value store.
type DB struct {
	root *Dir
	dirs *sync.Map
}

// New creates a new DB, with the specified directory as the root. If the
// directory does not exist, it will be created. Returns an error if the
// directory creation fails.
func New(dir string) (*DB, error) {
	root, err := newDir(dir)
	if err != nil {
		return nil, err
	}
	return &DB{root: root, dirs: &sync.Map{}}, nil
}

// Sub returns a subdirectory of the DB. Keys and values can be written directly
// to the directory. Think of directories as key spaces. Pass a path created by
// filepath.Join to create a deeper subdirectory. If the directory does not
// exist, it will be created. Returns an error if the directory creation fails.
// If the directory has been fetched previously, it will be returned immediately
// without checking for the existence of the directory on the file system.
func (db *DB) Sub(dir string) (*Dir, error) {
	if sub, ok := db.dirs.Load(dir); ok {
		return sub.(*Dir), nil
	}

	sub, err := newDir(filepath.Join(db.root.dir, dir))
	if err != nil {
		return nil, err
	}
	db.dirs.Store(dir, sub)
	return sub, nil
}

func newDir(path string) (*Dir, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	return &Dir{dir: path}, nil
}

// Get returns the value for the key by reading the file named for key from the
// root directory.
func (db *DB) Get(key string) ([]byte, error) {
	return db.root.Get(key)
}

// Set sets the value for the key by writing it to the file named for key in the
// root directory.
func (db *DB) Set(key string, val []byte) error {
	return db.root.Set(key, val)
}

// Delete deletes the key and its value by deleting the file named for key in
// the root directory.
func (db *DB) Delete(key string) error {
	return db.root.Delete(key)
}

// Dir represents a directoring into which keys and values can be written.
type Dir struct {
	dir string
}

// Get returns the value for the key by reading the file named for key from the
// directory.
func (dir *Dir) Get(key string) ([]byte, error) {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return nil, os.ErrInvalid
	}

	// Open the file.
	file := filepath.Join(dir.dir, key)
	fh, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	defer fh.Close()

	// Take a shared lock.
	lock, err := lockFile(fh, false)
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

// Set sets the value for the key by writing it to the file named for key in the
// directory.
func (dir *Dir) Set(key string, value []byte) error {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return os.ErrInvalid
	}

	// Create a temporary file to write to.
	file := filepath.Join(dir.dir, key)
	tmp := file + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer fh.Close()

	// Take an exclusive lock on the temp file.
	lock, err := lockFile(fh, true)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	// Write to the file.
	if _, err := fh.Write(value); err != nil {
		return err
	}

	// XXX Is it necessary to lock the destination file?
	// Open the key file.
	fh2, err := os.OpenFile(file, os.O_CREATE|os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer fh2.Close()

	// Take an exclusive lock on the key file.
	lock2, err := lockFile(fh2, true)
	if err != nil {
		return err
	}
	defer lock2.Unlock()
	// XXX Destination file lock code end.

	// Move the file.
	return os.Rename(tmp, file)
}

// Delete deletes the key and its value by deleting the file named for key in
// the directory.
func (dir *Dir) Delete(key string) error {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return os.ErrInvalid
	}

	// Open the file.
	file := filepath.Join(dir.dir, key)
	fh, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			// Already gone.
			return nil
		}
		return err
	}
	defer fh.Close()

	// Take an exclusive lock.
	lock, err := lockFile(fh, true)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	// Remove the file.
	return os.Remove(file)
}

func lockFile(fh *os.File, exclusive bool) (*flock.Flock, error) {
	flock := flock.New(fh)
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
