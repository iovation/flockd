package dirdb

import (
	"context"
	"github.com/theory/go-flock"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type DirDB struct {
	root *Dir
	dirs *sync.Map
}

func New(dir string) (*DirDB, error) {
	root, err := newDir(dir)
	if err != nil {
		return nil, err
	}
	return &DirDB{root: root, dirs: &sync.Map{}}, nil
}

func (db *DirDB) Sub(dirs []string) (*Dir, error) {
	path := filepath.Join(dirs...)
	if sub, ok := db.dirs.Load(path); ok {
		return sub.(*Dir), nil
	}

	sub, err := newDir(path)
	if err != nil {
		return nil, err
	}
	db.dirs.Store(path, sub)
	return sub, nil
}

func newDir(path string) (*Dir, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	return &Dir{dir: path}, nil
}

func (db *DirDB) Get(key string) ([]byte, error) {
	return db.root.Get(key)
}

func (db *DirDB) Set(key string, val []byte) error {
	return db.root.Set(key, val)
}

func (db *DirDB) Delete(key string) error {
	return db.root.Delete(key)
}

type Dir struct {
	dir string
}

func (dir *Dir) Get(key string) ([]byte, error) {
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

func (dir *Dir) Set(key string, value []byte) error {
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
	fh2, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDONLY, 0600)
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

	// Move the file.
	return os.Rename(tmp, file)
}

func (dir *Dir) Delete(key string) error {
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
