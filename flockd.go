/*

Package flockd provides a simple file system-based key/value database that uses
file locking for concurrency safety. Keys correspond to files, values to their
contents, and tables to directories. Files are share-locked on read (Get and
ForEach) and exclusive-locked on write (Set, Create, Update, and Delete).

This may be overkill if you have only one application using a set of files in a
directory. But if you need to sync files between multiple systems, like a
distributed database, assuming your sync software respects file system locks,
flockd might be a great way to go. This is especially true for modestly-sized
databases and databases with a single primary instance and multiple read-only
secondary instances.

In any event, your file system must support proper file locking for this to
work. If your file system does not, it might still work if file renaming and
unlinking is atomic and flockd is used exclusively to access files. If not, then
all bets are off, and you can expect occasional bad reads.

All of this may turn out to be a bad idea. YMMV. Warranty not included.

*/
package flockd

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

const (
	tblExt  = ".tbl"
	recExt  = ".kv"
	readNum = 1024
)

// DB defines a file system directory as the root for a simple key/value
// database.
type DB struct {
	root   *Table
	tables *sync.Map
}

// Table represents a diretory into which keys and values can be written.
type Table struct {
	name    string
	path    string
	timeout time.Duration
}

// New creates a new key/value database, with the specified directory as the
// root table. If the directory does not exist, it will be created. The timeout
// sets the maximum time flockd will wait for a file lock when attempting to
// read, write, or delete a file, in nanoseconds. Returns an error if the
// directory creation fails or if the timeout is less than or equal to zero.
func New(dir string, timeout time.Duration) (*DB, error) {
	if timeout <= 0 {
		return nil, errors.New("Invalid lock timeout")
	}
	root, err := newTable("", dir, timeout)
	if err != nil {
		return nil, err
	}
	return &DB{root: root, tables: &sync.Map{}}, nil
}

// Path returns the root path of the database, as passed to New().
func (db *DB) Path() string {
	return db.root.path
}

// Table creates a table in the database. The table corresponds to a
// subdirectory of the database root directory. Its name will be the table name
// plus the extension ".tbl". Keys and values can be written directly to the
// table. Pass a path created by filepath.Join to create a deeper subdirectory.
// If the directory does not exist, it will be created. Returns an error if the
// directory creation fails. If the table has been created previously for the
// instance of the database, it will be returned immediately without checking
// for the existence of the directory on the file system.
func (db *DB) Table(name string) (*Table, error) {
	if table, ok := db.tables.Load(name); ok {
		return table.(*Table), nil
	}

	table, err := newTable(
		name,
		filepath.Join(db.root.path, name+tblExt),
		db.root.timeout,
	)
	if err != nil {
		return nil, err
	}
	db.tables.Store(name, table)
	return table, nil
}

func newTable(name, path string, timeout time.Duration) (*Table, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	return &Table{name: name, path: path, timeout: timeout}, nil
}

// Get returns the value for the key by reading the file named for the key, plus
// the extension ".kv", from the root directory.
func (db *DB) Get(key string) ([]byte, error) {
	return db.root.Get(key)
}

// Create creates the key/value pair by writing it to a file named for the key,
// plus the extension ".kv", in the root directory, but only if the file does
// not already exist.
func (db *DB) Create(key string, val []byte) error {
	return db.root.Create(key, val)
}

// Update updates the key/value pair by writing it to a file named for the key,
// plus the extension ".kv", in the root directory, but only if the file
// already exists.
func (db *DB) Update(key string, val []byte) error {
	return db.root.Update(key, val)
}

// Set sets the value for the key by writing it to the file named for the key,
// plus the extension ".kv", in the root directory.
func (db *DB) Set(key string, val []byte) error {
	return db.root.Set(key, val)
}

// Delete deletes the key and its value by deleting the file named for the key,
// plus the extension ".kv", in the root directory.
func (db *DB) Delete(key string) error {
	return db.root.Delete(key)
}

// ForEach finds each file with the extension ".kv" in the root directory and
// calls the specified function, passing the file's key and value (file basename
// and contents).
func (db *DB) ForEach(feFunc ForEachFunc) error {
	return db.root.ForEach(feFunc)
}

// Tables returns all of the tables in the database. Tables are defined as the
// root directory and any subdirectory with the extension ".tbl". This function
// actively walks the file system from the root directory to find the table
// directories and does not cache the results.
func (db *DB) Tables() ([]*Table, error) {
	timeout := db.root.timeout
	rootPath := db.root.path
	prefix := rootPath + string(os.PathSeparator)
	tables := []*Table{}
	if err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() || (filepath.Ext(path) != tblExt && path != rootPath) {
			return nil
		}
		name := ""
		if path != rootPath {
			name = strings.TrimSuffix(strings.TrimPrefix(path, prefix), tblExt)
		}
		tables = append(tables, &Table{name: name, path: path, timeout: timeout})
		return nil
	}); err != nil {
		return nil, err
	}
	return tables, nil
}

// Name returns the name of the table, which corresponds to the name of the
// subdirectory without the extension ".tbl".
func (table *Table) Name() string {
	return table.name
}

// Get returns the value for the key by reading the file named for key, plus the
// extension ".kv", from the table directory. The key must not contain a path
// separator character; if it does, os.ErrInvalid will be returned. If the file
// does not exist, os.ErrNotExist will be returned. For concurrency safety, Get
// acquires a shared file system lock on the file before reading its contents.
// If the file has an exclusive lock on it, Get will wait up to the timeout set
// for the database for the shared lock before returning a
// context.DeadlineExceeded error.
func (table *Table) Get(key string) ([]byte, error) {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return nil, os.ErrInvalid
	}

	// Open the file.
	file := filepath.Join(table.path, key+recExt)
	fh, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	defer fh.Close()

	// Take a shared lock.
	lock, err := lockFile(file, false, table.timeout)
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
// exclusive lock, Set will wait up to the timeout set for the database to
// acquire the lock before returning a context.DeadlineExceeded error. Once it
// has the lock, it writes the value to the temporary file.
//
// Next, it tries to acquire an exclusive lock on the file with the key name,
// again waiting up to the database timeout before returning a
// context.DeadlineExceeded error. Once it has the lock, it moves the temporary
// file to the new file.
func (table *Table) Set(key string, value []byte) error {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return os.ErrInvalid
	}

	// Write to a temporary file.
	tmp, err := table.writeTemp(key, value)
	if err != nil {
		return err
	}
	defer tmp.Release()

	// XXX Is it necessary to lock the destination file?
	// Open the key file.
	// Take an exclusive lock on the key file.
	file := filepath.Join(table.path, key+recExt)
	lock, err := lockFile(file, true, table.timeout)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	// Move the file.
	return os.Rename(tmp.file, file)
}

// Create creates the key/value pair by writing it to the file named for key,
// plus the extension ".kv", in the table directory, but only if the file does
// not already exist. The key must not contain a path separator character; if it
// does, os.ErrInvalid will be returned. Returns os.ErrExist if the file already
// exists.
//
// To create the file, Create first opens it with the key name, but only if it
// doesn't already exist. It then tries to acquire an exclusive lock on the
// file. It tries only once, and doesn't wait for a lock, so that if any other
// process first got a lock, the file would be considered to already exist.
//
// Create then creates a temporary file in the table directory and tries to
// acquire an exclusive lock. If the temporary file already has exclusive lock,
// Create will wait up to the timeout set for the database to acquire the lock
// before returning a context.DeadlineExceeded error. Once it has the lock, it
// writes the value to the temporary file, then moves the temporary file to the
// new file.
func (table *Table) Create(key string, value []byte) error {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return os.ErrInvalid
	}

	// Open the destination file, but only if it doesn't already exist.
	file := filepath.Join(table.path, key+recExt)
	fh, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if os.IsExist(err) {
			return os.ErrExist
		}
		return err
	}
	defer fh.Close()

	// Take an exclusive lock on the file, but don't wait for it. Yes, there is
	// a race condition here.
	lock := flock.NewFlock(file)
	locked, err := lock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		// Someone beat us to it?
		return os.ErrExist
	}
	defer lock.Unlock()

	// Write to a temporary file.
	tmp, err := table.writeTemp(key, value)
	if err != nil {
		return err
	}
	defer tmp.Release()

	// Move the file.
	return os.Rename(tmp.file, file)
}

// Update updates the value for the key by writing it to an existing file named
// for key, plus the extension ".kv", in the table directory. The key must not
// contain a path separator character; if it does, os.ErrInvalid will be
// returned. If the file does not already exist, os.ErrNotExist will be
// returned.
//
// To update the file, Update first opens the file with the key name for
// writing. If the file does not exist, os.ErrNotExist will be returned.
//
// Next, Update creates a temporary file in the table directory and tries to
// acquire an exclusive lock. If the temporary file already has exclusive lock,
// Update will wait up to the timeout set for the database to acquire the lock
// before returning a context.DeadlineExceeded error. Once it has the lock, it
// writes the value to the temporary file.
//
// Next, it tries to acquire an exclusive lock on the opened file, again waiting
// up to the database timeout before returning a context.DeadlineExceeded error.
// Once it has the lock, it moves the temporary file to the new file.
func (table *Table) Update(key string, value []byte) error {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return os.ErrInvalid
	}

	// Open the file.
	file := filepath.Join(table.path, key+recExt)
	fh, err := os.OpenFile(file, os.O_WRONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return os.ErrNotExist
		}
		return err
	}
	defer fh.Close()

	// Write to a temporary file.
	tmp, err := table.writeTemp(key, value)
	if err != nil {
		return err
	}
	defer tmp.Release()

	// Take an exclusive lock on the key file.
	lock, err := lockFile(file, true, table.timeout)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	// Move the file.
	return os.Rename(tmp.file, file)
}

// Delete deletes the key and its value by deleting the file named for key, plus
// the extension ".kv", from the table directory. The key must not contain a
// path separator character; if it does, os.ErrInvalid will be returned. Before
// deleting the file, Delete tries to acquire an exclusive lock. If the file
// already has exclusive lock, Delete will wait up to the timeout set for the
// database to acquire the lock before returning a context.DeadlineExceeded
// error. Once it has acquired the lock, it deletes the file.
func (table *Table) Delete(key string) error {
	// Make sure there is no directory separator.
	if strings.ContainsRune(key, os.PathSeparator) {
		return os.ErrInvalid
	}

	// Open the file.
	file := filepath.Join(table.path, key+recExt)
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
	lock, err := lockFile(file, true, table.timeout)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	// Remove the file.
	return os.Remove(file)
}

// ForEachFunc is the type of the function called for each record fetched by
// ForEach. The arguments consist of the key and value to process. Returning an
// error halts the execution of ForEach.
type ForEachFunc func(key string, value []byte) error

// ForEach executes a function for each key/value pair in the table. Internally,
// ForEach reads the table directory to find record files, fetches its contents
// via Get(), and passes the key and retrieved value to feFunc. An error
// returned by any of these steps, including from the feFunc function, causes
// ForEach to halt the search and return the error. The feFunc function must not
// modify the table; doing so results in undefined behavior.
func (table *Table) ForEach(feFunc ForEachFunc) error {
	dh, err := os.Open(table.path)
	if err != nil {
		return err
	}
	var files []os.FileInfo
	for err != io.EOF {
		files, err = dh.Readdir(readNum)
		if err != nil && err != io.EOF {
			return err
		}
		for _, dir := range files {
			if filepath.Ext(dir.Name()) == recExt && !dir.IsDir() {
				key := strings.TrimSuffix(dir.Name(), recExt)
				val, err := table.Get(key)
				if err != nil {
					return err
				}
				if err := feFunc(key, val); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// lockFile tries to acquire a shared or exclusive lock on a file, waiting up to
// timeout for the lock, and returns the lock or an error.
func lockFile(path string, exclusive bool, timeout time.Duration) (*flock.Flock, error) {
	flock := flock.NewFlock(path)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	try := flock.TryRLockContext
	if exclusive {
		try = flock.TryLockContext
	}

	// Try to get the lock up to 100 times.
	if _, err := try(ctx, timeout/100); err != nil {
		return nil, err
	}
	return flock, nil
}

type tmpFile struct {
	file string
	lock *flock.Flock
}

func (tmp *tmpFile) Release() {
	tmp.lock.Unlock()
	os.Remove(tmp.file)

}

func (table *Table) writeTemp(key string, value []byte) (*tmpFile, error) {
	// Create a temporary file to write to.
	tf, err := ioutil.TempFile(table.path, key+recExt)
	if err != nil {
		return nil, err
	}
	defer tf.Close()
	tmp := &tmpFile{file: tf.Name()}

	// Take an exclusive lock on the temp file.
	lock, err := lockFile(tmp.file, true, table.timeout)
	if err != nil {
		os.Remove(tmp.file)
		return nil, err
	}
	tmp.lock = lock

	// Write to the temp file.
	if _, err := tf.Write(value); err != nil {
		tmp.Release()
		return nil, err
	}
	if err := tf.Sync(); err != nil {
		tmp.Release()
		return nil, err
	}
	return tmp, nil
}
