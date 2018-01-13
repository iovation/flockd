package flockv

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

type TS struct {
	db *DB
	suite.Suite
}

func TestDB(t *testing.T) {
	suite.Run(t, &TS{})
}

func (s *TS) SetupTest() {
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		s.T().Fatal("TempDir", err)
	}
	db, err := New(dir)
	s.NotNil(db, "Should have a db")
	s.Nil(err, "Should have no error")
	s.db = db
}

func (s *TS) TeardownTest() {
	os.RemoveAll(s.db.root.path)
	s.db = nil
}

func (s *TS) TestNew() {
	s.NotNil(s.db, "Should have a db")
}

func (s *TS) TestBasic() {
	db := s.db
	key := "foo"
	val, err := db.Get(key)
	s.Nil(val, "Should have no value")
	s.EqualError(err, os.ErrNotExist.Error(), "Should have ErrNotExist error")

	s.Nil(db.Set(key, []byte("hello")), "Should have no error on set")
	val, err = db.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal([]byte("hello"), val, "Should have the value")

	s.Nil(db.Delete(key), "Should have no error from Delete")
	val, err = db.Get(key)
	s.Nil(val, "Should again have no value")
	s.EqualError(err, os.ErrNotExist.Error(), "Should have ErrNotExist error")
}

func (s *TS) TestFiles() {
	db := s.db
	key := "foo"
	file := filepath.Join(db.root.path, key)
	s.fileNotExists(file)

	// Set should create a file.
	val := []byte("hello")
	s.Nil(db.Set(key, val), "Should have no error on set")
	s.FileExists(file, "File %q should now exist")
	s.fileContains(file, []byte("hello"))

	s.Nil(db.Delete(key), "Should have no error from Delete")
	s.fileNotExists(file)
}

func (s *TS) TestTable() {
	db := s.db
	dirName := "realm"
	subPath := filepath.Join(db.root.path, dirName)
	s.fileNotExists(subPath)

	tab, err := s.db.Table(dirName)
	s.Nil(err, "Should have no error from Table")
	s.DirExists(subPath, "Directory %q should now exist", dirName)

	key := "xoxoxoxoxoxo"
	file := filepath.Join(subPath, key)
	s.fileNotExists(file)

	// Set should create a file.
	val := []byte("hello")
	s.Nil(tab.Set(key, val), "Should have no error on set")
	s.FileExists(file, "File %q should now exist")
	s.fileContains(file, val)

	// Get should fetch the file.
	got, err := tab.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal(val, got, "Should have the value")

	// Delete should delete the file.
	s.Nil(tab.Delete(key), "Should have no error from Delete")
	s.fileNotExists(file)
	val, err = tab.Get(key)
	s.Nil(val, "Should again have no value")
	s.EqualError(err, os.ErrNotExist.Error(), "Should have ErrNotExist error")
}

func (s *TS) TestTables() {
	tables := []string{
		"yack",
		"this and that",
		filepath.Join("a", "b", "c"),
	}

	// Fill out a number of subdirectories.
	for _, subDir := range tables {
		subPath := filepath.Join(s.db.root.path, subDir)
		s.fileNotExists(subPath)
		tab, err := s.db.Table(subDir)
		s.Nil(err, "Should have no error creating Table %v", subDir)
		s.DirExists(subPath, "Directory %q should now exist", subDir)

		mapped, ok := s.db.tables.Load(subDir)
		s.True(ok, "Should have loaded Table %v", subDir)
		s.Equal(tab, mapped, "Should have retained %q", subDir)

		val := []byte(subDir)
		for _, key := range []string{"strongrrl", "theory", "lily"} {
			keyPath := filepath.Join(tab.path, key)
			keyTable := filepath.Join(subDir, key)
			s.fileNotExists(keyPath)
			s.Nil(tab.Set(key, val), "Should set val in %q", keyTable)
			s.FileExists(keyPath, "File %q should now exist", keyTable)
			s.fileContains(keyPath, val)

			got, err := tab.Get(key)
			s.Nil(err, "Should have no error fetching from %q", keyTable)
			s.Equal(val, got, "Should have the value from %q", keyTable)
		}
	}

	// Make sure they haven't overwritten each other and can be deleted.
	for _, subDir := range tables {
		tab, err := s.db.Table(subDir)
		s.Nil(err, "Should have no error creating Table %v", subDir)
		val := []byte(subDir)
		for _, key := range []string{"strongrrl", "theory", "lily"} {
			keyTable := filepath.Join(subDir, key)
			got, err := tab.Get(key)
			s.Nil(err, "Should have no error fetching %q again", keyTable)
			s.Equal(val, got, "Should again have the value from %q", keyTable)

			// Delete should delete the file.
			keyPath := filepath.Join(tab.path, key)
			s.Nil(tab.Delete(key), "Should have no error deleting %q", keyTable)
			s.fileNotExists(keyPath)
			got, err = tab.Get(key)
			s.Nil(got, "Should now have no value from %q", keyTable)
			s.EqualError(
				err, os.ErrNotExist.Error(),
				"Should have ErrNotExist error from %q", keyTable,
			)
		}
	}
}

func (s *TS) TestLock() {
	key := "whatever"
	value := []byte("🤘🎉💩")
	path := filepath.Join(s.db.root.path, key)
	s.Nil(s.db.Set(key, value), "Set %v", key)

	// Take an exclusive lock on the file.
	fh, err := os.Open(path)
	if err != nil {
		s.T().Fatal("open", err)
	}
	lock, err := lockFile(fh, true)
	if err != nil {
		s.T().Fatal("lockFile", err)
	}

	val, err := s.db.Get(key)
	s.Nil(val, "Should have no value from locked file")
	cx, cancel := context.WithTimeout(context.Background(), 0)
	cancel()
	timeoutErr := cx.Err().Error()
	s.EqualError(err, timeoutErr, "Should have timeout error from Get")
	s.EqualError(s.db.Set(key, nil), timeoutErr, "Should have timeout error from Set")
	s.EqualError(s.db.Delete(key), timeoutErr, "Should have timeout error from Delete")

	// Now take a shared lock.
	lock.Unlock()
	fh, err = os.Open(path)
	if err != nil {
		s.T().Fatal("open", err)
	}
	lock, err = lockFile(fh, false)
	if err != nil {
		s.T().Fatal("lockFile", err)
	}
	val, err = s.db.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal(string(value), string(val), "Should have value from sharelocked file")
	s.EqualError(s.db.Set(key, nil), timeoutErr, "Should have timeout error from Set")
	s.EqualError(s.db.Delete(key), timeoutErr, "Should have timeout error from Delete")
}

func (s *TS) TestKeyPathErrors() {
	badKey := filepath.Join("foo", "bar")
	val, err := s.db.Get(badKey)
	s.Nil(val, "Should have no value from Get for bad key")
	s.Equal(
		err, os.ErrInvalid,
		"Should have os.ErrInvalid from Get for bad key",
	)
	s.Equal(
		s.db.Set(badKey, nil), os.ErrInvalid,
		"Should have os.ErrInvalid from Set for bad key",
	)
	s.Equal(
		s.db.Delete(badKey), os.ErrInvalid,
		"Should have os.ErrInvalid from Delete for bad key",
	)
}

func (s *TS) TestDirKeyErrors() {
	// A directory should not work as a key.
	dirName := "aDirectory"
	subPath := filepath.Join(s.db.root.path, dirName)
	_, err := s.db.Table(dirName)
	s.Nil(err, "Should have no error from Table")
	s.DirExists(subPath, "Directory %q should now exist", dirName)

	val, err := s.db.Get(dirName)
	s.Nil(val, "Should have no value from Get for directory")
	s.NotNil(err, "Should have an error from Get for directory")
	s.DirExists(subPath, "Directory %q should still exist", dirName)
	s.NotNil(s.db.Set(dirName, nil), "Should have an error from Set for directory")
	s.DirExists(subPath, "Directory %q should still exist", dirName)
	s.NotNil(s.db.Delete(dirName), "Should have an error from Delete for directory")
	s.DirExists(subPath, "Directory %q should still exist", dirName)
}

func (s *TS) TestPathErrors() {
	db, err := New("README.md")
	s.Nil(db, "Should have no db for non-directory")
	s.EqualError(
		err, "mkdir README.md: not a directory",
		"Should have error for non-directory",
	)

	s.db.Set("foo", []byte{})
	tab, err := s.db.Table("foo")
	s.Nil(tab, "Should have no tab for non-directory")
	s.EqualError(
		err,
		fmt.Sprintf("mkdir %v: not a directory", filepath.Join(s.db.root.path, "foo")),
		"Should have error for non-directory",
	)
}

func (s *TS) fileContains(path string, data []byte) bool {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return s.Fail(
			fmt.Sprintf("error when reading file(%q): %s", path, err),
			"File %q should contain %q", path, string(data),
		)
	}
	return s.Equal(
		string(data), string(content),
		"File %q should contain %q", path, string(data),
	)
}

func (s *TS) fileNotExists(path string) bool {
	_, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s.True(true, "File %q should not exist", path)
		}
		return s.Fail(
			fmt.Sprintf("error when running os.Lstat(%q): %s", path, err),
			"File %q should not exist", path,
		)
	}
	return s.Fail(
		fmt.Sprintf("found file %q", path),
		"File %q should not exist", path,
	)
}
