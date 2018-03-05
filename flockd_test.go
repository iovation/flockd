package flockd

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type TS struct {
	db  *DB
	dir string
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
	db, err := New(dir, time.Millisecond)
	s.NotNil(db, "Should have a db")
	s.Nil(err, "Should have no error")
	s.db = db
	s.dir = dir
}

func (s *TS) TeardownTest() {
	os.RemoveAll(s.db.root.path)
	s.db = nil
}

func (s *TS) TestNew() {
	s.NotNil(s.db, "Should have a db")
	s.Equal(s.dir, s.db.root.path, "Path should be set")
	s.Equal(time.Millisecond, s.db.root.timeout, "Timeout should be set")
	s.NotNil(s.db.tables, "Should have tables map")
}

func (s *TS) TestBasic() {
	db := s.db
	key := "foo"
	val, err := db.Get(key)
	s.Nil(val, "Should have no value")
	s.EqualError(err, os.ErrNotExist.Error(), "Should have ErrNotExist error")
	s.Nil(db.Delete(key), "Should get no error deleting nonexistent key")
	s.EqualError(
		db.Update(key, []byte("hi")), os.ErrNotExist.Error(),
		"Should get ErrNotExist error updating nonexistent key",
	)

	s.Nil(db.Create(key, []byte("hello")), "Should have no error on create")
	val, err = db.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal([]byte("hello"), val, "Should have the created value")

	s.Nil(db.Set(key, []byte("goodbye")), "Should have no error on set")
	val, err = db.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal([]byte("goodbye"), val, "Should have the set value")

	s.Nil(db.Update(key, []byte("terminate")), "Should have no error on update")
	val, err = db.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal([]byte("terminate"), val, "Should have the updated value")

	s.Nil(db.Delete(key), "Should have no error from Delete")
	val, err = db.Get(key)
	s.Nil(val, "Should again have no value")
	s.EqualError(err, os.ErrNotExist.Error(), "Should have ErrNotExist error")
}

func (s *TS) TestBadTimeout() {
	for _, timeout := range []time.Duration{0, -1, -100000} {
		db, err := New("", timeout)
		s.Nil(db, "Should have no DB for timeout %v", timeout)
		s.EqualError(
			err, "Invalid lock timeout",
			"Should have error for timeout %v", timeout,
		)
	}
}

func (s *TS) TestFiles() {
	db := s.db
	key := "foo"
	file := filepath.Join(db.root.path, key+".kv")
	s.fileNotExists(file)

	// Set should create a file.
	val := []byte("hello")
	s.Nil(db.Set(key, val), "Should have no error on set")
	s.FileExists(file, "File %q should now exist")
	s.fileNotExists(file + tmpExt())
	s.fileContains(file, []byte("hello"))

	s.Nil(db.Delete(key), "Should have no error from Delete")
	s.fileNotExists(file)

	// Create should also create a file.
	s.Nil(db.Create(key, val), "Should have no error on create")
	s.FileExists(file, "File %q should now exist")
	s.fileNotExists(file + tmpExt())
	s.fileContains(file, []byte("hello"))

	// But it should fail if the file already exists.
	s.Equal(db.Create(key, nil), os.ErrExist, "Create should fail for existing file")
	s.Nil(db.Delete(key), "Should have no error from Delete")
	s.fileNotExists(file)

	// Update should not create a file.
	s.Equal(
		db.Update(key, nil), os.ErrNotExist,
		"Update should fail for nonexistant file",
	)
	s.fileNotExists(file)
}

func (s *TS) TestTable() {
	db := s.db
	dirName := "realm"
	subPath := filepath.Join(db.root.path, dirName+".tbl")
	s.fileNotExists(subPath)

	tbl, err := s.db.Table(dirName)
	s.Nil(err, "Should have no error from Table")
	s.DirExists(subPath, "Directory %q should now exist", dirName)
	s.Equal(db.root.timeout, tbl.timeout, "Should have timeout from DB")

	key := "xoxoxoxoxoxo"
	file := filepath.Join(subPath, key+".kv")
	s.fileNotExists(file)

	// Set should create a file.
	val := []byte("hello")
	s.Nil(tbl.Set(key, val), "Should have no error on set")
	s.FileExists(file, "File %q should now exist")
	s.fileContains(file, val)

	// Get should fetch the file.
	got, err := tbl.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal(val, got, "Should have the value")

	// Delete should delete the file.
	s.Nil(tbl.Delete(key), "Should have no error from Delete")
	s.fileNotExists(file)
	got, err = tbl.Get(key)
	s.Nil(got, "Should again have no value")
	s.EqualError(err, os.ErrNotExist.Error(), "Should have ErrNotExist error")

	// Create should also create a file.
	s.Nil(tbl.Create(key, val), "Should have no error on create")
	s.FileExists(file, "File %q should exist again")
	s.fileContains(file, val)

	// Get should fetch the file.
	got, err = tbl.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal(val, got, "Should have the value again")

	// But it should fail if the file already exists.
	s.Equal(tbl.Create(key, nil), os.ErrExist, "Create should fail for existing file")

	// Update should update the file.
	val = []byte("goodbye")
	s.Nil(tbl.Update(key, val), "Should have no error on update")
	s.FileExists(file, "File %q should still exist")
	s.fileContains(file, val)
}

func (s *TS) TestTables() {
	tables := []string{
		"yack",
		"this and that",
		filepath.Join("a", "b", "c"),
	}

	// Fill out a number of subdirectories.
	for i, subDir := range tables {
		subPath := filepath.Join(s.db.root.path, subDir+".tbl")
		s.fileNotExists(subPath)
		s.db.root.timeout = time.Millisecond * time.Duration(i+1)
		tbl, err := s.db.Table(subDir)
		s.Nil(err, "Should have no error creating Table %v", subDir)
		s.DirExists(subPath, "Directory %q should now exist", subDir)
		s.Equal(
			s.db.root.timeout, tbl.timeout,
			"Should have copied root timeout to %v", subDir,
		)

		mapped, ok := s.db.tables.Load(subDir)
		s.True(ok, "Should have loaded Table %v", subDir)
		s.Equal(tbl, mapped, "Should have retained %q", subDir)

		val := []byte(subDir)
		for _, key := range []string{"strongrrl", "theory", "lily"} {
			keyPath := filepath.Join(tbl.path, key+".kv")
			keyTable := filepath.Join(subDir, key+".kv")
			s.fileNotExists(keyPath)
			s.Nil(tbl.Set(key, val), "Should set val in %q", keyTable)
			s.FileExists(keyPath, "File %q should now exist", keyTable)
			s.fileContains(keyPath, val)

			got, err := tbl.Get(key)
			s.Nil(err, "Should have no error fetching from %q", keyTable)
			s.Equal(val, got, "Should have the value from %q", keyTable)
		}
	}

	// Make sure they haven't overwritten each other and can be deleted.
	for _, subDir := range tables {
		tbl, err := s.db.Table(subDir)
		s.Nil(err, "Should have no error creating Table %v", subDir)
		val := []byte(subDir)
		for _, key := range []string{"strongrrl", "theory", "lily"} {
			keyTable := filepath.Join(subDir, key+".kv")
			got, err := tbl.Get(key)
			s.Nil(err, "Should have no error fetching %q again", keyTable)
			s.Equal(val, got, "Should again have the value from %q", keyTable)

			// Delete should delete the file.
			keyPath := filepath.Join(tbl.path, key+".kv")
			s.Nil(tbl.Delete(key), "Should have no error deleting %q", keyTable)
			s.fileNotExists(keyPath)
			got, err = tbl.Get(key)
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
	path := filepath.Join(s.db.root.path, key+".kv")
	s.Nil(s.db.Set(key, value), "Set %v", key)

	// Take an exclusive lock on the file.
	lock, err := lockFile(path, true, time.Millisecond)
	if err != nil {
		s.T().Fatal("lockFile", err)
	}

	s.Equal(
		s.db.Create(key, nil), os.ErrExist,
		"Should have os.ErrExist error from Create",
	)
	val, err := s.db.Get(key)
	s.Nil(val, "Should have no value from locked file")
	s.Equal(err, context.DeadlineExceeded, "Should have timeout error from Get")
	s.Equal(s.db.Set(key, nil), context.DeadlineExceeded, "Should have timeout error from Set")
	s.fileNotExists(path + tmpExt())
	s.Equal(s.db.Update(key, nil), context.DeadlineExceeded, "Should have timeout error from Update")
	s.fileNotExists(path + tmpExt())
	s.Equal(s.db.Delete(key), context.DeadlineExceeded, "Should have timeout error from Delete")
	s.FileExists(path, "The file should still be present")

	// Now take a shared lock.
	lock.Unlock()
	lock, err = lockFile(path, false, time.Millisecond)
	if err != nil {
		s.T().Fatal("lockFile", err)
	}
	val, err = s.db.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal(string(value), string(val), "Should have value from sharelocked file")
	s.Equal(s.db.Set(key, nil), context.DeadlineExceeded, "Should have timeout error from Set")
	s.fileNotExists(path + tmpExt())
	s.Equal(s.db.Update(key, nil), context.DeadlineExceeded, "Should have timeout error from Update")
	s.FileExists(path, "The file should still be present")
	s.Equal(s.db.Delete(key), context.DeadlineExceeded, "Should have timeout error from Delete")
	s.FileExists(path, "The file should still be present")
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
		s.db.Create(badKey, nil), os.ErrInvalid,
		"Should have os.ErrInvalid from Create for bad key",
	)
	s.Equal(
		s.db.Set(badKey, nil), os.ErrInvalid,
		"Should have os.ErrInvalid from Set for bad key",
	)
	s.Equal(
		s.db.Update(badKey, nil), os.ErrInvalid,
		"Should have os.ErrInvalid from Update for bad key",
	)
	s.Equal(
		s.db.Delete(badKey), os.ErrInvalid,
		"Should have os.ErrInvalid from Delete for bad key",
	)
}

func (s *TS) TestDirKeyErrors() {
	// A directory should not work as a key.
	dirName := "aDirectory"
	dir := filepath.Join(s.db.root.path, dirName+".kv")
	if err := os.MkdirAll(dir, 0755); err != nil {
		s.T().Fatal("MkdirAll", err)
	}

	val, err := s.db.Get(dirName)
	s.Nil(val, "Should have no value from Get for directory")
	s.NotNil(err, "Should have an error from Get for directory")
	s.DirExists(dir, "Directory %q should still exist", dirName)
	s.NotNil(s.db.Create(dirName, nil), "Should have an error from Create for directory")
	s.NotNil(s.db.Update(dirName, nil), "Should have an error from Update for directory")
	s.NotNil(s.db.Set(dirName, nil), "Should have an error from Set for directory")
	s.DirExists(dir, "Directory %q should still exist", dirName)
	s.NotNil(s.db.Delete(dirName), "Should have an error from Delete for directory")
	s.DirExists(dir, "Directory %q should still exist", dirName)
}

func (s *TS) TestDirErrors() {
	db, err := New("README.md", time.Millisecond)
	s.Nil(db, "Should have no db for non-directory")
	s.EqualError(
		err, "mkdir README.md: not a directory",
		"Should have error for non-directory",
	)

	file := filepath.Join(s.db.root.path, "foo.tbl")
	if _, err := os.Create(file); err != nil {
		s.T().Fatal("os.Create", err)
	}
	tbl, err := s.db.Table("foo")
	s.Nil(tbl, "Should have no tbl for non-directory")
	s.EqualError(
		err,
		fmt.Sprintf("mkdir %v: not a directory", file),
		"Should have error for non-directory",
	)
}

func (s *TS) TestOpenErrors() {
	key := "deny"
	path := filepath.Join(s.db.root.path, key+".kv")
	s.Nil(s.db.Set(key, []byte("whatever")), "Set %v", key)

	// Remove all permissions.
	if err := os.Chmod(path, 0000); err != nil {
		s.T().Fatal("Chmod", err)
	}

	val, err := s.db.Get(key)
	s.Nil(val, "Should have no value from Get")
	s.True(os.IsPermission(err), "Should have permission error from Get")
	s.True(os.IsPermission(s.db.Set(key, nil)), "Should have peermission error from Set")
	s.True(os.IsPermission(s.db.Delete(key)), "Should have peermission error from Delete")
}

func (s *TS) TestKeys() {
	for chars, key := range map[string]string{
		"nothing interesting": "foo",
		"space":               "foo bar",
		"question mark":       "foo?bar",
		"bang":                "foo!bar",
		"emoji":               "🤘🎉💩",
	} {
		path := filepath.Join(s.db.root.path, key+".kv")
		// Make sure Create and Get work.
		s.Nil(
			s.db.Create(key, []byte("Create:"+key)),
			"Should get no error creating key with %v", chars,
		)
		s.FileExists(path, "Should have file with %v", chars)
		val, err := s.db.Get(key)
		s.Nil(err, "Should have no error getting key with %v", chars)
		s.Equal(string(val), "Create:"+key, "Should have value for with with %v", chars)

		// Make sure Set and Get work.
		s.Nil(
			s.db.Set(key, []byte("Set:"+key)),
			"Should get no error setting key with %v", chars,
		)
		val, err = s.db.Get(key)
		s.Nil(err, "Should have no error getting key with %v", chars)
		s.Equal(string(val), "Set:"+key, "Should have value for with with %v", chars)

		// Make sure Update and Get work.
		s.Nil(
			s.db.Update(key, []byte("Update:"+key)),
			"Should get no error updating key with %v", chars,
		)
		val, err = s.db.Get(key)
		s.Nil(err, "Should have no error getting key with %v", chars)
		s.Equal(string(val), "Update:"+key, "Should have value for with with %v", chars)

		// Make sure Delete works.
		s.Nil(
			s.db.Delete(key),
			"Should get no error deleting key with %v", chars,
		)
		s.fileNotExists(path)
	}
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

func tmpExt() string {
	return ".tmp" + strconv.Itoa(os.Getpid())
}
