package dirdb

import (
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
	os.RemoveAll(s.db.root.dir)
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
	file := filepath.Join(db.root.dir, key)
	s.fileNotExists(file)

	// Set should create a file.
	val := []byte("hello")
	s.Nil(db.Set(key, val), "Should have no error on set")
	s.FileExists(file, "File %q should now exist")
	s.fileContains(file, []byte("hello"))

	s.Nil(db.Delete(key), "Should have no error from Delete")
	s.fileNotExists(file)
}

func (s *TS) TestSub() {
	db := s.db
	dirName := "realm"
	subPath := filepath.Join(db.root.dir, dirName)
	s.fileNotExists(subPath)

	sub, err := s.db.Sub(dirName)
	s.Nil(err, "Should have no error from Sub")
	s.DirExists(subPath, "Directory %q should now exist", dirName)

	key := "xoxoxoxoxoxo"
	file := filepath.Join(subPath, key)
	s.fileNotExists(file)

	// Set should create a file.
	val := []byte("hello")
	s.Nil(sub.Set(key, val), "Should have no error on set")
	s.FileExists(file, "File %q should now exist")
	s.fileContains(file, val)

	// Get should fetch the file.
	got, err := sub.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal(val, got, "Should have the value")

	// Delete should delete the file.
	s.Nil(sub.Delete(key), "Should have no error from Delete")
	s.fileNotExists(file)
	val, err = sub.Get(key)
	s.Nil(val, "Should again have no value")
	s.EqualError(err, os.ErrNotExist.Error(), "Should have ErrNotExist error")
}

func (s *TS) TestSubs() {
	dirs := []string{
		"yack",
		"this and that",
		filepath.Join("a", "b", "c"),
	}

	// Fill out a number of subdirectories.
	for _, subDir := range dirs {
		subPath := filepath.Join(s.db.root.dir, subDir)
		s.fileNotExists(subPath)
		sub, err := s.db.Sub(subDir)
		s.Nil(err, "Should have no error creating Sub %v", subDir)
		s.DirExists(subPath, "Directory %q should now exist", subDir)
		val := []byte(subDir)
		for _, key := range []string{"strongrrl", "theory", "lily"} {
			keyPath := filepath.Join(sub.dir, key)
			keySub := filepath.Join(subDir, key)
			s.fileNotExists(keyPath)
			s.Nil(sub.Set(key, val), "Should set val in %q", keySub)
			s.FileExists(keyPath, "File %q should now exist", keySub)
			s.fileContains(keyPath, val)

			got, err := sub.Get(key)
			s.Nil(err, "Should have no error fetching from %q", keySub)
			s.Equal(val, got, "Should have the value from %q", keySub)
		}
	}

	// Make sure they haven't overwritten each other and can be deleted.
	for _, subDir := range dirs {
		sub, err := s.db.Sub(subDir)
		s.Nil(err, "Should have no error creating Sub %v", subDir)
		val := []byte(subDir)
		for _, key := range []string{"strongrrl", "theory", "lily"} {
			keySub := filepath.Join(subDir, key)
			got, err := sub.Get(key)
			s.Nil(err, "Should have no error fetching %q again", keySub)
			s.Equal(val, got, "Should again have the value from %q", keySub)

			// Delete should delete the file.
			keyPath := filepath.Join(sub.dir, key)
			s.Nil(sub.Delete(key), "Should have no error deleting %q", keySub)
			s.fileNotExists(keyPath)
			got, err = sub.Get(key)
			s.Nil(got, "Should now have no value from %q", keySub)
			s.EqualError(
				err, os.ErrNotExist.Error(),
				"Should have ErrNotExist error from %q", keySub,
			)
		}
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
