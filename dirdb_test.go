package dirdb

import (
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
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

	err = db.Set(key, []byte("hello"))
	s.Nil(err, "Should have no error on set")

	val, err = db.Get(key)
	s.Nil(err, "Should have no error from Get")
	s.Equal([]byte("hello"), val, "Should have the value")

	err = db.Delete(key)
	s.Nil(err, "Should have no error from Delete")

	val, err = db.Get(key)
	s.Nil(val, "Should again have no value")
	s.EqualError(err, os.ErrNotExist.Error(), "Should have ErrNotExist error")
}
