package dirdb

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type TS struct {
	suite.Suite
}

func TestDirDB(t *testing.T) {
	suite.Run(t, &TS{})
}

func (s *TS) TestNew() {
	db, err := New(".")
	s.NotNil(db, "Should have a db")
	s.Nil(err, "Should have no error")
}
