package sstest

import (
	"fmt"

	"golang.org/x/exp/slices"

	"github.com/stretchr/testify/suite"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/ss/types"
)

const (
	storeKey1 = "store1"
)

// StorageTestSuite defines a reusable test suite for all storage backends.
type StorageTestSuite struct {
	suite.Suite

	NewDB          func(dir string) (types.StateStore, error)
	EmptyBatchSize int
	SkipTests      []string
}

func (s *StorageTestSuite) TestDatabaseClose() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	s.Require().NoError(db.Close())

	// close should not be idempotent
	s.Require().Panics(func() { _ = db.Close() })
}

func (s *StorageTestSuite) TestDatabaseLatestVersion() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	lv, err := db.GetLatestVersion()
	s.Require().NoError(err)
	s.Require().Zero(lv)

	for i := int64(1); i <= 1001; i++ {
		err = db.SetLatestVersion(i)
		s.Require().NoError(err)

		lv, err = db.GetLatestVersion()
		s.Require().NoError(err)
		s.Require().Equal(i, lv)
	}
}

func (s *StorageTestSuite) TestDatabaseVersionedKeys() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	s.Require().NoError(FillData(db, 1, 100))

	for i := int64(1); i <= 100; i++ {
		bz, err := db.Get(storeKey1, i, []byte("key000"))
		s.Require().NoError(err)
		s.Require().Equal(fmt.Sprintf("val000-%03d", i), string(bz))
	}
}

func (s *StorageTestSuite) TestDatabaseGetVersionedKey() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	// store a key at version 1
	cs := &iavl.ChangeSet{
		Pairs: []*iavl.KVPair{{Key: []byte("key"), Value: []byte("value001")}},
	}
	ncs := &proto.NamedChangeSet{
		Name:      storeKey1,
		Changeset: *cs,
	}
	s.Require().NoError(db.ApplyChangeset(1, ncs))

	// assume chain progresses to version 10 w/o any changes to key
	bz, err := db.Get(storeKey1, 10, []byte("key"))
	s.Require().NoError(err)
	s.Require().Equal([]byte("value001"), bz)

	ok, err := db.Has(storeKey1, 10, []byte("key"))
	s.Require().NoError(err)
	s.Require().True(ok)

	// chain progresses to version 11 with an update to key
	cs = &iavl.ChangeSet{
		Pairs: []*iavl.KVPair{{Key: []byte("key"), Value: []byte("value011")}},
	}
	ncs = &proto.NamedChangeSet{
		Name:      storeKey1,
		Changeset: *cs,
	}
	s.Require().NoError(db.ApplyChangeset(11, ncs))

	bz, err = db.Get(storeKey1, 10, []byte("key"))
	s.Require().NoError(err)
	s.Require().Equal([]byte("value001"), bz)

	ok, err = db.Has(storeKey1, 10, []byte("key"))
	s.Require().NoError(err)
	s.Require().True(ok)

	for i := int64(11); i <= 14; i++ {
		bz, err = db.Get(storeKey1, i, []byte("key"))
		s.Require().NoError(err)
		s.Require().Equal([]byte("value011"), bz)

		ok, err = db.Has(storeKey1, i, []byte("key"))
		s.Require().NoError(err)
		s.Require().True(ok)
	}

	// chain progresses to version 15 with a delete to key
	cs = &iavl.ChangeSet{
		Pairs: []*iavl.KVPair{{Key: []byte("key")}},
	}
	ncs = &proto.NamedChangeSet{
		Name:      storeKey1,
		Changeset: *cs,
	}
	s.Require().NoError(db.ApplyChangeset(15, ncs))

	// all queries up to version 14 should return the latest value
	for i := int64(1); i <= 14; i++ {
		bz, err = db.Get(storeKey1, i, []byte("key"))
		s.Require().NoError(err)
		s.Require().NotNil(bz)

		ok, err = db.Has(storeKey1, i, []byte("key"))
		s.Require().NoError(err)
		s.Require().True(ok)
	}

	// all queries after version 15 should return nil
	for i := int64(15); i <= 17; i++ {
		bz, err = db.Get(storeKey1, i, []byte("key"))
		s.Require().NoError(err)
		s.Require().Nil(bz)

		ok, err = db.Has(storeKey1, i, []byte("key"))
		s.Require().NoError(err)
		s.Require().False(ok)
	}
}

func (s *StorageTestSuite) TestDatabaseApplyChangeset() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	s.Require().NoError(FillData(db, 100, 1))

	cs := &iavl.ChangeSet{}
	cs.Pairs = []*iavl.KVPair{}

	// Deletes
	for i := 0; i < 100; i++ {
		if i%10 == 0 {
			cs.Pairs = append(cs.Pairs, &iavl.KVPair{Key: []byte(fmt.Sprintf("key%03d", i))})
		}
	}

	ncs := &proto.NamedChangeSet{
		Name:      storeKey1,
		Changeset: *cs,
	}

	s.Require().NoError(db.ApplyChangeset(1, ncs))

	lv, err := db.GetLatestVersion()
	s.Require().NoError(err)
	s.Require().Equal(int64(1), lv)

	for i := 0; i < 1; i++ {
		ok, err := db.Has(storeKey1, 1, []byte(fmt.Sprintf("key%03d", i)))
		s.Require().NoError(err)

		if i%10 == 0 {
			s.Require().False(ok)
		} else {
			s.Require().True(ok)
		}
	}
}

func (s *StorageTestSuite) TestDatabaseIteratorEmptyDomain() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	iter, err := db.Iterator(storeKey1, 1, []byte{}, []byte{})
	s.Require().Error(err)
	s.Require().Nil(iter)
}

func (s *StorageTestSuite) TestDatabaseIteratorClose() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	iter, err := db.Iterator(storeKey1, 1, []byte("key000"), nil)
	s.Require().NoError(err)
	iter.Close()

	s.Require().False(iter.Valid())
	s.Require().Panics(func() { iter.Close() })
}

func (s *StorageTestSuite) TestDatabaseIteratorDomain() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	testCases := map[string]struct {
		start, end []byte
	}{
		"start without end domain": {
			start: []byte("key010"),
		},
		"start and end domain": {
			start: []byte("key010"),
			end:   []byte("key020"),
		},
	}

	for name, tc := range testCases {
		s.Run(name, func() {
			iter, err := db.Iterator(storeKey1, 1, tc.start, tc.end)
			s.Require().NoError(err)

			defer iter.Close()

			start, end := iter.Domain()
			s.Require().Equal(tc.start, start)
			s.Require().Equal(tc.end, end)
		})
	}
}

func (s *StorageTestSuite) TestDatabaseIterator() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	s.Require().NoError(FillData(db, 100, 1))

	// iterator without an end key over multiple versions
	for v := int64(1); v < 5; v++ {
		itr, err := db.Iterator(storeKey1, v, []byte("key000"), nil)
		s.Require().NoError(err)

		defer itr.Close()

		var i, count int
		for ; itr.Valid(); itr.Next() {
			s.Require().Equal([]byte(fmt.Sprintf("key%03d", i)), itr.Key(), string(itr.Key()))
			s.Require().Equal([]byte(fmt.Sprintf("val%03d-%03d", i, 1)), itr.Value())

			i++
			count++
		}
		s.Require().Equal(100, count)
		s.Require().NoError(itr.Error())

		// seek past domain, which should make the iterator invalid and produce an error
		itr.Next()
		s.Require().False(itr.Valid())
	}

	// iterator with with a start and end domain over multiple versions
	for v := int64(1); v < 5; v++ {
		itr2, err := db.Iterator(storeKey1, v, []byte("key010"), []byte("key019"))
		s.Require().NoError(err)

		defer itr2.Close()

		i, count := 10, 0
		for ; itr2.Valid(); itr2.Next() {
			s.Require().Equal([]byte(fmt.Sprintf("key%03d", i)), itr2.Key())
			s.Require().Equal([]byte(fmt.Sprintf("val%03d-%03d", i, 1)), itr2.Value())

			i++
			count++
		}
		s.Require().Equal(9, count)
		s.Require().NoError(itr2.Error())

		// seek past domain, which should make the iterator invalid and produce an error
		itr2.Next()
		s.Require().False(itr2.Valid())
	}

	// start must be <= end
	iter3, err := db.Iterator(storeKey1, 1, []byte("key020"), []byte("key019"))
	s.Require().Error(err)
	s.Require().Nil(iter3)
}

func (s *StorageTestSuite) TestDatabaseIteratorRangedDeletes() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	cs := &iavl.ChangeSet{
		Pairs: []*iavl.KVPair{{Key: []byte("key001"), Value: []byte("value001")}, {Key: []byte("key002"), Value: []byte("value001")}},
	}
	ncs := &proto.NamedChangeSet{
		Name:      storeKey1,
		Changeset: *cs,
	}
	s.Require().NoError(db.ApplyChangeset(1, ncs))

	cs = &iavl.ChangeSet{
		Pairs: []*iavl.KVPair{{Key: []byte("key002"), Value: []byte("value002")}},
	}
	ncs.Changeset = *cs
	s.Require().NoError(db.ApplyChangeset(5, ncs))

	cs = &iavl.ChangeSet{
		Pairs: []*iavl.KVPair{{Key: []byte("key002")}},
	}
	ncs.Changeset = *cs
	s.Require().NoError(db.ApplyChangeset(10, ncs))

	itr, err := db.Iterator(storeKey1, 11, []byte("key001"), nil)
	s.Require().NoError(err)

	defer itr.Close()

	// there should only be one valid key in the iterator -- key001
	var count int
	for ; itr.Valid(); itr.Next() {
		s.Require().Equal([]byte("key001"), itr.Key())
		count++
	}
	s.Require().Equal(1, count)
	s.Require().NoError(itr.Error())
}

func (s *StorageTestSuite) TestDatabaseIteratorMultiVersion() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	s.Require().NoError(FillData(db, 10, 50))

	// for versions 50-100, only update even keys
	for v := int64(51); v <= 100; v++ {
		cs := &iavl.ChangeSet{}
		cs.Pairs = []*iavl.KVPair{}
		for i := 0; i < 10; i++ {
			if i%2 == 0 {
				key := fmt.Sprintf("key%03d", i)
				val := fmt.Sprintf("val%03d-%03d", i, v)

				cs.Pairs = append(cs.Pairs, &iavl.KVPair{Key: []byte(key), Value: []byte(val)})
			}
		}

		ncs := &proto.NamedChangeSet{
			Name:      storeKey1,
			Changeset: *cs,
		}
		s.Require().NoError(db.ApplyChangeset(v, ncs))
	}

	itr, err := db.Iterator(storeKey1, 69, []byte("key000"), nil)
	s.Require().NoError(err)

	defer itr.Close()

	// All keys should be present; All odd keys should have a value that reflects
	// version 49, and all even keys should have a value that reflects the desired
	// version, 69.
	var i, count int
	for ; itr.Valid(); itr.Next() {
		s.Require().Equal([]byte(fmt.Sprintf("key%03d", i)), itr.Key(), string(itr.Key()))

		if i%2 == 0 {
			s.Require().Equal([]byte(fmt.Sprintf("val%03d-%03d", i, 69)), itr.Value())
		} else {
			s.Require().Equal([]byte(fmt.Sprintf("val%03d-%03d", i, 50)), itr.Value())
		}

		i = (i + 1) % 10
		count++
	}
	s.Require().Equal(10, count)
	s.Require().NoError(itr.Error())
}

func (s *StorageTestSuite) TestDatabaseIteratorNoDomain() {
	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	s.Require().NoError(FillData(db, 10, 50))

	// create an iterator over the entire domain
	itr, err := db.Iterator(storeKey1, 50, nil, nil)
	s.Require().NoError(err)

	defer itr.Close()

	var i, count int
	for ; itr.Valid(); itr.Next() {
		s.Require().Equal([]byte(fmt.Sprintf("key%03d", i)), itr.Key(), string(itr.Key()))
		s.Require().Equal([]byte(fmt.Sprintf("val%03d-%03d", i, 50)), itr.Value())

		i++
		count++
	}
	s.Require().Equal(10, count)
	s.Require().NoError(itr.Error())
}

func (s *StorageTestSuite) TestDatabasePrune() {
	if slices.Contains(s.SkipTests, s.T().Name()) {
		s.T().SkipNow()
	}

	db, err := s.NewDB(s.T().TempDir())
	s.Require().NoError(err)
	defer db.Close()

	s.Require().NoError(FillData(db, 10, 50))

	// prune the first 25 versions
	s.Require().NoError(db.Prune(25))

	latestVersion, err := db.GetLatestVersion()
	s.Require().NoError(err)
	s.Require().Equal(int64(50), latestVersion)

	// Ensure all keys are no longer present up to and including version 25 and
	// all keys are present after version 25.
	for v := int64(1); v <= 50; v++ {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d-%03d", i, v)

			bz, err := db.Get(storeKey1, v, []byte(key))
			s.Require().NoError(err)
			if v <= 25 {
				s.Require().Nil(bz)
			} else {
				s.Require().Equal([]byte(val), bz)
			}
		}
	}

	itr, err := db.Iterator(storeKey1, 25, []byte("key000"), nil)
	s.Require().NoError(err)
	s.Require().False(itr.Valid())

	// prune the latest version which should prune the entire dataset
	s.Require().NoError(db.Prune(50))

	for v := int64(1); v <= 50; v++ {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%03d", i)

			bz, err := db.Get(storeKey1, v, []byte(key))
			s.Require().NoError(err)
			s.Require().Nil(bz)
		}
	}
}