package rocksdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"

	"github.com/linxGnu/grocksdb"
	"golang.org/x/exp/slices"
)

// NOTE: Adapted from cosmos-sdk store-v2
// Please reference https://github.com/cosmos/cosmos-sdk/tree/main/store/storage

const (
	CFNameStateStorage = "state_storage"
	CFNameDefault      = "default"
	TimestampSize      = 8
	latestVersionKey   = "s/latest"
)

var (
	defaultWriteOpts = grocksdb.NewDefaultWriteOptions()
	defaultReadOpts  = grocksdb.NewDefaultReadOptions()
)

type RocksDBBackend struct{}

type Database struct {
	storage  *grocksdb.DB
	cfHandle *grocksdb.ColumnFamilyHandle
}

type Batch struct {
	version  uint64
	ts       [TimestampSize]byte
	storage  *grocksdb.DB
	cfHandle *grocksdb.ColumnFamilyHandle
	batch    *grocksdb.WriteBatch
}

func NewRocksDBOpts() *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	opts.SetComparator(CreateTSComparator())
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	opts.SetTargetFileSizeMultiplier(2)
	opts.SetLevelCompactionDynamicLevelBytes(true)
	opts.EnableStatistics()

	// block based table options
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	// 1G block cache
	bbto.SetBlockSize(32 * 1024)
	bbto.SetBlockCache(grocksdb.NewLRUCache(1 << 30))

	bbto.SetFilterPolicy(grocksdb.NewRibbonHybridFilterPolicy(9.9, 1))
	bbto.SetIndexType(grocksdb.KBinarySearchWithFirstKey)
	bbto.SetOptimizeFiltersForMemory(true)
	opts.SetBlockBasedTableFactory(bbto)
	// improve sst file creation speed: compaction or sst file writer.
	opts.SetCompressionOptionsParallelThreads(4)

	return opts
}

func OpenRocksDB(outputDBPath string) (*grocksdb.DB, *grocksdb.ColumnFamilyHandle, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(
		opts,
		outputDBPath,
		[]string{
			CFNameDefault,
			CFNameStateStorage,
		},
		[]*grocksdb.Options{
			opts,
			NewRocksDBOpts(),
		},
	)

	if err != nil {
		return nil, nil, err
	}

	return db, cfHandles[1], nil
}

func New(outputDBPath string) (*Database, error) {
	storage, cfHandle, err := OpenRocksDB(outputDBPath)
	if err != nil {
		return nil, err
	}

	return &Database{
		storage:  storage,
		cfHandle: cfHandle,
	}, nil
}

func (db *Database) Close() error {
	db.storage.Close()

	db.storage = nil
	db.cfHandle = nil

	return nil
}

func NewBatch(db *Database, version uint64) Batch {
	var ts [TimestampSize]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(version))

	batch := grocksdb.NewWriteBatch()
	batch.Put([]byte(latestVersionKey), ts[:])

	return Batch{
		version:  version,
		ts:       ts,
		storage:  db.storage,
		cfHandle: db.cfHandle,
		batch:    batch,
	}
}

func (b Batch) Set(key, value []byte) error {
	b.batch.PutCFWithTS(b.cfHandle, key, b.ts[:], value)
	return nil
}

func (b Batch) Write() error {
	defer b.batch.Destroy()
	return b.storage.Write(defaultWriteOpts, b.batch)
}

func (db *Database) Get(version uint64, key []byte) ([]byte, error) {
	slice, err := db.getSlice(version, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get RocksDB slice: %w", err)
	}

	return copyAndFreeSlice(slice), nil
}

func (db *Database) getSlice(version uint64, key []byte) (*grocksdb.Slice, error) {
	return db.storage.GetCF(
		db.newTSReadOptions(version),
		db.cfHandle,
		key,
	)
}

func copyAndFreeSlice(s *grocksdb.Slice) []byte {
	defer s.Free()
	if !s.Exists() {
		return nil
	}

	return slices.Clone(s.Data())
}

func (db *Database) newTSReadOptions(version uint64) *grocksdb.ReadOptions {
	var ts [TimestampSize]byte
	binary.LittleEndian.PutUint64(ts[:], version)

	readOpts := grocksdb.NewDefaultReadOptions()
	readOpts.SetTimestamp(ts[:])

	return readOpts
}

func CreateTSComparator() *grocksdb.Comparator {
	return grocksdb.NewComparatorWithTimestamp(
		"leveldb.BytewiseComparator.u64ts",
		TimestampSize,
		compare,
		compareTS,
		compareWithoutTS,
	)
}

func compareTS(bz1 []byte, bz2 []byte) int {
	ts1 := binary.LittleEndian.Uint64(bz1)
	ts2 := binary.LittleEndian.Uint64(bz2)

	switch {
	case ts1 < ts2:
		return -1

	case ts1 > ts2:
		return 1

	default:
		return 0
	}
}

func compare(a []byte, b []byte) int {
	ret := compareWithoutTS(a, true, b, true)
	if ret != 0 {
		return ret
	}

	return -compareTS(a[len(a)-TimestampSize:], b[len(b)-TimestampSize:])
}

func compareWithoutTS(a []byte, aHasTS bool, b []byte, bHasTS bool) int {
	if aHasTS {
		a = a[:len(a)-TimestampSize]
	}
	if bHasTS {
		b = b[:len(b)-TimestampSize]
	}

	return bytes.Compare(a, b)
}
