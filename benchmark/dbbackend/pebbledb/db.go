package pebbledb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// NOTE: Adapted from cosmos-sdk store-v2
// Please reference https://github.com/cosmos/cosmos-sdk/tree/main/store/storage

const (
	VersionSize = 8

	latestVersionKey = "s/_latest" // NB: latestVersionKey key must be lexically smaller than StorePrefixTpl
	tombstoneVal     = "TOMBSTONE"
)

var (
	defaultWriteOpts = pebble.Sync
)

type PebbleDBBackend struct{}

type Database struct {
	storage *pebble.DB
}

type Batch struct {
	storage *pebble.DB
	batch   *pebble.Batch
	version uint64
}

func New(dataDir string) (*Database, error) {
	opts := &pebble.Options{}
	opts = opts.EnsureDefaults()

	db, err := pebble.Open(dataDir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open PebbleDB: %w", err)
	}

	return &Database{
		storage: db,
	}, nil
}

func (db *Database) Close() error {
	err := db.storage.Close()
	db.storage = nil
	return err
}

func NewBatch(storage *pebble.DB, version uint64) (*Batch, error) {
	var versionBz [VersionSize]byte
	binary.LittleEndian.PutUint64(versionBz[:], version)

	batch := storage.NewBatch()

	if err := batch.Set([]byte(latestVersionKey), versionBz[:], nil); err != nil {
		return nil, fmt.Errorf("failed to write PebbleDB batch: %w", err)
	}

	return &Batch{
		storage: storage,
		batch:   batch,
		version: version,
	}, nil
}

func (b *Batch) Size() int {
	return b.batch.Len()
}

func (b *Batch) Reset() {
	b.batch.Reset()
}

func (b *Batch) set(tombstone uint64, key, value []byte) error {
	if err := b.batch.Set(key, value, nil); err != nil {
		return fmt.Errorf("failed to write PebbleDB batch: %w", err)
	}

	return nil
}

func (b *Batch) Set(key, value []byte) error {
	return b.set(0, key, value)
}

func (b *Batch) Delete(key []byte) error {
	return b.set(b.version, key, []byte(tombstoneVal))
}

func (b *Batch) Write() (err error) {
	defer func() {
		err = errors.Join(err, b.batch.Close())
	}()

	return b.batch.Commit(defaultWriteOpts)
}

func (db *Database) Get(targetVersion uint64, key []byte) ([]byte, error) {
	value, closer, err := db.storage.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	// the value is considered deleted
	return value, nil
}
