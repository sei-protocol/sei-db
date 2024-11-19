package pebbledb

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/sei-protocol/sei-db/common/errors"
)

type Batch struct {
	storage *pebble.DB
	batch   *pebble.Batch
	version int64
}

func NewBatch(storage *pebble.DB, version int64) (*Batch, error) {
	var versionBz [VersionSize]byte
	binary.LittleEndian.PutUint64(versionBz[:], uint64(version))

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

func (b *Batch) set(storeKey string, tombstone int64, key, value []byte) error {
	prefixedKey := MVCCEncode(prependStoreKey(storeKey, key), b.version)
	prefixedVal := MVCCEncode(value, tombstone)

	if err := b.batch.Set(prefixedKey, prefixedVal, nil); err != nil {
		return fmt.Errorf("failed to write PebbleDB batch: %w", err)
	}

	return nil
}

func (b *Batch) Set(storeKey string, key, value []byte) error {
	return b.set(storeKey, 0, key, value)
}

func (b *Batch) Delete(storeKey string, key []byte) error {
	return b.set(storeKey, b.version, key, []byte(tombstoneVal))
}

func (b *Batch) Write() (err error) {
	defer func() {
		err = errors.Join(err, b.batch.Close())
	}()

	return b.batch.Commit(defaultWriteOpts)
}

// RawBatch handles writing key-value pairs where versions and storeKeys can vary.
// It maintains separate batches for each storage (pebble.DB instance).
type RawBatch struct {
	batches map[*pebble.DB]*pebble.Batch // Map from storage to batch
}

func NewRawBatch() *RawBatch {
	return &RawBatch{
		batches: make(map[*pebble.DB]*pebble.Batch),
	}
}

func (rb *RawBatch) Size() int {
	size := 0
	for _, batch := range rb.batches {
		size += batch.Len()
	}
	return size
}

func (rb *RawBatch) Reset() {
	for _, batch := range rb.batches {
		batch.Reset()
	}
}

func (rb *RawBatch) set(db *pebble.DB, storeKey string, tombstone int64, key, value []byte, version int64) error {
	batch, ok := rb.batches[db]
	if !ok {
		batch = db.NewBatch()
		rb.batches[db] = batch
	}

	prefixedKey := MVCCEncode(prependStoreKey(storeKey, key), version)
	prefixedVal := MVCCEncode(value, tombstone)

	if err := batch.Set(prefixedKey, prefixedVal, nil); err != nil {
		return fmt.Errorf("failed to write PebbleDB batch: %w", err)
	}

	return nil
}

func (rb *RawBatch) Set(db *pebble.DB, storeKey string, key, value []byte, version int64) error {
	return rb.set(db, storeKey, 0, key, value, version)
}

func (rb *RawBatch) Delete(db *pebble.DB, storeKey string, key []byte, version int64) error {
	return rb.set(db, storeKey, version, key, []byte(tombstoneVal), version)
}

func (rb *RawBatch) Write() (err error) {
	for _, batch := range rb.batches {
		if batch.Count() > 0 {
			if commitErr := batch.Commit(defaultWriteOpts); commitErr != nil {
				err = errors.Join(err, commitErr)
			}
		}
		if closeErr := batch.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}
	// Reset the batches after writing
	rb.batches = make(map[*pebble.DB]*pebble.Batch)
	return err
}
