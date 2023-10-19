package pebbledb

import (
	"fmt"

	"github.com/cockroachdb/pebble"
)

type iterator struct {
	source  *pebble.Iterator
	start   []byte
	version uint64
	valid   bool
}

func NewPebbleDBIterator(src *pebble.Iterator, mvccStart []byte, version uint64) *iterator {
	// move the underlying PebbleDB iterator to the first key
	valid := src.First()
	if valid {
		// The first key may not represent the desired target version, so move the
		// cursor to the correct location.
		firstKey, _, ok := SplitMVCCKey(src.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			valid = false
		} else {
			valid = src.SeekLT(MVCCEncode(firstKey, version+1))
		}
	}

	itr := &iterator{
		source:  src,
		start:   mvccStart,
		version: version,
		valid:   valid,
	}

	// The cursor might now be pointing at a key/value pair that is tombstoned.
	// If so, we must move the cursor.
	if itr.valid && itr.cursorTombstoned() {
		itr.valid = itr.Next()
	}

	return itr
}

func (itr *iterator) Next() bool {
	// First move the iterator to the next prefix, which may not correspond to the
	// desired version for that key, e.g. if the key was written at a later version,
	// so we seek back to the latest desired version, s.t. the version is <= itr.version.
	if itr.source.NextPrefix() {
		nextKey, _, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			itr.valid = false
			return itr.valid
		}

		// Move the iterator to the closest version to the desired version, so we
		// seek to the current iterator key.
		itr.valid = itr.source.SeekLT(MVCCEncode(nextKey, itr.version+1))

		// The cursor might now be pointing at a key/value pair that is tombstoned.
		// If so, we must move the cursor.
		if itr.valid && itr.cursorTombstoned() {
			itr.valid = itr.Next()
		}

		return itr.valid
	}

	itr.valid = false
	return itr.valid
}

func (itr *iterator) Valid() bool {
	// once invalid, forever invalid
	if !itr.valid || !itr.source.Valid() {
		itr.valid = false
		return itr.valid
	}

	// if source has error, consider it invalid
	if err := itr.source.Error(); err != nil {
		itr.valid = false
		return itr.valid
	}

	return true
}

func (itr *iterator) Close() {
	_ = itr.source.Close()
	itr.source = nil
	itr.valid = false
}

// cursorTombstoned checks if the current cursor is pointing at a key/value pair
// that is tombstoned. If the cursor is tombstoned, <true> is returned, otherwise
// <false> is returned. In the case where the iterator is valid but the key/value
// pair is tombstoned, the caller should call Next(). Note, this method assumes
// the caller assures the iterator is valid first!
func (itr *iterator) cursorTombstoned() bool {
	_, tombBz, ok := SplitMVCCKey(itr.source.Value())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC value.
		panic(fmt.Sprintf("invalid PebbleDB MVCC value: %s", itr.source.Key()))
	}

	// If the tombstone suffix is empty, we consider this a zero value and thus it
	// is not tombstoned.
	if len(tombBz) == 0 {
		return false
	}

	// If the tombstone suffix is non-empty and greater than the target version,
	// the value is not tombstoned.
	tombstone, err := decodeUint64Ascending(tombBz)
	if err != nil {
		panic(fmt.Errorf("failed to decode value tombstone: %w", err))
	}
	if tombstone > itr.version {
		return false
	}

	return true
}

func (itr *iterator) DebugRawIterate() {
	valid := itr.source.Valid()
	if valid {
		// The first key may not represent the desired target version, so move the
		// cursor to the correct location.
		firstKey, _, _ := SplitMVCCKey(itr.source.Key())
		valid = itr.source.SeekLT(MVCCEncode(firstKey, itr.version+1))
	}

	for valid {
		key, vBz, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
		}

		version, err := decodeUint64Ascending(vBz)
		if err != nil {
			panic(fmt.Errorf("failed to decode key version: %w", err))
		}

		val, tombBz, ok := SplitMVCCKey(itr.source.Value())
		if !ok {
			panic(fmt.Sprintf("invalid PebbleDB MVCC value: %s", itr.source.Value()))
		}

		var tombstone uint64
		if len(tombBz) > 0 {
			tombstone, err = decodeUint64Ascending(vBz)
			if err != nil {
				panic(fmt.Errorf("failed to decode value tombstone: %w", err))
			}
		}

		fmt.Printf("KEY: %s, VALUE: %s, VERSION: %d, TOMBSTONE: %d\n", key, val, version, tombstone)

		if itr.source.NextPrefix() {
			nextKey, _, ok := SplitMVCCKey(itr.source.Key())
			if !ok {
				panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
			}

			valid = itr.source.SeekLT(MVCCEncode(nextKey, itr.version+1))
		} else {
			valid = false
		}
	}
}
