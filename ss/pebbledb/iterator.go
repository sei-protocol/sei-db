package pebbledb

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/sei-protocol/sei-db/ss/types"
	"golang.org/x/exp/slices"
)

var _ types.DBIterator = (*iterator)(nil)

// iterator implements the Iterator interface. It wraps a PebbleDB iterator
// with added MVCC key handling logic. The iterator will iterate over the key space
// in the provided domain for a given version. If a key has been written at the
// provided version, that key/value pair will be iterated over. Otherwise, the
// latest version for that key/value pair will be iterated over s.t. it's less
// than the provided version. Note:
//
// - The start key must not be empty.
// - Currently, reverse iteration is NOT supported.
type iterator struct {
	source             *pebble.Iterator
	prefix, start, end []byte
	version            int64
	valid              bool
	reverse            bool
}

// newPebbleDBIterator creates a new iterator with the given parameters.
func newPebbleDBIterator(src *pebble.Iterator, prefix, mvccStart, mvccEnd []byte, version int64, earliestVersion int64, reverse bool) *iterator {
	// Return invalid iterator if requested iterator height is lower than earliest version after pruning
	if version < earliestVersion {
		return &iterator{
			source:  src,
			prefix:  prefix,
			start:   mvccStart,
			end:     mvccEnd,
			version: version,
			valid:   false,
			reverse: reverse,
		}
	}

	// move the underlying PebbleDB iterator to the first key
	var valid bool
	if reverse {
		valid = src.Last()
	} else {
		valid = src.First()
	}

	itr := &iterator{
		source:  src,
		prefix:  prefix,
		start:   mvccStart,
		end:     mvccEnd,
		version: version,
		valid:   valid,
		reverse: reverse,
	}

	if reverse {
		itr.initializeReverse()
	} else {
		itr.initializeForward()
	}

	return itr
}

// Domain returns the domain of the iterator. The caller must not modify the
// return values.
func (itr *iterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Key returns the current key without the prefix.
func (itr *iterator) Key() []byte {
	itr.assertIsValid()

	key, _, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}

	keyCopy := slices.Clone(key)
	return keyCopy[len(itr.prefix):]
}

// Value returns the current value.
func (itr *iterator) Value() []byte {
	itr.assertIsValid()

	val, _, ok := SplitMVCCKey(itr.source.Value())
	if !ok {
		panic(fmt.Sprintf("invalid PebbleDB MVCC value: %s", itr.source.Key()))
	}

	return slices.Clone(val)
}

// Next moves the iterator to the next key.
func (itr *iterator) Next() {
	if itr.reverse {
		itr.nextReverse()
	} else {
		itr.nextForward()
	}
}

// Valid returns whether the iterator is valid.
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

	// if key is at the end or past it, consider it invalid
	if end := itr.end; end != nil {
		if bytes.Compare(end, itr.Key()) <= 0 {
			itr.valid = false
			return itr.valid
		}
	}

	return true
}

// Error returns any error encountered during iteration.
func (itr *iterator) Error() error {
	return itr.source.Error()
}

// Close closes the iterator.
func (itr *iterator) Close() error {
	_ = itr.source.Close()
	itr.source = nil
	itr.valid = false
	return nil
}

// assertIsValid panics if the iterator is invalid.
func (itr *iterator) assertIsValid() {
	if !itr.valid {
		panic("iterator is invalid")
	}
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

// validateKey validates the current key and returns it if valid.
func (itr *iterator) validateKey() ([]byte, bool) {
	key, version, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}
	if !bytes.HasPrefix(key, itr.prefix) {
		return key, false
	}
	_, err := decodeUint64Ascending(version)
	return key, err == nil
}

// goToNextKey moves to the next key in the sequence.
func (itr *iterator) goToNextKey(currKey []byte) bool {
	if !itr.source.SeekGE(MVCCEncode(currKey, math.MaxInt64)) {
		return false
	}
	seeked, _, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}
	if bytes.Equal(currKey, seeked) {
		return itr.source.Next()
	}
	return true
}

// seekToCurrKeyAtVersionNoTombstone seeks to the current key at the target version,
// ensuring it's not tombstoned.
// If there is no valid version, it will return false for `found` and move on to the
// next key (the meaning of "next" depends on the direction of iteration). If that
// results in iterator going out of bounds, it will return false for `hasNext`.
func (itr *iterator) seekToCurrKeyAtVersionNoTombstone(reverse bool) (found bool, hasNext bool) {
	currKey, _, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}

	// Seek to the last version of the current key that's less than or equal to our target version
	if !itr.source.SeekLT(MVCCEncode(currKey, itr.version+1)) {
		if reverse {
			return false, false
		}
		return false, itr.goToNextKey(currKey)
	}

	key, valid := itr.validateKey()
	if !valid {
		return false, false
	}

	if !bytes.Equal(currKey, key) {
		if reverse {
			return false, true
		}
		// If we're not reversing, we need to first move back to `currKey`
		if !itr.source.Next() {
			// this should be impossible given the existence of `currKey`
			panic("should be impossible for Next() to return false")
		}
		// Now we're at `currKey`, since we know it doesn't have a valid version,
		// we can move on to the next key.
		return false, itr.goToNextKey(currKey)
	}

	// Check if the current version is tombstoned
	if itr.cursorTombstoned() {
		if reverse {
			return false, itr.source.SeekLT(MVCCEncode(currKey, 0))
		}
		return false, itr.goToNextKey(currKey)
	}

	return true, true
}

// initializeForward initializes the iterator for forward iteration.
func (itr *iterator) initializeForward() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	// Find a valid version of this key that's not tombstoned
	for {
		found, hasNext := itr.seekToCurrKeyAtVersionNoTombstone(false)
		if found {
			return
		}
		if !hasNext {
			itr.valid = false
			return
		}
	}
}

// nextForward moves to the next key in forward iteration.
func (itr *iterator) nextForward() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	currKey, _, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}

	// Move to the next key
	if !itr.goToNextKey(currKey) {
		itr.valid = false
		return
	}

	// Find a valid version of the next key that's not tombstoned
	for {
		found, hasNext := itr.seekToCurrKeyAtVersionNoTombstone(false)
		if found {
			return
		}
		if !hasNext {
			itr.valid = false
			return
		}
	}
}

// initializeReverse initializes the iterator for reverse iteration.
func (itr *iterator) initializeReverse() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	// Find a valid version of this key that's not tombstoned
	for {
		found, hasNext := itr.seekToCurrKeyAtVersionNoTombstone(true)
		if found {
			return
		}
		if !hasNext {
			itr.valid = false
			return
		}
	}
}

// nextReverse moves to the next key in reverse iteration.
func (itr *iterator) nextReverse() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	currKey, _, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}

	// Seek to the previous key by seeking to the current key with version 0
	if !itr.source.SeekLT(MVCCEncode(currKey, 0)) {
		itr.valid = false
		return
	}

	// Find a valid version of the previous key that's not tombstoned
	for {
		found, hasNext := itr.seekToCurrKeyAtVersionNoTombstone(true)
		if found {
			return
		}
		if !hasNext {
			itr.valid = false
			return
		}
	}
}

// DebugRawIterate is a debugging function that prints all keys and values.
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

		var tombstone int64
		if len(tombBz) > 0 {
			tombstone, err = decodeUint64Ascending(vBz)
			if err != nil {
				panic(fmt.Errorf("failed to decode value tombstone: %w", err))
			}
		}

		fmt.Printf("KEY: %s, VALUE: %s, VERSION: %d, TOMBSTONE: %d\n", key, val, version, tombstone)

		var next bool
		if itr.reverse {
			next = itr.source.SeekLT(MVCCEncode(key, 0))
		} else {
			next = itr.source.NextPrefix()
		}

		if next {
			nextKey, _, ok := SplitMVCCKey(itr.source.Key())
			if !ok {
				panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
			}

			// the next key must have itr.prefix as the prefix
			if !bytes.HasPrefix(nextKey, itr.prefix) {
				valid = false
			} else {
				valid = itr.source.SeekLT(MVCCEncode(nextKey, itr.version+1))
			}
		} else {
			valid = false
		}
	}
}
