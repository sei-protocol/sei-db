package pebbledb

import (
	"bytes"
	"fmt"

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

	valid := true

	// move the underlying PebbleDB iterator to the first key
	itr := &iterator{
		source:  src,
		prefix:  prefix,
		start:   mvccStart,
		end:     mvccEnd,
		version: version,
		valid:   valid,
		reverse: reverse,
	}

	if valid {
		currKey, currKeyVersion, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed MVCC key.
			panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
		}
		curKeyVersionDecoded, err := decodeUint64Ascending(currKeyVersion)
		if err != nil {
			itr.valid = false
			return itr
		}
		fmt.Printf("[Debug] Intialize iterator with currKey %X, version %d\n", currKey, curKeyVersionDecoded)

		// We need to check whether initial key iterator visits has a version <= requested version
		// If larger version, call next to find another key which does
		if curKeyVersionDecoded > itr.version {
			itr.Next()
		} else {
			// If version is less, seek to the largest version of that key <= requested iterator version
			// It is guaranteed this won't move the iterator to a key that is invalid since
			// curKeyVersionDecoded <= requested iterator version, so there exists at least one version of currKey SeekLT may move to
			itr.valid = itr.source.SeekLT(MVCCEncode(currKey, itr.version+1))
		}
	}

	// Make sure we skip to the next key if the current one is tombstone
	if valTombstoned(itr.source.Value()) {
		if reverse {
			itr.nextReverse()
		} else {
			itr.nextForward()
		}
	}

	return itr
}

// Domain returns the domain of the iterator. The caller must not modify the
// return values.
func (itr *iterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *iterator) Key() []byte {
	itr.assertIsValid()

	key, _, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC key.
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}

	keyCopy := slices.Clone(key)
	return keyCopy[len(itr.prefix):]
}

func (itr *iterator) Value() []byte {
	itr.assertIsValid()

	val, _, ok := SplitMVCCKey(itr.source.Value())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC value.
		panic(fmt.Sprintf("invalid PebbleDB MVCC value: %s", itr.source.Key()))
	}

	return slices.Clone(val)
}

func (itr *iterator) nextForward() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	currKey, currKeyVersionBz, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC key.
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %s", itr.source.Key()))
	}
	currKeyVersion, _ := decodeUint64Ascending(currKeyVersionBz) // Assuming decode won't fail here based on prior checks
	fmt.Printf("nextForward: currKey=%s, currKeyVersion=%d\n", string(currKey), currKeyVersion)

	next := itr.source.NextPrefix()

	// First move the iterator to the next prefix, which may not correspond to the
	// desired version for that key, e.g. if the key was written at a later version,
	// so we seek back to the latest desired version, s.t. the version is <= itr.version.
	if next {
		nextKey, nextKeyVersionBz, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			itr.valid = false
			return
		}
		nextKeyVersion, _ := decodeUint64Ascending(nextKeyVersionBz) // Assuming decode won't fail here
		fmt.Printf("nextForward: nextPrefix moved to nextKey=%s, nextKeyVersion=%d\n", string(nextKey), nextKeyVersion)

		if !bytes.HasPrefix(nextKey, itr.prefix) {
			// the next key must have itr.prefix as the prefix
			fmt.Printf("nextForward: nextKey %s does not have prefix %s\n", string(nextKey), string(itr.prefix))
			itr.valid = false
			return
		}

		// Move the iterator to the closest version to the desired version, so we
		// append the current iterator key to the prefix and seek to that key.
		seekTarget := MVCCEncode(nextKey, itr.version+1)
		fmt.Printf("nextForward: seeking LT %s (key=%s, version=%d)\n", string(seekTarget), string(nextKey), itr.version+1)
		itr.valid = itr.source.SeekLT(seekTarget)

		if !itr.valid {
			fmt.Printf("nextForward: SeekLT returned invalid\n")
			return
		}

		tmpKey, tmpKeyVersion, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			itr.valid = false
			return
		}
		tmpKeyVersionDecoded, err := decodeUint64Ascending(tmpKeyVersion)
		if err != nil {
			itr.valid = false
			return
		}
		fmt.Printf("nextForward: SeekLT landed on tmpKey=%s, tmpKeyVersion=%d\n", string(tmpKey), tmpKeyVersionDecoded)

		// There exists cases where the SeekLT() call moved us back to the same key
		// we started at, so we must move to next key, i.e. two keys forward.
		if bytes.Equal(tmpKey, currKey) {
			fmt.Printf("nextForward: SeekLT moved back to original key %s, calling NextPrefix and nextForward recursively\n", string(currKey))
			if itr.source.NextPrefix() {
				itr.nextForward()

				// Need to re-check key and version after recursive call returns
				if !itr.valid {
					return // Recursive call invalidated the iterator
				}
				_, tmpKeyVersion, ok = SplitMVCCKey(itr.source.Key())
				if !ok {
					// XXX: This should not happen as that would indicate we have a malformed
					// MVCC key.
					itr.valid = false
					return
				}

			} else {
				fmt.Printf("nextForward: NextPrefix after SeekLT returned false\n")
				itr.valid = false
				return
			}
		}

		// We need to verify that every Next call either moves the iterator to a key whose version
		// is less than or equal to requested iterator version, or exhausts the iterator
		// Re-decode version after potential recursive call
		tmpKeyVersionDecoded, err = decodeUint64Ascending(tmpKeyVersion)
		if err != nil {
			itr.valid = false
			return
		}

		// If iterator is at a entry whose version is higher than requested version, call nextForward again
		if tmpKeyVersionDecoded > itr.version {
			fmt.Printf("nextForward: tmpKeyVersion %d > target version %d, calling nextForward recursively\n", tmpKeyVersionDecoded, itr.version)
			itr.nextForward()
		}

		// The cursor might now be pointing at a key/value pair that is tombstoned.
		// If so, we must move the cursor.
		if itr.valid && itr.cursorTombstoned() {
			fmt.Printf("nextForward: cursor is tombstoned, calling nextForward recursively\n")
			itr.nextForward()
		}

		return
	}
	fmt.Printf("nextForward: NextPrefix returned false\n")

	itr.valid = false
}

func (itr *iterator) nextReverse() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	currKey, currKeyVersionBz, ok := SplitMVCCKey(itr.source.Key())
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC key.
		panic(fmt.Sprintf("invalid PebbleDB MVCC key: %X", itr.source.Key()))
	}
	currKeyVersion, _ := decodeUint64Ascending(currKeyVersionBz) // Assuming decode won't fail here
	fmt.Printf("nextReverse: currKey=%X, currKeyVersion=%d\n", currKey, currKeyVersion)

	seekTarget := MVCCEncode(currKey, 0)
	fmt.Printf("nextReverse: seeking LT %s (key=%X, version=0)\n", string(seekTarget), currKey)
	next := itr.source.SeekLT(seekTarget)

	// First move the iterator to the next prefix, which may not correspond to the
	// desired version for that key, e.g. if the key was written at a later version,
	// so we seek back to the latest desired version, s.t. the version is <= itr.version.
	if next {
		nextKey, nextKeyVersionBz, ok := SplitMVCCKey(itr.source.Key())
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			itr.valid = false
			return
		}
		nextKeyVersion, _ := decodeUint64Ascending(nextKeyVersionBz) // Assuming decode won't fail here
		fmt.Printf("nextReverse: SeekLT(currKey, 0) moved to nextKey=%X, nextKeyVersion=%d\n", nextKey, nextKeyVersion)

		if !bytes.HasPrefix(nextKey, itr.prefix) {
			// the next key must have itr.prefix as the prefix
			fmt.Printf("nextReverse: nextKey %X does not have prefix %s\n", nextKey, string(itr.prefix))
			itr.valid = false
			return
		}

		// Move the iterator to the closest version to the desired version, so we
		// append the current iterator key to the prefix and seek to that key.
		seekTargetClosest := MVCCEncode(nextKey, itr.version+1)
		fmt.Printf("nextReverse: seeking LT %s (key=%X, version=%d)\n", string(seekTargetClosest), nextKey, itr.version+1)
		itr.valid = itr.source.SeekLT(seekTargetClosest)

		if !itr.valid {
			fmt.Printf("nextReverse: SeekLT(nextKey, version+1) returned invalid\n")
			return
		}

		tmpKey, tmpKeyVersion, ok := SplitMVCCKey(itr.source.Key()) // Renamed from _ to tmpKey for logging
		if !ok {
			// XXX: This should not happen as that would indicate we have a malformed
			// MVCC key.
			itr.valid = false
			return
		}

		// We need to verify that every Next call either moves the iterator to a key whose version
		// is less than or equal to requested iterator version, or exhausts the iterator
		tmpKeyVersionDecoded, err := decodeUint64Ascending(tmpKeyVersion)
		if err != nil {
			itr.valid = false
			return
		}
		fmt.Printf("nextReverse: SeekLT(nextKey, version+1) landed on tmpKey=%X, tmpKeyVersion=%d\n", tmpKey, tmpKeyVersionDecoded)

		// If iterator is at a entry whose version is higher than requested version, call nextReverse again
		if tmpKeyVersionDecoded > itr.version {
			fmt.Printf("nextReverse: tmpKeyVersion %d > target version %d, calling nextReverse recursively\n", tmpKeyVersionDecoded, itr.version)
			itr.nextReverse()
		}

		// The cursor might now be pointing at a key/value pair that is tombstoned.
		// If so, we must move the cursor.
		if itr.valid && itr.cursorTombstoned() {
			fmt.Printf("nextReverse: cursor is tombstoned, calling nextReverse recursively\n")
			itr.nextReverse()
		}

		return
	}
	fmt.Printf("nextReverse: SeekLT(currKey, 0) returned false\n")

	itr.valid = false
}

func (itr *iterator) Next() {
	if itr.reverse {
		itr.nextReverse()
	} else {
		itr.nextForward()
	}
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

	// if key is at the end or past it, consider it invalid
	if end := itr.end; end != nil {
		if bytes.Compare(end, itr.Key()) <= 0 {
			itr.valid = false
			return itr.valid
		}
	}

	return true
}

func (itr *iterator) Error() error {
	return itr.source.Error()
}

func (itr *iterator) Close() error {
	_ = itr.source.Close()
	itr.source = nil
	itr.valid = false
	return nil
}

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
