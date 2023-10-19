package pebbledb

import (
	"github.com/cockroachdb/pebble"
)

type iterator struct {
	source  *pebble.Iterator
	start   []byte
	version uint64
	valid   bool
}

func NewPebbleDBIterator(src *pebble.Iterator, start []byte, version uint64) *iterator {
	// move the underlying PebbleDB iterator to the first key
	valid := src.First()

	itr := &iterator{
		source:  src,
		start:   start,
		version: version,
		valid:   valid,
	}

	// The cursor might now be pointing at a key/value pair that is tombstoned.
	// If so, we must move the cursor.
	if itr.valid {
		itr.valid = itr.Next()
	}

	return itr
}

func (itr *iterator) Next() bool {
	// First move the iterator to the next prefix, which may not correspond to the
	// desired version for that key, e.g. if the key was written at a later version,
	// so we seek back to the latest desired version, s.t. the version is <= itr.version.
	return itr.source.Next()
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
