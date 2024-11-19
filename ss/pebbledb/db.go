package pebbledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	errorutils "github.com/sei-protocol/sei-db/common/errors"
	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/common/utils"
	"github.com/sei-protocol/sei-db/config"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/ss/types"
	"github.com/sei-protocol/sei-db/stream/changelog"
	"golang.org/x/exp/slices"
)

const (
	VersionSize = 8

	PrefixStore                  = "s/k:"
	LenPrefixStore               = 4
	StorePrefixTpl               = "s/k:%s/"   // s/k:<storeKey>
	latestVersionKey             = "s/_latest" // NB: latestVersionKey key must be lexically smaller than StorePrefixTpl
	earliestVersionKey           = "s/_earliest"
	latestMigratedKeyMetadata    = "s/_latestMigratedKey"
	latestMigratedModuleMetadata = "s/_latestMigratedModule"
	tombstoneVal                 = "TOMBSTONE"

	// TODO: Make configurable
	ImportCommitBatchSize = 10000
	PruneCommitBatchSize  = 50
)

var (
	_ types.StateStore = (*Database)(nil)

	defaultWriteOpts = pebble.NoSync
)

type Database struct {
	storages     map[string]*pebble.DB
	asyncWriteWG sync.WaitGroup
	config       config.StateStoreConfig
	// Earliest version for db after pruning
	earliestVersion int64

	// Map of module to when each was last updated
	// Used in pruning to skip over stores that have not been updated recently
	storeKeyDirty sync.Map

	// Changelog used to support async write
	streamHandler *changelog.Stream

	// Pending changes to be written to the DB
	pendingChanges chan VersionedChangesets
}

type VersionedChangesets struct {
	Version    int64
	Changesets []*proto.NamedChangeSet
}

var moduleGroups = [][]string{
	{"wasm", "aclaccesscontrol", "oracle"},
	{"epoch", "mint", "acc"},
	{"bank", "feegrant", "staking"},
	{"distribution", "slashing", "gov"},
	{"params", "ibc", "upgrade"},
	{"evidence", "transfer", "tokenfactory"},
}

func New(dataDir string, config config.StateStoreConfig) (*Database, error) {
	cache := pebble.NewCache(1024 * 1024 * 32)
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:                       cache,
		Comparer:                    MVCCComparer,
		FormatMajorVersion:          pebble.FormatNewest,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    func() int { return 3 }, // TODO: Make Configurable
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
	}

	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		// TODO: Consider compression only for specific layers like bottommost
		l.Compression = pebble.ZstdCompression
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}

	opts.Levels[6].FilterPolicy = nil
	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize
	opts = opts.EnsureDefaults()

	db := &Database{
		storages:       make(map[string]*pebble.DB),
		asyncWriteWG:   sync.WaitGroup{},
		config:         config,
		pendingChanges: make(chan VersionedChangesets, config.AsyncWriteBuffer),
	}
	groupIndex := 0
	for _, group := range moduleGroups {
		shardDataDir := fmt.Sprintf("%s/shard%d", dataDir, groupIndex)
		pebbleDB, err := pebble.Open(shardDataDir, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to open PebbleDB for shard %d: %w", groupIndex, err)
		}

		for _, storeKey := range group {
			db.storages[storeKey] = pebbleDB
		}

		groupIndex++
	}

	earliestVersion, err := retrieveEarliestVersion(db)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve earliest version: %w", err)
	}
	db.earliestVersion = earliestVersion

	if config.DedicatedChangelog {
		streamHandler, _ := changelog.NewStream(
			logger.NewNopLogger(),
			utils.GetChangelogPath(dataDir),
			changelog.Config{
				DisableFsync:  true,
				ZeroCopy:      true,
				KeepRecent:    uint64(config.KeepRecent),
				PruneInterval: 300 * time.Second,
			},
		)
		db.streamHandler = streamHandler
		go db.writeAsyncInBackground()
	}
	return db, nil
}

func (db *Database) getStorage(storeKey string) (*pebble.DB, error) {
	storage, ok := db.storages[storeKey]
	if !ok {
		return nil, fmt.Errorf("no storage for storeKey: %s", storeKey)
	}
	return storage, nil
}

func (db *Database) Close() error {
	if db.streamHandler != nil {
		db.streamHandler.Close()
		db.streamHandler = nil
		close(db.pendingChanges)
	}

	db.asyncWriteWG.Wait()
	var err error
	for _, storage := range db.storages {
		closeErr := storage.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}
	db.storages = nil
	return err
}

func (db *Database) SetLatestVersion(version int64) error {
	var ts [VersionSize]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(version))

	// Store the latest version in all storages
	for _, storage := range db.storages {
		err := storage.Set([]byte(latestVersionKey), ts[:], defaultWriteOpts)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) GetLatestVersion() (int64, error) {
	latestVersion := int64(0)
	for _, storage := range db.storages {
		bz, closer, err := storage.Get([]byte(latestVersionKey))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return 0, err
		}
		if len(bz) == 0 {
			closer.Close()
			continue
		}
		version := int64(binary.LittleEndian.Uint64(bz))
		if version > latestVersion {
			latestVersion = version
		}
		closer.Close()
	}
	return latestVersion, nil
}

func (db *Database) SetEarliestVersion(version int64) error {
	if version > db.earliestVersion {
		db.earliestVersion = version

		var ts [VersionSize]byte
		binary.LittleEndian.PutUint64(ts[:], uint64(version))

		for _, storage := range db.storages {
			err := storage.Set([]byte(earliestVersionKey), ts[:], defaultWriteOpts)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *Database) GetEarliestVersion() (int64, error) {
	return db.earliestVersion, nil
}

// Retrieves earliest version from db
func retrieveEarliestVersion(db *Database) (int64, error) {
	earliestVersion := int64(math.MaxInt64)
	for _, storage := range db.storages {
		bz, closer, err := storage.Get([]byte(earliestVersionKey))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return 0, err
		}
		if len(bz) == 0 {
			closer.Close()
			continue
		}
		version := int64(binary.LittleEndian.Uint64(bz))
		if version < earliestVersion {
			earliestVersion = version
		}
		closer.Close()
	}
	if earliestVersion == int64(math.MaxInt64) {
		return 0, nil
	}
	return earliestVersion, nil
}

// SetLatestKey sets the latest key processed during migration.
func (db *Database) SetLatestMigratedKey(key []byte) error {
	for _, storage := range db.storages {
		err := storage.Set([]byte(latestMigratedKeyMetadata), key, defaultWriteOpts)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetLatestKey retrieves the latest key processed during migration.
func (db *Database) GetLatestMigratedKey() ([]byte, error) {
	// Retrieve from the first storage
	for _, storage := range db.storages {
		bz, closer, err := storage.Get([]byte(latestMigratedKeyMetadata))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, err
		}
		defer closer.Close()
		return bz, nil
	}
	return nil, nil
}

// SetLatestModule sets the latest module processed during migration.
func (db *Database) SetLatestMigratedModule(module string) error {
	for _, storage := range db.storages {
		err := storage.Set([]byte(latestMigratedModuleMetadata), []byte(module), defaultWriteOpts)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetLatestModule retrieves the latest module processed during migration.
func (db *Database) GetLatestMigratedModule() (string, error) {
	// Retrieve from the first storage
	for _, storage := range db.storages {
		bz, closer, err := storage.Get([]byte(latestMigratedModuleMetadata))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return "", err
		}
		defer closer.Close()
		return string(bz), nil
	}
	return "", nil
}

func (db *Database) Has(storeKey string, version int64, key []byte) (bool, error) {
	if version < db.earliestVersion {
		return false, nil
	}

	val, err := db.Get(storeKey, version, key)
	if err != nil {
		return false, err
	}

	return val != nil, nil
}

func (db *Database) Get(storeKey string, targetVersion int64, key []byte) ([]byte, error) {
	if targetVersion < db.earliestVersion {
		return nil, nil
	}

	storage, err := db.getStorage(storeKey)
	if err != nil {
		return nil, err
	}

	prefixedVal, err := getMVCCSlice(storage, storeKey, key, targetVersion)
	if err != nil {
		if errors.Is(err, errorutils.ErrRecordNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to perform PebbleDB read: %w", err)
	}

	valBz, tombBz, ok := SplitMVCCKey(prefixedVal)
	if !ok {
		return nil, fmt.Errorf("invalid PebbleDB MVCC value: %s", prefixedVal)
	}

	if len(tombBz) == 0 {
		return valBz, nil
	}

	tombstone, err := decodeUint64Ascending(tombBz)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value tombstone: %w", err)
	}

	if targetVersion < tombstone {
		return valBz, nil
	}

	return nil, nil
}

func (db *Database) ApplyChangeset(version int64, cs *proto.NamedChangeSet) error {
	if version == 0 {
		version = 1
	}

	storage, err := db.getStorage(cs.Name)
	if err != nil {
		return err
	}

	b, err := NewBatch(storage, version)
	if err != nil {
		return err
	}

	for _, kvPair := range cs.Changeset.Pairs {
		if kvPair.Value == nil {
			if err := b.Delete(cs.Name, kvPair.Key); err != nil {
				return err
			}
		} else {
			if err := b.Set(cs.Name, kvPair.Key, kvPair.Value); err != nil {
				return err
			}
		}
	}

	db.storeKeyDirty.Store(cs.Name, version)

	return b.Write()
}

func (db *Database) ApplyChangesetAsync(version int64, changesets []*proto.NamedChangeSet) error {
	// Write to WAL first
	if db.streamHandler != nil {
		entry := proto.ChangelogEntry{
			Version: version,
		}
		entry.Changesets = changesets
		entry.Upgrades = nil
		err := db.streamHandler.WriteNextEntry(entry)
		if err != nil {
			return err
		}
	}
	// Then write to pending changes
	db.pendingChanges <- VersionedChangesets{
		Version:    version,
		Changesets: changesets,
	}
	return nil
}

func (db *Database) writeAsyncInBackground() {
	db.asyncWriteWG.Add(1)
	defer db.asyncWriteWG.Done()
	for nextChange := range db.pendingChanges {
		if db.streamHandler != nil {
			version := nextChange.Version
			for _, cs := range nextChange.Changesets {
				err := db.ApplyChangeset(version, cs)
				if err != nil {
					panic(err)
				}
			}
			err := db.SetLatestVersion(version)
			if err != nil {
				panic(err)
			}
		}
	}

}

// Prune attempts to prune all versions up to and including the current version
// Get the range of keys, manually iterate over them and delete them
// We add a heuristic to skip over a module's keys during pruning if it hasn't been updated
// since the last time pruning occurred.
// NOTE: There is a rare case when a module's keys are skipped during pruning even though
// it has been updated. This occurs when that module's keys are updated in between pruning runs, the node after is restarted.
// This is not a large issue given the next time that module is updated, it will be properly pruned thereafter.
func (db *Database) Prune(version int64) error {
	earliestVersion := version + 1 // we increment by 1 to include the provided version

	for _, storage := range db.storages {
		itr, err := storage.NewIter(nil)
		if err != nil {
			return err
		}
		defer itr.Close()

		batch := storage.NewBatch()
		defer batch.Close()

		var (
			counter                                 int
			prevKey, prevKeyEncoded, prevValEncoded []byte
			prevVersionDecoded                      int64
		)

		for itr.First(); itr.Valid(); {
			currKeyEncoded := slices.Clone(itr.Key())

			// Ignore metadata entry for version during pruning
			if bytes.Equal(currKeyEncoded, []byte(latestVersionKey)) || bytes.Equal(currKeyEncoded, []byte(earliestVersionKey)) {
				itr.Next()
				continue
			}

			// Store current key and version
			currKey, currVersion, currOK := SplitMVCCKey(currKeyEncoded)
			if !currOK {
				return fmt.Errorf("invalid MVCC key")
			}

			currVersionDecoded, err := decodeUint64Ascending(currVersion)
			if err != nil {
				return err
			}

			// Seek to next key if we are at a version which is higher than prune height
			if currVersionDecoded > version && (db.config.KeepLastVersion || prevVersionDecoded > version) {
				itr.NextPrefix()
				continue
			}

			// Delete logic as before
			if prevVersionDecoded <= version && (bytes.Equal(prevKey, currKey) || valTombstoned(prevValEncoded) || !db.config.KeepLastVersion) {
				err = batch.Delete(prevKeyEncoded, nil)
				if err != nil {
					return err
				}

				counter++
				if counter >= PruneCommitBatchSize {
					err = batch.Commit(defaultWriteOpts)
					if err != nil {
						return err
					}

					counter = 0
					batch.Reset()
				}
			}

			// Update prevKey and prevVersion for next iteration
			prevKey = currKey
			prevVersionDecoded = currVersionDecoded
			prevKeyEncoded = currKeyEncoded
			prevValEncoded = slices.Clone(itr.Value())

			itr.Next()
		}

		// Commit any leftover delete ops in batch
		if counter > 0 {
			err = batch.Commit(defaultWriteOpts)
			if err != nil {
				return err
			}
		}

		// Update earliest version in storage
		var ts [VersionSize]byte
		binary.LittleEndian.PutUint64(ts[:], uint64(earliestVersion))
		err = storage.Set([]byte(earliestVersionKey), ts[:], defaultWriteOpts)
		if err != nil {
			return err
		}
	}

	db.earliestVersion = earliestVersion
	return nil
}

func (db *Database) Iterator(storeKey string, version int64, start, end []byte) (types.DBIterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errorutils.ErrKeyEmpty
	}

	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, errorutils.ErrStartAfterEnd
	}

	storage, err := db.getStorage(storeKey)
	if err != nil {
		return nil, err
	}

	lowerBound := MVCCEncode(prependStoreKey(storeKey, start), 0)

	var upperBound []byte
	if end != nil {
		upperBound = MVCCEncode(prependStoreKey(storeKey, end), 0)
	}

	itr, err := storage.NewIter(&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		return nil, fmt.Errorf("failed to create PebbleDB iterator: %w", err)
	}

	return newPebbleDBIterator(itr, storePrefix(storeKey), start, end, version, db.earliestVersion, false), nil
}

func (db *Database) ReverseIterator(storeKey string, version int64, start, end []byte) (types.DBIterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errorutils.ErrKeyEmpty
	}

	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, errorutils.ErrStartAfterEnd
	}

	storage, err := db.getStorage(storeKey)
	if err != nil {
		return nil, err
	}

	lowerBound := MVCCEncode(prependStoreKey(storeKey, start), 0)

	var upperBound []byte
	if end != nil {
		upperBound = MVCCEncode(prependStoreKey(storeKey, end), 0)
	}

	itr, err := storage.NewIter(&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		return nil, fmt.Errorf("failed to create PebbleDB iterator: %w", err)
	}

	return newPebbleDBIterator(itr, storePrefix(storeKey), start, end, version, db.earliestVersion, true), nil
}

// Import loads the initial version of the state in parallel with numWorkers goroutines
// TODO: Potentially add retries instead of panics
func (db *Database) Import(version int64, ch <-chan types.SnapshotNode) error {
	var wg sync.WaitGroup
	var errCh = make(chan error, db.config.ImportNumWorkers)

	worker := func() {
		defer wg.Done()
		batches := make(map[string]*Batch)

		for entry := range ch {
			storage, err := db.getStorage(entry.StoreKey)
			if err != nil {
				errCh <- err
				return
			}

			b, ok := batches[entry.StoreKey]
			if !ok {
				b, err = NewBatch(storage, version)
				if err != nil {
					errCh <- err
					return
				}
				batches[entry.StoreKey] = b
			}

			err = b.Set(entry.StoreKey, entry.Key, entry.Value)
			if err != nil {
				errCh <- err
				return
			}

			if b.Size() >= ImportCommitBatchSize {
				err = b.Write()
				if err != nil {
					errCh <- err
					return
				}
				delete(batches, entry.StoreKey)
			}
		}

		// Write remaining batches
		for _, b := range batches {
			if b.Size() > 0 {
				err := b.Write()
				if err != nil {
					errCh <- err
					return
				}
			}
		}
	}

	wg.Add(db.config.ImportNumWorkers)
	for i := 0; i < db.config.ImportNumWorkers; i++ {
		go worker()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) RawImport(ch <-chan types.RawSnapshotNode) error {
	var wg sync.WaitGroup
	var errCh = make(chan error, db.config.ImportNumWorkers)

	worker := func() {
		defer wg.Done()
		rawBatch := NewRawBatch()
		var counter int
		var latestKey []byte
		var latestModule string

		for entry := range ch {
			storage, err := db.getStorage(entry.StoreKey)
			if err != nil {
				errCh <- err
				return
			}

			err = rawBatch.Set(storage, entry.StoreKey, entry.Key, entry.Value, entry.Version)
			if err != nil {
				errCh <- err
				return
			}

			latestKey = entry.Key // Track the latest key
			latestModule = entry.StoreKey
			counter++

			if counter%ImportCommitBatchSize == 0 {
				startTime := time.Now()

				// Commit the batch and record the latest key as metadata
				if err := rawBatch.Write(); err != nil {
					errCh <- err
					return
				}

				// Persist the latest key in the metadata
				if err := db.SetLatestMigratedKey(latestKey); err != nil {
					errCh <- err
					return
				}

				if err := db.SetLatestMigratedModule(latestModule); err != nil {
					errCh <- err
					return
				}

				if counter%1000000 == 0 {
					fmt.Printf("Time taken to write batch counter %d: %v\n", counter, time.Since(startTime))
					metrics.IncrCounterWithLabels([]string{"sei", "migration", "nodes_imported"}, float32(1000000), []metrics.Label{
						{Name: "module", Value: latestModule},
					})
				}

				rawBatch = NewRawBatch()
			}
		}

		// Final batch write
		if rawBatch.Size() > 0 {
			if err := rawBatch.Write(); err != nil {
				errCh <- err
				return
			}

			// Persist the final latest key
			if err := db.SetLatestMigratedKey(latestKey); err != nil {
				errCh <- err
				return
			}

			if err := db.SetLatestMigratedModule(latestModule); err != nil {
				errCh <- err
				return
			}
		}
	}

	wg.Add(db.config.ImportNumWorkers)
	for i := 0; i < db.config.ImportNumWorkers; i++ {
		go worker()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// RawIterate iterates over all keys and values for a store
func (db *Database) RawIterate(storeKey string, fn func(key []byte, value []byte, version int64) bool) (bool, error) {
	storage, err := db.getStorage(storeKey)
	if err != nil {
		return false, err
	}

	lowerBound := MVCCEncode(prependStoreKey(storeKey, nil), 0)

	itr, err := storage.NewIter(&pebble.IterOptions{LowerBound: lowerBound})
	if err != nil {
		return false, fmt.Errorf("failed to create PebbleDB iterator: %w", err)
	}
	defer itr.Close()

	for itr.First(); itr.Valid(); itr.Next() {
		currKeyEncoded := itr.Key()

		// Ignore metadata entries
		if bytes.Equal(currKeyEncoded, []byte(latestVersionKey)) || bytes.Equal(currKeyEncoded, []byte(earliestVersionKey)) {
			continue
		}

		// Decode the current key and version
		currKey, currVersion, currOK := SplitMVCCKey(currKeyEncoded)
		if !currOK {
			return false, fmt.Errorf("invalid MVCC key")
		}

		// Only iterate through the specified module
		if storeKey != "" && !bytes.HasPrefix(currKey, storePrefix(storeKey)) {
			break
		}

		currVersionDecoded, err := decodeUint64Ascending(currVersion)
		if err != nil {
			return false, err
		}

		// Decode the value
		currValEncoded := itr.Value()
		if valTombstoned(currValEncoded) {
			continue
		}
		valBz, _, ok := SplitMVCCKey(currValEncoded)
		if !ok {
			return false, fmt.Errorf("invalid PebbleDB MVCC value: %s", currKey)
		}

		// Call the callback function
		if fn(currKey, valBz, currVersionDecoded) {
			return true, nil
		}
	}

	return false, nil
}

func storePrefix(storeKey string) []byte {
	return []byte(fmt.Sprintf(StorePrefixTpl, storeKey))
}

func prependStoreKey(storeKey string, key []byte) []byte {
	if storeKey == "" {
		return key
	}
	return append(storePrefix(storeKey), key...)
}

// Parses store from key with format "s/k:{store}/..."
func parseStoreKey(key []byte) (string, error) {
	// Convert byte slice to string only once
	keyStr := string(key)

	if !strings.HasPrefix(keyStr, PrefixStore) {
		return "", fmt.Errorf("not a valid store key")
	}

	// Find the first occurrence of "/" after the prefix
	slashIndex := strings.Index(keyStr[LenPrefixStore:], "/")
	if slashIndex == -1 {
		return "", fmt.Errorf("not a valid store key")
	}

	// Return the substring between the prefix and the first "/"
	return keyStr[LenPrefixStore : LenPrefixStore+slashIndex], nil
}

func getMVCCSlice(db *pebble.DB, storeKey string, key []byte, version int64) ([]byte, error) {
	// end domain is exclusive, so we need to increment the version by 1
	if version < math.MaxInt64 {
		version++
	}

	itr, err := db.NewIter(&pebble.IterOptions{
		LowerBound: MVCCEncode(prependStoreKey(storeKey, key), 0),
		UpperBound: MVCCEncode(prependStoreKey(storeKey, key), version),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create PebbleDB iterator: %w", err)
	}
	defer func() {
		err = errorutils.Join(err, itr.Close())
	}()

	if !itr.Last() {
		return nil, errorutils.ErrRecordNotFound
	}

	_, vBz, ok := SplitMVCCKey(itr.Key())
	if !ok {
		return nil, fmt.Errorf("invalid PebbleDB MVCC key: %s", itr.Key())
	}

	keyVersion, err := decodeUint64Ascending(vBz)
	if err != nil {
		return nil, fmt.Errorf("failed to decode key version: %w", err)
	}
	if keyVersion > version {
		return nil, fmt.Errorf("key version too large: %d", keyVersion)
	}

	return slices.Clone(itr.Value()), nil
}

func valTombstoned(value []byte) bool {
	if value == nil {
		return false
	}
	_, tombBz, ok := SplitMVCCKey(value)
	if !ok {
		// XXX: This should not happen as that would indicate we have a malformed
		// MVCC value.
		panic(fmt.Sprintf("invalid PebbleDB MVCC value: %s", value))
	}

	// If the tombstone suffix is empty, we consider this a zero value and thus it
	// is not tombstoned.
	if len(tombBz) == 0 {
		return false
	}

	return true
}
