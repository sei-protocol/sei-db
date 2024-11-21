package pebbledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
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

type VersionedDB struct {
	startVersion int64
	endVersion   int64 // exclusive
	db           *pebble.DB
}

type Database struct {
	dbs          []*VersionedDB
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

func New(dataDir string, config config.StateStoreConfig) (*Database, error) {
	versionShardSize := config.VersionShardSize
	database := &Database{
		dbs:            []*VersionedDB{},
		config:         config,
		pendingChanges: make(chan VersionedChangesets, config.AsyncWriteBuffer),
	}

	// Initialize the first shard
	startVersion := int64(0)
	endVersion := startVersion + versionShardSize
	versionedDB, err := openVersionedDB(dataDir, startVersion, endVersion, database.getPebbleOptions())
	if err != nil {
		return nil, err
	}

	database.dbs = append(database.dbs, versionedDB)

	earliestVersion, err := retrieveEarliestVersion(versionedDB.db)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve earliest version: %w", err)
	}
	database.earliestVersion = earliestVersion

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
		database.streamHandler = streamHandler
		go database.writeAsyncInBackground()
	}

	return database, nil
}

func (db *Database) getPebbleOptions() *pebble.Options {
	cache := pebble.NewCache(1024 * 1024 * 32)
	// We should not defer cache.Unref() here because we need the cache to persist
	// for the lifetime of the database
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
		l.Compression = pebble.ZstdCompression
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}

	opts.Levels[6].FilterPolicy = nil
	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize
	opts = opts.EnsureDefaults()

	return opts
}

func openVersionedDB(dataDir string, startVersion, endVersion int64, opts *pebble.Options) (*VersionedDB, error) {
	dbDir := filepath.Join(dataDir, fmt.Sprintf("%d_%d", startVersion, endVersion))
	db, err := pebble.Open(dbDir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open PebbleDB: %w", err)
	}

	versionedDB := &VersionedDB{
		startVersion: startVersion,
		endVersion:   endVersion,
		db:           db,
	}

	return versionedDB, nil
}

func (db *Database) Close() error {
	if db.streamHandler != nil {
		db.streamHandler.Close()
		db.streamHandler = nil
		close(db.pendingChanges)
	}
	db.asyncWriteWG.Wait()

	for _, vdb := range db.dbs {
		err := vdb.db.Close()
		if err != nil {
			return err
		}
	}
	db.dbs = nil
	return nil
}

func (db *Database) SetLatestVersion(version int64) error {
	currentDB, err := db.getOrCreateDBForVersion(version)
	if err != nil {
		return err
	}
	var ts [VersionSize]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(version))
	err = currentDB.db.Set([]byte(latestVersionKey), ts[:], defaultWriteOpts)
	fmt.Printf("SetLatestVersion: version=%d, err=%v, latestVersionKey=%s\n", version, err, latestVersionKey)
	return err
}

func (db *Database) GetLatestVersion() (int64, error) {
	if len(db.dbs) == 0 {
		return 0, nil
	}
	currentDB := db.dbs[len(db.dbs)-1]
	bz, closer, err := currentDB.db.Get([]byte(latestVersionKey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()

	if len(bz) == 0 {
		return 0, nil
	}

	return int64(binary.LittleEndian.Uint64(bz)), nil
}

func (db *Database) SetEarliestVersion(version int64) error {
	if version > db.earliestVersion {
		db.earliestVersion = version
		currentDB := db.dbs[0]
		var ts [VersionSize]byte
		binary.LittleEndian.PutUint64(ts[:], uint64(version))
		return currentDB.db.Set([]byte(earliestVersionKey), ts[:], defaultWriteOpts)
	}
	return nil
}

func (db *Database) GetEarliestVersion() (int64, error) {
	return db.earliestVersion, nil
}

// Retrieves earliest version from db
func retrieveEarliestVersion(db *pebble.DB) (int64, error) {
	bz, closer, err := db.Get([]byte(earliestVersionKey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// in case of a fresh database
			return 0, nil
		}

		return 0, err
	}

	if len(bz) == 0 {
		return 0, closer.Close()
	}

	return int64(binary.LittleEndian.Uint64(bz)), closer.Close()
}

// SetLatestMigratedKey sets the latest key processed during migration.
func (db *Database) SetLatestMigratedKey(key []byte) error {
	currentDB := db.dbs[len(db.dbs)-1]
	return currentDB.db.Set([]byte(latestMigratedKeyMetadata), key, defaultWriteOpts)
}

// GetLatestMigratedKey retrieves the latest key processed during migration.
func (db *Database) GetLatestMigratedKey() ([]byte, error) {
	currentDB := db.dbs[len(db.dbs)-1]
	bz, closer, err := currentDB.db.Get([]byte(latestMigratedKeyMetadata))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	return slices.Clone(bz), nil
}

// SetLatestMigratedModule sets the latest module processed during migration.
func (db *Database) SetLatestMigratedModule(module string) error {
	currentDB := db.dbs[len(db.dbs)-1]
	return currentDB.db.Set([]byte(latestMigratedModuleMetadata), []byte(module), defaultWriteOpts)
}

// GetLatestMigratedModule retrieves the latest module processed during migration.
func (db *Database) GetLatestMigratedModule() (string, error) {
	currentDB := db.dbs[len(db.dbs)-1]
	bz, closer, err := currentDB.db.Get([]byte(latestMigratedModuleMetadata))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	defer closer.Close()
	return string(bz), nil
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

	// Index into it
	for i := len(db.dbs) - 1; i >= 0; i-- {
		vdb := db.dbs[i]
		if targetVersion < vdb.startVersion {
			continue
		}

		if targetVersion >= vdb.endVersion {
			// Target version not in this shard
			continue
		}

		prefixedVal, err := getMVCCSlice(vdb.db, storeKey, key, targetVersion)
		if err != nil {
			if errors.Is(err, errorutils.ErrRecordNotFound) {
				continue
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

	return nil, nil
}

func (db *Database) ApplyChangeset(version int64, cs *proto.NamedChangeSet) error {
	// Check if version is 0 and change it to 1
	// We do this specifically since keys written as part of genesis state come in as version 0
	// But pebbledb treats version 0 as special, so apply the changeset at version 1 instead
	if version == 0 {
		version = 1
	}

	currentDB, err := db.getOrCreateDBForVersion(version)
	if err != nil {
		return err
	}

	b, err := NewBatch(currentDB.db, version)
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

	// Mark the store as updated
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
	if len(db.dbs) != 0 {
		return fmt.Errorf("Pruning not enabled when sharding by version")
	}

	// Only one shard when pruning enabled
	database := db.dbs[0].db
	earliestVersion := version + 1 // we increment by 1 to include the provided version

	itr, err := database.NewIter(nil)
	if err != nil {
		return err
	}
	defer itr.Close()

	batch := database.NewBatch()
	defer batch.Close()

	var (
		counter                                 int
		prevKey, prevKeyEncoded, prevValEncoded []byte
		prevVersionDecoded                      int64
		prevStore                               string
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

		storeKey, err := parseStoreKey(currKey)
		if err != nil {
			// XXX: This should never happen given we skip the metadata keys.
			return err
		}

		// For every new module visited, check to see last time it was updated
		if storeKey != prevStore {
			prevStore = storeKey
			updated, ok := db.storeKeyDirty.Load(storeKey)
			versionUpdated, typeOk := updated.(int64)
			// Skip a store's keys if version it was last updated is less than last prune height
			if !ok || (typeOk && versionUpdated < db.earliestVersion) {
				itr.SeekGE(storePrefix(storeKey + "0"))
				continue
			}
		}

		currVersionDecoded, err := decodeUint64Ascending(currVersion)
		if err != nil {
			return err
		}

		// Seek to next key if we are at a version which is higher than prune height
		// Do not seek to next key if KeepLastVersion is false and we need to delete the previous key in pruning
		if currVersionDecoded > version && (db.config.KeepLastVersion || prevVersionDecoded > version) {
			itr.NextPrefix()
			continue
		}

		// Delete a key if another entry for that key exists at a larger version than original but leq to the prune height
		// Also delete a key if it has been tombstoned and its version is leq to the prune height
		// Also delete a key if KeepLastVersion is false and version is leq to the prune height
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

	return db.SetEarliestVersion(earliestVersion)
}

func (db *Database) Iterator(storeKey string, version int64, start, end []byte) (types.DBIterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errorutils.ErrKeyEmpty
	}

	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, errorutils.ErrStartAfterEnd
	}

	vdb, err := db.getDBForVersion(version)
	if err != nil {
		return nil, err
	}

	lowerBound := MVCCEncode(prependStoreKey(storeKey, start), 0)

	var upperBound []byte
	if end != nil {
		upperBound = MVCCEncode(prependStoreKey(storeKey, end), 0)
	}

	itr, err := vdb.db.NewIter(&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
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

	vdb, err := db.getDBForVersion(version)
	if err != nil {
		return nil, err
	}

	lowerBound := MVCCEncode(prependStoreKey(storeKey, start), 0)

	var upperBound []byte
	if end != nil {
		upperBound = MVCCEncode(prependStoreKey(storeKey, end), 0)
	}

	itr, err := vdb.NewIter(&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		return nil, fmt.Errorf("failed to create PebbleDB iterator: %w", err)
	}

	return newPebbleDBIterator(itr, storePrefix(storeKey), start, end, version, db.earliestVersion, true), nil
}

// Import loads the initial version of the state in parallel with numWorkers goroutines
// TODO: Potentially add retries instead of panics
func (db *Database) Import(version int64, ch <-chan types.SnapshotNode) error {
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		currentDB, err := db.getOrCreateDBForVersion(version)
		if err != nil {
			panic(err)
		}

		batch, err := NewBatch(currentDB.db, version)
		if err != nil {
			panic(err)
		}

		var counter int
		for entry := range ch {
			err := batch.Set(entry.StoreKey, entry.Key, entry.Value)
			if err != nil {
				panic(err)
			}

			counter++
			if counter%ImportCommitBatchSize == 0 {
				if err := batch.Write(); err != nil {
					panic(err)
				}

				batch, err = NewBatch(currentDB.db, version)
				if err != nil {
					panic(err)
				}
			}
		}

		if batch.Size() > 0 {
			if err := batch.Write(); err != nil {
				panic(err)
			}
		}
	}

	wg.Add(db.config.ImportNumWorkers)
	for i := 0; i < db.config.ImportNumWorkers; i++ {
		go worker()
	}

	wg.Wait()

	return nil
}

func (db *Database) getDBForVersion(version int64) (*VersionedDB, error) {
	if len(db.dbs) == 0 {
		return nil, fmt.Errorf("no database shards available")
	}
	if version < db.dbs[0].startVersion || version >= db.dbs[len(db.dbs)-1].endVersion {
		return nil, fmt.Errorf("version %d is out of bounds", version)
	}
	index := int((version - db.dbs[0].startVersion) / db.config.VersionShardSize)
	if index < 0 || index >= len(db.dbs) {
		return nil, fmt.Errorf("no database shard found for version %d", version)
	}
	return db.dbs[index], nil
}

func (db *Database) getOrCreateDBForVersion(version int64) (*VersionedDB, error) {
	vdb, err := db.getDBForVersion(version)
	if err == nil {
		return vdb, nil
	}
	// Need to create a new shard
	startVersion := (version / db.config.VersionShardSize) * db.config.VersionShardSize
	endVersion := startVersion + db.config.VersionShardSize
	opts := db.getPebbleOptions()
	newDB, err := openVersionedDB(db.config.DBDirectory, startVersion, endVersion, opts)
	if err != nil {
		return nil, err
	}
	db.dbs = append(db.dbs, newDB)
	return newDB, nil
}

// TODO: Raw import update with separate db per version
// Can't create a batch with multiple versions
// Create wrapper around
func (db *Database) RawImport(ch <-chan types.RawSnapshotNode) error {
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		batch, err := NewRawBatch(db.storage)
		if err != nil {
			panic(err)
		}

		var counter int
		var latestKey []byte // store the latest key from the batch
		var latestModule string
		for entry := range ch {
			err := batch.Set(entry.StoreKey, entry.Key, entry.Value, entry.Version)
			if err != nil {
				panic(err)
			}

			latestKey = entry.Key // track the latest key
			latestModule = entry.StoreKey
			counter++

			if counter%ImportCommitBatchSize == 0 {
				startTime := time.Now()

				// Commit the batch and record the latest key as metadata
				if err := batch.Write(); err != nil {
					panic(err)
				}

				// Persist the latest key in the metadata
				if err := db.SetLatestMigratedKey(latestKey); err != nil {
					panic(err)
				}

				if err := db.SetLatestMigratedModule(latestModule); err != nil {
					panic(err)
				}

				if counter%1000000 == 0 {
					fmt.Printf("Time taken to write batch counter %d: %v\n", counter, time.Since(startTime))
					metrics.IncrCounterWithLabels([]string{"sei", "migration", "nodes_imported"}, float32(1000000), []metrics.Label{
						{Name: "module", Value: latestModule},
					})
				}

				batch, err = NewRawBatch(db.storage)
				if err != nil {
					panic(err)
				}
			}
		}

		// Final batch write
		if batch.Size() > 0 {
			if err := batch.Write(); err != nil {
				panic(err)
			}

			// Persist the final latest key
			if err := db.SetLatestMigratedKey(latestKey); err != nil {
				panic(err)
			}

			if err := db.SetLatestMigratedModule(latestModule); err != nil {
				panic(err)
			}
		}
	}

	wg.Add(db.config.ImportNumWorkers)
	for i := 0; i < db.config.ImportNumWorkers; i++ {
		go worker()
	}

	wg.Wait()

	return nil
}

// RawIterate iterates over all keys and values for a store
func (db *Database) RawIterate(storeKey string, fn func(key []byte, value []byte, version int64) bool) (bool, error) {
	// Iterate through all keys and values for a store
	lowerBound := MVCCEncode(prependStoreKey(storeKey, nil), 0)

	itr, err := db.storage.NewIter(&pebble.IterOptions{LowerBound: lowerBound})
	if err != nil {
		return false, fmt.Errorf("failed to create PebbleDB iterator: %w", err)
	}
	defer itr.Close()

	for itr.First(); itr.Valid(); itr.Next() {
		currKeyEncoded := itr.Key()

		// Ignore metadata entry for version
		if bytes.Equal(currKeyEncoded, []byte(latestVersionKey)) || bytes.Equal(currKeyEncoded, []byte(earliestVersionKey)) {
			continue
		}

		// Store current key and version
		currKey, currVersion, currOK := SplitMVCCKey(currKeyEncoded)
		if !currOK {
			return false, fmt.Errorf("invalid MVCC key")
		}

		// Only iterate through module
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

		// Call callback fn
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
