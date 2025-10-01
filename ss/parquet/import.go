package parquet

import (
	"fmt"
	"time"

	"github.com/sei-protocol/sei-db/ss/types"
)

// Import imports initial state at a specific version
func (db *Database) Import(version int64, ch <-chan types.SnapshotNode) error {
	batch := make([]ParquetRecord, 0, writeBatchSize)
	timestamp := time.Now().Unix()
	
	for node := range ch {
		owner := db.getOwnerFromKey(node.Key)
		
		record := ParquetRecord{
			StoreKey:  node.StoreKey,
			Key:       node.Key,
			Value:     node.Value,
			Version:   version,
			Tombstone: false,
			Owner:     owner,
			Timestamp: timestamp,
		}
		
		batch = append(batch, record)
		
		// Write batch when full
		if len(batch) >= writeBatchSize {
			if err := db.writeBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	
	// Write remaining records
	if len(batch) > 0 {
		if err := db.writeBatch(batch); err != nil {
			return err
		}
	}
	
	// Update version
	if version > db.latestVersion {
		db.latestVersion = version
	}
	
	return db.flushWriteBuffer()
}

// RawImport imports raw snapshot nodes in any order
func (db *Database) RawImport(ch <-chan types.RawSnapshotNode) error {
	batch := make([]ParquetRecord, 0, writeBatchSize)
	timestamp := time.Now().Unix()
	
	for node := range ch {
		owner := db.getOwnerFromKey(node.Key)
		
		record := ParquetRecord{
			StoreKey:  node.StoreKey,
			Key:       node.Key,
			Value:     node.Value,
			Version:   node.Version,
			Tombstone: false,
			Owner:     owner,
			Timestamp: timestamp,
		}
		
		batch = append(batch, record)
		
		// Write batch when full
		if len(batch) >= writeBatchSize {
			if err := db.writeBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		
		// Update version tracking
		if node.Version > db.latestVersion {
			db.latestVersion = node.Version
		}
		if db.earliestVersion == 0 || node.Version < db.earliestVersion {
			db.earliestVersion = node.Version
		}
	}
	
	// Write remaining records
	if len(batch) > 0 {
		if err := db.writeBatch(batch); err != nil {
			return err
		}
	}
	
	return db.flushWriteBuffer()
}

// writeBatch writes a batch of records, grouping by partition
func (db *Database) writeBatch(records []ParquetRecord) error {
	// Group by partition
	partitions := make(map[string][]ParquetRecord)
	for _, record := range records {
		partitionKey := fmt.Sprintf("%d:%s", record.Version, record.Owner)
		partitions[partitionKey] = append(partitions[partitionKey], record)
	}
	
	// Write each partition
	for _, partitionRecords := range partitions {
		if len(partitionRecords) == 0 {
			continue
		}
		
		version := partitionRecords[0].Version
		owner := partitionRecords[0].Owner
		
		if err := db.writePartition(version, owner, partitionRecords); err != nil {
			return err
		}
	}
	
	return nil
}

// RawIterate iterates over all key-value pairs for a store
func (db *Database) RawIterate(storeKey string, fn func([]byte, []byte, int64) bool) (bool, error) {
	// First, iterate through write buffer
	db.bufferMu.Lock()
	bufferCopy := make([]ParquetRecord, len(db.writeBuffer))
	copy(bufferCopy, db.writeBuffer)
	db.bufferMu.Unlock()
	
	for _, record := range bufferCopy {
		if record.StoreKey == storeKey && !record.Tombstone {
			if stopped := fn(record.Key, record.Value, record.Version); stopped {
				return true, nil
			}
		}
	}
	
	// Then, iterate through Parquet files
	return db.iterateParquetFiles(storeKey, fn)
}

// iterateParquetFiles iterates through all Parquet files
func (db *Database) iterateParquetFiles(storeKey string, fn func([]byte, []byte, int64) bool) (bool, error) {
	// This is a simplified implementation
	// In production, you'd want to optimize this with parallel reading
	iter, err := db.Iterator(storeKey, db.latestVersion, nil, nil)
	if err != nil {
		return false, err
	}
	defer iter.Close()
	
	for iter.Valid() {
		// For raw iteration, we need to get the version information
		// This would require extending the iterator to expose version info
		if stopped := fn(iter.Key(), iter.Value(), db.latestVersion); stopped {
			return true, nil
		}
		iter.Next()
	}
	
	return false, iter.Error()
}

// Prune removes all versions up to and including the target version
func (db *Database) Prune(version int64) error {
	// TODO: Implement pruning logic
	// This would involve:
	// 1. Identifying partitions with versions <= target version
	// 2. Rewriting partitions to remove old versions
	// 3. Updating metadata
	
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if version >= db.earliestVersion {
		db.earliestVersion = version + 1
	}
	
	return nil
}

// WriteBlockRangeHash writes a hash for a block range
func (db *Database) WriteBlockRangeHash(storeKey string, beginBlockRange, endBlockRange int64, hash []byte) error {
	// Store in metadata
	key := fmt.Sprintf("hash:%s:%d-%d", storeKey, beginBlockRange, endBlockRange)
	
	db.metadataMu.Lock()
	defer db.metadataMu.Unlock()
	
	db.metadataStore[key] = hash
	return nil
}

// DeleteKeysAtVersion deletes all keys for a module at a specific version
func (db *Database) DeleteKeysAtVersion(module string, version int64) error {
	// Create tombstone records for all keys in the module at this version
	timestamp := time.Now().Unix()
	
	// This is a simplified implementation
	// In production, you'd need to efficiently find all keys for a module
	iter, err := db.Iterator(module, version, nil, nil)
	if err != nil {
		return err
	}
	defer iter.Close()
	
	batch := make([]ParquetRecord, 0)
	for iter.Valid() {
		owner := db.getOwnerFromKey(iter.Key())
		
		record := ParquetRecord{
			StoreKey:  module,
			Key:       iter.Key(),
			Value:     nil,
			Version:   version,
			Tombstone: true,
			Owner:     owner,
			Timestamp: timestamp,
		}
		
		batch = append(batch, record)
		iter.Next()
	}
	
	if len(batch) > 0 {
		return db.writeBatch(batch)
	}
	
	return nil
}

// Metadata getter methods
func (db *Database) GetLatestMigratedKey() ([]byte, error) {
	db.metadataMu.RLock()
	defer db.metadataMu.RUnlock()
	
	if val, ok := db.metadataStore[latestMigratedKeyMetadata]; ok {
		return val, nil
	}
	return nil, nil
}

func (db *Database) SetLatestMigratedKey(key []byte) error {
	db.metadataMu.Lock()
	defer db.metadataMu.Unlock()
	
	db.metadataStore[latestMigratedKeyMetadata] = key
	return nil
}

func (db *Database) GetLatestMigratedModule() (string, error) {
	db.metadataMu.RLock()
	defer db.metadataMu.RUnlock()
	
	if val, ok := db.metadataStore[latestMigratedModuleMetadata]; ok {
		return string(val), nil
	}
	return "", nil
}

func (db *Database) SetLatestMigratedModule(module string) error {
	db.metadataMu.Lock()
	defer db.metadataMu.Unlock()
	
	db.metadataStore[latestMigratedModuleMetadata] = []byte(module)
	return nil
}