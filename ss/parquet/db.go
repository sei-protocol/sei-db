package parquet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/config"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/ss/types"
	errorutils "github.com/sei-protocol/sei-db/common/errors"
)

const (
	// Directory structure for HDFS-style partitioning
	partitionPattern = "version=%d/owner=%s"
	
	// Parquet file naming
	dataFilePattern = "data_%d.parquet"
	
	// Batch size for writing
	writeBatchSize = 10000
	
	// Metadata keys
	latestVersionKey             = "_latest"
	earliestVersionKey           = "_earliest"
	latestMigratedKeyMetadata    = "_latestMigratedKey"
	latestMigratedModuleMetadata = "_latestMigratedModule"
)

var _ types.StateStore = (*Database)(nil)

// ParquetRecord represents a single key-value record in Parquet format
type ParquetRecord struct {
	StoreKey  string `parquet:"storeKey,dict,zstd"`
	Key       []byte `parquet:"key,zstd"`
	Value     []byte `parquet:"value,zstd"`
	Version   int64  `parquet:"version"`
	Tombstone bool   `parquet:"tombstone"`
	Owner     string `parquet:"owner,dict,zstd"`
	Timestamp int64  `parquet:"timestamp"`
}

// Database implements a Parquet-based state store with HDFS-style partitioning
type Database struct {
	mu              sync.RWMutex
	logger          logger.Logger
	dataDir         string
	config          config.StateStoreConfig
	
	// Version tracking
	latestVersion   int64
	earliestVersion int64
	
	// Write buffer for batching
	writeBuffer     []ParquetRecord
	bufferMu        sync.Mutex
	
	// Partition metadata cache
	partitionCache  map[string]*PartitionMetadata
	cacheMu         sync.RWMutex
	
	// File handles cache
	fileHandles     map[string]*parquet.File
	handlesMu       sync.RWMutex
	
	// Metadata store (using a simple key-value store for metadata)
	metadataStore   map[string][]byte
	metadataMu      sync.RWMutex
}

// PartitionMetadata holds metadata about a partition
type PartitionMetadata struct {
	Version   int64
	Owner     string
	FilePath  string
	RowCount  int64
	MinKey    []byte
	MaxKey    []byte
	LastWrite time.Time
}

// New creates a new Parquet-based database
func New(dataDir string, config config.StateStoreConfig) (*Database, error) {
	db := &Database{
		dataDir:        dataDir,
		config:         config,
		writeBuffer:    make([]ParquetRecord, 0, writeBatchSize),
		partitionCache: make(map[string]*PartitionMetadata),
		fileHandles:    make(map[string]*parquet.File),
		metadataStore:  make(map[string][]byte),
	}
	
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	
	// Load metadata
	if err := db.loadMetadata(); err != nil {
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}
	
	return db, nil
}

// getOwnerFromKey extracts the account owner from a key
// This is a simplified version - you may need to adjust based on your key format
func (db *Database) getOwnerFromKey(key []byte) string {
	// Extract owner from key - this is implementation specific
	// For now, we'll use a simple prefix extraction
	if len(key) >= 20 {
		return fmt.Sprintf("%x", key[:20]) // First 20 bytes as hex
	}
	return "default"
}

// getPartitionPath returns the partition path for a given version and owner
func (db *Database) getPartitionPath(version int64, owner string) string {
	return filepath.Join(db.dataDir, fmt.Sprintf(partitionPattern, version, owner))
}

// getDataFilePath returns the data file path within a partition
func (db *Database) getDataFilePath(version int64, owner string) string {
	partitionPath := db.getPartitionPath(version, owner)
	return filepath.Join(partitionPath, fmt.Sprintf(dataFilePattern, version))
}

// flushWriteBuffer flushes the write buffer to Parquet files
func (db *Database) flushWriteBuffer() error {
	db.bufferMu.Lock()
	defer db.bufferMu.Unlock()
	
	if len(db.writeBuffer) == 0 {
		return nil
	}
	
	// Group records by partition
	partitions := make(map[string][]ParquetRecord)
	for _, record := range db.writeBuffer {
		partitionKey := fmt.Sprintf("%d:%s", record.Version, record.Owner)
		partitions[partitionKey] = append(partitions[partitionKey], record)
	}
	
	// Write each partition
	for _, records := range partitions {
		if len(records) == 0 {
			continue
		}
		
		version := records[0].Version
		owner := records[0].Owner
		
		if err := db.writePartition(version, owner, records); err != nil {
			return err
		}
	}
	
	// Clear buffer
	db.writeBuffer = db.writeBuffer[:0]
	
	return nil
}

// writePartition writes records to a specific partition
func (db *Database) writePartition(version int64, owner string, records []ParquetRecord) error {
	// Create partition directory
	partitionPath := db.getPartitionPath(version, owner)
	if err := os.MkdirAll(partitionPath, 0755); err != nil {
		return fmt.Errorf("failed to create partition directory: %w", err)
	}
	
	// Get data file path
	dataFilePath := db.getDataFilePath(version, owner)
	
	// Create or append to Parquet file
	file, err := os.OpenFile(dataFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()
	
	// Write records
	writer := parquet.NewGenericWriter[ParquetRecord](file)
	defer writer.Close()
	
	_, err = writer.Write(records)
	if err != nil {
		return fmt.Errorf("failed to write records: %w", err)
	}
	
	// Update partition metadata
	db.updatePartitionMetadata(version, owner, records)
	
	return nil
}

// updatePartitionMetadata updates the metadata for a partition
func (db *Database) updatePartitionMetadata(version int64, owner string, records []ParquetRecord) {
	db.cacheMu.Lock()
	defer db.cacheMu.Unlock()
	
	partitionKey := fmt.Sprintf("%d:%s", version, owner)
	metadata, exists := db.partitionCache[partitionKey]
	if !exists {
		metadata = &PartitionMetadata{
			Version:  version,
			Owner:    owner,
			FilePath: db.getDataFilePath(version, owner),
		}
		db.partitionCache[partitionKey] = metadata
	}
	
	// Update metadata
	metadata.RowCount += int64(len(records))
	metadata.LastWrite = time.Now()
	
	// Update min/max keys
	for _, record := range records {
		if metadata.MinKey == nil || bytes.Compare(record.Key, metadata.MinKey) < 0 {
			metadata.MinKey = record.Key
		}
		if metadata.MaxKey == nil || bytes.Compare(record.Key, metadata.MaxKey) > 0 {
			metadata.MaxKey = record.Key
		}
	}
}

// Get retrieves a value at a specific version
func (db *Database) Get(storeKey string, version int64, key []byte) ([]byte, error) {
	owner := db.getOwnerFromKey(key)
	
	// Try to get from write buffer first
	db.bufferMu.Lock()
	for i := len(db.writeBuffer) - 1; i >= 0; i-- {
		record := db.writeBuffer[i]
		if record.StoreKey == storeKey && bytes.Equal(record.Key, key) && record.Version <= version {
			db.bufferMu.Unlock()
			if record.Tombstone {
				return nil, errorutils.ErrRecordNotFound
			}
			return record.Value, nil
		}
	}
	db.bufferMu.Unlock()
	
	// Read from Parquet files for MVCC query
	return db.readFromParquet(storeKey, version, key, owner)
}

// readFromParquet performs MVCC read from Parquet files
func (db *Database) readFromParquet(storeKey string, version int64, key []byte, owner string) ([]byte, error) {
	// Search backward from the requested version
	for v := version; v >= db.earliestVersion; v-- {
		dataFilePath := db.getDataFilePath(v, owner)
		
		// Check if file exists
		if _, err := os.Stat(dataFilePath); os.IsNotExist(err) {
			continue
		}
		
		// Open Parquet file
		file, err := os.Open(dataFilePath)
		if err != nil {
			continue
		}
		defer file.Close()
		
		reader := parquet.NewReader(file, parquet.SchemaOf(ParquetRecord{}))
		defer reader.Close()
		
		// Search for the key
		rows := make([]parquet.Row, 100)
		for {
			n, err := reader.ReadRows(rows)
			if err == io.EOF || n == 0 {
				break
			}
			if err != nil {
				return nil, err
			}
			
			for i := 0; i < n; i++ {
				var record ParquetRecord
				if err := reader.Schema().Reconstruct(&record, rows[i]); err != nil {
					continue
				}
				
				if record.StoreKey == storeKey && bytes.Equal(record.Key, key) {
					if record.Tombstone {
						return nil, errorutils.ErrRecordNotFound
				}
					return record.Value, nil
				}
			}
		}
	}
	
	return nil, errorutils.ErrRecordNotFound
}

// Has checks if a key exists at a specific version
func (db *Database) Has(storeKey string, version int64, key []byte) (bool, error) {
	_, err := db.Get(storeKey, version, key)
	if err != nil {
		if errors.Is(err, errorutils.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ApplyChangeset applies a changeset at a specific version
func (db *Database) ApplyChangeset(version int64, cs *proto.NamedChangeSet) error {
	db.bufferMu.Lock()
	defer db.bufferMu.Unlock()
	
	timestamp := time.Now().Unix()
	
	for _, change := range cs.Changeset.Pairs {
		owner := db.getOwnerFromKey(change.Key)
		
		record := ParquetRecord{
			StoreKey:  cs.Name,
			Key:       change.Key,
			Value:     change.Value,
			Version:   version,
			Tombstone: change.Delete,
			Owner:     owner,
			Timestamp: timestamp,
		}
		
		db.writeBuffer = append(db.writeBuffer, record)
		
		// Flush if buffer is full
		if len(db.writeBuffer) >= writeBatchSize {
			db.bufferMu.Unlock()
			if err := db.flushWriteBuffer(); err != nil {
				return err
			}
			db.bufferMu.Lock()
		}
	}
	
	// Update latest version
	if version > db.latestVersion {
		db.latestVersion = version
	}
	
	return nil
}

// ApplyChangesetAsync applies changesets asynchronously
func (db *Database) ApplyChangesetAsync(version int64, changesets []*proto.NamedChangeSet) error {
	// For now, just apply synchronously
	// TODO: Implement async processing with proper error handling
	for _, cs := range changesets {
		if err := db.ApplyChangeset(version, cs); err != nil {
			return err
		}
	}
	return db.flushWriteBuffer()
}

// GetLatestVersion returns the latest version
func (db *Database) GetLatestVersion() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.latestVersion, nil
}

// SetLatestVersion sets the latest version
func (db *Database) SetLatestVersion(version int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.latestVersion = version
	return db.saveMetadata()
}

// GetEarliestVersion returns the earliest version
func (db *Database) GetEarliestVersion() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.earliestVersion, nil
}

// SetEarliestVersion sets the earliest version
func (db *Database) SetEarliestVersion(version int64, ignoreVersion bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.earliestVersion = version
	return db.saveMetadata()
}

// Close closes the database
func (db *Database) Close() error {
	// Flush any pending writes
	if err := db.flushWriteBuffer(); err != nil {
		return err
	}
	
	// Close all file handles
	db.handlesMu.Lock()
	defer db.handlesMu.Unlock()
	
	// Clear file handles
	db.fileHandles = make(map[string]*parquet.File)
	
	return db.saveMetadata()
}

// loadMetadata loads metadata from disk
func (db *Database) loadMetadata() error {
	// TODO: Implement metadata loading from a metadata file
	// For now, initialize with defaults
	db.latestVersion = 0
	db.earliestVersion = 1
	return nil
}

// saveMetadata saves metadata to disk
func (db *Database) saveMetadata() error {
	// TODO: Implement metadata saving to a metadata file
	return nil
}