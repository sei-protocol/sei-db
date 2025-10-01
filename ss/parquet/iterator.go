package parquet

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/parquet-go/parquet-go"
	"github.com/sei-protocol/sei-db/ss/types"
)

// Iterator implements types.DBIterator for Parquet storage
type Iterator struct {
	db          *Database
	storeKey    string
	version     int64
	start       []byte
	end         []byte
	reverse     bool
	
	// Current state
	current     *iteratorItem
	items       []iteratorItem
	index       int
	err         error
	closed      bool
}

type iteratorItem struct {
	key   []byte
	value []byte
}

// Ensure Iterator implements DBIterator
var _ types.DBIterator = (*Iterator)(nil)

// newIterator creates a new iterator
func (db *Database) newIterator(storeKey string, version int64, start, end []byte, reverse bool) (*Iterator, error) {
	iter := &Iterator{
		db:       db,
		storeKey: storeKey,
		version:  version,
		start:    start,
		end:      end,
		reverse:  reverse,
		items:    make([]iteratorItem, 0),
		index:    -1,
	}
	
	// Load all relevant data
	if err := iter.loadData(); err != nil {
		return nil, err
	}
	
	// Position iterator at first valid item
	iter.Next()
	
	return iter, nil
}

// loadData loads all relevant data from Parquet files
func (iter *Iterator) loadData() error {
	// Collect all relevant records from write buffer and Parquet files
	records := make(map[string]ParquetRecord)
	
	// First, check write buffer
	iter.db.bufferMu.Lock()
	for _, record := range iter.db.writeBuffer {
		if record.StoreKey == iter.storeKey && record.Version <= iter.version {
			if iter.inRange(record.Key) {
				key := string(record.Key)
				if existing, ok := records[key]; !ok || record.Version > existing.Version {
					records[key] = record
				}
			}
		}
	}
	iter.db.bufferMu.Unlock()
	
	// Then, read from Parquet files
	if err := iter.readFromParquetFiles(records); err != nil {
		return err
	}
	
	// Convert map to sorted slice
	for _, record := range records {
		if !record.Tombstone {
			iter.items = append(iter.items, iteratorItem{
				key:   record.Key,
				value: record.Value,
			})
		}
	}
	
	// Sort items
	sort.Slice(iter.items, func(i, j int) bool {
		cmp := bytes.Compare(iter.items[i].key, iter.items[j].key)
		if iter.reverse {
			return cmp > 0
		}
		return cmp < 0
	})
	
	return nil
}

// readFromParquetFiles reads relevant records from Parquet files
func (iter *Iterator) readFromParquetFiles(records map[string]ParquetRecord) error {
	// Get all partition directories
	entries, err := os.ReadDir(iter.db.dataDir)
	if err != nil {
		return err
	}
	
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		
		// Parse version from directory name
		var version int64
		var owner string
		_, err := fmt.Sscanf(entry.Name(), "version=%d", &version)
		if err != nil || version > iter.version {
			continue
		}
		
		// Read owner directories
		versionPath := filepath.Join(iter.db.dataDir, entry.Name())
		ownerEntries, err := os.ReadDir(versionPath)
		if err != nil {
			continue
		}
		
		for _, ownerEntry := range ownerEntries {
			if !ownerEntry.IsDir() {
				continue
			}
			
			// Parse owner from directory name
			_, err := fmt.Sscanf(ownerEntry.Name(), "owner=%s", &owner)
			if err != nil {
				continue
			}
			
			// Read Parquet files in this partition
			if err := iter.readPartitionFiles(version, owner, records); err != nil {
				return err
			}
		}
	}
	
	return nil
}

// readPartitionFiles reads Parquet files from a specific partition
func (iter *Iterator) readPartitionFiles(version int64, owner string, records map[string]ParquetRecord) error {
	partitionPath := iter.db.getPartitionPath(version, owner)
	
	entries, err := os.ReadDir(partitionPath)
	if err != nil {
		return nil // Partition might not exist
	}
	
	for _, entry := range entries {
		if entry.IsDir() || !isParquetFile(entry.Name()) {
			continue
		}
		
		filePath := filepath.Join(partitionPath, entry.Name())
		if err := iter.readParquetFile(filePath, version, records); err != nil {
			return err
		}
	}
	
	return nil
}

// readParquetFile reads records from a single Parquet file
func (iter *Iterator) readParquetFile(filePath string, version int64, records map[string]ParquetRecord) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	reader := parquet.NewReader(file, parquet.SchemaOf(ParquetRecord{}))
	defer reader.Close()
	
	rows := make([]parquet.Row, 100)
	for {
		n, err := reader.ReadRows(rows)
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			return err
		}
		
		for i := 0; i < n; i++ {
			var record ParquetRecord
			if err := reader.Schema().Reconstruct(&record, rows[i]); err != nil {
				continue
			}
			
			// Filter by store key and version
			if record.StoreKey != iter.storeKey || record.Version > iter.version {
				continue
			}
			
			// Check if key is in range
			if !iter.inRange(record.Key) {
				continue
			}
			
			// Update records map with latest version for each key
			key := string(record.Key)
			if existing, ok := records[key]; !ok || record.Version > existing.Version {
				records[key] = record
			}
		}
	}
	
	return nil
}

// inRange checks if a key is within the iterator's range
func (iter *Iterator) inRange(key []byte) bool {
	if iter.start != nil && bytes.Compare(key, iter.start) < 0 {
		return false
	}
	if iter.end != nil && bytes.Compare(key, iter.end) >= 0 {
		return false
	}
	return true
}

// Domain returns the start and end of the iterator
func (iter *Iterator) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}

// Valid returns whether the iterator is positioned at a valid item
func (iter *Iterator) Valid() bool {
	if iter.closed || iter.err != nil {
		return false
	}
	return iter.index >= 0 && iter.index < len(iter.items)
}

// Next moves to the next item
func (iter *Iterator) Next() {
	if iter.closed {
		return
	}
	
	iter.index++
	if iter.index >= 0 && iter.index < len(iter.items) {
		iter.current = &iter.items[iter.index]
	} else {
		iter.current = nil
	}
}

// Key returns the current key
func (iter *Iterator) Key() []byte {
	if !iter.Valid() {
		panic("iterator is not valid")
	}
	return iter.current.key
}

// Value returns the current value
func (iter *Iterator) Value() []byte {
	if !iter.Valid() {
		panic("iterator is not valid")
	}
	return iter.current.value
}

// Error returns any error encountered
func (iter *Iterator) Error() error {
	return iter.err
}

// Close closes the iterator
func (iter *Iterator) Close() error {
	if iter.closed {
		return nil
	}
	iter.closed = true
	iter.items = nil
	iter.current = nil
	return nil
}

// Iterator creates a forward iterator
func (db *Database) Iterator(storeKey string, version int64, start, end []byte) (types.DBIterator, error) {
	return db.newIterator(storeKey, version, start, end, false)
}

// ReverseIterator creates a reverse iterator
func (db *Database) ReverseIterator(storeKey string, version int64, start, end []byte) (types.DBIterator, error) {
	return db.newIterator(storeKey, version, start, end, true)
}

// isParquetFile checks if a file name indicates a Parquet file
func isParquetFile(name string) bool {
	return filepath.Ext(name) == ".parquet"
}