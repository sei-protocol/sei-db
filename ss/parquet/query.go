package parquet

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// QueryOptions provides options for optimized queries
type QueryOptions struct {
	// Parallel processing
	MaxWorkers int
	
	// Predicate pushdown
	KeyPrefix   []byte
	KeySuffix   []byte
	ValueFilter func([]byte) bool
	
	// Column projection
	KeysOnly   bool
	ValuesOnly bool
}

// OptimizedQuery performs an optimized query for debug_trace operations
func (db *Database) OptimizedQuery(storeKey string, version int64, owner string, opts QueryOptions) ([]ParquetRecord, error) {
	if opts.MaxWorkers <= 0 {
		opts.MaxWorkers = 4
	}
	
	// Direct partition access for optimal performance
	partitionPath := db.getPartitionPath(version, owner)
	if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
		return nil, nil // No data for this partition
	}
	
	// List all Parquet files in the partition
	files, err := db.listParquetFiles(partitionPath)
	if err != nil {
		return nil, err
	}
	
	// Process files in parallel
	resultsChan := make(chan []ParquetRecord, len(files))
	errorsChan := make(chan error, len(files))
	
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, opts.MaxWorkers)
	
	for _, file := range files {
		wg.Add(1)
		go func(filePath string) {
			defer wg.Done()
			
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			
			records, err := db.queryParquetFile(filePath, storeKey, opts)
			if err != nil {
				errorsChan <- err
				return
			}
			
			resultsChan <- records
		}(file)
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	close(resultsChan)
	close(errorsChan)
	
	// Check for errors
	if err := <-errorsChan; err != nil {
		return nil, err
	}
	
	// Collect results
	var allRecords []ParquetRecord
	for records := range resultsChan {
		allRecords = append(allRecords, records...)
	}
	
	return allRecords, nil
}

// queryParquetFile queries a single Parquet file with optimizations
func (db *Database) queryParquetFile(filePath string, storeKey string, opts QueryOptions) ([]ParquetRecord, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	// Use column projection if applicable
	reader := parquet.NewReader(file, parquet.SchemaOf(ParquetRecord{}))
	defer reader.Close()
	
	var results []ParquetRecord
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
			
			// Apply filters
			if record.StoreKey != storeKey {
				continue
			}
			
			if opts.KeyPrefix != nil && !bytes.HasPrefix(record.Key, opts.KeyPrefix) {
				continue
			}
			
			if opts.KeySuffix != nil && !bytes.HasSuffix(record.Key, opts.KeySuffix) {
				continue
			}
			
			if opts.ValueFilter != nil && !opts.ValueFilter(record.Value) {
				continue
			}
			
			results = append(results, record)
		}
	}
	
	return results, nil
}

// GetAccountHistory retrieves the complete history of an account
func (db *Database) GetAccountHistory(storeKey string, owner string, startVersion, endVersion int64) ([]ParquetRecord, error) {
	var allRecords []ParquetRecord
	
	// Scan version range
	for version := startVersion; version <= endVersion; version++ {
		records, err := db.OptimizedQuery(storeKey, version, owner, QueryOptions{
			MaxWorkers: 8, // Use more workers for history queries
		})
		if err != nil {
			return nil, err
		}
		
		allRecords = append(allRecords, records...)
	}
	
	return allRecords, nil
}

// GetVersionSnapshot retrieves all data at a specific version efficiently
func (db *Database) GetVersionSnapshot(version int64) (map[string][]ParquetRecord, error) {
	snapshot := make(map[string][]ParquetRecord)
	mu := sync.Mutex{}
	
	// List all owner directories for this version
	versionDir := filepath.Join(db.dataDir, fmt.Sprintf("version=%d", version))
	if _, err := os.Stat(versionDir); os.IsNotExist(err) {
		return snapshot, nil
	}
	
	owners, err := os.ReadDir(versionDir)
	if err != nil {
		return nil, err
	}
	
	// Process owners in parallel
	var wg sync.WaitGroup
	errorsChan := make(chan error, len(owners))
	
	for _, ownerEntry := range owners {
		if !ownerEntry.IsDir() {
			continue
		}
		
		wg.Add(1)
		go func(ownerName string) {
			defer wg.Done()
			
			// Parse owner from directory name
			var owner string
			fmt.Sscanf(ownerName, "owner=%s", &owner)
			
			// Read all files for this owner
			ownerPath := filepath.Join(versionDir, ownerName)
			files, err := db.listParquetFiles(ownerPath)
			if err != nil {
				errorsChan <- err
				return
			}
			
			for _, file := range files {
				records, err := db.readAllRecords(file)
				if err != nil {
					errorsChan <- err
					return
				}
				
				mu.Lock()
				snapshot[owner] = append(snapshot[owner], records...)
				mu.Unlock()
			}
		}(ownerEntry.Name())
	}
	
	wg.Wait()
	close(errorsChan)
	
	// Check for errors
	if err := <-errorsChan; err != nil {
		return nil, err
	}
	
	return snapshot, nil
}

// listParquetFiles lists all Parquet files in a directory
func (db *Database) listParquetFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && isParquetFile(entry.Name()) {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	
	return files, nil
}

// readAllRecords reads all records from a Parquet file
func (db *Database) readAllRecords(filePath string) ([]ParquetRecord, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	reader := parquet.NewReader(file, parquet.SchemaOf(ParquetRecord{}))
	defer reader.Close()
	
	var records []ParquetRecord
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
			records = append(records, record)
		}
	}
	
	return records, nil
}

// BulkQuery performs a bulk query across multiple versions and owners
func (db *Database) BulkQuery(queries []struct {
	StoreKey string
	Version  int64
	Owner    string
	Options  QueryOptions
}) (map[string][]ParquetRecord, error) {
	results := make(map[string][]ParquetRecord)
	mu := sync.Mutex{}
	
	var wg sync.WaitGroup
	errorsChan := make(chan error, len(queries))
	
	for i, query := range queries {
		wg.Add(1)
		go func(idx int, q struct {
			StoreKey string
			Version  int64
			Owner    string
			Options  QueryOptions
		}) {
			defer wg.Done()
			
			records, err := db.OptimizedQuery(q.StoreKey, q.Version, q.Owner, q.Options)
			if err != nil {
				errorsChan <- err
				return
			}
			
			key := fmt.Sprintf("%s:%d:%s", q.StoreKey, q.Version, q.Owner)
			mu.Lock()
			results[key] = records
			mu.Unlock()
		}(i, query)
	}
	
	wg.Wait()
	close(errorsChan)
	
	// Check for errors
	select {
	case err := <-errorsChan:
		if err != nil {
			return nil, err
		}
	default:
	}
	
	return results, nil
}