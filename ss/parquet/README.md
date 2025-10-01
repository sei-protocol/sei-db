# Parquet Storage Backend for Sei StateStore

This is a Parquet-based storage implementation for the Sei StateStore that uses HDFS-style partitioning to optimize MVCC-based queries, particularly for debug_trace operations from RPC nodes.

## Key Features

1. **HDFS-style Partitioning**: Data is partitioned by:
   - Version (block height)
   - Account owner (derived from key prefix)
   
2. **Optimized for Debug Trace Queries**: 
   - Direct partition access for specific version/owner combinations
   - Parallel query execution across multiple Parquet files
   - Column projection and predicate pushdown support

3. **MVCC Support**:
   - Efficient version-based queries
   - Tombstone support for deletions
   - Point-in-time snapshots

## Architecture

### Directory Structure
```
/data/
├── version=100/
│   ├── owner=account1/
│   │   └── data_100.parquet
│   └── owner=account2/
│       └── data_100.parquet
├── version=101/
│   ├── owner=account1/
│   │   └── data_101.parquet
│   └── owner=account2/
│       └── data_101.parquet
```

### Parquet Schema
```go
type ParquetRecord struct {
    StoreKey  string `parquet:"storeKey,dict,zstd"`
    Key       []byte `parquet:"key,zstd"`
    Value     []byte `parquet:"value,zstd"`
    Version   int64  `parquet:"version"`
    Tombstone bool   `parquet:"tombstone"`
    Owner     string `parquet:"owner,dict,zstd"`
    Timestamp int64  `parquet:"timestamp"`
}
```

## Usage

### Configuration
```go
config := config.StateStoreConfig{
    Backend: "parquet",
    // Other configuration options
}

db, err := ss.NewStateStore(logger, dataDir, config)
```

### Optimized Queries

#### 1. Debug Trace Query for Specific Account
```go
// Query all data for a specific account at a given height
records, err := db.(*parquet.Database).OptimizedQuery(
    "bank", // store key
    12345,  // version/height
    "account1", // owner
    parquet.QueryOptions{
        MaxWorkers: 8, // Parallel processing
    },
)
```

#### 2. Account History Query
```go
// Get complete history of an account across versions
history, err := db.(*parquet.Database).GetAccountHistory(
    "bank",     // store key
    "account1", // owner
    10000,      // start version
    12000,      // end version
)
```

#### 3. Version Snapshot
```go
// Get all data at a specific version
snapshot, err := db.(*parquet.Database).GetVersionSnapshot(12345)
```

#### 4. Bulk Query
```go
// Execute multiple queries in parallel
queries := []struct{
    StoreKey string
    Version  int64
    Owner    string
    Options  parquet.QueryOptions
}{
    {StoreKey: "bank", Version: 100, Owner: "account1"},
    {StoreKey: "staking", Version: 100, Owner: "account2"},
}

results, err := db.(*parquet.Database).BulkQuery(queries)
```

## Performance Optimizations

1. **Partitioning Benefits**:
   - Queries only read relevant partitions
   - Parallel processing across partitions
   - Efficient pruning of old versions

2. **Compression**:
   - ZSTD compression for strings and byte arrays
   - Dictionary encoding for repeated values (storeKey, owner)

3. **Write Buffering**:
   - Batched writes to reduce file operations
   - Configurable buffer size (default: 10,000 records)

4. **Query Optimizations**:
   - Predicate pushdown for key prefix/suffix filtering
   - Column projection when only keys or values are needed
   - Parallel file reading with configurable worker count

## Implementation Details

### Write Path
1. Records are buffered in memory
2. When buffer is full or flush is called, records are grouped by partition
3. Each partition is written to its own Parquet file
4. Metadata is updated to track partition information

### Read Path
1. For point queries: Direct partition lookup based on version and owner
2. For range queries: Parallel scanning of relevant partitions
3. MVCC resolution: Latest version <= requested version is returned

### Pruning
- Entire version directories can be removed for efficient pruning
- No need to rewrite files when pruning old versions

## Future Enhancements

1. **Bloom Filters**: Add bloom filters for faster key existence checks
2. **Compaction**: Merge small files within partitions
3. **Caching**: Add in-memory cache for frequently accessed partitions
4. **Statistics**: Maintain partition statistics for query optimization