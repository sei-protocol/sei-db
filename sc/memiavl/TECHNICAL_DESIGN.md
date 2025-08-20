# MemIAVL Incremental Snapshots - Technical Design

## Architecture Overview

The incremental snapshot system implements a hybrid snapshot strategy that combines full snapshots with incremental snapshots to optimize performance while maintaining data integrity and backward compatibility.

## Core Components

### 1. HybridSnapshotManager

The `HybridSnapshotManager` is the central orchestrator that decides when and what type of snapshots to create.

```go
type HybridSnapshotManager struct {
    config          *SnapshotConfig
    dbDir           string
    lastFull        uint32
    lastIncremental uint32
}
```

**Key Responsibilities:**
- Determine snapshot creation timing based on configuration
- Select base versions for incremental snapshots
- Manage snapshot creation for different tree types
- Track snapshot history

### 2. SnapshotConfig

Configuration structure that controls snapshot behavior:

```go
type SnapshotConfig struct {
    FullSnapshotInterval        uint32   // Full snapshot interval (default: 50,000)
    IncrementalSnapshotInterval uint32   // Incremental snapshot interval (default: 1,000)
    IncrementalSnapshotTrees    []string // Trees to use incremental snapshots
    SnapshotWriterLimit         int      // Concurrency limit
}
```

### 3. IncrementalSnapshotMetadata

Metadata structure for incremental snapshots:

```go
type IncrementalSnapshotMetadata struct {
    Version        uint32            // Current snapshot version
    BaseVersion    uint32            // Base snapshot version
    TreeCount      uint32            // Number of trees
    TreeNames      []string          // Tree names
    RootHashes     map[string][]byte // Root hashes for each tree
    ModifiedCounts map[string]uint32 // Number of modified nodes per tree
}
```

### 4. SnapshotInterface

Polymorphic interface that abstracts both full and merged snapshots:

```go
type SnapshotInterface interface {
    Close() error
    IsEmpty() bool
    Version() uint32
    RootHash() []byte
    Key(offset uint64) []byte
    KeyValue(offset uint64) ([]byte, []byte)
    LeafKey(index uint32) []byte
    LeafKeyValue(index uint32) ([]byte, []byte)
    Export() *Exporter
    
    // Internal methods for PersistedNode
    nodesLen() int
    leavesLen() int
    getNodesLayout() Nodes
    getLeavesLayout() Leaves
    getNodes() []byte
    getLeaves() []byte
    getKvs() []byte
}
```

### 5. MergedSnapshot

Combines base and incremental snapshots to provide a unified view:

```go
type MergedSnapshot struct {
    baseSnapshot         *Snapshot
    incrementalSnapshots []*Snapshot
    version              uint32
    
    // Combined data from base + incremental snapshots
    nodesMap  *MmapFile
    leavesMap *MmapFile
    kvsMap    *MmapFile
    
    nodes  []byte
    leaves []byte
    kvs    []byte
    
    // Combined layouts
    nodesLayout  Nodes
    leavesLayout Leaves
    
    // Root node from the final incremental snapshot
    root *MergedPersistedNode
}
```

## Implementation Details

### Snapshot Creation Decision Logic

```go
func (hsm *HybridSnapshotManager) ShouldCreateSnapshot(currentVersion uint32) (bool, bool) {
    // Check if we should create a full snapshot
    if currentVersion%hsm.config.FullSnapshotInterval == 0 {
        return true, false // full snapshot
    }

    // Check if we should create an incremental snapshot
    if hsm.config.IncrementalSnapshotInterval > 0 && 
       currentVersion%hsm.config.IncrementalSnapshotInterval == 0 {
        return true, true // incremental snapshot
    }

    return false, false // no snapshot
}
```

### Base Version Selection Algorithm

```go
func (hsm *HybridSnapshotManager) findBaseVersion(currentVersion uint32) uint32 {
    // First, check if there's a full snapshot before currentVersion
    fullSnapshotVersion := (currentVersion / hsm.config.FullSnapshotInterval) * hsm.config.FullSnapshotInterval
    if fullSnapshotVersion < currentVersion && fullSnapshotVersion > 0 {
        return fullSnapshotVersion
    }

    // Otherwise, find the most recent incremental snapshot
    if hsm.config.IncrementalSnapshotInterval > 0 {
        incrementalVersion := ((currentVersion - 1) / hsm.config.IncrementalSnapshotInterval) * hsm.config.IncrementalSnapshotInterval
        if incrementalVersion > 0 {
            return incrementalVersion
        }
    }

    return 0 // Fallback to genesis
}
```

### Incremental Snapshot Creation

The system only writes nodes that have been modified since the base version:

```go
func (t *Tree) writeModifiedNodesRecursive(w *snapshotWriter, node Node, baseVersion uint32) (uint32, error) {
    // Skip unmodified nodes
    if node.Version() <= baseVersion {
        return 0, nil
    }

    // Write modified nodes
    if node.IsLeaf() {
        return w.writeLeaf(node.Version(), node.Key(), node.Value(), node.Hash())
    } else {
        // Recursively process children
        leftCount, err := t.writeModifiedNodesRecursive(w, node.Left(), baseVersion)
        if err != nil {
            return 0, err
        }
        rightCount, err := t.writeModifiedNodesRecursive(w, node.Right(), baseVersion)
        if err != nil {
            return 0, err
        }
        return w.writeBranch(node.Version(), node.Size(), node.Height(), 
                           uint8(leftCount), 0, node.Hash())
    }
}
```

### Tree Reconstruction Process

When loading an incremental snapshot, the system reconstructs a complete tree:

```go
func (ms *MergedSnapshot) reconstructTree(modifiedNodes map[string]*types.SnapshotNode) (*treeData, error) {
    // Create tree from base snapshot
    baseTree := NewFromSnapshot(ms.baseSnapshot, true, 0)
    
    // Apply modifications
    for _, node := range modifiedNodes {
        if node.Value == nil {
            baseTree.Remove(node.Key)
        } else {
            baseTree.Set(node.Key, node.Value)
        }
    }
    
    // Write reconstructed tree to temporary directory
    tempDir, err := os.MkdirTemp("", "merged-snapshot-*")
    if err != nil {
        return nil, err
    }
    defer os.RemoveAll(tempDir)
    
    if err := baseTree.WriteSnapshot(context.Background(), tempDir); err != nil {
        return nil, err
    }
    
    // Read the complete tree data
    return ms.readTreeData(tempDir)
}
```

## File Format Specifications

### Incremental Snapshot Metadata Format

The `incremental_metadata` file uses a binary format:

```
Header (20 bytes):
- Magic (4 bytes): 0x52524349 ("INCR" in little-endian)
- Format (4 bytes): 0x00000000 (format version)
- Version (4 bytes): Current snapshot version
- BaseVersion (4 bytes): Base snapshot version
- TreeCount (4 bytes): Number of trees

Tree Names Section:
For each tree:
- NameLength (4 bytes): Length of tree name
- Name (NameLength bytes): Tree name string

Tree Data Section:
For each tree:
- RootHashLength (4 bytes): Length of root hash
- RootHash (RootHashLength bytes): Root hash
- ModifiedCount (4 bytes): Number of modified nodes
```

### Tree-Specific Metadata Format

Each tree in an incremental snapshot includes metadata about modified nodes:

```go
type TreeIncrementalMetadata struct {
    IsIncremental bool
    BaseVersion   uint32
    ModifiedNodes uint32
    RootHash      []byte
}
```

## Thread Safety Implementation

### MultiTree Mutex Protection

```go
type MultiTree struct {
    // ... other fields ...
    mtx sync.RWMutex // mutex for thread-safe access to MultiTree fields
}
```

### Protected Operations

- **Read Operations**: Use `RLock()` for concurrent reads
  ```go
  func (t *MultiTree) TreeByName(name string) *Tree {
      t.mtx.RLock()
      defer t.mtx.RUnlock()
      // ... implementation
  }
  ```

- **Write Operations**: Use `Lock()` for exclusive writes
  ```go
  func (t *MultiTree) ReplaceWith(other *MultiTree) error {
      t.mtx.Lock()
      defer t.mtx.Unlock()
      // ... implementation
  }
  ```

### DB-Level Protection

The DB struct provides additional mutex protection for snapshot operations:

```go
type DB struct {
    // ... other fields ...
    mtx sync.Mutex // protects all DB operations
}
```

## Performance Optimizations

### 1. Memory-Mapped Files

All snapshot data is accessed through memory-mapped files for efficient zero-copy access:

```go
type Snapshot struct {
    nodesMap  *MmapFile
    leavesMap *MmapFile
    kvsMap    *MmapFile
    // ... other fields
}
```

### 2. Selective Node Writing

Only modified nodes are written to incremental snapshots, significantly reducing I/O:

```go
// Skip unmodified nodes
if node.Version() <= baseVersion {
    return 0, nil
}
```

### 3. Efficient Tree Reconstruction

The reconstruction process uses temporary files to avoid excessive memory usage:

```go
// Write reconstructed tree to temporary directory
tempDir, err := os.MkdirTemp("", "merged-snapshot-*")
if err != nil {
    return nil, err
}
defer os.RemoveAll(tempDir)
```

### 4. Concurrent Snapshot Writing

Multiple trees can be written concurrently using worker pools:

```go
// write the snapshots in parallel and wait all jobs done
group, _ := wp.GroupContext(ctx)

for _, entry := range t.trees {
    tree, name := entry.Tree, entry.Name
    group.Submit(func() error {
        return tree.WriteSnapshot(ctx, filepath.Join(dir, name))
    })
}
```

## Error Handling and Recovery

### 1. Graceful Degradation

If incremental snapshot loading fails, the system falls back to full snapshot loading:

```go
func LoadSnapshotWithMerge(snapshotDir string) (SnapshotInterface, error) {
    // Try incremental snapshot first
    incMetadata, err := readIncrementalSnapshotMetadata(snapshotDir)
    if err == nil {
        // ... incremental loading logic
    }
    
    // Fall back to regular snapshot loading
    return OpenSnapshot(snapshotDir)
}
```

### 2. Resource Cleanup

Temporary resources are properly cleaned up:

```go
tempDir, err := os.MkdirTemp("", "merged-snapshot-*")
if err != nil {
    return nil, err
}
defer os.RemoveAll(tempDir) // Always cleanup
```

### 3. Validation

The system validates snapshot integrity:

```go
// Validate magic number
if magic != IncrementalSnapshotMagic {
    return nil, fmt.Errorf("invalid incremental snapshot magic: %d", magic)
}

// Validate format version
if format != IncrementalSnapshotFormat {
    return nil, fmt.Errorf("unknown incremental snapshot format: %d", format)
}
```

## Configuration Management

### Default Values

```go
const (
    DefaultSnapshotInterval            = 10000
    DefaultIncrementalSnapshotInterval = 1000
    DefaultSnapshotWriterLimit         = 1
)
```

### Configuration Validation

```go
func (opts Options) Validate() error {
    if opts.ReadOnly && opts.CreateIfMissing {
        return errors.New("can't create db in read-only mode")
    }
    
    if opts.ReadOnly && opts.LoadForOverwriting {
        return errors.New("can't rollback db in read-only mode")
    }
    
    return nil
}
```

### Configuration Application

```go
func (opts *Options) FillDefaults() {
    if opts.SnapshotInterval <= 0 {
        opts.SnapshotInterval = config.DefaultSnapshotInterval
    }
    
    if opts.SnapshotWriterLimit <= 0 {
        opts.SnapshotWriterLimit = config.DefaultSnapshotWriterLimit
    }
    
    if opts.IncrementalSnapshotInterval == 0 {
        opts.IncrementalSnapshotInterval = config.DefaultIncrementalSnapshotInterval
    }
}
```

## Testing Strategy

### Unit Tests

1. **Snapshot Creation Tests**: Verify correct snapshot type selection
2. **Base Version Tests**: Verify correct base version calculation
3. **Tree Reconstruction Tests**: Verify proper tree merging
4. **Thread Safety Tests**: Verify concurrent access safety

### Integration Tests

1. **End-to-End Tests**: Full snapshot creation and loading cycle
2. **Performance Tests**: Measure snapshot creation and loading times
3. **Stress Tests**: High-concurrency snapshot operations

### Test Coverage

- Snapshot creation decision logic
- Base version selection algorithm
- Tree reconstruction process
- Thread safety mechanisms
- Error handling and recovery
- Configuration management

## Monitoring and Observability

### Key Metrics

1. **Snapshot Creation Time**: Time to create full vs incremental snapshots
2. **Snapshot Size**: Size of full vs incremental snapshots
3. **Loading Time**: Time to load and merge snapshots
4. **Memory Usage**: Memory consumption during tree reconstruction
5. **Error Rates**: Frequency of snapshot creation/loading failures

### Logging

The system provides detailed logging for debugging:

```go
logger.Info("Creating incremental snapshot", 
    "version", currentVersion, 
    "baseVersion", baseVersion, 
    "modifiedNodes", modifiedCount)
```

### Health Checks

The system includes health checks for snapshot integrity:

```go
// Validate snapshot metadata
if err := validateSnapshotMetadata(metadata); err != nil {
    return fmt.Errorf("invalid snapshot metadata: %w", err)
}
```

## Future Enhancements

### 1. Compression

Add compression for incremental snapshots to further reduce storage requirements:

```go
// Future implementation
func compressIncrementalSnapshot(data []byte) ([]byte, error) {
    // Implement compression algorithm
}
```

### 2. Deduplication

Remove duplicate nodes across incremental snapshots:

```go
// Future implementation
func deduplicateNodes(nodes []*types.SnapshotNode) []*types.SnapshotNode {
    // Implement deduplication logic
}
```

### 3. Validation

Add integrity checks for merged snapshots:

```go
// Future implementation
func validateMergedSnapshot(snapshot *MergedSnapshot) error {
    // Implement validation logic
}
```

### 4. Metrics

Add comprehensive performance metrics:

```go
// Future implementation
type SnapshotMetrics struct {
    CreationTime    time.Duration
    Size            int64
    ModifiedNodes   uint32
    LoadingTime     time.Duration
    MemoryUsage     int64
}
```

### 5. Cleanup

Automatic cleanup of old incremental snapshots:

```go
// Future implementation
func cleanupOldSnapshots(dbDir string, keepRecent uint32) error {
    // Implement cleanup logic
}
```

## Conclusion

The incremental snapshot system provides a robust, efficient, and scalable solution for MemIAVL snapshot operations. The hybrid approach ensures optimal performance while maintaining data integrity and backward compatibility. The comprehensive thread safety measures and error handling make the system suitable for production use in high-throughput blockchain environments. 