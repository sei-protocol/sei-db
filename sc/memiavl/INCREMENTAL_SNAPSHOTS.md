# MemIAVL Incremental Snapshots

## Overview

MemIAVL incremental snapshots provide an efficient way to create and load snapshots by only storing modified nodes since the last snapshot, significantly reducing snapshot size and creation/loading times. This document describes the design, implementation, and usage of the incremental snapshot system.

## Architecture

### Hybrid Snapshot Strategy

The incremental snapshot system implements a hybrid approach:

- **Full Snapshots**: Complete snapshots taken at regular intervals (default: every 50,000 blocks)
- **Incremental Snapshots**: Partial snapshots containing only modified nodes, taken between full snapshots (default: every 1,000 blocks)

### Key Components

1. **HybridSnapshotManager**: Orchestrates snapshot creation decisions
2. **IncrementalSnapshotMetadata**: Stores metadata for incremental snapshots
3. **MergedSnapshot**: Combines base and incremental snapshots during loading
4. **SnapshotInterface**: Polymorphic interface for both full and merged snapshots

## Configuration

### SnapshotConfig

```go
type SnapshotConfig struct {
    FullSnapshotInterval        uint32  // Interval for full snapshots (default: 50,000)
    IncrementalSnapshotInterval uint32  // Interval for incremental snapshots (default: 1,000)
    IncrementalSnapshotTrees    []string // Trees to use incremental snapshots (empty = all trees)
    SnapshotWriterLimit         int     // Concurrency limit for snapshot writers
}
```

### Default Configuration

```go
const (
    DefaultSnapshotInterval            = 10000
    DefaultIncrementalSnapshotInterval = 1000
    DefaultSnapshotWriterLimit         = 1
)
```

## File Structure

### Full Snapshot Structure
```
snapshot-50000/
├── __metadata          # Multi-tree metadata
├── bank/
│   ├── metadata        # Tree metadata
│   ├── nodes           # Branch nodes
│   ├── leaves          # Leaf nodes
│   └── kvs             # Key-value pairs
└── acc/
    ├── metadata
    ├── nodes
    ├── leaves
    └── kvs
```

### Incremental Snapshot Structure
```
snapshot-51000/
├── incremental_metadata # Incremental snapshot metadata
├── bank/
│   ├── metadata        # Tree metadata (only modified nodes)
│   ├── nodes           # Modified branch nodes
│   ├── leaves          # Modified leaf nodes
│   └── kvs             # Modified key-value pairs
└── acc/
    ├── metadata
    ├── nodes
    ├── leaves
    └── kvs
```

## Implementation Details

### 1. Snapshot Creation Decision

The `HybridSnapshotManager` determines when to create snapshots:

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

### 2. Base Version Selection

For incremental snapshots, the system finds the most recent base snapshot:

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

### 3. Incremental Snapshot Creation

Only modified nodes (MemNodes) are written to incremental snapshots:

```go
func (t *Tree) WriteIncrementalSnapshot(ctx context.Context, snapshotDir string, baseVersion uint32) (uint32, error) {
    return writeSnapshot(ctx, snapshotDir, t.version, func(w *snapshotWriter) (uint32, error) {
        if t.root == nil {
            return 0, nil
        }
        return t.writeModifiedNodesRecursive(w, t.root, baseVersion)
    })
}
```

The `writeModifiedNodesRecursive` function only writes nodes that have been modified since the base version:

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

### 4. Incremental Snapshot Metadata

Incremental snapshots include metadata describing the base version and modified nodes:

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

### 5. Snapshot Loading and Merging

When loading an incremental snapshot, the system automatically merges it with the base snapshot:

```go
func LoadSnapshotWithMerge(snapshotDir string) (SnapshotInterface, error) {
    // Check if it's an incremental snapshot
    incMetadata, err := readIncrementalSnapshotMetadata(snapshotDir)
    if err == nil {
        // Load base snapshot
        baseSnapshotDir := filepath.Join(filepath.Dir(snapshotDir), 
                                       fmt.Sprintf("snapshot-%d", incMetadata.BaseVersion))
        baseSnapshot, err := OpenSnapshot(baseSnapshotDir)
        if err != nil {
            return nil, fmt.Errorf("failed to load base snapshot %s: %w", baseSnapshotDir, err)
        }

        // Create merged snapshot
        merged, err := NewMergedSnapshot(baseSnapshot, []*Snapshot{incSnapshot})
        if err != nil {
            return nil, fmt.Errorf("failed to merge snapshots: %w", err)
        }

        return merged, nil
    }

    // Fall back to regular snapshot loading
    return OpenSnapshot(snapshotDir)
}
```

### 6. Tree Reconstruction

The `MergedSnapshot` reconstructs a complete tree by:

1. Loading the base snapshot
2. Collecting all modified nodes from incremental snapshots
3. Creating a temporary tree with base data
4. Applying incremental modifications
5. Writing the reconstructed tree to temporary files
6. Loading the complete tree data

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

## Thread Safety

The implementation includes comprehensive thread safety measures:

### MultiTree Mutex Protection

```go
type MultiTree struct {
    // ... other fields ...
    mtx sync.RWMutex // mutex for thread-safe access to MultiTree fields
}
```

### Protected Operations

- **Read Operations**: Use `RLock()` for concurrent reads
- **Write Operations**: Use `Lock()` for exclusive writes
- **Snapshot Operations**: Protected by DB-level mutex

## Performance Characteristics

### Benefits

1. **Reduced Snapshot Size**: Only modified nodes are stored
2. **Faster Creation**: No need to traverse unmodified nodes
3. **Faster Loading**: Memory-mapped access to merged snapshots
4. **Configurable**: Per-tree incremental snapshot support

### Trade-offs

1. **Complexity**: More complex loading logic
2. **Dependencies**: Incremental snapshots depend on base snapshots
3. **Storage**: Requires both base and incremental snapshots

## Usage Examples

### Basic Configuration

```go
config := &SnapshotConfig{
    FullSnapshotInterval:        50000,
    IncrementalSnapshotInterval: 1000,
    IncrementalSnapshotTrees:    []string{"bank", "acc"}, // Only these trees
    SnapshotWriterLimit:         4,
}

snapshotManager := NewHybridSnapshotManager(config, dbDir)
```

### Automatic Snapshot Creation

```go
// The system automatically creates snapshots based on configuration
db, err := OpenDB(logger, 0, Options{
    Dir:                           dbDir,
    CreateIfMissing:               true,
    SnapshotInterval:              50000,
    IncrementalSnapshotInterval:   1000,
    IncrementalSnapshotTrees:      []string{"bank", "acc"},
})
```

### Manual Snapshot Creation

```go
// Force snapshot creation regardless of interval
err := db.RewriteSnapshot(context.Background())
```

### Loading Snapshots

```go
// Automatically handles both full and incremental snapshots
snapshot, err := LoadSnapshotWithMerge(snapshotDir)
if err != nil {
    return err
}
defer snapshot.Close()

// Use snapshot normally
tree := NewFromSnapshot(snapshot, true, 0)
```

## Testing

The implementation includes comprehensive tests:

- `TestHybridSnapshotManager`: Tests snapshot creation decisions
- `TestTreeWriteIncrementalSnapshot`: Tests incremental snapshot creation
- `TestIncrementalSnapshotLoadingSimple`: Tests snapshot loading and merging
- `TestShouldUseIncrementalSnapshot`: Tests tree-specific configuration

## Migration and Compatibility

### Backward Compatibility

- Existing full snapshots continue to work unchanged
- New incremental snapshots are automatically detected and handled
- Fallback to full snapshot loading if incremental loading fails

### Migration Path

1. Deploy with incremental snapshots disabled
2. Enable incremental snapshots for specific trees
3. Gradually expand to more trees as needed

## Future Enhancements

1. **Compression**: Add compression for incremental snapshots
2. **Deduplication**: Remove duplicate nodes across incremental snapshots
3. **Validation**: Add integrity checks for merged snapshots
4. **Metrics**: Add performance metrics for snapshot operations
5. **Cleanup**: Automatic cleanup of old incremental snapshots

## Troubleshooting

### Common Issues

1. **Missing Base Snapshot**: Ensure base snapshots exist before creating incremental snapshots
2. **Corrupted Metadata**: Check incremental_metadata file integrity
3. **Memory Issues**: Monitor memory usage during tree reconstruction
4. **Performance**: Adjust snapshot intervals based on workload

### Debug Information

Enable debug logging to see snapshot creation and loading details:

```go
logger := logger.NewLogger("debug")
db, err := OpenDB(logger, 0, opts)
```

## Conclusion

The incremental snapshot system provides significant performance improvements for MemIAVL snapshot operations while maintaining full backward compatibility. The hybrid approach ensures optimal performance for different use cases and allows for gradual adoption of the new features. 