package memiavl

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"
)

func TestHybridSnapshotManager(t *testing.T) {
	// Test configuration
	config := &SnapshotConfig{
		FullSnapshotInterval:        50000,
		IncrementalSnapshotInterval: 1000,
		IncrementalSnapshotTrees:    []string{"bank", "acc"},
		SnapshotWriterLimit:         1,
	}

	manager := NewHybridSnapshotManager(config, t.TempDir())

	// Test full snapshot intervals
	shouldCreate, isIncremental := manager.ShouldCreateSnapshot(50000)
	require.True(t, shouldCreate)
	require.False(t, isIncremental)

	shouldCreate, isIncremental = manager.ShouldCreateSnapshot(100000)
	require.True(t, shouldCreate)
	require.False(t, isIncremental)

	// Test incremental snapshot intervals
	shouldCreate, isIncremental = manager.ShouldCreateSnapshot(1000)
	require.True(t, shouldCreate)
	require.True(t, isIncremental)

	shouldCreate, isIncremental = manager.ShouldCreateSnapshot(2000)
	require.True(t, shouldCreate)
	require.True(t, isIncremental)

	// Test no snapshot
	shouldCreate, isIncremental = manager.ShouldCreateSnapshot(1500)
	require.False(t, shouldCreate)
	require.False(t, isIncremental)

	// Test base version calculation
	baseVersion := manager.findBaseVersion(51000)
	require.Equal(t, uint32(50000), baseVersion)

	baseVersion = manager.findBaseVersion(2000)
	require.Equal(t, uint32(1000), baseVersion)
}

func TestTreeWriteIncrementalSnapshot(t *testing.T) {
	// Create a tree with some data
	tree := New(0)

	// Add some initial data
	changes := iavl.ChangeSet{
		Pairs: []*iavl.KVPair{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		},
	}
	tree.ApplyChangeSet(changes)
	tree.SaveVersion(true)

	// Create a full snapshot first
	snapshotDir := t.TempDir()
	err := tree.WriteSnapshot(context.Background(), snapshotDir)
	require.NoError(t, err)

	// Add more data
	changes2 := iavl.ChangeSet{
		Pairs: []*iavl.KVPair{
			{Key: []byte("key3"), Value: []byte("value3")},
			{Key: []byte("key1"), Value: []byte("value1_updated")}, // Update existing key
		},
	}
	tree.ApplyChangeSet(changes2)
	tree.SaveVersion(true)

	// Create incremental snapshot
	incrementalDir := t.TempDir()
	modifiedCount, err := tree.WriteIncrementalSnapshot(context.Background(), incrementalDir, 1)
	require.NoError(t, err)
	require.Greater(t, modifiedCount, uint32(0))

	// Verify the incremental snapshot contains only modified nodes
	// This would require loading and comparing the snapshots
	// For now, we just verify the method works without error
}

func TestShouldUseIncrementalSnapshot(t *testing.T) {
	// Test with specific tree configuration
	config := &SnapshotConfig{
		FullSnapshotInterval:        50000,
		IncrementalSnapshotInterval: 1000,
		IncrementalSnapshotTrees:    []string{"bank", "acc"},
		SnapshotWriterLimit:         1,
	}

	manager := NewHybridSnapshotManager(config, t.TempDir())

	// Test configured trees - should use incremental snapshots
	require.True(t, manager.shouldUseIncrementalSnapshot("bank"))
	require.True(t, manager.shouldUseIncrementalSnapshot("acc"))

	// Test non-configured trees - should NOT use incremental snapshots
	require.False(t, manager.shouldUseIncrementalSnapshot("staking"))
	require.False(t, manager.shouldUseIncrementalSnapshot("gov"))
	require.False(t, manager.shouldUseIncrementalSnapshot("other"))

	// Test with empty configuration (all trees should use incremental)
	config2 := &SnapshotConfig{
		FullSnapshotInterval:        50000,
		IncrementalSnapshotInterval: 1000,
		IncrementalSnapshotTrees:    []string{},
		SnapshotWriterLimit:         1,
	}

	manager2 := NewHybridSnapshotManager(config2, t.TempDir())
	require.True(t, manager2.shouldUseIncrementalSnapshot("bank"))
	require.True(t, manager2.shouldUseIncrementalSnapshot("acc"))
	require.True(t, manager2.shouldUseIncrementalSnapshot("staking"))
	require.True(t, manager2.shouldUseIncrementalSnapshot("other"))
}

func TestIncrementalSnapshotLoadingSimple(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "incremental-snapshot-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create a tree with some initial data
	tree := New(0)
	changes := iavl.ChangeSet{
		Pairs: []*iavl.KVPair{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		},
	}
	tree.ApplyChangeSet(changes)
	tree.SaveVersion(false)

	// Create a full snapshot at version 1
	fullSnapshotDir := filepath.Join(tmpDir, "snapshot-1")
	err = tree.WriteSnapshot(context.Background(), fullSnapshotDir)
	require.NoError(t, err)

	// Add more data
	changes2 := iavl.ChangeSet{
		Pairs: []*iavl.KVPair{
			{Key: []byte("key3"), Value: []byte("value3")},
			{Key: []byte("key1"), Value: []byte("value1-updated")}, // Update existing key
		},
	}
	tree.ApplyChangeSet(changes2)
	tree.SaveVersion(false)

	// Create a full snapshot at version 2 (this represents the incremental state)
	incSnapshotDir := filepath.Join(tmpDir, "snapshot-2")
	err = tree.WriteSnapshot(context.Background(), incSnapshotDir)
	require.NoError(t, err)

	// Write the incremental metadata to indicate this is an incremental snapshot
	metadata := &IncrementalSnapshotMetadata{
		Version:        2,
		BaseVersion:    1,
		TreeCount:      1,
		TreeNames:      []string{"test"},
		RootHashes:     map[string][]byte{"test": tree.RootHash()},
		ModifiedCounts: map[string]uint32{"test": 2}, // 2 modifications: key1 update and key3 addition
	}

	err = WriteIncrementalSnapshotMetadata(incSnapshotDir, metadata)
	require.NoError(t, err)

	// Test loading the incremental snapshot
	snapshotInterface, err := LoadSnapshotWithMerge(incSnapshotDir)
	require.NoError(t, err)
	require.NotNil(t, snapshotInterface)

	// Verify it's an overlay snapshot
	overlaySnapshot, ok := snapshotInterface.(*OverlaySnapshot)
	require.True(t, ok, "Expected OverlaySnapshot type")
	require.Equal(t, uint32(2), overlaySnapshot.Version())

	// Verify the data is correct
	// Test that we can access the modified data
	value, err := overlaySnapshot.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1-updated"), value)

	value, err = overlaySnapshot.Get([]byte("key3"))
	require.NoError(t, err)
	require.Equal(t, []byte("value3"), value)

	// Test that unmodified data is still accessible
	value, err = overlaySnapshot.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), value)

	require.False(t, overlaySnapshot.IsEmpty())
}
