package memiavl

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOverlaySnapshotFastRestart(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "overlay-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a base tree with some data
	baseTree := NewEmptyTree(0, 1000)
	baseTree.Set([]byte("key1"), []byte("value1"))
	baseTree.Set([]byte("key2"), []byte("value2"))
	baseTree.Set([]byte("key3"), []byte("value3"))

	// Write base snapshot
	baseSnapshotDir := filepath.Join(tempDir, "snapshot-1000")
	err = baseTree.WriteSnapshot(context.Background(), baseSnapshotDir)
	require.NoError(t, err)

	// Load base snapshot
	baseSnapshot, err := OpenSnapshot(baseSnapshotDir)
	require.NoError(t, err)
	defer baseSnapshot.Close()

	// Create a tree that represents the final state
	finalTree := NewEmptyTree(2000, 2000)
	finalTree.Set([]byte("key1"), []byte("value1-updated")) // Update
	finalTree.Set([]byte("key3"), []byte("value3"))         // Keep unchanged
	finalTree.Set([]byte("key4"), []byte("value4"))         // Add
	// key2 is deleted

	// Write final snapshot (this represents what the incremental snapshot would contain)
	finalSnapshotDir := filepath.Join(tempDir, "snapshot-2000")
	err = finalTree.WriteSnapshot(context.Background(), finalSnapshotDir)
	require.NoError(t, err)

	// Load final snapshot
	finalSnapshot, err := OpenSnapshot(finalSnapshotDir)
	require.NoError(t, err)
	defer finalSnapshot.Close()

	// Create overlay snapshot
	overlay, err := NewOverlaySnapshot(baseSnapshot, []*Snapshot{finalSnapshot})
	require.NoError(t, err)

	// Test fast access to modified keys
	value, err := overlay.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1-updated"), value)

	value, err = overlay.Get([]byte("key4"))
	require.NoError(t, err)
	require.Equal(t, []byte("value4"), value)

	// Test deleted key
	_, err = overlay.Get([]byte("key2"))
	require.Error(t, err)

	// Test unmodified key (should come from base snapshot)
	value, err = overlay.Get([]byte("key3"))
	require.NoError(t, err)
	require.Equal(t, []byte("value3"), value)

	// Test non-existent key
	_, err = overlay.Get([]byte("nonexistent"))
	require.Error(t, err)

	// Verify modification tracking
	require.True(t, overlay.HasModification([]byte("key1")))
	require.True(t, overlay.HasModification([]byte("key2")))
	require.True(t, overlay.HasModification([]byte("key4")))
	require.False(t, overlay.HasModification([]byte("key3")))

	require.True(t, overlay.IsDeleted([]byte("key2")))
	require.False(t, overlay.IsDeleted([]byte("key1")))

	require.Equal(t, 3, overlay.GetModificationCount()) // key1 (updated), key4 (new), key2 (deleted)

	// Test root hash calculation
	expectedRootHash := finalTree.RootHash()
	actualRootHash := overlay.RootHash()
	require.Equal(t, expectedRootHash, actualRootHash)

	// Test version (should match the final tree version)
	require.Equal(t, uint32(finalTree.Version()), overlay.Version())
}

func TestOverlaySnapshotEmptyBase(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "overlay-test-empty")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create an empty base snapshot
	emptyTree := NewEmptyTree(0, 0)
	baseSnapshotDir := filepath.Join(tempDir, "snapshot-0")
	err = emptyTree.WriteSnapshot(context.Background(), baseSnapshotDir)
	require.NoError(t, err)

	baseSnapshot, err := OpenSnapshot(baseSnapshotDir)
	require.NoError(t, err)
	defer baseSnapshot.Close()

	// Create incremental snapshot with data
	incTree := NewEmptyTree(1000, 1000)
	incTree.Set([]byte("key1"), []byte("value1"))
	incTree.Set([]byte("key2"), []byte("value2"))

	incSnapshotDir := filepath.Join(tempDir, "snapshot-1000")
	err = incTree.WriteSnapshot(context.Background(), incSnapshotDir)
	require.NoError(t, err)

	incSnapshot, err := OpenSnapshot(incSnapshotDir)
	require.NoError(t, err)
	defer incSnapshot.Close()

	// Create overlay snapshot
	overlay, err := NewOverlaySnapshot(baseSnapshot, []*Snapshot{incSnapshot})
	require.NoError(t, err)

	// Test access to incremental data
	value, err := overlay.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), value)

	value, err = overlay.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), value)

	// Test root hash
	expectedRootHash := incTree.RootHash()
	actualRootHash := overlay.RootHash()
	require.Equal(t, expectedRootHash, actualRootHash)

	require.Equal(t, uint32(incTree.Version()), overlay.Version())
}

func TestOverlaySnapshotMultipleIncremental(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "overlay-test-multiple")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create base snapshot
	baseTree := NewEmptyTree(0, 1000)
	baseTree.Set([]byte("key1"), []byte("value1"))
	baseTree.Set([]byte("key2"), []byte("value2"))

	baseSnapshotDir := filepath.Join(tempDir, "snapshot-1000")
	err = baseTree.WriteSnapshot(context.Background(), baseSnapshotDir)
	require.NoError(t, err)

	baseSnapshot, err := OpenSnapshot(baseSnapshotDir)
	require.NoError(t, err)
	defer baseSnapshot.Close()

	// Create first incremental snapshot
	inc1Tree := NewEmptyTree(2000, 2000)
	inc1Tree.Set([]byte("key1"), []byte("value1-updated"))
	inc1Tree.Set([]byte("key3"), []byte("value3"))

	inc1SnapshotDir := filepath.Join(tempDir, "snapshot-2000")
	err = inc1Tree.WriteSnapshot(context.Background(), inc1SnapshotDir)
	require.NoError(t, err)

	inc1Snapshot, err := OpenSnapshot(inc1SnapshotDir)
	require.NoError(t, err)
	defer inc1Snapshot.Close()

	// Create second incremental snapshot
	inc2Tree := NewEmptyTree(3000, 3000)
	inc2Tree.Set([]byte("key1"), []byte("value1-final"))
	inc2Tree.Remove([]byte("key2"))
	inc2Tree.Set([]byte("key3"), []byte("value3")) // Keep key3 from first incremental
	inc2Tree.Set([]byte("key4"), []byte("value4"))

	inc2SnapshotDir := filepath.Join(tempDir, "snapshot-3000")
	err = inc2Tree.WriteSnapshot(context.Background(), inc2SnapshotDir)
	require.NoError(t, err)

	inc2Snapshot, err := OpenSnapshot(inc2SnapshotDir)
	require.NoError(t, err)
	defer inc2Snapshot.Close()

	// Create overlay snapshot with multiple incremental snapshots
	overlay, err := NewOverlaySnapshot(baseSnapshot, []*Snapshot{inc1Snapshot, inc2Snapshot})
	require.NoError(t, err)

	// Test that latest modifications win
	value, err := overlay.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1-final"), value) // Latest wins

	value, err = overlay.Get([]byte("key3"))
	require.NoError(t, err)
	require.Equal(t, []byte("value3"), value)

	value, err = overlay.Get([]byte("key4"))
	require.NoError(t, err)
	require.Equal(t, []byte("value4"), value)

	// Test deleted key
	_, err = overlay.Get([]byte("key2"))
	require.Error(t, err)

	// Test unmodified key
	value, err = overlay.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1-final"), value)

	// Verify modification count (should be 4: key1, key2, key3, key4)
	require.Equal(t, 4, overlay.GetModificationCount())

	// Test root hash matches the final tree
	expectedRootHash := inc2Tree.RootHash()
	actualRootHash := overlay.RootHash()
	require.Equal(t, expectedRootHash, actualRootHash)

	require.Equal(t, uint32(inc2Tree.Version()), overlay.Version())
}
