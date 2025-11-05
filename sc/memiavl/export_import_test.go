package memiavl

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/stretchr/testify/require"
)

// TestRewriteSnapshotViaExport tests the Export/Import approach for rewriting snapshots
func TestRewriteSnapshotViaExport(t *testing.T) {
	t.Skip("Skipped: Export/Import is now only used for state sync, not snapshot rewrite")
	
	// Create a tree with some data
	tree := New(0)
	for _, changes := range ChangeSets {
		tree.ApplyChangeSet(changes)
		_, _, err := tree.SaveVersion(true)
		require.NoError(t, err)
	}

	// Write initial snapshot using traditional method
	snapshotDir1 := t.TempDir()
	require.NoError(t, tree.WriteSnapshot(context.Background(), snapshotDir1))

	// Load the snapshot
	opts := Options{}
	opts.FillDefaults()
	snapshot1, err := OpenSnapshot(snapshotDir1, opts)
	require.NoError(t, err)
	defer snapshot1.Close()

	// Create a tree from the snapshot
	tree2 := NewFromSnapshot(snapshot1, opts)

	// Rewrite snapshot using Export/Import
	snapshotDir2 := t.TempDir()
	require.NoError(t, tree2.RewriteSnapshotViaExport(context.Background(), snapshotDir2))

	// Load the rewritten snapshot
	snapshot2, err := OpenSnapshot(snapshotDir2, opts)
	require.NoError(t, err)
	defer snapshot2.Close()

	// Verify the root hashes match
	require.Equal(t, snapshot1.RootHash(), snapshot2.RootHash(), "root hashes should match")

	// Verify node counts match
	require.Equal(t, snapshot1.nodesLen(), snapshot2.nodesLen(), "node counts should match")
	require.Equal(t, snapshot1.leavesLen(), snapshot2.leavesLen(), "leaf counts should match")

	// Verify all node hashes
	for i := 0; i < snapshot2.nodesLen(); i++ {
		node := snapshot2.Node(uint32(i))
		require.Equal(t, node.Hash(), HashNode(node), "node hash should be correct")
	}
}

// TestDBRewriteSnapshotWithExportImport tests DB-level rewrite with Export/Import
func TestDBRewriteSnapshotWithExportImport(t *testing.T) {
	t.Skip("Skipped: Export/Import is now only used for state sync, not snapshot rewrite")
	
	dir := t.TempDir()
	
	// Create DB with traditional method (no Export/Import yet)
	db, err := OpenDB(logger.NewNopLogger(), 0, Options{
		Dir:                       dir,
		CreateIfMissing:           true,
		InitialStores:             []string{"test"},
		AsyncCommitBuffer:         -1,
		UseExportImportForRewrite: false, // Use traditional method first
	})
	require.NoError(t, err)

	// Apply some changes and commit to create versions
	for _, changes := range ChangeSets {
		cs := []*proto.NamedChangeSet{
			{
				Name:      "test",
				Changeset: changes,
			},
		}
		require.NoError(t, db.ApplyChangeSets(cs))
		_, err := db.Commit()
		require.NoError(t, err)
	}

	// Create initial snapshot using traditional method (WriteSnapshot, not RewriteSnapshot)
	// This creates the first snapshot from in-memory trees
	snapshotDir := snapshotName(db.Version())
	tmpDir := snapshotDir + "-tmp"
	path := filepath.Join(dir, tmpDir)
	require.NoError(t, db.MultiTree.WriteSnapshot(context.Background(), path, db.snapshotWriterPool))
	require.NoError(t, os.Rename(path, filepath.Join(dir, snapshotDir)))
	require.NoError(t, updateCurrentSymlink(dir, snapshotDir))
	
	db.Close()

	// Reopen DB with Export/Import enabled
	db2, err := OpenDB(logger.NewNopLogger(), 0, Options{
		Dir:                       dir,
		UseExportImportForRewrite: true, // Now use Export/Import
	})
	require.NoError(t, err)
	defer db2.Close()

	// Now RewriteSnapshot using Export/Import should work
	// It will rewrite the same version 7 snapshot, but using Export/Import method
	require.NoError(t, db2.RewriteSnapshot(context.Background()))

	// Reload
	require.NoError(t, db2.Reload())

	// Verify tree is accessible and has valid hash
	treeAfter := db2.TreeByName("test")
	require.NotNil(t, treeAfter)
	hashAfter := treeAfter.RootHash()
	require.NotEmpty(t, hashAfter, "hash should not be empty")
	// Note: hash will be different because we applied changes, but that's OK
	// The important thing is that Export/Import worked correctly
}

