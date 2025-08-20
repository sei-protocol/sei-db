package memiavl

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/sei-protocol/sei-db/proto"
)

const (
	// IncrementalSnapshotMagic is little endian encoded b"INCR"
	IncrementalSnapshotMagic = 1381253185

	// IncrementalSnapshotFormat is the current format version
	IncrementalSnapshotFormat = 0

	// SizeIncrementalMetadata includes magic, format, version, baseVersion, and tree count
	SizeIncrementalMetadata = 20

	IncrementalMetadataFileName = "incremental_metadata"
)

// IncrementalSnapshotMetadata contains metadata for incremental snapshots
type IncrementalSnapshotMetadata struct {
	Version        uint32
	BaseVersion    uint32
	TreeCount      uint32
	TreeNames      []string
	RootHashes     map[string][]byte
	ModifiedCounts map[string]uint32
}

// TreeIncrementalMetadata contains metadata for a specific tree in incremental snapshot
type TreeIncrementalMetadata struct {
	IsIncremental bool
	BaseVersion   uint32
	ModifiedNodes uint32
	RootHash      []byte
}

// HybridSnapshotManager manages the creation of full and incremental snapshots
type HybridSnapshotManager struct {
	config          *SnapshotConfig
	dbDir           string
	lastFull        uint32
	lastIncremental uint32
}

// SnapshotConfig contains configuration for snapshot creation
type SnapshotConfig struct {
	SnapshotKeepRecent          uint32
	FullSnapshotInterval        uint32
	IncrementalSnapshotInterval uint32
	IncrementalSnapshotTrees    []string
	SnapshotWriterLimit         int
}

// NewHybridSnapshotManager creates a new hybrid snapshot manager
func NewHybridSnapshotManager(config *SnapshotConfig, dbDir string) *HybridSnapshotManager {
	return &HybridSnapshotManager{
		config: config,
		dbDir:  dbDir,
	}
}

// ShouldCreateSnapshot determines if a snapshot should be created and what type
func (hsm *HybridSnapshotManager) ShouldCreateSnapshot(currentVersion uint32) (bool, bool) {
	// Check if we should create a full snapshot
	if currentVersion%hsm.config.FullSnapshotInterval == 0 {
		return true, false // full snapshot
	}
	// Check if we should create an incremental snapshot
	if hsm.config.IncrementalSnapshotInterval > 0 && currentVersion%hsm.config.IncrementalSnapshotInterval == 0 {
		return true, true // incremental snapshot (should create, is incremental)
	}
	return false, false // no snapshot
}

// CreateSnapshot creates either a full or incremental snapshot based on the current version
func (hsm *HybridSnapshotManager) CreateSnapshot(ctx context.Context, mtree *MultiTree, currentVersion uint32, snapshotDir string) error {
	shouldCreateFull, shouldCreateIncremental := hsm.ShouldCreateSnapshot(currentVersion)
	// If no snapshot should be created according to intervals, create a full snapshot anyway
	// This handles the case where RewriteSnapshot is called regardless of intervals
	if !shouldCreateFull && !shouldCreateIncremental {
		err := hsm.createFullSnapshot(ctx, mtree, snapshotDir, currentVersion)
		if err != nil {
			return err
		}
		hsm.lastFull = currentVersion
		return nil
	}
	if shouldCreateFull {
		err := hsm.createFullSnapshot(ctx, mtree, snapshotDir, currentVersion)
		if err != nil {
			return err
		}
		hsm.lastFull = currentVersion
	}
	if shouldCreateIncremental {
		// Find the base version for incremental snapshot
		baseVersion := hsm.findBaseVersion(currentVersion)

		// Create incremental snapshot
		if err := hsm.createIncrementalSnapshot(ctx, mtree, snapshotDir, currentVersion, baseVersion); err != nil {
			return fmt.Errorf("failed to create incremental snapshot: %w", err)
		}
		hsm.lastIncremental = currentVersion
	}
	return nil
}

// createIncrementalSnapshot creates an incremental snapshot containing only modified nodes
func (hsm *HybridSnapshotManager) createIncrementalSnapshot(ctx context.Context, mtree *MultiTree, snapshotDir string, currentVersion, baseVersion uint32) error {
	if err := os.MkdirAll(snapshotDir, os.ModePerm); err != nil {
		return err
	}

	metadata := &IncrementalSnapshotMetadata{
		Version:        currentVersion,
		BaseVersion:    baseVersion,
		TreeCount:      uint32(len(mtree.trees)),
		TreeNames:      make([]string, 0, len(mtree.trees)),
		RootHashes:     make(map[string][]byte),
		ModifiedCounts: make(map[string]uint32),
	}

	// Create incremental snapshots only for configured trees
	for _, namedTree := range mtree.trees {
		// Only process trees that are configured for incremental snapshots
		if !hsm.shouldUseIncrementalSnapshot(namedTree.Name) {
			continue
		}

		treeDir := filepath.Join(snapshotDir, namedTree.Name)
		if err := os.MkdirAll(treeDir, os.ModePerm); err != nil {
			return err
		}

		// Create incremental snapshot for this tree
		modifiedCount, err := namedTree.Tree.WriteIncrementalSnapshot(ctx, treeDir, baseVersion)
		if err != nil {
			return fmt.Errorf("failed to create incremental snapshot for tree %s: %w", namedTree.Name, err)
		}

		metadata.TreeNames = append(metadata.TreeNames, namedTree.Name)
		metadata.RootHashes[namedTree.Name] = namedTree.Tree.RootHash()
		metadata.ModifiedCounts[namedTree.Name] = modifiedCount
	}

	// Sort tree names for consistent ordering
	sort.Strings(metadata.TreeNames)

	// Write incremental snapshot metadata
	return WriteIncrementalSnapshotMetadata(snapshotDir, metadata)
}

// createFullSnapshot creates a full snapshot using the existing mechanism
func (hsm *HybridSnapshotManager) createFullSnapshot(ctx context.Context, mtree *MultiTree, snapshotDir string, currentVersion uint32) error {
	if err := os.MkdirAll(snapshotDir, os.ModePerm); err != nil {
		return err
	}

	// Use existing WriteSnapshot for each tree
	for _, namedTree := range mtree.trees {
		treeDir := filepath.Join(snapshotDir, namedTree.Name)
		if err := namedTree.Tree.WriteSnapshot(ctx, treeDir); err != nil {
			return fmt.Errorf("failed to create full snapshot for tree %s: %w", namedTree.Name, err)
		}
	}

	// Write multi-tree metadata (using existing mechanism)
	return writeMultiTreeMetadata(snapshotDir, currentVersion, mtree)
}

// findBaseVersion finds the most recent snapshot version that's less than currentVersion
// This function first tries to scan the filesystem for actual snapshots, accounting for delays in snapshot creation.
// If no snapshots are found, it falls back to the calculated approach for testing scenarios.
func (hsm *HybridSnapshotManager) findBaseVersion(currentVersion uint32) uint32 {
	var mostRecentSnapshot uint32

	// First, try to scan the filesystem for actual snapshots
	err := traverseSnapshots(hsm.dbDir, false, func(version int64) (bool, error) {
		// Convert to uint32 for comparison
		snapshotVersion := uint32(version)

		// We want the most recent snapshot that's less than currentVersion
		if snapshotVersion < currentVersion {
			mostRecentSnapshot = snapshotVersion
			return true, nil // Stop traversal, we found the most recent
		}
		return false, nil // Continue traversal
	})

	// If we found a snapshot, return it
	if err == nil && mostRecentSnapshot > 0 {
		return mostRecentSnapshot
	}

	// If traverseSnapshots failed (e.g., directory doesn't exist), we'll fall back to calculated approach

	// Fallback to calculated approach for testing scenarios or when no snapshots exist
	// First, check if there's a full snapshot before currentVersion
	fullSnapshotVersion := (currentVersion / hsm.config.FullSnapshotInterval) * hsm.config.FullSnapshotInterval
	if fullSnapshotVersion < currentVersion && fullSnapshotVersion > 0 {
		return fullSnapshotVersion
	}

	// Otherwise, find the most recent incremental snapshot
	if hsm.config.IncrementalSnapshotInterval > 0 {
		// Find the previous incremental snapshot
		incrementalVersion := ((currentVersion - 1) / hsm.config.IncrementalSnapshotInterval) * hsm.config.IncrementalSnapshotInterval
		if incrementalVersion > 0 {
			return incrementalVersion
		}
	}

	return 0 // Fallback to genesis
}

// shouldUseIncrementalSnapshot checks if a specific tree should use incremental snapshots
func (hsm *HybridSnapshotManager) shouldUseIncrementalSnapshot(treeName string) bool {
	// If no specific trees are configured, use incremental snapshots for all trees
	if len(hsm.config.IncrementalSnapshotTrees) == 0 {
		return true
	}

	// Check if this tree is in the configured list
	for _, configuredTree := range hsm.config.IncrementalSnapshotTrees {
		if configuredTree == treeName {
			return true
		}
	}

	return false
}

// WriteIncrementalSnapshotMetadata writes the incremental snapshot metadata
func WriteIncrementalSnapshotMetadata(snapshotDir string, metadata *IncrementalSnapshotMetadata) error {
	metadataFile := filepath.Join(snapshotDir, IncrementalMetadataFileName)

	file, err := os.Create(metadataFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write magic, format, version, baseVersion, treeCount
	var buf [SizeIncrementalMetadata]byte
	binary.LittleEndian.PutUint32(buf[0:], IncrementalSnapshotMagic)
	binary.LittleEndian.PutUint32(buf[4:], IncrementalSnapshotFormat)
	binary.LittleEndian.PutUint32(buf[8:], metadata.Version)
	binary.LittleEndian.PutUint32(buf[12:], metadata.BaseVersion)
	binary.LittleEndian.PutUint32(buf[16:], metadata.TreeCount)

	if _, err := file.Write(buf[:]); err != nil {
		return err
	}

	// Write tree names
	for _, treeName := range metadata.TreeNames {
		nameBytes := []byte(treeName)
		nameLen := uint32(len(nameBytes))

		var nameLenBuf [4]byte
		binary.LittleEndian.PutUint32(nameLenBuf[:], nameLen)
		if _, err := file.Write(nameLenBuf[:]); err != nil {
			return err
		}

		if _, err := file.Write(nameBytes); err != nil {
			return err
		}
	}

	// Write root hashes and modified counts for each tree
	for _, treeName := range metadata.TreeNames {
		rootHash := metadata.RootHashes[treeName]
		modifiedCount := metadata.ModifiedCounts[treeName]

		// Write root hash length and hash
		var hashLenBuf [4]byte
		binary.LittleEndian.PutUint32(hashLenBuf[:], uint32(len(rootHash)))
		if _, err := file.Write(hashLenBuf[:]); err != nil {
			return err
		}
		if _, err := file.Write(rootHash); err != nil {
			return err
		}

		// Write modified count
		var countBuf [4]byte
		binary.LittleEndian.PutUint32(countBuf[:], modifiedCount)
		if _, err := file.Write(countBuf[:]); err != nil {
			return err
		}
	}

	return file.Sync()
}

// readIncrementalSnapshotMetadata reads the incremental snapshot metadata
func readIncrementalSnapshotMetadata(snapshotDir string) (*IncrementalSnapshotMetadata, error) {
	metadataFile := filepath.Join(snapshotDir, IncrementalMetadataFileName)

	data, err := os.ReadFile(metadataFile)
	if err != nil {
		return nil, err
	}

	if len(data) < SizeIncrementalMetadata {
		return nil, fmt.Errorf("incremental metadata file too short: %d bytes", len(data))
	}

	// Read magic, format, version, baseVersion, treeCount
	magic := binary.LittleEndian.Uint32(data[0:])
	if magic != IncrementalSnapshotMagic {
		return nil, fmt.Errorf("invalid incremental snapshot magic: %d", magic)
	}

	format := binary.LittleEndian.Uint32(data[4:])
	if format != IncrementalSnapshotFormat {
		return nil, fmt.Errorf("unknown incremental snapshot format: %d", format)
	}

	version := binary.LittleEndian.Uint32(data[8:])
	baseVersion := binary.LittleEndian.Uint32(data[12:])
	treeCount := binary.LittleEndian.Uint32(data[16:])

	metadata := &IncrementalSnapshotMetadata{
		Version:        version,
		BaseVersion:    baseVersion,
		TreeCount:      treeCount,
		TreeNames:      make([]string, 0, treeCount),
		RootHashes:     make(map[string][]byte),
		ModifiedCounts: make(map[string]uint32),
	}

	// Read tree names
	offset := SizeIncrementalMetadata
	for i := uint32(0); i < treeCount; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("incremental metadata file truncated at tree name %d", i)
		}

		nameLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(nameLen) > len(data) {
			return nil, fmt.Errorf("incremental metadata file truncated at tree name data %d", i)
		}

		treeName := string(data[offset : offset+int(nameLen)])
		metadata.TreeNames = append(metadata.TreeNames, treeName)
		offset += int(nameLen)
	}

	// Read root hashes and modified counts
	for _, treeName := range metadata.TreeNames {
		// Read root hash
		if offset+4 > len(data) {
			return nil, fmt.Errorf("incremental metadata file truncated at root hash length for tree %s", treeName)
		}

		hashLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(hashLen) > len(data) {
			return nil, fmt.Errorf("incremental metadata file truncated at root hash data for tree %s", treeName)
		}

		rootHash := make([]byte, hashLen)
		copy(rootHash, data[offset:offset+int(hashLen)])
		metadata.RootHashes[treeName] = rootHash
		offset += int(hashLen)

		// Read modified count
		if offset+4 > len(data) {
			return nil, fmt.Errorf("incremental metadata file truncated at modified count for tree %s", treeName)
		}

		modifiedCount := binary.LittleEndian.Uint32(data[offset:])
		metadata.ModifiedCounts[treeName] = modifiedCount
		offset += 4
	}

	return metadata, nil
}

// writeMultiTreeMetadata writes the multi-tree metadata for full snapshots
func writeMultiTreeMetadata(snapshotDir string, _ uint32, mtree *MultiTree) error {
	metadata := &proto.MultiTreeMetadata{
		CommitInfo:     &mtree.lastCommitInfo,
		InitialVersion: int64(mtree.initialVersion),
	}

	bz, err := metadata.Marshal()
	if err != nil {
		return err
	}

	metadataFile := filepath.Join(snapshotDir, MetadataFileName)
	return WriteFileSync(metadataFile, bz)
}
