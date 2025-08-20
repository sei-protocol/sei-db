package memiavl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sei-protocol/sei-db/common/errors"
	"github.com/sei-protocol/sei-db/common/utils"
	"github.com/sei-protocol/sei-db/sc/types"
)

const (
	// SnapshotFileMagic is little endian encoded b"IAVL"
	SnapshotFileMagic = 1280721225

	// the initial snapshot format
	SnapshotFormat = 0

	// magic: uint32, format: uint32, version: uint32
	SizeMetadata = 12

	FileNameNodes    = "nodes"
	FileNameLeaves   = "leaves"
	FileNameKVs      = "kvs"
	FileNameMetadata = "metadata"
)

// Snapshot manage the lifecycle of mmap-ed files for the snapshot,
// it must out live the objects that derived from it.
type Snapshot struct {
	nodesMap  *MmapFile
	leavesMap *MmapFile
	kvsMap    *MmapFile

	nodes  []byte
	leaves []byte
	kvs    []byte

	// parsed from metadata file
	version uint32

	// wrapping the raw nodes buffer
	nodesLayout  Nodes
	leavesLayout Leaves

	// nil means empty snapshot
	root *PersistedNode
}

func NewEmptySnapshot(version uint32) *Snapshot {
	return &Snapshot{
		version: version,
	}
}

// OpenSnapshot parse the version number and the root node index from metadata file,
// and mmap the other files.
func OpenSnapshot(snapshotDir string) (*Snapshot, error) {
	// read metadata file
	bz, err := os.ReadFile(filepath.Join(snapshotDir, FileNameMetadata))
	if err != nil {
		return nil, err
	}
	if len(bz) != SizeMetadata {
		return nil, fmt.Errorf("wrong metadata file size, expcted: %d, found: %d", SizeMetadata, len(bz))
	}

	magic := binary.LittleEndian.Uint32(bz)
	if magic != SnapshotFileMagic {
		return nil, fmt.Errorf("invalid metadata file magic: %d", magic)
	}
	format := binary.LittleEndian.Uint32(bz[4:])
	if format != SnapshotFormat {
		return nil, fmt.Errorf("unknown snapshot format: %d", format)
	}
	version := binary.LittleEndian.Uint32(bz[8:])

	var nodesMap, leavesMap, kvsMap *MmapFile
	cleanupHandles := func(err error) error {
		errs := []error{err}
		if nodesMap != nil {
			errs = append(errs, nodesMap.Close())
		}
		if leavesMap != nil {
			errs = append(errs, leavesMap.Close())
		}
		if kvsMap != nil {
			errs = append(errs, kvsMap.Close())
		}
		return errors.Join(errs...)
	}

	if nodesMap, err = NewMmap(filepath.Join(snapshotDir, FileNameNodes)); err != nil {
		return nil, cleanupHandles(err)
	}
	if leavesMap, err = NewMmap(filepath.Join(snapshotDir, FileNameLeaves)); err != nil {
		return nil, cleanupHandles(err)
	}
	if kvsMap, err = NewMmap(filepath.Join(snapshotDir, FileNameKVs)); err != nil {
		return nil, cleanupHandles(err)
	}

	nodes := nodesMap.Data()
	leaves := leavesMap.Data()
	kvs := kvsMap.Data()

	// validate nodes length
	if len(nodes)%SizeNode != 0 {
		return nil, cleanupHandles(
			fmt.Errorf("corrupted snapshot, nodes file size %d is not a multiple of %d", len(nodes), SizeNode),
		)
	}
	if len(leaves)%SizeLeaf != 0 {
		return nil, cleanupHandles(
			fmt.Errorf("corrupted snapshot, leaves file size %d is not a multiple of %d", len(leaves), SizeLeaf),
		)
	}

	nodesLen := len(nodes) / SizeNode
	leavesLen := len(leaves) / SizeLeaf
	if (leavesLen > 0 && nodesLen+1 != leavesLen) || (leavesLen == 0 && nodesLen != 0) {
		return nil, cleanupHandles(
			fmt.Errorf("corrupted snapshot, branch nodes size %d don't match leaves size %d", nodesLen, leavesLen),
		)
	}

	nodesData, err := NewNodes(nodes)
	if err != nil {
		return nil, cleanupHandles(err)
	}

	leavesData, err := NewLeaves(leaves)
	if err != nil {
		return nil, cleanupHandles(err)
	}

	snapshot := &Snapshot{
		nodesMap:  nodesMap,
		leavesMap: leavesMap,
		kvsMap:    kvsMap,

		// cache the pointers
		nodes:  nodes,
		leaves: leaves,
		kvs:    kvs,

		version: version,

		nodesLayout:  nodesData,
		leavesLayout: leavesData,
	}

	if nodesLen > 0 {
		snapshot.root = &PersistedNode{
			snapshot: snapshot,
			isLeaf:   false,
			index:    uint32(nodesLen - 1),
		}
	} else if leavesLen > 0 {
		snapshot.root = &PersistedNode{
			snapshot: snapshot,
			isLeaf:   true,
			index:    0,
		}
	}

	return snapshot, nil
}

// Close closes the file and mmap handles, clears the buffers.
func (snapshot *Snapshot) Close() error {
	var errs []error

	if snapshot.nodesMap != nil {
		errs = append(errs, snapshot.nodesMap.Close())
	}
	if snapshot.leavesMap != nil {
		errs = append(errs, snapshot.leavesMap.Close())
	}
	if snapshot.kvsMap != nil {
		errs = append(errs, snapshot.kvsMap.Close())
	}

	// reset to an empty tree
	*snapshot = *NewEmptySnapshot(snapshot.version)
	return errors.Join(errs...)
}

// IsEmpty returns if the snapshot is an empty tree.
func (snapshot *Snapshot) IsEmpty() bool {
	return snapshot.root == nil
}

// Node returns the branch node by index
func (snapshot *Snapshot) Node(index uint32) PersistedNode {
	return PersistedNode{
		snapshot: snapshot,
		index:    index,
		isLeaf:   false,
	}
}

// Leaf returns the leaf node by index
func (snapshot *Snapshot) Leaf(index uint32) PersistedNode {
	return PersistedNode{
		snapshot: snapshot,
		index:    index,
		isLeaf:   true,
	}
}

// Version returns the version of the snapshot
func (snapshot *Snapshot) Version() uint32 {
	return snapshot.version
}

// RootNode returns the root node
func (snapshot *Snapshot) RootNode() PersistedNode {
	if snapshot.IsEmpty() {
		panic("RootNode not supported on an empty snapshot")
	}
	return *snapshot.root
}

func (snapshot *Snapshot) RootHash() []byte {
	if snapshot.IsEmpty() {
		return emptyHash
	}
	return snapshot.RootNode().Hash()
}

// nodesLen returns the number of nodes in the snapshot
func (snapshot *Snapshot) nodesLen() int {
	return len(snapshot.nodes) / SizeNode
}

// leavesLen returns the number of nodes in the snapshot
func (snapshot *Snapshot) leavesLen() int {
	return len(snapshot.leaves) / SizeLeaf
}

// ScanNodes iterate over the nodes in the snapshot order (depth-first post-order, leaf nodes before branch nodes)
func (snapshot *Snapshot) ScanNodes(callback func(node PersistedNode) error) error {
	for i := 0; i < snapshot.leavesLen(); i++ {
		if err := callback(snapshot.Leaf(uint32(i))); err != nil {
			return err
		}
	}
	for i := 0; i < snapshot.nodesLen(); i++ {
		if err := callback(snapshot.Node(uint32(i))); err != nil {
			return err
		}
	}
	return nil
}

// Key returns a zero-copy slice of key by offset
func (snapshot *Snapshot) Key(offset uint64) []byte {
	keyLen := binary.LittleEndian.Uint32(snapshot.kvs[offset:])
	offset += 4
	return snapshot.kvs[offset : offset+uint64(keyLen)]
}

// KeyValue returns a zero-copy slice of key/value pair by offset
func (snapshot *Snapshot) KeyValue(offset uint64) ([]byte, []byte) {
	len := uint64(binary.LittleEndian.Uint32(snapshot.kvs[offset:]))
	offset += 4
	key := snapshot.kvs[offset : offset+len]
	offset += len
	len = uint64(binary.LittleEndian.Uint32(snapshot.kvs[offset:]))
	offset += 4
	value := snapshot.kvs[offset : offset+len]
	return key, value
}

func (snapshot *Snapshot) LeafKey(index uint32) []byte {
	leaf := snapshot.leavesLayout.Leaf(index)
	offset := leaf.KeyOffset() + 4
	return snapshot.kvs[offset : offset+uint64(leaf.KeyLength())]
}

func (snapshot *Snapshot) LeafKeyValue(index uint32) ([]byte, []byte) {
	leaf := snapshot.leavesLayout.Leaf(index)
	offset := leaf.KeyOffset() + 4
	length := uint64(leaf.KeyLength())
	key := snapshot.kvs[offset : offset+length]
	offset += length
	length = uint64(binary.LittleEndian.Uint32(snapshot.kvs[offset:]))
	offset += 4
	return key, snapshot.kvs[offset : offset+length]
}

// Internal methods needed by SnapshotInterface
func (snapshot *Snapshot) getNodesLayout() Nodes {
	return snapshot.nodesLayout
}

func (snapshot *Snapshot) getLeavesLayout() Leaves {
	return snapshot.leavesLayout
}

func (snapshot *Snapshot) getNodes() []byte {
	return snapshot.nodes
}

func (snapshot *Snapshot) getLeaves() []byte {
	return snapshot.leaves
}

func (snapshot *Snapshot) getKvs() []byte {
	return snapshot.kvs
}

// Export exports the nodes from snapshot file sequentially, more efficient than a post-order traversal.
func (snapshot *Snapshot) Export() *Exporter {
	return newExporter(snapshot.export)
}

func (snapshot *Snapshot) export(callback func(*types.SnapshotNode) bool) {
	if snapshot.leavesLen() == 0 {
		return
	}

	if snapshot.leavesLen() == 1 {
		leaf := snapshot.Leaf(0)
		callback(&types.SnapshotNode{
			Height:  0,
			Version: int64(leaf.Version()),
			Key:     leaf.Key(),
			Value:   leaf.Value(),
		})
		return
	}

	var pendingTrees int
	var i, j uint32
	for ; i < uint32(snapshot.nodesLen()); i++ {
		// pending branch node
		node := snapshot.nodesLayout.Node(i)
		for pendingTrees < int(node.PreTrees())+2 {
			// add more leaf nodes
			leaf := snapshot.leavesLayout.Leaf(j)
			key, value := snapshot.KeyValue(leaf.KeyOffset())
			enode := &types.SnapshotNode{
				Height:  0,
				Version: int64(leaf.Version()),
				Key:     key,
				Value:   value,
			}
			j++
			pendingTrees++

			if callback(enode) {
				return
			}
		}
		enode := &types.SnapshotNode{
			Height:  int8(node.Height()),
			Version: int64(node.Version()),
			Key:     snapshot.LeafKey(node.KeyLeaf()),
		}
		pendingTrees--

		if callback(enode) {
			return
		}
	}
}

// WriteSnapshot save the IAVL tree to a new snapshot directory.
func (t *Tree) WriteSnapshot(ctx context.Context, snapshotDir string) error {
	return writeSnapshot(ctx, snapshotDir, t.version, func(w *snapshotWriter) (uint32, error) {
		if t.root == nil {
			return 0, nil
		}

		if err := w.writeRecursive(t.root); err != nil {
			return 0, err
		}
		return w.leafCounter, nil
	})
}

// WriteIncrementalSnapshot saves only the modified nodes since baseVersion to a new snapshot directory.
func (t *Tree) WriteIncrementalSnapshot(ctx context.Context, snapshotDir string, baseVersion uint32) (uint32, error) {
	var modifiedCount uint32
	err := writeSnapshot(ctx, snapshotDir, t.version, func(w *snapshotWriter) (uint32, error) {
		if t.root == nil {
			return 0, nil
		}

		// Only write nodes that have been modified since baseVersion
		count, err := t.writeModifiedNodesRecursive(w, t.root, baseVersion)
		if err != nil {
			return 0, err
		}

		modifiedCount = count
		return count, nil
	})

	return modifiedCount, err
}

// writeModifiedNodesRecursive writes only the nodes that have been modified since baseVersion
func (t *Tree) writeModifiedNodesRecursive(w *snapshotWriter, node Node, baseVersion uint32) (uint32, error) {
	// Check if this node has been modified since baseVersion
	if memNode, ok := node.(*MemNode); ok && memNode.Version() > baseVersion {
		// This node was modified, write it
		if node.IsLeaf() {
			err := w.writeLeaf(node.Version(), node.Key(), node.Value(), node.Hash())
			if err != nil {
				return 0, err
			}
			return 1, nil
		}

		// For branch nodes, recursively write modified children
		leftCount, err := t.writeModifiedNodesRecursive(w, node.Left(), baseVersion)
		if err != nil {
			return 0, err
		}

		keyLeaf := w.leafCounter
		rightCount, err := t.writeModifiedNodesRecursive(w, node.Right(), baseVersion)
		if err != nil {
			return 0, err
		}

		totalCount := leftCount + rightCount
		err = w.writeBranch(node.Version(), totalCount, node.Height(),
			uint8(w.leafCounter-w.branchCounter), keyLeaf, node.Hash())
		if err != nil {
			return 0, err
		}
		return totalCount, nil
	}

	// For PersistedNodes or unmodified MemNodes, don't write them
	// Just return the count for parent nodes to calculate size
	if node.IsLeaf() {
		return 1, nil
	}

	leftCount, err := t.writeModifiedNodesRecursive(w, node.Left(), baseVersion)
	if err != nil {
		return 0, err
	}

	rightCount, err := t.writeModifiedNodesRecursive(w, node.Right(), baseVersion)
	if err != nil {
		return 0, err
	}

	return leftCount + rightCount, nil
}

func writeSnapshot(
	ctx context.Context,
	dir string, version uint32,
	doWrite func(*snapshotWriter) (uint32, error),
) (returnErr error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	nodesFile := filepath.Join(dir, FileNameNodes)
	leavesFile := filepath.Join(dir, FileNameLeaves)
	kvsFile := filepath.Join(dir, FileNameKVs)

	fpNodes, err := createFile(nodesFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpNodes.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	fpLeaves, err := createFile(leavesFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpLeaves.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	fpKVs, err := createFile(kvsFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpKVs.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	nodesWriter := bufio.NewWriterSize(fpNodes, bufIOSize)
	leavesWriter := bufio.NewWriterSize(fpLeaves, bufIOSize)
	kvsWriter := bufio.NewWriterSize(fpKVs, bufIOSize)

	w := newSnapshotWriter(ctx, nodesWriter, leavesWriter, kvsWriter)
	leaves, err := doWrite(w)
	if err != nil {
		return err
	}

	if leaves > 0 {
		if err := nodesWriter.Flush(); err != nil {
			return err
		}
		if err := leavesWriter.Flush(); err != nil {
			return err
		}
		if err := kvsWriter.Flush(); err != nil {
			return err
		}

		if err := fpKVs.Sync(); err != nil {
			return err
		}
		if err := fpLeaves.Sync(); err != nil {
			return err
		}
		if err := fpNodes.Sync(); err != nil {
			return err
		}
	}

	// write metadata
	var metadataBuf [SizeMetadata]byte
	binary.LittleEndian.PutUint32(metadataBuf[:], SnapshotFileMagic)
	binary.LittleEndian.PutUint32(metadataBuf[4:], SnapshotFormat)
	binary.LittleEndian.PutUint32(metadataBuf[8:], version)

	metadataFile := filepath.Join(dir, FileNameMetadata)
	fpMetadata, err := createFile(metadataFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := fpMetadata.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	if _, err := fpMetadata.Write(metadataBuf[:]); err != nil {
		return err
	}

	return fpMetadata.Sync()
}

type snapshotWriter struct {
	// context for cancel the writing process
	ctx context.Context

	nodesWriter, leavesWriter, kvWriter io.Writer

	// count how many nodes have been written
	branchCounter, leafCounter uint32

	// record the current writing offset in kvs file
	kvsOffset uint64
}

func newSnapshotWriter(ctx context.Context, nodesWriter, leavesWriter, kvsWriter io.Writer) *snapshotWriter {
	return &snapshotWriter{
		ctx:          ctx,
		nodesWriter:  nodesWriter,
		leavesWriter: leavesWriter,
		kvWriter:     kvsWriter,
	}
}

// writeKeyValue append key-value pair to kvs file and record the offset
func (w *snapshotWriter) writeKeyValue(key, value []byte) error {
	var numBuf [4]byte

	binary.LittleEndian.PutUint32(numBuf[:], uint32(len(key)))
	if _, err := w.kvWriter.Write(numBuf[:]); err != nil {
		return err
	}
	if _, err := w.kvWriter.Write(key); err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(numBuf[:], uint32(len(value)))
	if _, err := w.kvWriter.Write(numBuf[:]); err != nil {
		return err
	}
	if _, err := w.kvWriter.Write(value); err != nil {
		return err
	}

	w.kvsOffset += 4 + 4 + uint64(len(key)) + uint64(len(value))
	return nil
}

func (w *snapshotWriter) writeLeaf(version uint32, key, value, hash []byte) error {
	var buf [SizeLeafWithoutHash]byte
	binary.LittleEndian.PutUint32(buf[OffsetLeafVersion:], version)
	binary.LittleEndian.PutUint32(buf[OffsetLeafKeyLen:], uint32(len(key)))
	binary.LittleEndian.PutUint64(buf[OffsetLeafKeyOffset:], w.kvsOffset)

	if err := w.writeKeyValue(key, value); err != nil {
		return err
	}

	if _, err := w.leavesWriter.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.leavesWriter.Write(hash); err != nil {
		return err
	}

	w.leafCounter++
	return nil
}

func (w *snapshotWriter) writeBranch(version, size uint32, height, preTrees uint8, keyLeaf uint32, hash []byte) error {
	var buf [SizeNodeWithoutHash]byte
	buf[OffsetHeight] = height
	buf[OffsetPreTrees] = preTrees
	binary.LittleEndian.PutUint32(buf[OffsetVersion:], version)
	binary.LittleEndian.PutUint32(buf[OffsetSize:], size)
	binary.LittleEndian.PutUint32(buf[OffsetKeyLeaf:], keyLeaf)

	if _, err := w.nodesWriter.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.nodesWriter.Write(hash); err != nil {
		return err
	}

	w.branchCounter++
	return nil
}

// writeRecursive write the node recursively in depth-first post-order,
// returns `(nodeIndex, err)`.
func (w *snapshotWriter) writeRecursive(node Node) error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
	}
	if node.IsLeaf() {
		return w.writeLeaf(node.Version(), node.Key(), node.Value(), node.Hash())
	}

	// record the number of pending subtrees before the current one,
	// it's always positive and won't exceed the tree height, so we can use an uint8 to store it.
	preTrees := uint8(w.leafCounter - w.branchCounter)

	if err := w.writeRecursive(node.Left()); err != nil {
		return err
	}
	keyLeaf := w.leafCounter
	if err := w.writeRecursive(node.Right()); err != nil {
		return err
	}

	return w.writeBranch(node.Version(), uint32(node.Size()), node.Height(), preTrees, keyLeaf, node.Hash())
}

func createFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
}

// SnapshotInterface defines the interface that both Snapshot and MergedSnapshot implement
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

	// Internal methods needed by PersistedNode
	nodesLen() int
	leavesLen() int
	getNodesLayout() Nodes
	getLeavesLayout() Leaves
	getNodes() []byte
	getLeaves() []byte
	getKvs() []byte
}

// MergedSnapshot represents a snapshot that combines a base snapshot with incremental snapshots
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

// NewMergedSnapshot creates a merged snapshot by combining a base snapshot with incremental snapshots
func NewMergedSnapshot(baseSnapshot *Snapshot, incrementalSnapshots []*Snapshot) (*MergedSnapshot, error) {
	if len(incrementalSnapshots) == 0 {
		return nil, fmt.Errorf("no incremental snapshots provided")
	}

	// Get the final version from the last incremental snapshot
	finalVersion := incrementalSnapshots[len(incrementalSnapshots)-1].version

	merged := &MergedSnapshot{
		baseSnapshot:         baseSnapshot,
		incrementalSnapshots: incrementalSnapshots,
		version:              finalVersion,
	}

	// Merge the snapshots
	if err := merged.merge(); err != nil {
		return nil, err
	}

	return merged, nil
}

// merge combines the base snapshot with incremental snapshots
func (ms *MergedSnapshot) merge() error {
	// Build a map of modified nodes from incremental snapshots
	modifiedNodes := make(map[string]*types.SnapshotNode)

	// Collect all modified nodes from incremental snapshots
	for _, incSnapshot := range ms.incrementalSnapshots {
		// Check if this incremental snapshot has actual data or is just metadata
		if len(incSnapshot.nodes) > 0 {
			// This snapshot has actual data, export it
			exporter := incSnapshot.Export()
			for {
				snapshotNode, err := exporter.Next()
				if err != nil {
					break // End of export
				}
				key := string(snapshotNode.Key)
				modifiedNodes[key] = snapshotNode
			}
			exporter.Close()
		}
		// Note: metadata-only snapshots are skipped for now
		// In a real implementation, we'd need to track modifications differently
	}

	// Create a new snapshot writer to build the merged tree
	var mergedNodes, mergedLeaves, mergedKVs []byte
	var mergedNodesLayout Nodes
	var mergedLeavesLayout Leaves

	if !ms.baseSnapshot.IsEmpty() {
		// Reconstruct the complete tree by traversing base snapshot and applying modifications
		mergedData, err := ms.reconstructTree(modifiedNodes)
		if err != nil {
			return fmt.Errorf("failed to reconstruct tree: %w", err)
		}

		mergedNodes = mergedData.nodes
		mergedLeaves = mergedData.leaves
		mergedKVs = mergedData.kvs
		mergedNodesLayout = mergedData.nodesLayout
		mergedLeavesLayout = mergedData.leavesLayout
	} else if len(modifiedNodes) > 0 {
		// Empty base snapshot, just use incremental data
		mergedData, err := ms.buildTreeFromNodes(modifiedNodes)
		if err != nil {
			return fmt.Errorf("failed to build tree from nodes: %w", err)
		}

		mergedNodes = mergedData.nodes
		mergedLeaves = mergedData.leaves
		mergedKVs = mergedData.kvs
		mergedNodesLayout = mergedData.nodesLayout
		mergedLeavesLayout = mergedData.leavesLayout
	}

	// Create memory-mapped files for the merged data
	if len(mergedNodes) > 0 {
		nodesMap, err := ms.createMmapFromData(mergedNodes, "merged-nodes")
		if err != nil {
			return fmt.Errorf("failed to create merged nodes mmap: %w", err)
		}
		ms.nodesMap = nodesMap
	}

	if len(mergedLeaves) > 0 {
		leavesMap, err := ms.createMmapFromData(mergedLeaves, "merged-leaves")
		if err != nil {
			return fmt.Errorf("failed to create merged leaves mmap: %w", err)
		}
		ms.leavesMap = leavesMap
	}

	if len(mergedKVs) > 0 {
		kvsMap, err := ms.createMmapFromData(mergedKVs, "merged-kvs")
		if err != nil {
			return fmt.Errorf("failed to create merged kvs mmap: %w", err)
		}
		ms.kvsMap = kvsMap
	}

	// Set the merged data
	ms.nodes = mergedNodes
	ms.leaves = mergedLeaves
	ms.kvs = mergedKVs
	ms.nodesLayout = mergedNodesLayout
	ms.leavesLayout = mergedLeavesLayout

	// Set the root node
	if len(mergedNodes) > 0 || len(mergedLeaves) > 0 {
		if len(mergedLeaves) > 0 && len(mergedNodes) == 0 {
			// Single leaf tree
			ms.root = &MergedPersistedNode{
				snapshot: ms,
				isLeaf:   true,
				index:    0,
			}
		} else if len(mergedNodes) > 0 {
			// Branch tree
			ms.root = &MergedPersistedNode{
				snapshot: ms,
				isLeaf:   false,
				index:    uint32(len(mergedNodes)/SizeNode - 1),
			}
		}
	}

	return nil
}

// treeData represents the reconstructed tree data
type treeData struct {
	nodes        []byte
	leaves       []byte
	kvs          []byte
	nodesLayout  Nodes
	leavesLayout Leaves
}

// reconstructTree rebuilds the complete tree by traversing the base snapshot and applying modifications
func (ms *MergedSnapshot) reconstructTree(modifiedNodes map[string]*types.SnapshotNode) (*treeData, error) {
	// Create a new tree from the base snapshot
	baseTree := NewFromSnapshot(ms.baseSnapshot, true, 0)

	// Apply all modifications to the tree
	for _, node := range modifiedNodes {
		if node.Value == nil {
			// Delete operation
			baseTree.Remove(node.Key)
		} else {
			// Set operation
			baseTree.Set(node.Key, node.Value)
		}
	}

	// Write the reconstructed tree to get the merged data
	ctx := context.Background()
	var tempDir string
	var err error
	tempDir, err = os.MkdirTemp("", "merged-snapshot")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	err = baseTree.WriteSnapshot(ctx, tempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to write reconstructed tree: %w", err)
	}

	// Read the written data
	nodes, err := os.ReadFile(filepath.Join(tempDir, FileNameNodes))
	if err != nil {
		return nil, fmt.Errorf("failed to read nodes: %w", err)
	}

	leaves, err := os.ReadFile(filepath.Join(tempDir, FileNameLeaves))
	if err != nil {
		return nil, fmt.Errorf("failed to read leaves: %w", err)
	}

	kvs, err := os.ReadFile(filepath.Join(tempDir, FileNameKVs))
	if err != nil {
		return nil, fmt.Errorf("failed to read kvs: %w", err)
	}

	// Create layouts
	nodesLayout, err := NewNodes(nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to create nodes layout: %w", err)
	}

	leavesLayout, err := NewLeaves(leaves)
	if err != nil {
		return nil, fmt.Errorf("failed to create leaves layout: %w", err)
	}

	return &treeData{
		nodes:        nodes,
		leaves:       leaves,
		kvs:          kvs,
		nodesLayout:  nodesLayout,
		leavesLayout: leavesLayout,
	}, nil
}

// buildTreeFromNodes creates a tree from a map of nodes
func (ms *MergedSnapshot) buildTreeFromNodes(nodes map[string]*types.SnapshotNode) (*treeData, error) {
	// Create a new empty tree
	tree := NewEmptyTree(0, ms.version)

	// Add all nodes to the tree
	for _, node := range nodes {
		if node.Value != nil {
			tree.Set(node.Key, node.Value)
		}
	}

	// Write the tree to get the data
	ctx := context.Background()
	var tempDir string
	var err error
	tempDir, err = os.MkdirTemp("", "merged-snapshot")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	err = tree.WriteSnapshot(ctx, tempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to write tree: %w", err)
	}

	// Read the written data
	nodesData, err := os.ReadFile(filepath.Join(tempDir, FileNameNodes))
	if err != nil {
		return nil, fmt.Errorf("failed to read nodes: %w", err)
	}

	leavesData, err := os.ReadFile(filepath.Join(tempDir, FileNameLeaves))
	if err != nil {
		return nil, fmt.Errorf("failed to read leaves: %w", err)
	}

	kvsData, err := os.ReadFile(filepath.Join(tempDir, FileNameKVs))
	if err != nil {
		return nil, fmt.Errorf("failed to read kvs: %w", err)
	}

	// Create layouts
	nodesLayout, err := NewNodes(nodesData)
	if err != nil {
		return nil, fmt.Errorf("failed to create nodes layout: %w", err)
	}

	leavesLayout, err := NewLeaves(leavesData)
	if err != nil {
		return nil, fmt.Errorf("failed to create leaves layout: %w", err)
	}

	return &treeData{
		nodes:        nodesData,
		leaves:       leavesData,
		kvs:          kvsData,
		nodesLayout:  nodesLayout,
		leavesLayout: leavesLayout,
	}, nil
}

// createMmapFromData creates a memory-mapped file from data
func (ms *MergedSnapshot) createMmapFromData(data []byte, name string) (*MmapFile, error) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", name)
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name()) // Clean up temp file

	// Write data to temp file
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return nil, err
	}

	if err := tmpFile.Close(); err != nil {
		return nil, err
	}

	// Create mmap from the temp file
	return NewMmap(tmpFile.Name())
}

// Implement Snapshot interface methods for MergedSnapshot
func (ms *MergedSnapshot) Close() error {
	var errs []error

	if ms.nodesMap != nil {
		errs = append(errs, ms.nodesMap.Close())
	}
	if ms.leavesMap != nil {
		errs = append(errs, ms.leavesMap.Close())
	}
	if ms.kvsMap != nil {
		errs = append(errs, ms.kvsMap.Close())
	}

	return errors.Join(errs...)
}

func (ms *MergedSnapshot) IsEmpty() bool {
	return ms.root == nil
}

func (ms *MergedSnapshot) Version() uint32 {
	return ms.version
}

func (ms *MergedSnapshot) RootNode() MergedPersistedNode {
	if ms.IsEmpty() {
		panic("RootNode not supported on an empty snapshot")
	}
	return *ms.root
}

func (ms *MergedSnapshot) RootHash() []byte {
	if ms.IsEmpty() {
		return emptyHash
	}
	return ms.RootNode().Hash()
}

// MergedPersistedNode is a PersistedNode that works with MergedSnapshot
type MergedPersistedNode struct {
	snapshot *MergedSnapshot
	isLeaf   bool
	index    uint32
}

var _ Node = MergedPersistedNode{}

func (node MergedPersistedNode) branchNode() NodeLayout {
	return node.snapshot.nodesLayout.Node(node.index)
}

func (node MergedPersistedNode) leafNode() LeafLayout {
	return node.snapshot.leavesLayout.Leaf(node.index)
}

func (node MergedPersistedNode) Height() uint8 {
	if node.isLeaf {
		return 0
	}
	return node.branchNode().Height()
}

func (node MergedPersistedNode) IsLeaf() bool {
	return node.isLeaf
}

func (node MergedPersistedNode) Version() uint32 {
	if node.isLeaf {
		return node.leafNode().Version()
	}
	return node.branchNode().Version()
}

func (node MergedPersistedNode) Size() int64 {
	if node.isLeaf {
		return 1
	}
	return int64(node.branchNode().Size())
}

func (node MergedPersistedNode) Key() []byte {
	if node.isLeaf {
		return node.snapshot.LeafKey(node.index)
	}
	index := node.branchNode().KeyLeaf()
	return node.snapshot.LeafKey(index)
}

func (node MergedPersistedNode) Value() []byte {
	if !node.isLeaf {
		return nil
	}
	_, value := node.snapshot.LeafKeyValue(node.index)
	return value
}

func (node MergedPersistedNode) Left() Node {
	if node.isLeaf {
		panic("can't call Left on leaf node")
	}

	data := node.branchNode()
	preTrees := uint32(data.PreTrees())
	startLeaf := getStartLeaf(node.index, data.Size(), preTrees)
	keyLeaf := data.KeyLeaf()
	if startLeaf+1 == keyLeaf {
		return MergedPersistedNode{snapshot: node.snapshot, index: startLeaf, isLeaf: true}
	}
	return MergedPersistedNode{snapshot: node.snapshot, index: getLeftBranch(keyLeaf, preTrees)}
}

func (node MergedPersistedNode) Right() Node {
	if node.isLeaf {
		panic("can't call Right on leaf node")
	}

	data := node.branchNode()
	keyLeaf := data.KeyLeaf()
	preTrees := uint32(data.PreTrees())
	if keyLeaf == getEndLeaf(node.index, preTrees) {
		return MergedPersistedNode{snapshot: node.snapshot, index: keyLeaf, isLeaf: true}
	}
	return MergedPersistedNode{snapshot: node.snapshot, index: node.index - 1}
}

func (node MergedPersistedNode) SafeHash() []byte {
	return utils.Clone(node.Hash())
}

func (node MergedPersistedNode) Hash() []byte {
	if node.isLeaf {
		return node.leafNode().Hash()
	}
	return node.branchNode().Hash()
}

func (node MergedPersistedNode) Get(key []byte) ([]byte, uint32) {
	if node.isLeaf {
		if bytes.Equal(node.Key(), key) {
			return node.Value(), node.Version()
		}
		return nil, 0
	}

	// For branch nodes, traverse down the tree
	if bytes.Compare(key, node.Key()) < 0 {
		return node.Left().Get(key)
	}
	return node.Right().Get(key)
}

func (node MergedPersistedNode) GetByIndex(leafIndex uint32) ([]byte, []byte) {
	if node.isLeaf {
		if node.index == leafIndex {
			return node.Key(), node.Value()
		}
		return nil, nil
	}

	// For branch nodes, traverse down the tree
	data := node.branchNode()
	preTrees := uint32(data.PreTrees())
	startLeaf := getStartLeaf(node.index, data.Size(), preTrees)
	endLeaf := getEndLeaf(node.index, preTrees)

	if leafIndex >= startLeaf && leafIndex <= endLeaf {
		keyLeaf := data.KeyLeaf()
		if leafIndex < keyLeaf {
			return node.Left().GetByIndex(leafIndex)
		}
		return node.Right().GetByIndex(leafIndex)
	}

	return nil, nil
}

func (node MergedPersistedNode) Mutate(version, cowVersion uint32) *MemNode {
	// Convert to MemNode for modification
	memNode := &MemNode{
		version: version,
		key:     node.Key(),
		value:   node.Value(),
		height:  node.Height(),
		size:    node.Size(),
		hash:    node.Hash(),
	}

	if !node.isLeaf {
		memNode.left = node.Left()
		memNode.right = node.Right()
	}

	return memNode
}

func (ms *MergedSnapshot) Node(index uint32) MergedPersistedNode {
	return MergedPersistedNode{
		snapshot: ms,
		index:    index,
		isLeaf:   false,
	}
}

func (ms *MergedSnapshot) Leaf(index uint32) MergedPersistedNode {
	return MergedPersistedNode{
		snapshot: ms,
		index:    index,
		isLeaf:   true,
	}
}

func (ms *MergedSnapshot) Key(offset uint64) []byte {
	keyLen := binary.LittleEndian.Uint32(ms.kvs[offset:])
	offset += 4
	return ms.kvs[offset : offset+uint64(keyLen)]
}

func (ms *MergedSnapshot) KeyValue(offset uint64) ([]byte, []byte) {
	len := uint64(binary.LittleEndian.Uint32(ms.kvs[offset:]))
	offset += 4
	key := ms.kvs[offset : offset+len]
	offset += len
	len = uint64(binary.LittleEndian.Uint32(ms.kvs[offset:]))
	offset += 4
	value := ms.kvs[offset : offset+len]
	return key, value
}

func (ms *MergedSnapshot) LeafKey(index uint32) []byte {
	leaf := ms.leavesLayout.Leaf(index)
	offset := leaf.KeyOffset() + 4
	return ms.kvs[offset : offset+uint64(leaf.KeyLength())]
}

func (ms *MergedSnapshot) LeafKeyValue(index uint32) ([]byte, []byte) {
	leaf := ms.leavesLayout.Leaf(index)
	offset := leaf.KeyOffset() + 4
	length := uint64(leaf.KeyLength())
	key := ms.kvs[offset : offset+length]
	offset += length
	length = uint64(binary.LittleEndian.Uint32(ms.kvs[offset:]))
	offset += 4
	return key, ms.kvs[offset : offset+length]
}

// Internal methods needed by PersistedNode
func (ms *MergedSnapshot) nodesLen() int {
	return len(ms.nodes) / SizeNode
}

func (ms *MergedSnapshot) leavesLen() int {
	return len(ms.leaves) / SizeLeaf
}

func (ms *MergedSnapshot) getNodesLayout() Nodes {
	return ms.nodesLayout
}

func (ms *MergedSnapshot) getLeavesLayout() Leaves {
	return ms.leavesLayout
}

func (ms *MergedSnapshot) getNodes() []byte {
	return ms.nodes
}

func (ms *MergedSnapshot) getLeaves() []byte {
	return ms.leaves
}

func (ms *MergedSnapshot) getKvs() []byte {
	return ms.kvs
}

func (ms *MergedSnapshot) Export() *Exporter {
	return newExporter(ms.export)
}

func (ms *MergedSnapshot) export(callback func(*types.SnapshotNode) bool) {
	if ms.leavesLen() == 0 {
		return
	}

	if ms.leavesLen() == 1 {
		leaf := ms.Leaf(0)
		callback(&types.SnapshotNode{
			Height:  0,
			Version: int64(leaf.Version()),
			Key:     leaf.Key(),
			Value:   leaf.Value(),
		})
		return
	}

	var pendingTrees int
	var i, j uint32
	for ; i < uint32(ms.nodesLen()); i++ {
		// pending branch node
		node := ms.nodesLayout.Node(i)
		for pendingTrees < int(node.PreTrees())+2 {
			// add more leaf nodes
			leaf := ms.leavesLayout.Leaf(j)
			key, value := ms.KeyValue(leaf.KeyOffset())
			enode := &types.SnapshotNode{
				Height:  0,
				Version: int64(leaf.Version()),
				Key:     key,
				Value:   value,
			}
			j++
			pendingTrees++

			if callback(enode) {
				return
			}
		}
		enode := &types.SnapshotNode{
			Height:  int8(node.Height()),
			Version: int64(node.Version()),
			Key:     ms.LeafKey(node.KeyLeaf()),
		}
		pendingTrees--

		if callback(enode) {
			return
		}
	}
}

// LoadSnapshotWithMerge loads a snapshot, automatically merging incremental snapshots with their base snapshots
func LoadSnapshotWithMerge(snapshotDir string) (SnapshotInterface, error) {
	// First, check if it's an incremental snapshot
	incMetadata, err := readIncrementalSnapshotMetadata(snapshotDir)
	if err == nil {
		// It's an incremental snapshot, load base and merge using overlay approach
		baseSnapshotDir := filepath.Join(filepath.Dir(snapshotDir), fmt.Sprintf("snapshot-%d", incMetadata.BaseVersion))
		baseSnapshot, err := OpenSnapshot(baseSnapshotDir)
		if err != nil {
			return nil, fmt.Errorf("failed to load base snapshot %s: %w", baseSnapshotDir, err)
		}

		// Load the incremental snapshot data
		incSnapshot, err := OpenSnapshot(snapshotDir)
		if err != nil {
			return nil, fmt.Errorf("failed to load incremental snapshot: %w", err)
		}

		// Create overlay snapshot for fast restart
		overlay, err := NewOverlaySnapshot(baseSnapshot, []*Snapshot{incSnapshot})
		if err != nil {
			return nil, fmt.Errorf("failed to create overlay snapshot: %w", err)
		}

		return overlay, nil
	}

	// If that fails, try to load as a regular snapshot
	snapshot, err := OpenSnapshot(snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load snapshot: %w", err)
	}

	return snapshot, nil
}
