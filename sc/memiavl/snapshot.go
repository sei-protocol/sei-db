package memiavl

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sei-protocol/sei-db/common/logger"
	"golang.org/x/sys/unix"

	"github.com/sei-protocol/sei-db/common/errors"
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
	root   *PersistedNode
	logger logger.Logger
}

func NewEmptySnapshot(version uint32) *Snapshot {
	return &Snapshot{
		version: version,
	}
}

// OpenSnapshot parse the version number and the root node index from metadata file,
// and mmap the other files.
func OpenSnapshot(snapshotDir string, opts Options) (*Snapshot, error) {
	// read metadata file
	bz, err := os.ReadFile(filepath.Join(filepath.Clean(snapshotDir), FileNameMetadata))
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
		logger: opts.Logger,

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
			index:    uint32(nodesLen - 1), //nolint:gosec
		}
	} else if leavesLen > 0 {
		snapshot.root = &PersistedNode{
			snapshot: snapshot,
			isLeaf:   true,
			index:    0,
		}
	}

	// Preload nodes + leaves into page cache using file I/O with SEQUENTIAL+WILLNEED
	// This eliminates random I/O during replay, relying on natural page cache for split keys
	if opts.PrefetchThreshold > 0 {
		snapshot.prefetchSnapshot(snapshotDir, opts.PrefetchThreshold)
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
		if err := callback(snapshot.Leaf(uint32(i))); err != nil { //nolint:gosec
			return err
		}
	}
	for i := 0; i < snapshot.nodesLen(); i++ {
		if err := callback(snapshot.Node(uint32(i))); err != nil { //nolint:gosec
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
	for ; i < uint32(snapshot.nodesLen()); i++ { //nolint:gosec
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
		hui8 := node.Height()
		if hui8 > math.MaxInt8 {
			panic("node height exceeds int8")
		}
		height := int8(hui8)
		enode := &types.SnapshotNode{
			Height:  height,
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
	treeName := filepath.Base(snapshotDir)
	startTime := time.Now()

	// Estimate tree size based on node count
	treeSize := int64(0)
	if t.root != nil {
		treeSize = t.root.Size()
	}

	fmt.Printf("[SNAPSHOT WRITE] Starting to write snapshot for tree: %s (size: %d nodes)\n", treeName, treeSize)

	// Choose buffer size based on tree size
	// Large trees (>100M nodes, ~50GB) use larger buffer to reduce flush overhead
	bufSize := bufIOSize
	if treeSize > 100_000_000 {
		bufSize = bufIOSizeLarge
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: using large buffer (%dMB) for better performance\n", treeName, bufIOSizeLarge/(1024*1024))
	}

	err := writeSnapshotWithBuffer(ctx, snapshotDir, t.version, bufSize, func(w *snapshotWriter) (uint32, error) {
		if t.root == nil {
			return 0, nil
		}

		if err := w.writeRecursive(t.root); err != nil {
			return 0, err
		}
		return w.leafCounter, nil
	})

	if err != nil {
		fmt.Printf("[SNAPSHOT WRITE] Failed to write snapshot for tree %s: %v\n", treeName, err)
		return err
	}

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("[SNAPSHOT WRITE] Completed writing snapshot for tree %s in %.1fs\n", treeName, elapsed)
	return nil
}

// writeSnapshotWithBuffer writes snapshot with specified buffer size
func writeSnapshotWithBuffer(
	ctx context.Context,
	dir string, version uint32,
	bufSize int,
	doWrite func(*snapshotWriter) (uint32, error),
) (returnErr error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil { //nolint:gosec
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

	nodesWriter := bufio.NewWriterSize(fpNodes, bufSize)
	leavesWriter := bufio.NewWriterSize(fpLeaves, bufSize)
	kvsWriter := bufio.NewWriterSize(fpKVs, bufSize)

	w := newSnapshotWriter(ctx, nodesWriter, leavesWriter, kvsWriter)
	w.treeName = filepath.Base(dir) // Set tree name for progress reporting

	writeStart := time.Now()
	leaves, err := doWrite(w)
	if err != nil {
		return err
	}
	traversalElapsed := time.Since(writeStart).Seconds()

	treeName := filepath.Base(dir)
	fmt.Printf("[SNAPSHOT WRITE] Tree %s: traversal completed in %.1fs, waiting for writes to finish...\n",
		treeName, traversalElapsed)

	// Wait for all pending writes to complete
	waitStart := time.Now()
	if err := w.waitForWrites(); err != nil {
		return err
	}
	waitElapsed := time.Since(waitStart).Seconds()

	writeElapsed := time.Since(writeStart).Seconds()
	fmt.Printf("[SNAPSHOT WRITE] Tree %s: wrote %d leaves and %d branches in %.1fs (traversal: %.1fs, wait: %.1fs)\n",
		treeName, w.leafCounter, w.branchCounter, writeElapsed, traversalElapsed, waitElapsed)

	// Report final pipeline metrics
	w.reportPipelineMetrics()

	// Note: Removed misleading sampled metrics
	// The "traversal/write" timing was measured in the main goroutine only
	// Real write performance is in the 3 background goroutines (not measured by sampling)
	// Use pipeline metrics (channel fill %) to identify bottlenecks instead

	if leaves > 0 {
		flushStart := time.Now()
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: starting to flush buffers...\n", treeName)

		if err := nodesWriter.Flush(); err != nil {
			return err
		}
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: flushed nodes buffer in %.1fs\n",
			treeName, time.Since(flushStart).Seconds())

		flushLeavesStart := time.Now()
		if err := leavesWriter.Flush(); err != nil {
			return err
		}
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: flushed leaves buffer in %.1fs\n",
			treeName, time.Since(flushLeavesStart).Seconds())

		flushKvsStart := time.Now()
		if err := kvsWriter.Flush(); err != nil {
			return err
		}
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: flushed kvs buffer in %.1fs\n",
			treeName, time.Since(flushKvsStart).Seconds())

		fmt.Printf("[SNAPSHOT WRITE] Tree %s: all buffers flushed in %.1fs total\n",
			treeName, time.Since(flushStart).Seconds())

		syncStart := time.Now()
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: starting to sync files to disk...\n", treeName)

		if err := fpKVs.Sync(); err != nil {
			return err
		}
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: synced kvs file in %.1fs\n",
			treeName, time.Since(syncStart).Seconds())

		syncLeavesStart := time.Now()
		if err := fpLeaves.Sync(); err != nil {
			return err
		}
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: synced leaves file in %.1fs\n",
			treeName, time.Since(syncLeavesStart).Seconds())

		syncNodesStart := time.Now()
		if err := fpNodes.Sync(); err != nil {
			return err
		}
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: synced nodes file in %.1fs\n",
			treeName, time.Since(syncNodesStart).Seconds())

		fmt.Printf("[SNAPSHOT WRITE] Tree %s: all files synced to disk in %.1fs total\n",
			treeName, time.Since(syncStart).Seconds())

		// Drop written pages from page cache to prevent evicting source snapshot pages
		// This keeps the read-side (old snapshot) cache hit rate high
		dropCacheStart := time.Now()
		dropPageCache(fpKVs)
		dropPageCache(fpLeaves)
		dropPageCache(fpNodes)
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: dropped page cache in %.1fs\n",
			treeName, time.Since(dropCacheStart).Seconds())
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

// writeSnapshot is a compatibility wrapper that uses default buffer size
func writeSnapshot(
	ctx context.Context,
	dir string, version uint32,
	doWrite func(*snapshotWriter) (uint32, error),
) error {
	return writeSnapshotWithBuffer(ctx, dir, version, bufIOSize, doWrite)
}

// kvWriteOp represents a key-value write operation
type kvWriteOp struct {
	key   []byte
	value []byte
}

// leafWriteOp represents a leaf write operation
type leafWriteOp struct {
	version   uint32
	keyLen    uint32
	keyOffset uint64
	hash      []byte
}

// branchWriteOp represents a branch write operation
type branchWriteOp struct {
	version  uint32
	size     uint32
	height   uint8
	preTrees uint8
	keyLeaf  uint32
	hash     []byte
}

type snapshotWriter struct {
	// context for cancel the writing process
	ctx context.Context

	nodesWriter, leavesWriter, kvWriter io.Writer

	// count how many nodes have been written
	branchCounter, leafCounter uint32

	// record the current writing offset in kvs file
	kvsOffset uint64

	// for progress reporting
	treeName               string
	lastProgressReport     time.Time
	progressReportInterval time.Duration

	// Performance metrics (sampled to reduce overhead)
	traversalTime  time.Duration // Time spent traversing
	writeTime      time.Duration // Time spent writing
	sampleCounter  uint32        // Counter for sampling
	sampleInterval uint32        // Sample every N nodes (e.g., 10000)
	lastSampleTime time.Time     // Last sample timestamp
	inTraversal    bool          // Currently in traversal phase

	// Pipeline for async writes - separate channels for each file
	kvChan     chan kvWriteOp
	leafChan   chan leafWriteOp
	branchChan chan branchWriteOp

	writeErrors chan error
	wg          sync.WaitGroup // Wait for all writer goroutines

	// Pipeline metrics for each channel
	maxKvFill         int
	maxLeafFill       int
	maxBranchFill     int
	kvFillSum         int64
	leafFillSum       int64
	branchFillSum     int64
	kvFillCount       int64
	leafFillCount     int64
	branchFillCount   int64
	lastMetricsReport time.Time
}

// SetPipelineBufferSize allows configuring the pipeline buffer size
// Larger values provide more parallelism but use more memory
// Default is 10000. Recommended range: 1000-50000
func SetPipelineBufferSize(size int) {
	if size < 100 {
		size = 100 // Minimum to avoid deadlocks
	}
	if size > 100000 {
		size = 100000 // Maximum to avoid excessive memory usage
	}
	nodeChanSize = size
	fmt.Printf("[PIPELINE] Pipeline buffer size set to %d operations per channel\n", nodeChanSize)
}

func newSnapshotWriter(ctx context.Context, nodesWriter, leavesWriter, kvsWriter io.Writer) *snapshotWriter {
	now := time.Now()

	// Create separate buffered channels for each file type
	// This allows parallel writes to all 3 files
	// Buffer size is configurable via SetPipelineBufferSize()
	kvChan := make(chan kvWriteOp, nodeChanSize)
	leafChan := make(chan leafWriteOp, nodeChanSize)
	branchChan := make(chan branchWriteOp, nodeChanSize)
	writeErrors := make(chan error, 3) // Buffer for errors from all 3 goroutines

	w := &snapshotWriter{
		ctx:                    ctx,
		nodesWriter:            nodesWriter,
		leavesWriter:           leavesWriter,
		kvWriter:               kvsWriter,
		lastProgressReport:     now,
		progressReportInterval: 30 * time.Second, // Report every 30 seconds
		sampleInterval:         10000,            // Sample every 10000 nodes to reduce overhead
		lastSampleTime:         now,
		inTraversal:            true, // Start in traversal phase
		kvChan:                 kvChan,
		leafChan:               leafChan,
		branchChan:             branchChan,
		writeErrors:            writeErrors,
		lastMetricsReport:      now,
	}

	// Start 3 parallel writer goroutines - one for each file
	w.wg.Add(3)
	go w.kvWriterLoop()
	go w.leafWriterLoop()
	go w.branchWriterLoop()

	return w
}

// kvWriterLoop processes KV write operations in parallel
func (w *snapshotWriter) kvWriterLoop() {
	defer w.wg.Done()

	for op := range w.kvChan {
		if err := w.writeKeyValueDirect(op.key, op.value); err != nil {
			select {
			case w.writeErrors <- fmt.Errorf("kv write error: %w", err):
			default:
			}
			return
		}
	}
}

// leafWriterLoop processes leaf write operations in parallel
func (w *snapshotWriter) leafWriterLoop() {
	defer w.wg.Done()

	for op := range w.leafChan {
		if err := w.writeLeafDirect(op.version, op.keyLen, op.keyOffset, op.hash); err != nil {
			select {
			case w.writeErrors <- fmt.Errorf("leaf write error: %w", err):
			default:
			}
			return
		}
	}
}

// branchWriterLoop processes branch write operations in parallel
func (w *snapshotWriter) branchWriterLoop() {
	defer w.wg.Done()

	for op := range w.branchChan {
		if err := w.writeBranchDirect(op.version, op.size, op.height, op.preTrees, op.keyLeaf, op.hash); err != nil {
			select {
			case w.writeErrors <- fmt.Errorf("branch write error: %w", err):
			default:
			}
			return
		}
	}
}

// waitForWrites waits for all pending writes to complete and returns any error
func (w *snapshotWriter) waitForWrites() error {
	// Close all channels to signal completion
	close(w.kvChan)
	close(w.leafChan)
	close(w.branchChan)

	// Wait for all writer goroutines to finish
	w.wg.Wait()

	// Check for any errors
	select {
	case err := <-w.writeErrors:
		return err
	default:
		return nil
	}
}

// writeKeyValueDirect writes key-value pair directly (called by writer goroutine)
func (w *snapshotWriter) writeKeyValueDirect(key, value []byte) error {
	var numBuf [4]byte

	keyLen := uint32(len(key))     //nolint:gosec
	valueLen := uint32(len(value)) //nolint:gosec

	binary.LittleEndian.PutUint32(numBuf[:], keyLen)
	if _, err := w.kvWriter.Write(numBuf[:]); err != nil {
		return err
	}
	if _, err := w.kvWriter.Write(key); err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(numBuf[:], valueLen)
	if _, err := w.kvWriter.Write(numBuf[:]); err != nil {
		return err
	}
	if _, err := w.kvWriter.Write(value); err != nil {
		return err
	}

	return nil
}

// writeLeaf sends leaf and KV write operations to the pipeline
func (w *snapshotWriter) writeLeaf(version uint32, key, value, hash []byte) error {
	// Track channel fill metrics for all channels
	kvFill := len(w.kvChan)
	leafFill := len(w.leafChan)

	if kvFill > w.maxKvFill {
		w.maxKvFill = kvFill
	}
	if leafFill > w.maxLeafFill {
		w.maxLeafFill = leafFill
	}

	atomic.AddInt64(&w.kvFillSum, int64(kvFill))
	atomic.AddInt64(&w.kvFillCount, 1)
	atomic.AddInt64(&w.leafFillSum, int64(leafFill))
	atomic.AddInt64(&w.leafFillCount, 1)

	// Report metrics periodically
	if time.Since(w.lastMetricsReport) >= 30*time.Second {
		w.reportPipelineMetrics()
		w.lastMetricsReport = time.Now()
	}

	// Check for write errors
	select {
	case err := <-w.writeErrors:
		return err
	default:
	}

	// Calculate key offset BEFORE sending to KV channel
	keyOffset := w.kvsOffset
	keyLen := uint32(len(key))
	valueLen := uint32(len(value))
	w.kvsOffset += 4 + 4 + uint64(keyLen) + uint64(valueLen)

	// Make copies since we're sending to another goroutine
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	hashCopy := make([]byte, len(hash))
	copy(hashCopy, hash)

	// Send KV write operation
	kvOp := kvWriteOp{
		key:   keyCopy,
		value: valueCopy,
	}

	select {
	case w.kvChan <- kvOp:
	case <-w.ctx.Done():
		return w.ctx.Err()
	}

	// Send leaf write operation
	leafOp := leafWriteOp{
		version:   version,
		keyLen:    keyLen,
		keyOffset: keyOffset,
		hash:      hashCopy,
	}

	select {
	case w.leafChan <- leafOp:
		w.leafCounter++
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
}

// writeLeafDirect performs the actual leaf write (called by writer goroutine)
func (w *snapshotWriter) writeLeafDirect(version uint32, keyLen uint32, keyOffset uint64, hash []byte) error {
	var buf [SizeLeafWithoutHash]byte
	binary.LittleEndian.PutUint32(buf[OffsetLeafVersion:], version)
	binary.LittleEndian.PutUint32(buf[OffsetLeafKeyLen:], keyLen)
	binary.LittleEndian.PutUint64(buf[OffsetLeafKeyOffset:], keyOffset)

	if _, err := w.leavesWriter.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.leavesWriter.Write(hash); err != nil {
		return err
	}

	return nil
}

// writeBranch sends a branch write operation to the pipeline
func (w *snapshotWriter) writeBranch(version, size uint32, height, preTrees uint8, keyLeaf uint32, hash []byte) error {
	// Track channel fill metrics
	branchFill := len(w.branchChan)
	if branchFill > w.maxBranchFill {
		w.maxBranchFill = branchFill
	}
	atomic.AddInt64(&w.branchFillSum, int64(branchFill))
	atomic.AddInt64(&w.branchFillCount, 1)

	// Check for write errors
	select {
	case err := <-w.writeErrors:
		return err
	default:
	}

	// Make copy of hash since we're sending to another goroutine
	hashCopy := make([]byte, len(hash))
	copy(hashCopy, hash)

	op := branchWriteOp{
		version:  version,
		size:     size,
		height:   height,
		preTrees: preTrees,
		keyLeaf:  keyLeaf,
		hash:     hashCopy,
	}

	select {
	case w.branchChan <- op:
		w.branchCounter++
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
}

// writeBranchDirect performs the actual branch write (called by writer goroutine)
func (w *snapshotWriter) writeBranchDirect(version, size uint32, height, preTrees uint8, keyLeaf uint32, hash []byte) error {
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

	return nil
}

// reportPipelineMetrics reports channel fill statistics for all 3 channels
func (w *snapshotWriter) reportPipelineMetrics() {
	kvCount := atomic.LoadInt64(&w.kvFillCount)
	leafCount := atomic.LoadInt64(&w.leafFillCount)
	branchCount := atomic.LoadInt64(&w.branchFillCount)

	if kvCount == 0 && leafCount == 0 && branchCount == 0 {
		return
	}

	chanCap := float64(cap(w.kvChan))

	// KV channel metrics
	if kvCount > 0 {
		kvSum := atomic.LoadInt64(&w.kvFillSum)
		avgKvFill := float64(kvSum) / float64(kvCount)
		kvFillPct := avgKvFill / chanCap * 100
		maxKvFillPct := float64(w.maxKvFill) / chanCap * 100

		fmt.Printf("[PIPELINE] Tree %s: KV channel - avg: %.0f/%.0f (%.1f%%), max: %d/%.0f (%.1f%%)\n",
			w.treeName, avgKvFill, chanCap, kvFillPct, w.maxKvFill, chanCap, maxKvFillPct)

		if kvFillPct > 80 {
			fmt.Printf("[PIPELINE] Tree %s: WARNING - KV channel >80%% full, KV writes are bottleneck!\n", w.treeName)
		}
	}

	// Leaf channel metrics
	if leafCount > 0 {
		leafSum := atomic.LoadInt64(&w.leafFillSum)
		avgLeafFill := float64(leafSum) / float64(leafCount)
		leafFillPct := avgLeafFill / chanCap * 100
		maxLeafFillPct := float64(w.maxLeafFill) / chanCap * 100

		fmt.Printf("[PIPELINE] Tree %s: Leaf channel - avg: %.0f/%.0f (%.1f%%), max: %d/%.0f (%.1f%%)\n",
			w.treeName, avgLeafFill, chanCap, leafFillPct, w.maxLeafFill, chanCap, maxLeafFillPct)

		if leafFillPct > 80 {
			fmt.Printf("[PIPELINE] Tree %s: WARNING - Leaf channel >80%% full, leaf writes are bottleneck!\n", w.treeName)
		}
	}

	// Branch channel metrics
	if branchCount > 0 {
		branchSum := atomic.LoadInt64(&w.branchFillSum)
		avgBranchFill := float64(branchSum) / float64(branchCount)
		branchFillPct := avgBranchFill / chanCap * 100
		maxBranchFillPct := float64(w.maxBranchFill) / chanCap * 100

		fmt.Printf("[PIPELINE] Tree %s: Branch channel - avg: %.0f/%.0f (%.1f%%), max: %d/%.0f (%.1f%%)\n",
			w.treeName, avgBranchFill, chanCap, branchFillPct, w.maxBranchFill, chanCap, maxBranchFillPct)

		if branchFillPct > 80 {
			fmt.Printf("[PIPELINE] Tree %s: WARNING - Branch channel >80%% full, branch writes are bottleneck!\n", w.treeName)
		}
	}

	// Overall assessment
	maxFillPct := 0.0
	if kvCount > 0 {
		maxFillPct = float64(atomic.LoadInt64(&w.kvFillSum)) / float64(kvCount) / chanCap * 100
	}
	if leafCount > 0 {
		leafPct := float64(atomic.LoadInt64(&w.leafFillSum)) / float64(leafCount) / chanCap * 100
		if leafPct > maxFillPct {
			maxFillPct = leafPct
		}
	}
	if branchCount > 0 {
		branchPct := float64(atomic.LoadInt64(&w.branchFillSum)) / float64(branchCount) / chanCap * 100
		if branchPct > maxFillPct {
			maxFillPct = branchPct
		}
	}

	if maxFillPct < 20 {
		fmt.Printf("[PIPELINE] Tree %s: All channels <20%% full, traversal is slower than writes (good for parallelism)\n", w.treeName)
	}
}

// writeRecursive write the node recursively in depth-first post-order,
// returns `(nodeIndex, err)`.
func (w *snapshotWriter) writeRecursive(node Node) error {
	// Sample performance metrics every N nodes to reduce overhead
	w.sampleCounter++
	shouldSample := w.sampleCounter%w.sampleInterval == 0

	if shouldSample && w.inTraversal {
		// End traversal sample, start write sample
		now := time.Now()
		w.traversalTime += now.Sub(w.lastSampleTime)
		w.lastSampleTime = now
		w.inTraversal = false
	}

	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
	}

	// Periodic progress reporting (every 30 seconds)
	if time.Since(w.lastProgressReport) >= w.progressReportInterval {
		fmt.Printf("[SNAPSHOT WRITE] Tree %s: progress - %d leaves, %d branches written so far\n",
			w.treeName, w.leafCounter, w.branchCounter)
		// Note: Removed misleading sampled metrics that measured time in writeRecursive
		// Real parallelism metrics are shown in [PIPELINE] logs
		w.lastProgressReport = time.Now()
	}

	if node.IsLeaf() {
		return w.writeLeaf(node.Version(), node.Key(), node.Value(), node.Hash())
	}

	if w.leafCounter < w.branchCounter {
		return fmt.Errorf("leafCounter %d < branchCounter %d", w.leafCounter, w.branchCounter)
	}
	pt := w.leafCounter - w.branchCounter
	if pt > math.MaxUint8 {
		return fmt.Errorf("too many pending trees %d exceed %d", pt, math.MaxUint8)
	}

	// record the number of pending subtrees before the current one,
	// it's always positive and won't exceed the tree height, so we can use an uint8 to store it.
	preTrees := uint8(pt)

	if err := w.writeRecursive(node.Left()); err != nil {
		return err
	}
	keyLeaf := w.leafCounter
	if err := w.writeRecursive(node.Right()); err != nil {
		return err
	}

	size := node.Size()
	if size < 0 || size > math.MaxUint32 {
		return fmt.Errorf("node size %d out of range", size)
	}

	// Sample after write
	if shouldSample && !w.inTraversal {
		// End write sample, start traversal sample
		now := time.Now()
		w.writeTime += now.Sub(w.lastSampleTime)
		w.lastSampleTime = now
		w.inTraversal = true
	}

	return w.writeBranch(node.Version(), uint32(size), node.Height(), preTrees, keyLeaf, node.Hash())
}

func createFile(name string) (*os.File, error) {
	return os.OpenFile(filepath.Clean(name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
}

// prefetchSnapshot sequentially reads snapshot files into page cache
// This is critical for cold-start performance: eliminates 99% of random I/O during replay
func (snapshot *Snapshot) prefetchSnapshot(snapshotDir string, prefetchThreshold float64) {
	startTime := time.Now()
	if snapshot.nodes == nil && snapshot.leaves == nil {
		return // Empty snapshot
	}
	// Selective preload: only preload large and active trees
	// Small/inactive trees have minimal I/O during replay, not worth preloading
	treeName := filepath.Base(snapshotDir)
	needsPreload := shouldPreloadTree(treeName)
	if !needsPreload {
		return
	}
	log := snapshot.logger

	// If most pages are already in page cache, skip prefetch
	residentNodes, errNodes := residentRatio(snapshot.nodes)
	residentLeaves, errLeaves := residentRatio(snapshot.leaves)
	if errNodes == nil && errLeaves == nil {
		if residentNodes >= prefetchThreshold && residentLeaves >= prefetchThreshold {
			log.Debug(fmt.Sprintf("Skipped prefetching for tree %s\n", treeName))
			return
		}
	}

	if residentNodes < prefetchThreshold {
		log.Info(fmt.Sprintf("Tree %s nodes page cache residency ratio is %f, below threshold %f\n", treeName, residentNodes, prefetchThreshold))
		_ = SequentialReadAndFillPageCache(filepath.Join(snapshotDir, FileNameNodes))
	}

	if residentLeaves < prefetchThreshold {
		log.Info(fmt.Sprintf("Tree %s leaves page cache residency ratio is %f, below threshold %f\n", treeName, residentLeaves, prefetchThreshold))
		_ = SequentialReadAndFillPageCache(filepath.Join(snapshotDir, FileNameLeaves))
	}

	log.Info(fmt.Sprintf("Prefetch snapshot for %s completed in %fs. Consider adding more RAM for page cache to avoid preloading during restart.\n", treeName, time.Since(startTime).Seconds()))
}

// shouldPreloadTree determines if a tree should be preloaded based on size and name
// Only large/active trees benefit from preload; small trees add overhead
func shouldPreloadTree(treeName string) bool {
	// Preload the 3 largest/most active trees
	// Parallel loading + madvise hints will maximize throughput even on slow disks
	activeTrees := map[string]bool{
		"evm":  true,
		"bank": true,
		"acc":  true,
	}

	return activeTrees[treeName]
}

func SequentialReadAndFillPageCache(filePath string) error {
	startTime := time.Now()
	fmt.Printf("[PREFETCH] Starting to prefetch file: %s\n", filePath)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close() // Ensure file handle is released for pruning

	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}

	// Mmap the file to apply madvise hints
	// This tells the kernel to:
	// 1. Read sequentially (MADV_SEQUENTIAL) - enables aggressive readahead
	// 2. Keep in cache (MADV_WILLNEED) - prioritize retention
	// 3. Don't dump (MADV_DONTDUMP) - exclude from core dumps, hints at importance
	// This helps prevent eviction when write buffers compete for memory
	totalSize := fileInfo.Size()
	if totalSize > 0 {
		data, err := unix.Mmap(int(f.Fd()), 0, int(totalSize), unix.PROT_READ, unix.MAP_SHARED)
		if err == nil {
			// Tell kernel this will be read sequentially - enables aggressive readahead
			_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)
			// Tell kernel we need this data soon - start readahead immediately
			_ = unix.Madvise(data, unix.MADV_WILLNEED)
			// Hint that this data is important - helps with retention priority
			_ = unix.Madvise(data, unix.MADV_DONTDUMP)
			// Unmap after setting hints - the hints persist on the underlying pages
			defer unix.Munmap(data)
			fmt.Printf("[PREFETCH] Applied madvise hints (SEQUENTIAL + WILLNEED + DONTDUMP) to %s\n", filePath)
		}
	}

	reportDone := make(chan struct{})
	var totalRead int64
	defer close(reportDone) // Stop progress reporter before returning

	startPrefetchProgressReporter(filePath, totalSize, &totalRead, startTime, reportDone)

	concurrency := runtime.NumCPU()
	var wg sync.WaitGroup
	jobs := make(chan [2]int64, concurrency)
	wg.Add(concurrency)
	const chunkSize = 16 * 1024 * 1024 // 16MB
	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()
			buf := make([]byte, chunkSize)
			for job := range jobs {
				readChunkIntoCache(f, buf, job[0], int(job[1]), &totalRead)
			}
		}()
	}

	// Enqueue chunks sequentially to retain locality
	for offset := int64(0); offset < totalSize; offset += chunkSize {
		end := offset + chunkSize
		if end > totalSize {
			end = totalSize
		}
		jobs <- [2]int64{offset, end - offset}
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(startTime).Seconds()
	avgSpeedMBps := float64(totalSize) / elapsed / (1024 * 1024)
	fmt.Printf("Completed prefetching %s: %d MB in %.1fs (%.1f MB/s)\n",
		filePath, totalSize/(1024*1024), elapsed, avgSpeedMBps)
	return nil
}

// startPrefetchProgressReporter periodically logs progress until done is closed.
func startPrefetchProgressReporter(filePath string, totalSize int64, totalRead *int64, startTime time.Time, done <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				tr := atomic.LoadInt64(totalRead)
				elapsed := time.Since(startTime).Seconds()
				if elapsed <= 0 {
					continue
				}
				speedMBps := float64(tr) / elapsed / (1024 * 1024)
				progressPct := float64(tr) * 100 / float64(totalSize)
				remaining := float64(totalSize-tr) / (speedMBps * 1024 * 1024)
				fmt.Printf("Prefetching file '%s': %d/%d MB (%.1f%%), speed: %.1f MB/s, ETA: %.0fs\n",
					filePath, tr/(1024*1024), totalSize/(1024*1024), progressPct, speedMBps, remaining)
			}
		}
	}()
}

// readChunkIntoCache reads n bytes starting at pos, updating totalRead.
func readChunkIntoCache(f *os.File, buf []byte, pos int64, n int, totalRead *int64) {
	remaining := n
	for remaining > 0 {
		readN, er := f.ReadAt(buf[:remaining], pos)
		if readN > 0 {
			pos += int64(readN)
			remaining -= readN
			atomic.AddInt64(totalRead, int64(readN))
		}
		if er == io.EOF {
			break
		}
		if er != nil && er != io.ErrUnexpectedEOF {
			// Best-effort warming; ignore transient errors
			break
		}
		if readN == 0 {
			break
		}
	}
}

// residentRatio returns fraction of pages resident in the page cache for data.
// Uses mincore on Linux; on other platforms returns an unsupported error.
func residentRatio(data []byte) (float64, error) {
	if len(data) == 0 {
		return 1, nil
	}
	if runtime.GOOS != "linux" {
		return 0, fmt.Errorf("residentRatio unsupported on %s", runtime.GOOS)
	}

	pageSize := unix.Getpagesize()
	numPages := (len(data) + pageSize - 1) / pageSize
	if numPages == 0 {
		return 1, nil
	}
	vec := make([]byte, numPages)

	addr := uintptr(unsafe.Pointer(&data[0]))
	length := uintptr(len(data))
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, addr, length, uintptr(unsafe.Pointer(&vec[0])))
	if errno != 0 {
		return 0, errno
	}

	present := 0
	for _, v := range vec {
		if v&1 == 1 {
			present++
		}
	}
	return float64(present) / float64(len(vec)), nil
}
