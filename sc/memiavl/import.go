package memiavl

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/sc/types"
)

var (
	// Pipeline buffer size - controls how many operations can be queued
	// Larger values allow more parallelism between traversal and writes
	// Increased to 2000000 to prevent channel saturation (was seeing 82.8% fill)
	// Memory usage: ~2000000 * (avg_op_size ~120 bytes) * 3 channels = ~720MB
	// This allows ~0.4% of EVM tree (512M nodes) to buffer, preventing bottleneck
	// Trade-off: 720MB memory for preventing write goroutines from blocking traversal
	nodeChanSize = 2000000

	// Increased from 64MB to 256MB for better write performance
	// Larger buffer reduces system calls and improves throughput
	// For EVM tree (81GB), this reduces flush count from 633 to 316
	bufIOSize = 256 * 1024 * 1024

	// Extra large buffer for very large trees (like EVM)
	// Used when tree size > 50GB to further reduce flush overhead
	// Analysis shows:
	//   - 256MB buffer: frequent auto-flush blocks writer goroutines → channel fills → traversal blocks → 66k nodes/s
	//   - 2GB buffer: rare auto-flush, writers keep up with traversal → stable 270k nodes/s
	// Root cause: bufio.Writer.Write() blocks when buffer is full during flush
	// Trade-off: 6GB memory (3 files × 2GB) for 4x better performance
	// Note: Real bottleneck is disk (1800 read IOPS, 100% util) not memory
	// TODO: Consider Export/Import approach for sequential I/O instead of recursive traversal
	bufIOSizeLarge = 2 * 1024 * 1024 * 1024 // 2GB
)

type MultiTreeImporter struct {
	dir         string
	snapshotDir string
	height      int64
	importer    *TreeImporter
	fileLock    FileLock
}

func NewMultiTreeImporter(dir string, height uint64) (*MultiTreeImporter, error) {
	if height > math.MaxUint32 {
		return nil, fmt.Errorf("version overflows uint32: %d", height)
	}

	var fileLock FileLock
	fileLock, err := LockFile(filepath.Join(dir, LockFileName))
	if err != nil {
		return nil, fmt.Errorf("fail to lock db: %w", err)
	}

	return &MultiTreeImporter{
		dir:         dir,
		height:      int64(height),
		snapshotDir: snapshotName(int64(height)),
		fileLock:    fileLock,
	}, nil
}

func (mti *MultiTreeImporter) tmpDir() string {
	return filepath.Join(mti.dir, mti.snapshotDir+"-tmp")
}

func (mti *MultiTreeImporter) Add(item interface{}) error {
	switch item := item.(type) {
	case *types.SnapshotNode:
		mti.AddNode(item)
		return nil
	case string:
		return mti.AddTree(item)
	default:
		return fmt.Errorf("unknown item type: %T", item)
	}
}

func (mti *MultiTreeImporter) AddTree(name string) error {
	if mti.importer != nil {
		if err := mti.importer.Close(); err != nil {
			return err
		}
	}
	mti.importer = NewTreeImporter(filepath.Join(mti.tmpDir(), name), mti.height)
	return nil
}

func (mti *MultiTreeImporter) AddNode(node *types.SnapshotNode) {
	mti.importer.Add(node)
}

func (mti *MultiTreeImporter) Close() error {
	if mti.importer != nil {
		if err := mti.importer.Close(); err != nil {
			return err
		}
		mti.importer = nil
	}

	tmpDir := mti.tmpDir()
	if err := updateMetadataFile(tmpDir, mti.height); err != nil {
		return err
	}

	if err := os.Rename(tmpDir, filepath.Join(mti.dir, mti.snapshotDir)); err != nil {
		return err
	}

	if err := updateCurrentSymlink(mti.dir, mti.snapshotDir); err != nil {
		return err
	}
	return mti.fileLock.Unlock()
}

// TreeImporter import a single memiavl tree from state-sync snapshot
type TreeImporter struct {
	nodesChan chan *types.SnapshotNode
	quitChan  chan error
}

func NewTreeImporter(dir string, version int64) *TreeImporter {
	nodesChan := make(chan *types.SnapshotNode, nodeChanSize)
	quitChan := make(chan error)
	go func() {
		defer close(quitChan)
		quitChan <- doImport(dir, version, nodesChan)
	}()
	return &TreeImporter{nodesChan, quitChan}
}

func (ai *TreeImporter) Add(node *types.SnapshotNode) {
	ai.nodesChan <- node
}

func (ai *TreeImporter) Close() error {
	var err error
	// tolerate double close
	if ai.nodesChan != nil {
		close(ai.nodesChan)
		err = <-ai.quitChan
	}
	ai.nodesChan = nil
	ai.quitChan = nil
	return err
}

// doImport a stream of `types.SnapshotNode`s into a new snapshot.
func doImport(dir string, version int64, nodes <-chan *types.SnapshotNode) (returnErr error) {
	if version < 0 || version > int64(math.MaxUint32) {
		return fmt.Errorf("version under/overflows uint32: %d", version)
	}

	return writeSnapshot(context.Background(), dir, uint32(version), func(w *snapshotWriter) (uint32, error) {
		i := &importer{
			w:           w,
			leavesStack: make([]uint32, 0),
			nodeStack:   make([]*MemNode, 0),
		}

		for node := range nodes {
			if err := i.Add(node); err != nil {
				return 0, err
			}
		}

		switch len(i.leavesStack) {
		case 0:
			return 0, nil
		case 1:
			return i.w.leafCounter, nil
		default:
			return 0, fmt.Errorf("invalid node structure, found stack size %v after imported", len(i.leavesStack))
		}
	})
}

type importer struct {
	w *snapshotWriter

	// keep track of how many leaves has been written before the pending nodes
	leavesStack []uint32
	// keep track of the pending nodes
	nodeStack []*MemNode
}

func (i *importer) Add(n *types.SnapshotNode) error {
	if n.Version < 0 || n.Version > math.MaxUint32 {
		return fmt.Errorf("node version under/overflows uint32: %d", n.Version)
	}
	version := uint32(n.Version)

	if n.Height == 0 {
		node := &MemNode{
			height:  0,
			size:    1,
			version: version,
			key:     n.Key,
			value:   n.Value,
		}
		nodeHash := node.Hash()
		if err := i.w.writeLeaf(node.version, node.key, node.value, nodeHash); err != nil {
			return err
		}
		i.leavesStack = append(i.leavesStack, i.w.leafCounter)
		i.nodeStack = append(i.nodeStack, node)
		return nil
	}

	// branch node
	keyLeaf := i.leavesStack[len(i.leavesStack)-2]
	leftNode := i.nodeStack[len(i.nodeStack)-2]
	rightNode := i.nodeStack[len(i.nodeStack)-1]

	if n.Height < 0 {
		return fmt.Errorf("node height under/overflows uint8: %d", n.Height)
	}

	node := &MemNode{
		height:  uint8(n.Height),
		size:    leftNode.size + rightNode.size,
		version: version,
		key:     n.Key,
		left:    leftNode,
		right:   rightNode,
	}
	nodeHash := node.Hash()

	// remove unnecessary reference to avoid memory leak
	node.left = nil
	node.right = nil

	pt := len(i.nodeStack) - 2
	if pt < 0 || pt > math.MaxUint8 {
		return fmt.Errorf("preTrees out of range: %d", pt)
	}
	preTrees := uint8(pt)
	if node.size < 0 || node.size > math.MaxUint32 {
		return fmt.Errorf("node size under/overflows uint32: %d", node.size)
	}
	if err := i.w.writeBranch(node.version, uint32(node.size), node.height, preTrees, keyLeaf, nodeHash); err != nil {
		return err
	}

	i.leavesStack = i.leavesStack[:len(i.leavesStack)-2]
	i.leavesStack = append(i.leavesStack, i.w.leafCounter)

	i.nodeStack = i.nodeStack[:len(i.nodeStack)-2]
	i.nodeStack = append(i.nodeStack, node)
	return nil
}

func updateMetadataFile(dir string, height int64) (returnErr error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	storeInfos := make([]proto.StoreInfo, 0, len(entries))
	opts := Options{PrefetchThreshold: 0}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		snapshot, err := OpenSnapshot(filepath.Join(dir, name), opts)
		if err != nil {
			return err
		}
		defer func() {
			if err := snapshot.Close(); returnErr == nil {
				returnErr = err
			}
		}()
		storeInfos = append(storeInfos, proto.StoreInfo{
			Name: name,
			CommitId: proto.CommitID{
				Version: height,
				Hash:    snapshot.RootHash(),
			},
		})
	}
	metadata := proto.MultiTreeMetadata{
		CommitInfo: &proto.CommitInfo{
			Version:    height,
			StoreInfos: storeInfos,
		},
		// initial version should correspond to the first rlog entry
		InitialVersion: height + 1,
	}
	bz, err := metadata.Marshal()
	if err != nil {
		return err
	}
	return WriteFileSync(filepath.Join(dir, MetadataFileName), bz)
}
