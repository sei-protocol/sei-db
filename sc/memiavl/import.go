package memiavl

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/sc/types"
)

var (
	nodeChanSize = 10000
	bufIOSize    = 64 * 1024 * 1024
)

type MultiTreeImporter struct {
	dir         string
	snapshotDir string
	height      int64
	importer    *TreeImporter
	fileLock    FileLock
}

func NewMultiTreeImporter(dir string, height uint64) (*MultiTreeImporter, error) {
	fmt.Printf("Creating new MultiTreeImporter for dir: %s, height: %d\n", dir, height)
	if height > math.MaxUint32 {
		return nil, fmt.Errorf("version overflows uint32: %d", height)
	}

	fmt.Printf("Attempting to lock file: %s\n", filepath.Join(dir, LockFileName))
	var fileLock FileLock
	fileLock, err := LockFile(filepath.Join(dir, LockFileName))
	if err != nil {
		fmt.Printf("Failed to lock file: %v\n", err)
		return nil, fmt.Errorf("fail to lock db: %w", err)
	}
	fmt.Printf("File locked successfully\n")

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
	fmt.Printf("Adding item of type: %T\n", item)
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
	fmt.Printf("Adding tree: %s\n", name)
	if mti.importer != nil {
		fmt.Printf("Closing existing importer\n")
		if err := mti.importer.Close(); err != nil {
			fmt.Printf("Error closing existing importer: %v\n", err)
			return err
		}
	}
	mti.importer = NewTreeImporter(filepath.Join(mti.tmpDir(), name), mti.height)
	fmt.Printf("New TreeImporter created for: %s\n", name)
	return nil
}

func (mti *MultiTreeImporter) AddNode(node *types.SnapshotNode) {
	mti.importer.Add(node)
}

func (mti *MultiTreeImporter) Close() error {
	fmt.Printf("Closing MultiTreeImporter\n")
	if mti.importer != nil {
		fmt.Printf("Closing TreeImporter\n")
		if err := mti.importer.Close(); err != nil {
			fmt.Printf("Error closing TreeImporter: %v\n", err)
			return err
		}
		mti.importer = nil
	}

	tmpDir := mti.tmpDir()
	fmt.Printf("Updating metadata file in: %s\n", tmpDir)
	if err := updateMetadataFile(tmpDir, mti.height); err != nil {
		fmt.Printf("Error updating metadata file: %v\n", err)
		return err
	}

	fmt.Printf("Renaming temporary directory to: %s\n", filepath.Join(mti.dir, mti.snapshotDir))
	if err := os.Rename(tmpDir, filepath.Join(mti.dir, mti.snapshotDir)); err != nil {
		fmt.Printf("Error renaming directory: %v\n", err)
		return err
	}

	fmt.Printf("Updating current symlink\n")
	if err := updateCurrentSymlink(mti.dir, mti.snapshotDir); err != nil {
		fmt.Printf("Error updating current symlink: %v\n", err)
		return err
	}
	fmt.Printf("Unlocking file\n")
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
	fmt.Printf("Starting import for dir: %s, version: %d\n", dir, version)
	if version > int64(math.MaxUint32) {
		return errors.New("version overflows uint32")
	}

	return writeSnapshot(context.Background(), dir, uint32(version), func(w *snapshotWriter) (uint32, error) {
		fmt.Printf("Writing snapshot\n")
		i := &importer{
			snapshotWriter: *w,
		}

		nodeCount := 0
		for node := range nodes {
			nodeCount++
			if nodeCount%1000 == 0 {
				fmt.Printf("Processed %d nodes\n", nodeCount)
			}
			if err := i.Add(node); err != nil {
				fmt.Printf("Error adding node: %v\n", err)
				return 0, err
			}
		}
		fmt.Printf("Finished processing nodes, total count: %d\n", nodeCount)

		switch len(i.leavesStack) {
		case 0:
			return 0, nil
		case 1:
			return i.leafCounter, nil
		default:
			return 0, fmt.Errorf("invalid node structure, found stack size %v after imported", len(i.leavesStack))
		}
	})
}

type importer struct {
	snapshotWriter

	// keep track of how many leaves has been written before the pending nodes
	leavesStack []uint32
	// keep track of the pending nodes
	nodeStack []*MemNode
}

func (i *importer) Add(n *types.SnapshotNode) error {
	if n.Version > int64(math.MaxUint32) {
		return errors.New("version overflows uint32")
	}

	if n.Height == 0 {
		node := &MemNode{
			height:  0,
			size:    1,
			version: uint32(n.Version),
			key:     n.Key,
			value:   n.Value,
		}
		nodeHash := node.Hash()
		if err := i.writeLeaf(node.version, node.key, node.value, nodeHash); err != nil {
			return err
		}
		i.leavesStack = append(i.leavesStack, i.leafCounter)
		i.nodeStack = append(i.nodeStack, node)
		return nil
	}

	// branch node
	keyLeaf := i.leavesStack[len(i.leavesStack)-2]
	leftNode := i.nodeStack[len(i.nodeStack)-2]
	rightNode := i.nodeStack[len(i.nodeStack)-1]

	node := &MemNode{
		height:  uint8(n.Height),
		size:    leftNode.size + rightNode.size,
		version: uint32(n.Version),
		key:     n.Key,
		left:    leftNode,
		right:   rightNode,
	}
	nodeHash := node.Hash()

	// remove unnecessary reference to avoid memory leak
	node.left = nil
	node.right = nil

	preTrees := uint8(len(i.nodeStack) - 2)
	if err := i.writeBranch(node.version, uint32(node.size), node.height, preTrees, keyLeaf, nodeHash); err != nil {
		return err
	}

	i.leavesStack = i.leavesStack[:len(i.leavesStack)-2]
	i.leavesStack = append(i.leavesStack, i.leafCounter)

	i.nodeStack = i.nodeStack[:len(i.nodeStack)-2]
	i.nodeStack = append(i.nodeStack, node)
	return nil
}

func updateMetadataFile(dir string, height int64) (returnErr error) {
	fmt.Printf("Updating metadata file for dir: %s, height: %d\n", dir, height)
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("Error reading directory: %v\n", err)
		return err
	}
	storeInfos := make([]proto.StoreInfo, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		snapshot, err := OpenSnapshot(filepath.Join(dir, name))
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
