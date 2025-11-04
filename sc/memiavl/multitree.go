package memiavl

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/alitto/pond"
	"golang.org/x/exp/slices"
	"golang.org/x/sys/unix"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/common/errors"
	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/common/metrics"
	"github.com/sei-protocol/sei-db/common/utils"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/stream/types"
)

const MetadataFileName = "__metadata"

type NamedTree struct {
	*Tree
	Name string
}

// MultiTree manages multiple memiavl tree together,
// all the trees share the same latest version, the snapshots are always created at the same version.
//
// The snapshot structure is like this:
// ```
// > snapshot-V
// >  metadata
// >  bank
// >   kvs
// >   nodes
// >   metadata
// >  acc
// >  other stores...
// ```
type MultiTree struct {
	// if the tree is start from genesis, it's the initial version of the chain,
	// if the tree is imported from snapshot, it's the imported version plus one,
	// it always corresponds to the rlog entry with index 1.
	initialVersion uint32

	zeroCopy bool
	logger   logger.Logger

	trees          []NamedTree    // always ordered by tree name
	treesByName    map[string]int // index of the trees by name
	lastCommitInfo proto.CommitInfo

	// the initial metadata loaded from disk snapshot
	metadata proto.MultiTreeMetadata
}

func NewEmptyMultiTree(initialVersion uint32) *MultiTree {
	return &MultiTree{
		initialVersion: initialVersion,
		treesByName:    make(map[string]int),
		zeroCopy:       true,
		logger:         logger.NewNopLogger(),
	}
}

func LoadMultiTree(dir string, opts Options) (*MultiTree, error) {
	startTime := time.Now()
	log := opts.Logger
	metadata, err := readMetadata(dir)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	treeMap := make(map[string]*Tree, len(entries))
	treeNames := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		treeNames = append(treeNames, name)
		snapshot, err := OpenSnapshot(filepath.Join(dir, name), opts)
		if err != nil {
			return nil, err
		}
		treeMap[name] = NewFromSnapshot(snapshot, opts)
	}
	timeElapsed := time.Since(startTime).Seconds()
	log.Info(fmt.Sprintf("All %d memIAVL trees loaded in %.1fs\n", len(treeNames), timeElapsed))
	if timeElapsed > 600 {
		log.Info("Loading MemIAVL tree from disk is too slow! Consider increasing the disk bandwidth to speed up the initialization time.\n")
	}
	slices.Sort(treeNames)

	trees := make([]NamedTree, len(treeNames))
	treesByName := make(map[string]int, len(trees))
	for i, name := range treeNames {
		tree := treeMap[name]
		trees[i] = NamedTree{Tree: tree, Name: name}
		treesByName[name] = i
	}

	mtree := &MultiTree{
		trees:          trees,
		treesByName:    treesByName,
		lastCommitInfo: *metadata.CommitInfo,
		metadata:       *metadata,
		zeroCopy:       opts.ZeroCopy,
		logger:         opts.Logger,
	}
	// initial version is necessary for rlog index conversion
	mtree.setInitialVersion(metadata.InitialVersion)
	return mtree, nil
}

// TreeByName returns the tree by name, returns nil if not found
func (t *MultiTree) TreeByName(name string) *Tree {
	if i, ok := t.treesByName[name]; ok {
		return t.trees[i].Tree
	}
	return nil
}

// Trees returns all the trees together with the name, ordered by name.
func (t *MultiTree) Trees() []NamedTree {
	return t.trees
}

func (t *MultiTree) SetInitialVersion(initialVersion int64) error {
	if initialVersion >= math.MaxUint32 {
		return fmt.Errorf("version overflows uint32: %d", initialVersion)
	}

	if t.Version() != 0 {
		return fmt.Errorf("multi tree is not empty: %d", t.Version())
	}

	for _, entry := range t.trees {
		if !entry.IsEmpty() {
			return fmt.Errorf("tree is not empty: %s", entry.Name)
		}
	}

	t.setInitialVersion(initialVersion)
	return nil
}

func (t *MultiTree) setInitialVersion(initialVersion int64) {
	if initialVersion < 0 || initialVersion > math.MaxUint32 {
		panic(fmt.Sprintf("initial version %d is out of range", initialVersion))
	}
	iv := uint32(initialVersion)
	t.initialVersion = iv
	for _, entry := range t.trees {
		entry.initialVersion = t.initialVersion
	}
}

func (t *MultiTree) SetZeroCopy(zeroCopy bool) {
	t.zeroCopy = zeroCopy
	for _, entry := range t.trees {
		entry.SetZeroCopy(zeroCopy)
	}
}

// Copy returns a snapshot of the tree which won't be corrupted by further modifications on the main tree.
func (t *MultiTree) Copy() *MultiTree {
	trees := make([]NamedTree, len(t.trees))
	treesByName := make(map[string]int, len(t.trees))
	for i, entry := range t.trees {
		tree := entry.Copy()
		trees[i] = NamedTree{Tree: tree, Name: entry.Name}
		treesByName[entry.Name] = i
	}

	clone := *t
	clone.trees = trees
	clone.treesByName = treesByName
	clone.logger = t.logger
	return &clone
}

func (t *MultiTree) Version() int64 {
	return t.lastCommitInfo.Version
}

func (t *MultiTree) SnapshotVersion() int64 {
	return t.metadata.CommitInfo.Version
}

func (t *MultiTree) LastCommitInfo() *proto.CommitInfo {
	return &t.lastCommitInfo
}

func (t *MultiTree) apply(entry proto.ChangelogEntry) error {
	if err := t.ApplyUpgrades(entry.Upgrades); err != nil {
		return err
	}
	return t.ApplyChangeSets(entry.Changesets)
}

// ApplyUpgrades store name upgrades
func (t *MultiTree) ApplyUpgrades(upgrades []*proto.TreeNameUpgrade) error {
	if len(upgrades) == 0 {
		return nil
	}

	t.treesByName = nil // rebuild in the end

	for _, upgrade := range upgrades {
		switch {
		case upgrade.Delete:
			i := slices.IndexFunc(t.trees, func(entry NamedTree) bool {
				return entry.Name == upgrade.Name
			})
			if i < 0 {
				return fmt.Errorf("unknown tree name %s", upgrade.Name)
			}
			// swap deletion
			t.trees[i], t.trees[len(t.trees)-1] = t.trees[len(t.trees)-1], t.trees[i]
			t.trees = t.trees[:len(t.trees)-1]
		case upgrade.RenameFrom != "":
			// rename tree
			i := slices.IndexFunc(t.trees, func(entry NamedTree) bool {
				return entry.Name == upgrade.RenameFrom
			})
			if i < 0 {
				return fmt.Errorf("unknown tree name %s", upgrade.RenameFrom)
			}
			t.trees[i].Name = upgrade.Name
		default:
			// add tree
			v := utils.NextVersion(t.Version(), t.initialVersion)
			if v < 0 || v > math.MaxUint32 {
				return fmt.Errorf("version overflows uint32: %d", v)
			}
			version := uint32(v)
			tree := NewWithInitialVersion(version)
			t.trees = append(t.trees, NamedTree{Tree: tree, Name: upgrade.Name})
		}
	}

	sort.SliceStable(t.trees, func(i, j int) bool {
		return t.trees[i].Name < t.trees[j].Name
	})
	t.treesByName = make(map[string]int, len(t.trees))
	for i, tree := range t.trees {
		if _, ok := t.treesByName[tree.Name]; ok {
			return fmt.Errorf("memiavl tree name conflicts: %s", tree.Name)
		}
		t.treesByName[tree.Name] = i
	}

	return nil
}

// ApplyChangeSet applies change set for a single tree.
func (t *MultiTree) ApplyChangeSet(name string, changeSet iavl.ChangeSet) error {
	i, found := t.treesByName[name]
	if !found {
		return fmt.Errorf("unknown tree name %s", name)
	}
	metrics.SeiDBMetrics.NumOfKVPairs.Add(context.Background(), int64(len(changeSet.Pairs)))
	t.trees[i].ApplyChangeSet(changeSet)
	return nil
}

// ApplyChangeSets applies change sets for multiple trees.
func (t *MultiTree) ApplyChangeSets(changeSets []*proto.NamedChangeSet) error {
	for _, cs := range changeSets {
		if err := t.ApplyChangeSet(cs.Name, cs.Changeset); err != nil {
			return err
		}
	}
	return nil
}

// WorkingCommitInfo returns the commit info for the working tree
func (t *MultiTree) WorkingCommitInfo() *proto.CommitInfo {
	version := utils.NextVersion(t.lastCommitInfo.Version, t.initialVersion)
	return t.buildCommitInfo(version)
}

// SaveVersion bumps the versions of all the stores and optionally returns the new app hash
func (t *MultiTree) SaveVersion(updateCommitInfo bool) (int64, error) {
	t.lastCommitInfo.Version = utils.NextVersion(t.lastCommitInfo.Version, t.initialVersion)
	for _, entry := range t.trees {
		if _, _, err := entry.SaveVersion(updateCommitInfo); err != nil {
			return 0, err
		}
	}

	if updateCommitInfo {
		t.UpdateCommitInfo()
	} else {
		// clear the dirty informaton
		t.lastCommitInfo.StoreInfos = []proto.StoreInfo{}
	}

	return t.lastCommitInfo.Version, nil
}

func (t *MultiTree) buildCommitInfo(version int64) *proto.CommitInfo {
	var infos = make([]proto.StoreInfo, 0, len(t.trees))
	for _, entry := range t.trees {
		infos = append(infos, proto.StoreInfo{
			Name: entry.Name,
			CommitId: proto.CommitID{
				Version: entry.Version(),
				Hash:    entry.RootHash(),
			},
		})
	}

	return &proto.CommitInfo{
		Version:    version,
		StoreInfos: infos,
	}
}

// UpdateCommitInfo update lastCommitInfo based on current status of trees.
// it's needed if `updateCommitInfo` is set to `false` in `ApplyChangeSet`.
func (t *MultiTree) UpdateCommitInfo() {
	t.lastCommitInfo = *t.buildCommitInfo(t.lastCommitInfo.Version)
}

// Catchup replay the new entries in the Rlog file on the tree to catch up to the target or latest version.
func (t *MultiTree) Catchup(stream types.Stream[proto.ChangelogEntry], endVersion int64) error {
	startTime := time.Now()
	lastIndex, err := stream.LastOffset()
	if err != nil {
		return fmt.Errorf("read rlog last index failed, %w", err)
	}

	firstIndex := utils.VersionToIndex(utils.NextVersion(t.Version(), t.initialVersion), t.initialVersion)
	if firstIndex > lastIndex {
		// already up-to-date
		return nil
	}

	endIndex := lastIndex
	if endVersion != 0 {
		endIndex = utils.VersionToIndex(endVersion, t.initialVersion)
	}

	if endIndex < firstIndex {
		return fmt.Errorf("target index %d is pruned", endIndex)
	}

	if endIndex > lastIndex {
		return fmt.Errorf("target index %d is in the future, latest index: %d", endIndex, lastIndex)
	}

	var replayCount = 0
	err = stream.Replay(firstIndex, endIndex, func(index uint64, entry proto.ChangelogEntry) error {
		if err := t.ApplyUpgrades(entry.Upgrades); err != nil {
			return err
		}
		updatedTrees := make(map[string]bool)
		for _, cs := range entry.Changesets {
			treeName := cs.Name
			t.TreeByName(treeName).ApplyChangeSetAsync(cs.Changeset)
			updatedTrees[treeName] = true
		}
		for _, tree := range t.trees {
			if _, found := updatedTrees[tree.Name]; !found {
				tree.ApplyChangeSetAsync(iavl.ChangeSet{})
			}
		}
		t.lastCommitInfo.Version = utils.NextVersion(t.lastCommitInfo.Version, t.initialVersion)
		t.lastCommitInfo.StoreInfos = []proto.StoreInfo{}
		replayCount++
		if replayCount%1000 == 0 {
			t.logger.Info(fmt.Sprintf("Replayed %d changelog entries\n", replayCount))
		}
		return nil
	})

	for _, tree := range t.trees {
		tree.WaitToCompleteAsyncWrite()
	}

	if err != nil {
		return err
	}
	t.UpdateCommitInfo()

	replayElapsed := time.Since(startTime).Seconds()
	t.logger.Info(fmt.Sprintf("Total replayed %d entries in %.1fs (%.1f entries/sec).\n",
		replayCount, replayElapsed, float64(replayCount)/replayElapsed))
	return nil
}

// PrefetchSnapshot prefetches all snapshot files into page cache
// This is critical for cold-start rewrite performance
// NOTE: This function directly reads files from disk, not using in-memory snapshot objects
func (t *MultiTree) PrefetchSnapshot(snapshotDir string, prefetchThreshold float64) error {
	fmt.Printf("[PREFETCH] Starting to prefetch snapshot from: %s\n", snapshotDir)
	startTime := time.Now()

	// Strategy: Prefetch EVM tree first (largest), then others in parallel
	// This matches the write order and maximizes cache utilization

	// Get list of tree directories
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to read snapshot dir: %w", err)
	}

	var evmDir string
	otherDirs := make([]string, 0, len(entries))

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		treeName := entry.Name()
		treeDir := filepath.Join(snapshotDir, treeName)

		// Check if this tree should be prefetched
		if !shouldPreloadTree(treeName) {
			continue
		}

		if treeName == "evm" {
			evmDir = treeDir
		} else {
			otherDirs = append(otherDirs, treeDir)
		}
	}

	// Phase 1: Prefetch EVM tree first (if it exists)
	if evmDir != "" {
		fmt.Printf("[PREFETCH] Phase 1: Prefetching EVM tree first (largest tree)\n")
		evmStart := time.Now()
		prefetchTreeFiles(evmDir, prefetchThreshold)
		evmElapsed := time.Since(evmStart).Seconds()
		fmt.Printf("[PREFETCH] Phase 1 completed: EVM tree prefetched in %.1fs\n", evmElapsed)
	}

	// Phase 2: Prefetch all other trees in parallel
	if len(otherDirs) > 0 {
		fmt.Printf("[PREFETCH] Phase 2: Prefetching %d remaining trees in parallel\n", len(otherDirs))
		phase2Start := time.Now()

		jobs := make(chan string, len(otherDirs))
		done := make(chan struct{})
		workers := 4 // Limit parallel prefetch to avoid I/O thrashing

		for i := 0; i < workers; i++ {
			go func() {
				for treeDir := range jobs {
					prefetchTreeFiles(treeDir, prefetchThreshold)
				}
				done <- struct{}{}
			}()
		}

		for _, treeDir := range otherDirs {
			jobs <- treeDir
		}
		close(jobs)

		for i := 0; i < workers; i++ {
			<-done
		}

		phase2Elapsed := time.Since(phase2Start).Seconds()
		fmt.Printf("[PREFETCH] Phase 2 completed: %d trees prefetched in %.1fs\n", len(otherDirs), phase2Elapsed)
	}

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("[PREFETCH] All trees prefetched in %.1fs\n", elapsed)
	return nil
}

// prefetchTreeFiles prefetches a single tree's files into page cache
// This function directly reads files from disk to check cache residency
func prefetchTreeFiles(treeDir string, prefetchThreshold float64) {
	treeName := filepath.Base(treeDir)

	nodesFile := filepath.Join(treeDir, FileNameNodes)
	leavesFile := filepath.Join(treeDir, FileNameLeaves)

	// Check if files exist
	if _, err := os.Stat(nodesFile); err != nil {
		return // Tree doesn't exist or is empty
	}

	// Check cache residency by mmaping the files temporarily
	needsPrefetch := false

	// Check nodes file
	if f, err := os.Open(nodesFile); err == nil {
		if fi, err := f.Stat(); err == nil && fi.Size() > 0 {
			// Mmap to check residency
			if data, err := unix.Mmap(int(f.Fd()), 0, int(fi.Size()), unix.PROT_READ, unix.MAP_SHARED); err == nil {
				if ratio, err := residentRatio(data); err == nil {
					if ratio < prefetchThreshold {
						fmt.Printf("[PREFETCH] Tree %s nodes cache residency: %.2f (below threshold %.2f)\n", treeName, ratio, prefetchThreshold)
						needsPrefetch = true
					} else {
						fmt.Printf("[PREFETCH] Tree %s nodes cache residency: %.2f (above threshold %.2f, skipping)\n", treeName, ratio, prefetchThreshold)
					}
				}
				unix.Munmap(data)
			}
		}
		f.Close()
	}

	if !needsPrefetch {
		// Check leaves file too
		if f, err := os.Open(leavesFile); err == nil {
			if fi, err := f.Stat(); err == nil && fi.Size() > 0 {
				if data, err := unix.Mmap(int(f.Fd()), 0, int(fi.Size()), unix.PROT_READ, unix.MAP_SHARED); err == nil {
					if ratio, err := residentRatio(data); err == nil {
						if ratio < prefetchThreshold {
							fmt.Printf("[PREFETCH] Tree %s leaves cache residency: %.2f (below threshold %.2f)\n", treeName, ratio, prefetchThreshold)
							needsPrefetch = true
						}
					}
					unix.Munmap(data)
				}
			}
			f.Close()
		}
	}

	if !needsPrefetch {
		fmt.Printf("[PREFETCH] Tree %s: skipping (already in cache)\n", treeName)
		return
	}

	// Prefetch files
	fmt.Printf("[PREFETCH] Tree %s: starting prefetch\n", treeName)
	startTime := time.Now()

	_ = SequentialReadAndFillPageCache(nodesFile)
	_ = SequentialReadAndFillPageCache(leavesFile)

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("[PREFETCH] Tree %s: completed in %.1fs\n", treeName, elapsed)
}

func (t *MultiTree) WriteSnapshot(ctx context.Context, dir string, wp *pond.WorkerPool) error {
	fmt.Printf("[SNAPSHOT WRITE] Starting to write %d trees\n", len(t.trees))

	if err := os.MkdirAll(dir, os.ModePerm); err != nil { //nolint:gosec
		return err
	}

	// Use priority EVM strategy: write EVM tree first, then others in parallel
	// Testing shows this is faster than full parallel because:
	// 1. EVM tree is 73% of total data - writing it alone avoids disk I/O contention
	// 2. Other trees can write in parallel after EVM is done
	// 3. With 1GB buffer, EVM tree writes faster without competition
	return t.writeSnapshotPriorityEVM(ctx, dir, wp)
}

// writeSnapshotPriorityEVM writes EVM tree first, then others in parallel
// Best strategy: reduces disk I/O contention for the largest tree
func (t *MultiTree) writeSnapshotPriorityEVM(ctx context.Context, dir string, wp *pond.WorkerPool) error {
	startTime := time.Now()

	// Phase 1: Write EVM tree first (if it exists)
	var evmTree *Tree
	var evmName string
	otherTrees := make([]NamedTree, 0, len(t.trees))

	for _, entry := range t.trees {
		if entry.Name == "evm" {
			evmTree = entry.Tree
			evmName = entry.Name
		} else {
			otherTrees = append(otherTrees, entry)
		}
	}

	if evmTree != nil {
		fmt.Printf("[SNAPSHOT WRITE] Phase 1: Writing EVM tree first (largest tree, 73%% of total data)\n")
		evmStart := time.Now()
		if err := evmTree.WriteSnapshot(ctx, filepath.Join(dir, evmName)); err != nil {
			return err
		}
		evmElapsed := time.Since(evmStart).Seconds()
		fmt.Printf("[SNAPSHOT WRITE] Phase 1 completed: EVM tree written in %.1fs\n", evmElapsed)
		fmt.Printf("[SNAPSHOT WRITE] Progress: 1/%d trees completed\n", len(t.trees))
	}

	// Phase 2: Write all other trees in parallel
	if len(otherTrees) > 0 {
		fmt.Printf("[SNAPSHOT WRITE] Phase 2: Writing %d remaining trees in parallel\n", len(otherTrees))
		phase2Start := time.Now()

		group, _ := wp.GroupContext(ctx)
		completed := int32(1) // Start from 1 (EVM already done)

		for _, entry := range otherTrees {
			tree, name := entry.Tree, entry.Name
			group.Submit(func() error {
				err := tree.WriteSnapshot(ctx, filepath.Join(dir, name))
				if err == nil {
					current := atomic.AddInt32(&completed, 1)
					fmt.Printf("[SNAPSHOT WRITE] Progress: %d/%d trees completed\n", current, len(t.trees))
				}
				return err
			})
		}

		if err := group.Wait(); err != nil {
			return err
		}

		phase2Elapsed := time.Since(phase2Start).Seconds()
		fmt.Printf("[SNAPSHOT WRITE] Phase 2 completed: %d trees written in %.1fs\n", len(otherTrees), phase2Elapsed)
	}

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("[SNAPSHOT WRITE] All %d trees completed in %.1fs\n", len(t.trees), elapsed)

	// write commit info
	fmt.Printf("[SNAPSHOT WRITE] Writing metadata file\n")
	metadata := proto.MultiTreeMetadata{
		CommitInfo:     &t.lastCommitInfo,
		InitialVersion: int64(t.initialVersion),
	}
	bz, err := metadata.Marshal()
	if err != nil {
		return err
	}
	return WriteFileSync(filepath.Join(dir, MetadataFileName), bz)
}

// writeSnapshotAllParallel writes all trees in parallel
// Best for cold cache: better disk I/O utilization
func (t *MultiTree) writeSnapshotAllParallel(ctx context.Context, dir string, wp *pond.WorkerPool) error {
	startTime := time.Now()
	fmt.Printf("[SNAPSHOT WRITE] Writing all %d trees in parallel\n", len(t.trees))

	group, _ := wp.GroupContext(ctx)
	var completed int32

	for _, entry := range t.trees {
		tree, name := entry.Tree, entry.Name
		group.Submit(func() error {
			err := tree.WriteSnapshot(ctx, filepath.Join(dir, name))
			if err == nil {
				current := atomic.AddInt32(&completed, 1)
				fmt.Printf("[SNAPSHOT WRITE] Progress: %d/%d trees completed\n", current, len(t.trees))
			}
			return err
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("[SNAPSHOT WRITE] All %d trees completed in %.1fs\n", len(t.trees), elapsed)

	// write commit info
	fmt.Printf("[SNAPSHOT WRITE] Writing metadata file\n")
	metadata := proto.MultiTreeMetadata{
		CommitInfo:     &t.lastCommitInfo,
		InitialVersion: int64(t.initialVersion),
	}
	bz, err := metadata.Marshal()
	if err != nil {
		return err
	}
	return WriteFileSync(filepath.Join(dir, MetadataFileName), bz)
}

// WriteFileSync calls `f.Sync` after before closing the file
func WriteFileSync(name string, data []byte) error {
	f, err := os.OpenFile(filepath.Clean(name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm) //nolint:gosec
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

func (t *MultiTree) Close() error {
	errs := make([]error, 0, len(t.trees))
	for _, entry := range t.trees {
		errs = append(errs, entry.Close())
	}
	t.trees = nil
	t.treesByName = nil
	t.lastCommitInfo = proto.CommitInfo{}
	return errors.Join(errs...)
}

func (t *MultiTree) ReplaceWith(other *MultiTree) error {
	errs := make([]error, 0, len(t.trees))
	for _, entry := range t.trees {
		errs = append(errs, entry.ReplaceWith(other.TreeByName(entry.Name)))
	}
	t.treesByName = other.treesByName
	t.lastCommitInfo = other.lastCommitInfo
	t.metadata = other.metadata
	return errors.Join(errs...)
}

func readMetadata(dir string) (*proto.MultiTreeMetadata, error) {
	// load commit info
	bz, err := os.ReadFile(filepath.Join(filepath.Clean(dir), MetadataFileName))
	if err != nil {
		return nil, err
	}
	var metadata proto.MultiTreeMetadata
	if err := metadata.Unmarshal(bz); err != nil {
		return nil, err
	}
	if metadata.CommitInfo.Version > math.MaxUint32 {
		return nil, fmt.Errorf("commit info version overflows uint32: %d", metadata.CommitInfo.Version)
	}
	if metadata.InitialVersion > math.MaxUint32 {
		return nil, fmt.Errorf("initial version overflows uint32: %d", metadata.InitialVersion)
	}

	return &metadata, nil
}

// WriteSnapshotViaExport writes snapshot using Export/Import approach
// This is significantly faster than recursive traversal because it uses sequential I/O.
//
// disablePrefetch: Set to true during background rewrite (main chain running) to avoid cache interference.
//
//	Set to false for cold start or manual rewrite (no active main chain).
//
// Strategy:
//
//	Priority EVM: Write EVM tree first (serial), then other trees in parallel
//
// Why Priority EVM instead of full parallel:
//   - Page cache limitation: With 128GB RAM (~100GB cache), parallel writes cause cache eviction
//   - EVM tree is 73% of data (81GB), needs dedicated cache to maintain 280k nodes/s speed
//   - Parallel writes cause speed degradation: 280k → 150k nodes/s after cache fills
//   - Serial EVM + parallel others: 66min total (stable 280k nodes/s)
//   - Full parallel: 95min total (degrades to 150k nodes/s due to cache pressure)
//
// Performance improvement over recursive traversal:
//   - Recursive: 240k nodes/s (random I/O, 1800 read IOPS)
//   - Export/Import Priority EVM: 280k nodes/s (sequential I/O, stable speed)
func (t *MultiTree) WriteSnapshotViaExport(ctx context.Context, dir string, wp *pond.WorkerPool, disablePrefetch bool) error {
	fmt.Printf("[SNAPSHOT WRITE] Starting to write %d trees using Export/Import (Priority EVM)\n", len(t.trees))

	if err := os.MkdirAll(dir, os.ModePerm); err != nil { //nolint:gosec
		return err
	}

	// Use Priority EVM strategy: write EVM first, then others in parallel
	return t.writeSnapshotPriorityEVMViaExport(ctx, dir, wp, disablePrefetch)
}

// writeSnapshotParallelViaExport writes all trees in parallel using Export/Import
func (t *MultiTree) writeSnapshotParallelViaExport(ctx context.Context, dir string, wp *pond.WorkerPool) error {
	startTime := time.Now()

	// Phase 1: Prefetch all snapshots in parallel BEFORE export
	// This loads all snapshot files (nodes, leaves, kvs) into page cache
	// Converting random I/O during Export into fast cache hits
	fmt.Printf("[PREFETCH] Phase 1: Prefetching snapshots for %d trees in parallel\n", len(t.trees))
	prefetchStart := time.Now()

	prefetchGroup, _ := wp.GroupContext(ctx)
	var prefetchCompleted int32

	for _, entry := range t.trees {
		tree := entry.Tree
		name := entry.Name
		prefetchGroup.Submit(func() error {
			if tree.snapshot != nil {
				treeStart := time.Now()
				fmt.Printf("[PREFETCH] Starting prefetch for tree: %s\n", name)

				if err := tree.snapshot.PrefetchFiles(); err != nil {
					fmt.Printf("[PREFETCH] Warning: prefetch failed for tree %s: %v (continuing anyway)\n", name, err)
				} else {
					elapsed := time.Since(treeStart).Seconds()
					current := atomic.AddInt32(&prefetchCompleted, 1)
					fmt.Printf("[PREFETCH] Completed prefetch for tree %s in %.1fs (%d/%d trees)\n",
						name, elapsed, current, len(t.trees))
				}
			}
			return nil // Don't fail on prefetch errors
		})
	}

	if err := prefetchGroup.Wait(); err != nil {
		// Prefetch errors are non-fatal, log and continue
		fmt.Printf("[PREFETCH] Warning: prefetch phase had errors: %v (continuing with export)\n", err)
	}

	prefetchElapsed := time.Since(prefetchStart).Seconds()
	fmt.Printf("[PREFETCH] Phase 1 completed: All snapshots prefetched in %.1fs\n", prefetchElapsed)

	// Phase 2: Export/Import all trees in parallel
	// Now Export reads from page cache (fast!) instead of disk (slow random I/O)
	fmt.Printf("[EXPORT/IMPORT] Phase 2: Exporting/Importing %d trees in parallel\n", len(t.trees))
	exportStart := time.Now()

	group, _ := wp.GroupContext(ctx)
	var completed int32

	// Submit all trees for parallel writing
	for _, entry := range t.trees {
		tree, name := entry.Tree, entry.Name
		group.Submit(func() error {
			treeStart := time.Now()
			fmt.Printf("[SNAPSHOT WRITE] Starting to write snapshot for tree: %s\n", name)

			err := tree.RewriteSnapshotViaExport(ctx, filepath.Join(dir, name))

			if err == nil {
				current := atomic.AddInt32(&completed, 1)
				elapsed := time.Since(treeStart).Seconds()
				fmt.Printf("[SNAPSHOT WRITE] Completed writing snapshot for tree %s in %.1fs\n", name, elapsed)
				fmt.Printf("[SNAPSHOT WRITE] Progress: %d/%d trees completed\n", current, len(t.trees))
			} else {
				fmt.Printf("[SNAPSHOT WRITE] Failed to write snapshot for tree %s: %v\n", name, err)
			}
			return err
		})
	}

	// Wait for all trees to complete
	if err := group.Wait(); err != nil {
		return err
	}

	exportElapsed := time.Since(exportStart).Seconds()
	fmt.Printf("[EXPORT/IMPORT] Phase 2 completed: All trees exported/imported in %.1fs\n", exportElapsed)

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("[SNAPSHOT WRITE] All %d trees completed in %.1fs using Export/Import (parallel)\n", len(t.trees), elapsed)
	fmt.Printf("[SNAPSHOT WRITE] Time breakdown: Prefetch %.1fs + Export/Import %.1fs = Total %.1fs\n",
		prefetchElapsed, exportElapsed, elapsed)

	// Write commit info
	fmt.Printf("[SNAPSHOT WRITE] Writing metadata file\n")
	metadata := proto.MultiTreeMetadata{
		CommitInfo:     &t.lastCommitInfo,
		InitialVersion: int64(t.initialVersion),
	}
	bz, err := metadata.Marshal()
	if err != nil {
		return err
	}
	return WriteFileSync(filepath.Join(dir, MetadataFileName), bz)
}

// writeSnapshotPriorityEVMViaExport writes EVM tree first (serial), then others in parallel
// Uses staged prefetch to avoid cache eviction:
//  1. Prefetch EVM only (81GB)
//  2. Write EVM (with cache drop)
//  3. Prefetch large trees only (bank+acc, 35GB) - small trees don't need prefetch
//  4. Write all remaining trees in parallel
func (t *MultiTree) writeSnapshotPriorityEVMViaExport(ctx context.Context, dir string, wp *pond.WorkerPool, disablePrefetch bool) error {
	startTime := time.Now()

	// Find EVM tree first
	var evmTree *Tree
	var evmName string
	otherTrees := make([]NamedTree, 0, len(t.trees))

	for _, entry := range t.trees {
		if entry.Name == "evm" {
			evmTree = entry.Tree
			evmName = entry.Name
		} else {
			otherTrees = append(otherTrees, entry)
		}
	}

	var phase0Elapsed float64
	var prefetch1Elapsed float64

	// Phase 0: REMOVED - Read-side cache drop via madvise
	//
	// After extensive testing, we found that madvise(MADV_DONTNEED) has negligible
	// effectiveness in background rewrite mode:
	//
	// Evidence from production testing:
	// - Phase 0 drops 44.4GB of bank/acc cache
	// - Main chain immediately reloads 30GB within minutes (shared mmap)
	// - On 256GB RAM: no performance difference with/without Phase 0
	// - On 128GB RAM: still bottlenecks at ~35% regardless of Phase 0
	//
	// Why madvise doesn't work here:
	// - Main chain and background clone share the same mmap files (shallow copy)
	// - Main chain actively processes blocks, continuously accessing bank/acc
	// - Kernel cannot drop pages that are being actively referenced
	//
	// Real cache cleanup happens in ReplaceWith() via munmap() after snapshot switch,
	// which is automatic and effective (verified: 115GB freed instantly).
	//
	// Decision: Remove this code to reduce complexity and avoid false expectations.
	// If 128GB RAM systems experience bottlenecks, the solution is hardware upgrade,
	// not software tricks that don't work.

	phase0Elapsed = 0
	fmt.Printf("[CACHE] Phase 0: SKIPPED (read-side cache drop removed - ineffective in background mode)\n")

	if disablePrefetch {
		// Background rewrite mode: Main chain is running, sharing same snapshot files
		// Prefetch would cause cache interference and AppHash mismatches
		fmt.Printf("[PREFETCH] DISABLED: Background rewrite mode (main chain running)\n")
		prefetch1Elapsed = 0
	} else {
		// Cold start mode: No main chain running, need prefetch for performance
		// Phase 1: Prefetch EVM snapshot for fast export
		if evmTree != nil && evmTree.snapshot != nil {
			fmt.Printf("[PREFETCH] Phase 1: Prefetching EVM tree (81GB)\n")
			prefetch1Start := time.Now()
			if err := evmTree.snapshot.PrefetchFiles(); err != nil {
				fmt.Printf("[PREFETCH] Warning: EVM prefetch failed: %v (continuing anyway)\n", err)
			}
			prefetch1Elapsed = time.Since(prefetch1Start).Seconds()
			fmt.Printf("[PREFETCH] Phase 1 completed in %.1fs\n", prefetch1Elapsed)
		}
	}

	// Phase 2: Write EVM tree (serial) with periodic cache maintenance
	var evmElapsed float64
	if evmTree != nil {
		fmt.Printf("[EXPORT/IMPORT] Phase 2: Writing EVM tree (serial)\n")

		evmStart := time.Now()

		// Cache maintenance goroutine REMOVED
		// Previous code: periodically dropped non-EVM cache every 5 minutes via madvise
		// Reality: Ineffective in background mode (main chain immediately reloads dropped pages)
		// Evidence: Dropped 44.4GB → 30GB reloaded within minutes
		// Decision: Keep code simple, rely on aggressive write-side cache drop instead

		if err := evmTree.RewriteSnapshotViaExport(ctx, filepath.Join(dir, evmName)); err != nil {
			return fmt.Errorf("failed to write EVM tree: %w", err)
		}

		evmElapsed = time.Since(evmStart).Seconds()
		fmt.Printf("[EXPORT/IMPORT] Phase 2 completed: EVM tree written in %.1fs (%.1fmin)\n", evmElapsed, evmElapsed/60)
	}

	// Phase 3: Prefetch large trees (bank + acc + wasm) in cold start mode
	// Note: Phase 3 EVM cache drop has been REMOVED (same reason as Phase 0)
	var largeTrees []NamedTree
	for _, entry := range otherTrees {
		// Prefetch trees with significant node count:
		// - bank: 278M nodes
		// - acc: 155M nodes
		// - wasm: 27M nodes (worth prefetching in cold start)
		// Skip very small trees like ibc (2.6M), etc.
		if entry.Name == "bank" || entry.Name == "acc" || entry.Name == "wasm" {
			largeTrees = append(largeTrees, entry)
		}
	}

	var prefetch2Elapsed float64
	if !disablePrefetch && len(largeTrees) > 0 {
		// Cold start mode: Prefetch large trees (bank+acc+wasm) for better performance
		fmt.Printf("[PREFETCH] Phase 3: Prefetching %d large trees (bank+acc+wasm)\n", len(largeTrees))
		prefetch2Start := time.Now()

		prefetchGroup, _ := wp.GroupContext(ctx)
		var prefetchCompleted int32

		for _, entry := range largeTrees {
			tree := entry.Tree
			name := entry.Name
			prefetchGroup.Submit(func() error {
				if tree.snapshot != nil {
					if err := tree.snapshot.PrefetchFiles(); err != nil {
						fmt.Printf("[PREFETCH] Warning: prefetch failed for tree %s: %v\n", name, err)
					} else {
						current := atomic.AddInt32(&prefetchCompleted, 1)
						fmt.Printf("[PREFETCH] Completed prefetch for tree %s (%d/%d large trees)\n",
							name, current, len(largeTrees))
					}
				}
				return nil
			})
		}

		prefetchGroup.Wait()
		prefetch2Elapsed = time.Since(prefetch2Start).Seconds()
		fmt.Printf("[PREFETCH] Phase 3 completed: %d large trees prefetched in %.1fs\n", len(largeTrees), prefetch2Elapsed)
	} else if disablePrefetch {
		fmt.Printf("[PREFETCH] Phase 3 DISABLED: Skipping prefetch (background rewrite mode)\n")
		prefetch2Elapsed = 0
	}

	// Phase 4: Write other trees in parallel
	var phase4Elapsed float64
	if len(otherTrees) > 0 {
		fmt.Printf("[EXPORT/IMPORT] Phase 4: Writing %d remaining trees in parallel\n", len(otherTrees))
		phase4Start := time.Now()

		group, _ := wp.GroupContext(ctx)
		var completed int32

		for _, entry := range otherTrees {
			tree, name := entry.Tree, entry.Name
			group.Submit(func() error {
				treeStart := time.Now()
				err := tree.RewriteSnapshotViaExport(ctx, filepath.Join(dir, name))
				if err == nil {
					elapsed := time.Since(treeStart).Seconds()
					current := atomic.AddInt32(&completed, 1)
					fmt.Printf("[EXPORT/IMPORT] Tree %s completed in %.1fs (%d/%d remaining trees)\n",
						name, elapsed, current, len(otherTrees))
				}
				return err
			})
		}

		if err := group.Wait(); err != nil {
			return err
		}

		phase4Elapsed = time.Since(phase4Start).Seconds()
		fmt.Printf("[EXPORT/IMPORT] Phase 4 completed: %d trees written in %.1fs\n", len(otherTrees), phase4Elapsed)
	}

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("[SNAPSHOT WRITE] All %d trees completed in %.1fs using Export/Import (Priority EVM)\n", len(t.trees), elapsed)
	fmt.Printf("[SNAPSHOT WRITE] Time breakdown: DropCache %.1fs + Prefetch-EVM %.1fs + Write-EVM %.1fs + Prefetch-Large %.1fs + Write-All %.1fs = Total %.1fs\n",
		phase0Elapsed, prefetch1Elapsed, evmElapsed, prefetch2Elapsed, phase4Elapsed, elapsed)

	// Write commit info
	fmt.Printf("[SNAPSHOT WRITE] Writing metadata file\n")
	metadata := proto.MultiTreeMetadata{
		CommitInfo:     &t.lastCommitInfo,
		InitialVersion: int64(t.initialVersion),
	}
	bz, err := metadata.Marshal()
	if err != nil {
		return err
	}
	return WriteFileSync(filepath.Join(dir, MetadataFileName), bz)
}
