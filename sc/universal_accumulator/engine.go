package universalaccumulator

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"golang.org/x/crypto/sha3"
)

// AccumulatorEngine represents the low-level universal accumulator implementation.
type AccumulatorEngine struct {
	// Core accumulator state
	mu          sync.RWMutex
	accumulator unsafe.Pointer // *C.t_state
	initialized bool

	// State management (aligned with repo patterns)
	currentVersion uint64
	dirty          bool

	// Snapshot management (similar to repo's snapshot system)
	lastSnapshotVersion uint64
	snapshotInterval    uint64

	// Processing state
	totalElements int
}

// AccumulatorChangeset represents changes to apply to the accumulator.
type AccumulatorChangeset struct {
	Version uint64              // Block height/version
	Entries []AccumulatorKVPair // Changes to apply
	Name    string              // Optional name for tracking
}

// AccumulatorKVPair represents a key-value pair change.
type AccumulatorKVPair struct {
	Key     []byte
	Value   []byte
	Deleted bool // true if this is a deletion
}

// AccumulatorStateHash represents the accumulator's state hash.
type AccumulatorStateHash struct {
	Hash    []byte
	Version uint64
}

// Factor represents the serialized fVa (cumulative product factor)
// used for O(1) accumulator state reconstruction at any height
type Factor []byte

// NewAccumulatorEngine creates a new accumulator engine instance.
func NewAccumulatorEngine(snapshotInterval uint64) (*AccumulatorEngine, error) {
	accumulator, err := createAccumulator()
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator: %w", err)
	}

	acc := &AccumulatorEngine{
		accumulator:      accumulator,
		initialized:      true,
		snapshotInterval: snapshotInterval,
		mu:               sync.RWMutex{},
	}

	// Set finalizer to ensure cleanup
	runtime.SetFinalizer(acc, (*AccumulatorEngine).finalize)

	return acc, nil
}

// ApplyChangeset applies a changeset to the accumulator.
func (acc *AccumulatorEngine) ApplyChangeset(changeset AccumulatorChangeset) error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if !acc.initialized {
		return errors.New("accumulator not initialized")
	}

	// Update version
	acc.currentVersion = changeset.Version

	// Process the changeset entries
	if len(changeset.Entries) > 0 {
		if err := acc.processEntries(changeset.Entries); err != nil {
			return fmt.Errorf("failed to process changeset entries: %w", err)
		}
		acc.dirty = true
	}

	return nil
}

// ApplyChangesetAsync applies a changeset asynchronously.
func (acc *AccumulatorEngine) ApplyChangesetAsync(changeset AccumulatorChangeset) {
	// For now, just apply synchronously
	// In a full implementation, this would use a channel like the repo's DBStore
	_ = acc.ApplyChangeset(changeset)
}

// GetCurrentVersion returns the current version of the accumulator.
func (acc *AccumulatorEngine) GetCurrentVersion() (uint64, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if !acc.initialized {
		return 0, errors.New("accumulator not initialized")
	}

	return acc.currentVersion, nil
}

// CalculateStateHash calculates and returns the current state hash.
func (acc *AccumulatorEngine) CalculateStateHash() AccumulatorStateHash {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if !acc.initialized {
		return AccumulatorStateHash{Version: acc.currentVersion}
	}

	hash, err := acc.calculateRoot()
	if err != nil {
		return AccumulatorStateHash{Version: acc.currentVersion}
	}

	return AccumulatorStateHash{
		Hash:    hash,
		Version: acc.currentVersion,
	}
}

// GetStateHash returns the current state hash.
func (acc *AccumulatorEngine) GetStateHash() AccumulatorStateHash {
	return acc.CalculateStateHash()
}

// Reset resets the accumulator to a clean state.
func (acc *AccumulatorEngine) Reset() error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.accumulator != nil {
		freeAccumulatorWrapper(acc.accumulator)
	}

	newAcc, err := createAccumulator()
	if err != nil {
		return fmt.Errorf("failed to reset accumulator: %w", err)
	}

	acc.accumulator = newAcc
	acc.totalElements = 0
	acc.currentVersion = 0
	acc.dirty = false

	return nil
}

// Close closes the accumulator and frees resources.
func (acc *AccumulatorEngine) Close() error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.accumulator != nil {
		freeAccumulatorWrapper(acc.accumulator)
		acc.accumulator = nil
	}

	acc.initialized = false
	return nil
}

// Internal helper methods

func (acc *AccumulatorEngine) processEntries(entries []AccumulatorKVPair) error {
	if len(entries) == 0 {
		return nil
	}

	// Separate additions and deletions
	var additions []AccumulatorKVPair
	var deletions []AccumulatorKVPair

	for _, entry := range entries {
		if entry.Deleted {
			deletions = append(deletions, entry)
		} else {
			additions = append(additions, entry)
		}
	}

	// Process additions
	if len(additions) > 0 {
		if err := acc.processAdditions(additions); err != nil {
			return fmt.Errorf("failed to process additions: %w", err)
		}
	}

	// Process deletions
	if len(deletions) > 0 {
		if err := acc.processDeletions(deletions); err != nil {
			return fmt.Errorf("failed to process deletions: %w", err)
		}
	}

	return nil
}

// processEntriesDirect bypasses changeset overhead for performance.
func (acc *AccumulatorEngine) processEntriesDirect(entries []AccumulatorKVPair) error {
	if len(entries) == 0 {
		return nil
	}

	// Separate additions and deletions
	var additionCount, deletionCount int
	for _, entry := range entries {
		if entry.Deleted {
			deletionCount++
		} else {
			additionCount++
		}
	}

	// Process additions if any
	if additionCount > 0 {
		// Pre-allocate flat hash buffer for all additions
		flatHashes := make([]byte, additionCount*32)
		hashIndex := 0

		// Create a single hasher instance for streaming (Keccak-256)
		hasher := sha3.NewLegacyKeccak256()

		// Optimized loop: streaming hash computation
		for _, entry := range entries {
			if !entry.Deleted {
				hasher.Reset()
				hasher.Write(entry.Key)
				if len(entry.Value) > 0 {
					hasher.Write(entry.Value)
				}
				hash := hasher.Sum(nil)
				copy(flatHashes[hashIndex*32:(hashIndex+1)*32], hash)
				hashIndex++
			}
		}

		// Call C function directly
		if err := addHashedElementsWrapper(acc.accumulator, flatHashes, additionCount); err != nil {
			return fmt.Errorf("failed to add elements: %w", err)
		}

		acc.totalElements += additionCount
	}

	// Process deletions if any
	if deletionCount > 0 {
		// Pre-allocate flat hash buffer for all deletions
		flatHashes := make([]byte, deletionCount*32)
		hashIndex := 0

		// Create a single hasher instance for streaming (Keccak-256)
		hasher := sha3.NewLegacyKeccak256()

		// Optimized loop: streaming hash computation
		for _, entry := range entries {
			if entry.Deleted {
				hasher.Reset()
				hasher.Write(entry.Key)
				if len(entry.Value) > 0 {
					hasher.Write(entry.Value)
				}
				hash := hasher.Sum(nil)
				copy(flatHashes[hashIndex*32:(hashIndex+1)*32], hash)
				hashIndex++
			}
		}

		// Call C function for deletions
		if err := batchDelHashedElementsWrapper(acc.accumulator, flatHashes, deletionCount); err != nil {
			return fmt.Errorf("failed to delete elements: %w", err)
		}

		acc.totalElements -= deletionCount
	}

	return nil
}

func (acc *AccumulatorEngine) processAdditions(additions []AccumulatorKVPair) error {
	if len(additions) == 0 {
		return nil
	}

	// Pre-allocate flat hash buffer for all additions
	flatHashes := make([]byte, len(additions)*32)

	// Create a single hasher instance for streaming (Keccak-256)
	hasher := sha3.NewLegacyKeccak256()

	// Optimized loop: streaming hash computation
	for i, entry := range additions {
		hasher.Reset()
		hasher.Write(entry.Key)
		if len(entry.Value) > 0 {
			hasher.Write(entry.Value)
		}
		hash := hasher.Sum(nil)
		copy(flatHashes[i*32:(i+1)*32], hash)
	}

	// Call C function
	if err := addHashedElementsWrapper(acc.accumulator, flatHashes, len(additions)); err != nil {
		return fmt.Errorf("failed to add elements: %w", err)
	}

	acc.totalElements += len(additions)
	return nil
}

func (acc *AccumulatorEngine) processDeletions(deletions []AccumulatorKVPair) error {
	if len(deletions) == 0 {
		return nil
	}

	// Pre-allocate flat hash buffer for all deletions
	flatHashes := make([]byte, len(deletions)*32)

	// Create a single hasher instance for streaming (Keccak-256)
	hasher := sha3.NewLegacyKeccak256()

	// Optimized loop: streaming hash computation
	for i, entry := range deletions {
		hasher.Reset()
		hasher.Write(entry.Key)
		if len(entry.Value) > 0 {
			hasher.Write(entry.Value)
		}
		hash := hasher.Sum(nil)
		copy(flatHashes[i*32:(i+1)*32], hash)
	}

	// Call C function for deletions
	if err := batchDelHashedElementsWrapper(acc.accumulator, flatHashes, len(deletions)); err != nil {
		return fmt.Errorf("failed to delete elements: %w", err)
	}

	acc.totalElements -= len(deletions)
	return nil
}

func (acc *AccumulatorEngine) calculateRoot() ([]byte, error) {
	if acc.accumulator == nil {
		return nil, errors.New("accumulator not initialized")
	}

	// Create buffer for root hash
	buffer := make([]byte, 128)
	actualSize := calculateRootWrapper(acc.accumulator, buffer)
	if actualSize < 0 {
		return nil, errors.New("failed to calculate root")
	}

	return buffer[:actualSize], nil
}

func (acc *AccumulatorEngine) finalize() {
	if acc.accumulator != nil {
		freeAccumulatorWrapper(acc.accumulator)
	}
}

// Factor returns the current accumulator factor fVa in serialized form.
func (acc *AccumulatorEngine) Factor() (Factor, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	if !acc.initialized {
		return nil, errors.New("accumulator not initialized")
	}
	b, err := getAccumulatorFactor(acc.accumulator)
	if err != nil {
		return nil, err
	}
	return Factor(b), nil
}

// SetStateFromFactor sets the state to match the provided factor and recomputes V and eVPt.
func (acc *AccumulatorEngine) SetStateFromFactor(f Factor) error {
	acc.mu.Lock()
	defer acc.mu.Unlock()
	if !acc.initialized {
		return errors.New("accumulator not initialized")
	}
	return setAccumulatorStateFromFactor(acc.accumulator, []byte(f))
}
