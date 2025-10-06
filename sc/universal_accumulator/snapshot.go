package universalaccumulator

/*
#include "universal_accumulator.h"

// Forward declare the functions from snapshot.c
int serialize_accumulator_state(t_state *acc, unsigned char *buffer, int buffer_size);
int deserialize_accumulator_state(t_state *acc, unsigned char *buffer, int buffer_size);
int get_serialized_state_size(t_state *acc);
int get_total_snapshot_size(t_state *acc);
int validate_accumulator_state(t_state *acc);
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

// AccumulatorSnapshot represents a complete snapshot of the accumulator state.
type AccumulatorSnapshot struct {
	// Basic metadata
	Version       uint64 `json:"version"`
	Hash          []byte `json:"hash"` // Root hash for verification
	TotalElements int    `json:"total_elements"`

	// Complete cryptographic state - all elements from t_state struct
	AccumulatorState []byte `json:"accumulator_state"` // Serialized complete C state

	// Metadata for state management
	LastSnapshotVersion uint64 `json:"last_snapshot_version"`
	SnapshotInterval    uint64 `json:"snapshot_interval"`

	// Verification data
	StateSize int `json:"state_size"` // Size of serialized state for validation
}

// ===== UniversalAccumulator (high-level API) snapshot methods =====

// CreateCompleteSnapshot creates a complete snapshot including all cryptographic state.
func (acc *UniversalAccumulator) CreateCompleteSnapshot(version uint64) (*AccumulatorSnapshot, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.engine == nil {
		return nil, errors.New("accumulator not initialized")
	}

	return acc.engine.CreateCompleteSnapshot(version)
}

// RestoreFromCompleteSnapshot restores the accumulator from a complete snapshot.
func (acc *UniversalAccumulator) RestoreFromCompleteSnapshot(snapshot *AccumulatorSnapshot) error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.engine == nil {
		return errors.New("accumulator not initialized")
	}

	return acc.engine.RestoreFromCompleteSnapshot(snapshot)
}

// Snapshot creates a snapshot of the current state (legacy method - now calls CreateCompleteSnapshot).
func (acc *UniversalAccumulator) Snapshot() (AccumulatorSnapshot, error) {
	snapshot, err := acc.CreateCompleteSnapshot(acc.engine.currentVersion)
	if err != nil {
		return AccumulatorSnapshot{}, err
	}
	return *snapshot, nil
}

// RestoreFromSnapshot restores the accumulator from a snapshot (legacy method).
func (acc *UniversalAccumulator) RestoreFromSnapshot(snapshot AccumulatorSnapshot) error {
	return acc.RestoreFromCompleteSnapshot(&snapshot)
}

// ShouldSnapshot determines if a snapshot should be taken.
func (acc *UniversalAccumulator) ShouldSnapshot() bool {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.engine == nil {
		return false
	}

	return acc.engine.ShouldSnapshot()
}

// SaveSnapshot saves the current accumulator state for fast recovery (legacy method).
func (acc *UniversalAccumulator) SaveSnapshot(version uint64) (*AccumulatorSnapshot, error) {
	return acc.CreateCompleteSnapshot(version)
}

// LoadSnapshot restores accumulator state from a snapshot (legacy method).
func (acc *UniversalAccumulator) LoadSnapshot(snapshot *AccumulatorSnapshot) error {
	return acc.RestoreFromCompleteSnapshot(snapshot)
}

// ===== AccumulatorEngine (low-level) snapshot methods =====

// CreateCompleteSnapshot creates a complete snapshot of the accumulator state.
func (acc *AccumulatorEngine) CreateCompleteSnapshot(version uint64) (*AccumulatorSnapshot, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if !acc.initialized {
		return nil, errors.New("accumulator not initialized")
	}

	// Calculate current root hash
	rootHash, err := acc.calculateRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to calculate root for snapshot: %w", err)
	}

	// Get the size needed for serialization
	cAcc := (*C.t_state)(acc.accumulator)
	// We need the total snapshot size, which includes the header as well as the serialized state data.
	// Using only the serialized state size (without header) results in an insufficient buffer and
	// causes serialize_accumulator_state to fail with a negative return value.
	totalSize := int(C.get_total_snapshot_size(cAcc))
	if totalSize < 0 {
		return nil, errors.New("failed to calculate total snapshot size")
	}

	// Serialize the complete accumulator state (header + data).
	stateBuffer := make([]byte, totalSize)
	actualSize := C.serialize_accumulator_state(cAcc, (*C.uchar)(unsafe.Pointer(&stateBuffer[0])), C.int(totalSize))
	if actualSize < 0 {
		return nil, errors.New("failed to serialize accumulator state")
	}

	snapshot := &AccumulatorSnapshot{
		Version:             version,
		Hash:                make([]byte, len(rootHash)),
		TotalElements:       acc.totalElements,
		AccumulatorState:    stateBuffer[:actualSize],
		LastSnapshotVersion: acc.lastSnapshotVersion,
		SnapshotInterval:    acc.snapshotInterval,
		StateSize:           int(actualSize),
	}

	copy(snapshot.Hash, rootHash)
	acc.lastSnapshotVersion = version

	return snapshot, nil
}

// RestoreFromCompleteSnapshot restores the accumulator from a complete snapshot.
func (acc *AccumulatorEngine) RestoreFromCompleteSnapshot(snapshot *AccumulatorSnapshot) error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if !acc.initialized {
		return errors.New("accumulator not initialized")
	}

	// Validate snapshot
	if len(snapshot.AccumulatorState) == 0 {
		return errors.New("snapshot contains no accumulator state")
	}

	if snapshot.StateSize != len(snapshot.AccumulatorState) {
		return errors.New("snapshot state size mismatch")
	}

	// Deserialize the complete accumulator state
	cAcc := (*C.t_state)(acc.accumulator)
	actualSize := C.deserialize_accumulator_state(
		cAcc,
		(*C.uchar)(unsafe.Pointer(&snapshot.AccumulatorState[0])),
		C.int(len(snapshot.AccumulatorState)),
	)
	if actualSize < 0 {
		return errors.New("failed to deserialize accumulator state")
	}

	// Restore metadata
	acc.currentVersion = snapshot.Version
	acc.totalElements = snapshot.TotalElements
	acc.lastSnapshotVersion = snapshot.LastSnapshotVersion
	acc.snapshotInterval = snapshot.SnapshotInterval

	// Verify the restored state by calculating root hash
	restoredHash, err := acc.calculateRoot()
	if err != nil {
		return fmt.Errorf("failed to verify restored state: %w", err)
	}

	// Compare with snapshot hash
	if len(restoredHash) != len(snapshot.Hash) {
		return errors.New("restored state hash length mismatch")
	}

	for i := range restoredHash {
		if restoredHash[i] != snapshot.Hash[i] {
			return errors.New("restored state hash verification failed")
		}
	}

	return nil
}

// Snapshot creates a snapshot of the current state (legacy method).
func (acc *AccumulatorEngine) Snapshot() (AccumulatorSnapshot, error) {
	snapshot, err := acc.CreateCompleteSnapshot(acc.currentVersion)
	if err != nil {
		return AccumulatorSnapshot{}, err
	}
	return *snapshot, nil
}

// RestoreFromSnapshot restores the accumulator from a snapshot (legacy method).
func (acc *AccumulatorEngine) RestoreFromSnapshot(snapshot AccumulatorSnapshot) error {
	return acc.RestoreFromCompleteSnapshot(&snapshot)
}

// ShouldSnapshot determines if a snapshot should be taken.
func (acc *AccumulatorEngine) ShouldSnapshot() bool {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.snapshotInterval == 0 {
		return false
	}

	return (acc.currentVersion - acc.lastSnapshotVersion) >= acc.snapshotInterval
}

// IncrementalUpdate updates the accumulator with only the changes since the last snapshot.
func (acc *UniversalAccumulator) IncrementalUpdate(changeset AccumulatorChangeset) error {
	if acc.engine == nil {
		return errors.New("accumulator not initialized")
	}

	// Apply the changeset
	return acc.ApplyChangeset(changeset)
}

// ProcessBlock processes a block of changes and automatically manages snapshots.
func (acc *UniversalAccumulator) ProcessBlock(version uint64, changeset AccumulatorChangeset) error {
	// Update current version
	acc.engine.currentVersion = version

	// Apply changeset to accumulator
	if err := acc.ApplyChangeset(changeset); err != nil {
		return fmt.Errorf("failed to apply block changeset: %w", err)
	}

	// Check if we should create a snapshot
	if acc.engine.snapshotInterval > 0 && (version-acc.engine.lastSnapshotVersion) >= acc.engine.snapshotInterval {
		snapshot, err := acc.CreateCompleteSnapshot(version)
		if err != nil {
			return fmt.Errorf("failed to create snapshot at version %d: %w", version, err)
		}

		acc.engine.lastSnapshotVersion = version
		fmt.Printf("Created complete snapshot at version %d with %d elements (state size: %d bytes)\n",
			version, snapshot.TotalElements, snapshot.StateSize)
	}

	return nil
}

// FastStartup recovers from the most recent snapshot and applies incremental changes.
func (acc *UniversalAccumulator) FastStartup(
	targetVersion uint64,
	getSnapshotFunc func(version uint64) (*AccumulatorSnapshot, error),
	getChangesFunc func(fromVersion, toVersion uint64) ([]AccumulatorChangeset, error),
) error {
	// Find the most recent snapshot before target version
	var bestSnapshot *AccumulatorSnapshot
	var bestVersion uint64

	// Look for snapshots in reverse order
	for v := targetVersion; v > 0; {
		snapshot, err := getSnapshotFunc(v)
		if err == nil && snapshot != nil {
			bestSnapshot = snapshot
			bestVersion = v
			break
		}
		// Check for underflow before subtraction
		if v <= acc.engine.snapshotInterval {
			break
		}
		v -= acc.engine.snapshotInterval
	}

	if bestSnapshot == nil {
		// No snapshot found, start from genesis
		fmt.Printf("No snapshot found, building from genesis to version %d\n", targetVersion)
		changes, err := getChangesFunc(0, targetVersion)
		if err != nil {
			return fmt.Errorf("failed to get changes from genesis: %w", err)
		}

		// Apply all changes
		for _, changeset := range changes {
			if err := acc.ProcessBlock(changeset.Version, changeset); err != nil {
				return fmt.Errorf("failed to process changeset at version %d: %w", changeset.Version, err)
			}
		}
		return nil
	}

	// Load the complete snapshot
	fmt.Printf("Loading complete snapshot from version %d (state size: %d bytes)\n", bestVersion, bestSnapshot.StateSize)
	if err := acc.RestoreFromCompleteSnapshot(bestSnapshot); err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	// Apply incremental changes from snapshot to target
	if targetVersion > bestVersion {
		fmt.Printf("Applying incremental changes from version %d to %d\n", bestVersion, targetVersion)
		changes, err := getChangesFunc(bestVersion+1, targetVersion)
		if err != nil {
			return fmt.Errorf("failed to get incremental changes: %w", err)
		}

		for _, changeset := range changes {
			if err := acc.ProcessBlock(changeset.Version, changeset); err != nil {
				return fmt.Errorf("failed to apply incremental changeset at version %d: %w", changeset.Version, err)
			}
		}
	}

	fmt.Printf("Fast startup completed to version %d\n", targetVersion)
	return nil
}

// GetSnapshotSize returns the size of the complete snapshot in bytes.
func (snapshot *AccumulatorSnapshot) GetSnapshotSize() int {
	return len(snapshot.AccumulatorState) + len(snapshot.Hash) + 64 // approximate metadata size
}

// ValidateSnapshot validates the integrity of a snapshot.
func (snapshot *AccumulatorSnapshot) ValidateSnapshot() error {
	if snapshot.Version == 0 {
		return errors.New("invalid snapshot version")
	}

	if len(snapshot.Hash) == 0 {
		return errors.New("snapshot missing root hash")
	}

	if len(snapshot.AccumulatorState) == 0 {
		return errors.New("snapshot missing accumulator state")
	}

	if snapshot.StateSize != len(snapshot.AccumulatorState) {
		return errors.New("snapshot state size mismatch")
	}

	if snapshot.TotalElements < 0 {
		return errors.New("invalid total elements count")
	}

	return nil
}
