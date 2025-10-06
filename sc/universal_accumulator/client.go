package universalaccumulator

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// UniversalAccumulator provides the main interface for the Universal Accumulator.
type UniversalAccumulator struct {
	engine *AccumulatorEngine
	mu     sync.RWMutex
}

// NewUniversalAccumulator creates a new universal accumulator instance.
func NewUniversalAccumulator(snapshotInterval uint64) (*UniversalAccumulator, error) {
	engine, err := NewAccumulatorEngine(snapshotInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator engine: %w", err)
	}

	return &UniversalAccumulator{
		engine: engine,
	}, nil
}

// AddEntries adds multiple entries to the accumulator.
func (acc *UniversalAccumulator) AddEntries(entries []AccumulatorKVPair) error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.engine == nil {
		return errors.New("accumulator not initialized")
	}

	// Fast path: direct processing without changeset overhead.
	return acc.engine.processEntriesDirect(entries)
}

// AddEntriesStream adds multiple entries to the accumulator via a channel.
func (acc *UniversalAccumulator) AddEntriesStream(
	ctx context.Context,
	entries <-chan AccumulatorKVPair,
	bufferSize int,
) error {
	if acc.engine == nil {
		return errors.New("accumulator not initialized")
	}

	buffer := make([]AccumulatorKVPair, 0, bufferSize)

	for {
		select {
		case entry, ok := <-entries:
			if !ok {
				// Channel closed, process remaining buffer.
				if len(buffer) > 0 {
					if err := acc.AddEntries(buffer); err != nil {
						return fmt.Errorf("failed to add final buffer: %w", err)
					}
				}
				return nil
			}

			buffer = append(buffer, entry)

			// Process buffer when it's full
			if len(buffer) >= bufferSize {
				if err := acc.AddEntries(buffer); err != nil {
					return fmt.Errorf("failed to add buffer: %w", err)
				}
				buffer = buffer[:0] // Reset buffer
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// DeleteEntries removes multiple entries from the accumulator.
func (acc *UniversalAccumulator) DeleteEntries(entries []AccumulatorKVPair) error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.engine == nil {
		return errors.New("accumulator not initialized")
	}

	// Mark all entries as deleted
	for i := range entries {
		entries[i].Deleted = true
	}

	// Create a changeset for the deletions
	changeset := AccumulatorChangeset{
		Version: acc.engine.currentVersion + 1,
		Entries: entries,
		Name:    "api_batch",
	}

	return acc.engine.ApplyChangeset(changeset)
}

// CalculateRoot calculates and returns the current root hash.
func (acc *UniversalAccumulator) CalculateRoot() ([]byte, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.engine == nil {
		return nil, errors.New("accumulator not initialized")
	}

	stateHash := acc.engine.CalculateStateHash()
	return stateHash.Hash, nil
}

// GetTotalElements returns the total number of elements in the accumulator.
func (acc *UniversalAccumulator) GetTotalElements() int {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.engine == nil {
		return 0
	}

	return acc.engine.totalElements
}

// GetCurrentVersion returns the current version of the accumulator.
func (acc *UniversalAccumulator) GetCurrentVersion() (uint64, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.engine == nil {
		return 0, errors.New("accumulator not initialized")
	}

	return acc.engine.GetCurrentVersion()
}

// GetStateHash returns the current state hash with version.
func (acc *UniversalAccumulator) GetStateHash() (AccumulatorStateHash, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.engine == nil {
		return AccumulatorStateHash{}, errors.New("accumulator not initialized")
	}

	return acc.engine.GetStateHash(), nil
}

// ExportPerHeightState returns (version, root, factor) for external persistence.
func (acc *UniversalAccumulator) ExportPerHeightState() (uint64, []byte, Factor, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	if acc.engine == nil {
		return 0, nil, nil, errors.New("accumulator not initialized")
	}
	ver := acc.engine.currentVersion
	state := acc.engine.CalculateStateHash()
	factor, err := acc.engine.Factor()
	if err != nil {
		return 0, nil, nil, err
	}
	return ver, state.Hash, factor, nil
}

// Factor exposes the current fVa for persistence at a given height.
func (acc *UniversalAccumulator) Factor() (Factor, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	if acc.engine == nil {
		return nil, errors.New("accumulator not initialized")
	}
	return acc.engine.Factor()
}

// SetStateFromFactor restores state fast from a stored factor.
func (acc *UniversalAccumulator) SetStateFromFactor(f Factor) error {
	acc.mu.Lock()
	defer acc.mu.Unlock()
	if acc.engine == nil {
		return errors.New("accumulator not initialized")
	}
	return acc.engine.SetStateFromFactor(f)
}

// ApplyChangeset applies a changeset to the accumulator.
func (acc *UniversalAccumulator) ApplyChangeset(changeset AccumulatorChangeset) error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.engine == nil {
		return errors.New("accumulator not initialized")
	}

	return acc.engine.ApplyChangeset(changeset)
}

// ApplyChangesetAsync applies a changeset asynchronously.
func (acc *UniversalAccumulator) ApplyChangesetAsync(changeset AccumulatorChangeset) {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.engine == nil {
		return
	}

	acc.engine.ApplyChangesetAsync(changeset)
}

// Reset resets the accumulator to a clean state.
func (acc *UniversalAccumulator) Reset() error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.engine == nil {
		return errors.New("accumulator not initialized")
	}

	return acc.engine.Reset()
}

// Close closes the accumulator and frees resources.
func (acc *UniversalAccumulator) Close() error {
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.engine == nil {
		return nil
	}

	err := acc.engine.Close()
	acc.engine = nil
	return err
}
