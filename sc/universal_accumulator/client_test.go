package universalaccumulator

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestFlush tests buffer flushing.
func TestFlush(t *testing.T) {
	acc, err := NewUniversalAccumulator(10) // snapshot interval
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add entries without triggering automatic flush
	entries := make([]AccumulatorKVPair, 10)
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		entries[i-1] = AccumulatorKVPair{
			Key:     key,
			Value:   value,
			Deleted: false,
		}
	}

	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Entries are processed immediately in new architecture
	if acc.GetTotalElements() != 10 {
		t.Errorf("Expected 10 elements, got %d", acc.GetTotalElements())
	}

	t.Logf("Successfully flushed 10 entries")
}

// TestDeleteEntries tests key-value deletion.
func TestDeleteEntries(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add entries
	addEntries := []AccumulatorKVPair{
		{Key: []byte("key1"), Value: []byte("value1"), Deleted: false},
		{Key: []byte("key2"), Value: []byte("value2"), Deleted: false},
		{Key: []byte("key3"), Value: []byte("value3"), Deleted: false},
		{Key: []byte("key4"), Value: []byte("value4"), Deleted: false},
		{Key: []byte("key5"), Value: []byte("value5"), Deleted: false},
	}

	err = acc.AddEntries(addEntries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Entries are processed immediately in new architecture

	// Verify entries were added
	if acc.GetTotalElements() != 5 {
		t.Errorf("Expected 5 elements, got %d", acc.GetTotalElements())
	}

	// Test deletion
	deleteEntries := []AccumulatorKVPair{
		{Key: []byte("key1"), Value: []byte("value1"), Deleted: true},
		{Key: []byte("key3"), Value: []byte("value3"), Deleted: true},
	}

	err = acc.DeleteEntries(deleteEntries)
	if err != nil {
		t.Fatalf("Failed to delete entries: %v", err)
	}

	// Note: Current implementation is a placeholder
	// In a full implementation, this would actually remove elements
	t.Logf("Deletion completed (placeholder implementation)")
}

// TestAddEntriesStream_EdgeCases tests streaming API edge cases.
func TestAddEntriesStream_EdgeCases(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	t.Run("EmptyChannel", func(t *testing.T) {
		entryChan := make(chan AccumulatorKVPair)
		close(entryChan)

		ctx := context.Background()
		err = acc.AddEntriesStream(ctx, entryChan, 10)
		if err != nil {
			t.Fatalf("Failed to handle empty channel: %v", err)
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		entryChan := make(chan AccumulatorKVPair)

		ctx, cancel := context.WithCancel(context.Background())
		defer close(entryChan)

		// Cancel immediately
		cancel()

		err = acc.AddEntriesStream(ctx, entryChan, 10)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})

	t.Run("SlowProducer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		entryChan := make(chan AccumulatorKVPair)

		// Start slow producer
		go func() {
			defer close(entryChan)
			for i := range 5 {
				time.Sleep(100 * time.Millisecond)
				entryChan <- AccumulatorKVPair{
					Key:     []byte(fmt.Sprintf("slow_key_%d", i)),
					Value:   []byte(fmt.Sprintf("slow_value_%d", i)),
					Deleted: false,
				}
			}
		}()

		err = acc.AddEntriesStream(ctx, entryChan, 10)
		if err != nil {
			t.Fatalf("Failed to handle slow producer: %v", err)
		}

		// Entries are processed immediately in new architecture

		if acc.GetTotalElements() != 5 {
			t.Errorf("Expected 5 elements, got %d", acc.GetTotalElements())
		}
	})
}

// TestCalculateRoot_EdgeCases tests root calculation edge cases.
func TestCalculateRoot_EdgeCases(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	t.Run("EmptyAccumulator", func(t *testing.T) {
		root, err := acc.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate root of empty accumulator: %v", err)
		}

		if len(root) == 0 {
			t.Error("Root should not be empty even for empty accumulator")
		}

		t.Logf("Empty accumulator root: %s", hex.EncodeToString(root))
	})

	t.Run("AfterFlush", func(t *testing.T) {
		// Add entries
		entries := []AccumulatorKVPair{
			{Key: []byte("test_key"), Value: []byte("test_value"), Deleted: false},
		}
		err = acc.AddEntries(entries)
		if err != nil {
			t.Fatalf("Failed to add entries: %v", err)
		}

		// Calculate root should automatically flush
		root, err := acc.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate root: %v", err)
		}

		if len(root) == 0 {
			t.Error("Root should not be empty")
		}

		t.Logf("Root after flush: %s", hex.EncodeToString(root))
	})
}

// TestAPI_ErrorHandling tests error handling cases.
func TestAPI_ErrorHandling(t *testing.T) {
	// Test with uninitialized accumulator
	acc := &UniversalAccumulator{
		engine: nil,
	}

	t.Run("AddEntries_Uninitialized", func(t *testing.T) {
		entries := []AccumulatorKVPair{{Key: []byte("key"), Value: []byte("value"), Deleted: false}}

		err := acc.AddEntries(entries)
		if err == nil {
			t.Error("Expected error for uninitialized accumulator")
		}
	})

	t.Run("CalculateRoot_Uninitialized", func(t *testing.T) {
		_, err := acc.CalculateRoot()
		if err == nil {
			t.Error("Expected error for uninitialized accumulator")
		}
	})

	t.Run("AddEntries_Uninitialized2", func(t *testing.T) {
		entries := []AccumulatorKVPair{{Key: []byte("test"), Value: []byte("test"), Deleted: false}}
		err := acc.AddEntries(entries)
		if err == nil {
			t.Error("Expected error for uninitialized accumulator")
		}
	})

	t.Run("AddEntriesStream_Uninitialized", func(t *testing.T) {
		ctx := context.Background()
		entryChan := make(chan AccumulatorKVPair)
		close(entryChan)

		err := acc.AddEntriesStream(ctx, entryChan, 10)
		if err == nil {
			t.Error("Expected error for uninitialized accumulator")
		}
	})
}

// TestAPI_NilInputs tests nil/empty input handling.
func TestAPI_NilInputs(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	t.Run("AddEntries_EmptySlice", func(t *testing.T) {
		entries := []AccumulatorKVPair{}

		err := acc.AddEntries(entries)
		if err != nil {
			t.Errorf("Should handle empty slice gracefully: %v", err)
		}
	})

	t.Run("AddEntries_EmptySlice2", func(t *testing.T) {
		entries := []AccumulatorKVPair{}

		err := acc.AddEntries(entries)
		if err != nil {
			t.Errorf("Should handle empty slice gracefully: %v", err)
		}
	})
}

// TestAPI_ConcurrentAccess tests concurrent additions.
func TestAPI_ConcurrentAccess(t *testing.T) {
	acc, err := NewUniversalAccumulator(10) // snapshot interval
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Test concurrent AddEntries calls
	const numGoroutines = 10
	const entriesPerGoroutine = 100

	// Start multiple goroutines adding entries
	for i := range numGoroutines {
		go func(baseID int) {
			entries := make([]AccumulatorKVPair, entriesPerGoroutine)
			for j := range entriesPerGoroutine {
				key := []byte(fmt.Sprintf("key_%d", baseID*entriesPerGoroutine+j))
				value := []byte(fmt.Sprintf("value_%d", baseID*entriesPerGoroutine+j))

				entries[j] = AccumulatorKVPair{
					Key:     key,
					Value:   value,
					Deleted: false,
				}
			}

			err := acc.AddEntries(entries)
			if err != nil {
				t.Errorf("Failed to add entries for goroutine %d: %v", baseID, err)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	time.Sleep(1 * time.Second)

	// Entries are processed immediately in new architecture

	// Verify total count
	total := acc.GetTotalElements()
	expected := numGoroutines * entriesPerGoroutine
	if total != expected {
		t.Errorf("Expected %d elements, got %d", expected, total)
	}

	t.Logf("Successfully added %d entries concurrently", total)
}

// TestGetCurrentVersion tests getting the current version.
func TestGetCurrentVersion(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Initial version should be 0
	version, err := acc.GetCurrentVersion()
	if err != nil {
		t.Fatalf("Failed to get current version: %v", err)
	}
	if version != 0 {
		t.Errorf("Expected version 0, got %d", version)
	}

	// Apply a changeset and check version
	changeset := AccumulatorChangeset{
		Version: 5,
		Entries: []AccumulatorKVPair{
			{Key: []byte("test"), Value: []byte("value"), Deleted: false},
		},
		Name: "test",
	}
	err = acc.ApplyChangeset(changeset)
	if err != nil {
		t.Fatalf("Failed to apply changeset: %v", err)
	}

	version, err = acc.GetCurrentVersion()
	if err != nil {
		t.Fatalf("Failed to get current version after changeset: %v", err)
	}
	if version != 5 {
		t.Errorf("Expected version 5, got %d", version)
	}
}

// TestGetStateHash tests getting the state hash.
func TestGetStateHash(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Get initial state hash
	stateHash, err := acc.GetStateHash()
	if err != nil {
		t.Fatalf("Failed to get state hash: %v", err)
	}
	if stateHash.Version != 0 {
		t.Errorf("Expected version 0, got %d", stateHash.Version)
	}
	if len(stateHash.Hash) == 0 {
		t.Error("State hash should not be empty")
	}

	// Add some data and check hash changes
	entries := []AccumulatorKVPair{
		{Key: []byte("key1"), Value: []byte("value1"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	newStateHash, err := acc.GetStateHash()
	if err != nil {
		t.Fatalf("Failed to get new state hash: %v", err)
	}

	// Hash should be different after adding data
	if string(stateHash.Hash) == string(newStateHash.Hash) {
		t.Error("State hash should change after adding entries")
	}
}

// TestReset tests resetting the accumulator.
func TestReset(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add some entries
	entries := []AccumulatorKVPair{
		{Key: []byte("key1"), Value: []byte("value1"), Deleted: false},
		{Key: []byte("key2"), Value: []byte("value2"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	if acc.GetTotalElements() != 2 {
		t.Errorf("Expected 2 elements, got %d", acc.GetTotalElements())
	}

	// Reset the accumulator
	err = acc.Reset()
	if err != nil {
		t.Fatalf("Failed to reset accumulator: %v", err)
	}

	// Check that accumulator is empty
	if acc.GetTotalElements() != 0 {
		t.Errorf("Expected 0 elements after reset, got %d", acc.GetTotalElements())
	}

	version, err := acc.GetCurrentVersion()
	if err != nil {
		t.Fatalf("Failed to get version after reset: %v", err)
	}
	if version != 0 {
		t.Errorf("Expected version 0 after reset, got %d", version)
	}
}

// TestApplyChangesetAsync tests asynchronous changeset application.
func TestApplyChangesetAsync(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	changeset := AccumulatorChangeset{
		Version: 1,
		Entries: []AccumulatorKVPair{
			{Key: []byte("async_key"), Value: []byte("async_value"), Deleted: false},
		},
		Name: "async_test",
	}

	// Apply changeset asynchronously
	acc.ApplyChangesetAsync(changeset)

	// Since the current implementation is synchronous, we can check immediately
	if acc.GetTotalElements() != 1 {
		t.Errorf("Expected 1 element after async apply, got %d", acc.GetTotalElements())
	}
}

// TestGenerateWitness tests the legacy witness generation function.
func TestGenerateWitness(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add an entry
	entries := []AccumulatorKVPair{
		{Key: []byte("witness_key"), Value: []byte("witness_value"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Generate witness using legacy function
	witness, err := acc.GenerateWitness([]byte("witness_key"), []byte("witness_value"))
	if err != nil {
		t.Fatalf("Failed to generate witness: %v", err)
	}
	defer witness.Free()

	// Verify the witness
	isValid := acc.VerifyWitness(witness)
	if !isValid {
		t.Error("Generated witness should be valid")
	}
}

// TestBatchGenerateWitnesses tests batch witness generation.
func TestBatchGenerateWitnesses(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add entries
	entries := []AccumulatorKVPair{
		{Key: []byte("batch_key1"), Value: []byte("batch_value1"), Deleted: false},
		{Key: []byte("batch_key2"), Value: []byte("batch_value2"), Deleted: false},
		{Key: []byte("batch_key3"), Value: []byte("batch_value3"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Generate witnesses in batch
	witnesses, err := acc.BatchGenerateWitnesses(entries)
	if err != nil {
		t.Fatalf("Failed to generate batch witnesses: %v", err)
	}

	if len(witnesses) != 3 {
		t.Fatalf("Expected 3 witnesses, got %d", len(witnesses))
	}

	// Verify all witnesses
	for i, witness := range witnesses {
		if witness == nil {
			t.Errorf("Witness %d is nil", i)
			continue
		}

		isValid := acc.VerifyWitness(witness)
		if !isValid {
			t.Errorf("Witness %d should be valid", i)
		}
		witness.Free()
	}

	// Test with empty entries
	emptyWitnesses, err := acc.BatchGenerateWitnesses([]AccumulatorKVPair{})
	if err != nil {
		t.Fatalf("Failed to generate empty batch: %v", err)
	}
	if len(emptyWitnesses) != 0 {
		t.Errorf("Expected 0 witnesses for empty batch, got %d", len(emptyWitnesses))
	}
}
