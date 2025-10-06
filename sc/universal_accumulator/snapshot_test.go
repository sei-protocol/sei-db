package universalaccumulator

import (
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCompleteSnapshot(t *testing.T) {
	// Create accumulator
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add some test data
	testEntries := []AccumulatorKVPair{
		{Key: []byte("account:alice"), Value: []byte("balance:1000"), Deleted: false},
		{Key: []byte("account:bob"), Value: []byte("balance:2000"), Deleted: false},
		{Key: []byte("contract:token"), Value: []byte("supply:10000"), Deleted: false},
		{Key: []byte("storage:config"), Value: []byte("version:1.0"), Deleted: false},
	}

	err = acc.AddEntries(testEntries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Get initial state
	initialHash, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate initial root: %v", err)
	}

	initialElements := acc.GetTotalElements()
	t.Logf("Initial state - Elements: %d, Hash: %s", initialElements, hex.EncodeToString(initialHash))

	// Create complete snapshot
	version := uint64(100)
	snapshot, err := acc.CreateCompleteSnapshot(version)
	if err != nil {
		t.Fatalf("Failed to create complete snapshot: %v", err)
	}

	// Validate snapshot
	err = snapshot.ValidateSnapshot()
	if err != nil {
		t.Fatalf("Snapshot validation failed: %v", err)
	}

	t.Logf("Created snapshot - Version: %d, Elements: %d, State size: %d bytes",
		snapshot.Version, snapshot.TotalElements, snapshot.StateSize)

	// Verify snapshot contains complete state
	if len(snapshot.AccumulatorState) == 0 {
		t.Fatal("Snapshot should contain accumulator state")
	}

	if snapshot.StateSize != len(snapshot.AccumulatorState) {
		t.Fatal("Snapshot state size mismatch")
	}

	// Test witness generation before restoration
	witness1, err := acc.IssueWitness([]byte("account:alice"), []byte("balance:1000"), true)
	if err != nil {
		t.Fatalf("Failed to issue witness before restoration: %v", err)
	}
	defer witness1.Free()

	if !acc.VerifyWitness(witness1) {
		t.Fatal("Witness should be valid before restoration")
	}

	// Create a new accumulator and restore from snapshot
	newAcc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create new accumulator: %v", err)
	}
	defer newAcc.Close()

	// Restore from complete snapshot
	err = newAcc.RestoreFromCompleteSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Failed to restore from complete snapshot: %v", err)
	}

	// Verify restored state
	restoredHash, err := newAcc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate restored root: %v", err)
	}

	restoredElements := newAcc.GetTotalElements()

	if restoredElements != initialElements {
		t.Fatalf("Element count mismatch - Expected: %d, Got: %d", initialElements, restoredElements)
	}

	if len(restoredHash) != len(initialHash) {
		t.Fatal("Hash length mismatch")
	}

	for i := range restoredHash {
		if restoredHash[i] != initialHash[i] {
			t.Fatal("Hash mismatch after restoration")
		}
	}

	t.Logf("Restored state - Elements: %d, Hash: %s", restoredElements, hex.EncodeToString(restoredHash))

	// 🔥 Critical test: Witness generation and verification after restoration
	witness2, err := newAcc.IssueWitness([]byte("account:alice"), []byte("balance:1000"), true)
	if err != nil {
		t.Fatalf("Failed to issue witness after restoration: %v", err)
	}
	defer witness2.Free()

	if !newAcc.VerifyWitness(witness2) {
		t.Fatal("Witness should be valid after restoration")
	}

	// Test non-membership witness
	nonMemberWitness, err := newAcc.IssueWitness([]byte("account:charlie"), []byte("balance:0"), false)
	if err != nil {
		t.Fatalf("Failed to issue non-membership witness: %v", err)
	}
	defer nonMemberWitness.Free()

	if !newAcc.VerifyWitness(nonMemberWitness) {
		t.Fatal("Non-membership witness should be valid after restoration")
	}

	t.Log("✓ Complete snapshot test passed - witness generation works after restoration!")
}

func TestSnapshotWithStateChanges(t *testing.T) {
	acc, err := NewUniversalAccumulator(5) // Snapshot every 5 versions
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Initial state
	initialEntries := []AccumulatorKVPair{
		{Key: []byte("key1"), Value: []byte("value1"), Deleted: false},
		{Key: []byte("key2"), Value: []byte("value2"), Deleted: false},
	}

	changeset1 := AccumulatorChangeset{
		Version: 1,
		Entries: initialEntries,
		Name:    "initial",
	}

	err = acc.ApplyChangeset(changeset1)
	if err != nil {
		t.Fatalf("Failed to apply initial changeset: %v", err)
	}

	// Add more entries
	additionalEntries := []AccumulatorKVPair{
		{Key: []byte("key3"), Value: []byte("value3"), Deleted: false},
		{Key: []byte("key4"), Value: []byte("value4"), Deleted: false},
	}

	changeset2 := AccumulatorChangeset{
		Version: 6, // This should trigger a snapshot
		Entries: additionalEntries,
		Name:    "additional",
	}

	err = acc.ApplyChangeset(changeset2)
	if err != nil {
		t.Fatalf("Failed to apply additional changeset: %v", err)
	}

	// Create snapshot at version 6
	snapshot, err := acc.CreateCompleteSnapshot(6)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Verify snapshot
	if snapshot.Version != 6 {
		t.Fatalf("Expected version 6, got %d", snapshot.Version)
	}

	if snapshot.TotalElements != 4 {
		t.Fatalf("Expected 4 elements, got %d", snapshot.TotalElements)
	}

	// Test restoration
	newAcc, err := NewUniversalAccumulator(5)
	if err != nil {
		t.Fatalf("Failed to create new accumulator: %v", err)
	}
	defer newAcc.Close()

	err = newAcc.RestoreFromCompleteSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Failed to restore from snapshot: %v", err)
	}

	// Verify all entries can be witnessed
	testEntries := []AccumulatorKVPair{
		{Key: []byte("key1"), Value: []byte("value1"), Deleted: false},
		{Key: []byte("key2"), Value: []byte("value2"), Deleted: false},
		{Key: []byte("key3"), Value: []byte("value3"), Deleted: false},
		{Key: []byte("key4"), Value: []byte("value4"), Deleted: false},
	}

	for i, entry := range testEntries {
		witness, err := newAcc.IssueWitness(entry.Key, entry.Value, true)
		if err != nil {
			t.Fatalf("Failed to issue witness for entry %d: %v", i, err)
		}
		defer witness.Free()

		if !newAcc.VerifyWitness(witness) {
			t.Fatalf("Witness verification failed for entry %d", i)
		}
	}

	t.Log("✓ Snapshot with state changes test passed!")
}

func TestSnapshotSizeAndPerformance(t *testing.T) {
	acc, err := NewUniversalAccumulator(100)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add a moderate number of entries
	entries := make([]AccumulatorKVPair, 1000)
	for i := range entries {
		entries[i] = AccumulatorKVPair{
			Key:     []byte(fmt.Sprintf("key_%d", i)),
			Value:   []byte(fmt.Sprintf("value_%d", i)),
			Deleted: false,
		}
	}

	start := time.Now()
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}
	addTime := time.Since(start)

	// Create snapshot
	start = time.Now()
	snapshot, err := acc.CreateCompleteSnapshot(1)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	snapshotTime := time.Since(start)

	// Restore snapshot
	newAcc, err := NewUniversalAccumulator(100)
	if err != nil {
		t.Fatalf("Failed to create new accumulator: %v", err)
	}
	defer newAcc.Close()

	start = time.Now()
	err = newAcc.RestoreFromCompleteSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Failed to restore snapshot: %v", err)
	}
	restoreTime := time.Since(start)

	// Test witness generation performance
	start = time.Now()
	witness, err := newAcc.IssueWitness([]byte("key_500"), []byte("value_500"), true)
	if err != nil {
		t.Fatalf("Failed to issue witness: %v", err)
	}
	witnessTime := time.Since(start)
	defer witness.Free()

	if !newAcc.VerifyWitness(witness) {
		t.Fatal("Witness verification failed")
	}

	t.Logf("Performance metrics:")
	t.Logf("  Add 1000 entries: %v", addTime)
	t.Logf("  Create snapshot: %v", snapshotTime)
	t.Logf("  Restore snapshot: %v", restoreTime)
	t.Logf("  Issue witness: %v", witnessTime)
	t.Logf("  Snapshot size: %d bytes", snapshot.GetSnapshotSize())
	t.Logf("  State size: %d bytes", snapshot.StateSize)
}

func TestSnapshotValidation(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add test data
	entries := []AccumulatorKVPair{
		{Key: []byte("test"), Value: []byte("data"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Create valid snapshot
	snapshot, err := acc.CreateCompleteSnapshot(1)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Test valid snapshot
	err = snapshot.ValidateSnapshot()
	if err != nil {
		t.Fatalf("Valid snapshot should pass validation: %v", err)
	}

	// Test invalid snapshots
	invalidSnapshots := []*AccumulatorSnapshot{
		{Version: 0, Hash: []byte("hash"), AccumulatorState: []byte("state"), StateSize: 5, TotalElements: 1},
		{Version: 1, Hash: []byte{}, AccumulatorState: []byte("state"), StateSize: 5, TotalElements: 1},
		{Version: 1, Hash: []byte("hash"), AccumulatorState: []byte{}, StateSize: 0, TotalElements: 1},
		{Version: 1, Hash: []byte("hash"), AccumulatorState: []byte("state"),
			StateSize: 10, TotalElements: 1}, // size mismatch
		{Version: 1, Hash: []byte("hash"), AccumulatorState: []byte("state"), StateSize: 5, TotalElements: -1},
	}

	for i, invalidSnapshot := range invalidSnapshots {
		err = invalidSnapshot.ValidateSnapshot()
		if err == nil {
			t.Fatalf("Invalid snapshot %d should fail validation", i)
		}
	}

	t.Log("✓ Snapshot validation test passed!")
}

// TestLegacySnapshotMethods tests the legacy snapshot API methods.
func TestLegacySnapshotMethods(t *testing.T) {
	acc, err := NewUniversalAccumulator(5) // Snapshot every 5 versions
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add some test data
	entries := []AccumulatorKVPair{
		{Key: []byte("legacy_key1"), Value: []byte("legacy_value1"), Deleted: false},
		{Key: []byte("legacy_key2"), Value: []byte("legacy_value2"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Test legacy Snapshot method
	snapshot, err := acc.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create legacy snapshot: %v", err)
	}

	if snapshot.TotalElements != 2 {
		t.Errorf("Expected 2 elements in snapshot, got %d", snapshot.TotalElements)
	}
	if len(snapshot.AccumulatorState) == 0 {
		t.Error("Snapshot accumulator state should not be empty")
	}

	// Test legacy RestoreFromSnapshot method
	newAcc, err := NewUniversalAccumulator(5)
	if err != nil {
		t.Fatalf("Failed to create new accumulator: %v", err)
	}
	defer newAcc.Close()

	err = newAcc.RestoreFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Failed to restore from legacy snapshot: %v", err)
	}

	if newAcc.GetTotalElements() != 2 {
		t.Errorf("Expected 2 elements after restore, got %d", newAcc.GetTotalElements())
	}

	// Verify restored accumulator has same root
	originalRoot, _ := acc.CalculateRoot()
	restoredRoot, _ := newAcc.CalculateRoot()
	if string(originalRoot) != string(restoredRoot) {
		t.Error("Restored accumulator should have same root as original")
	}
}

// TestShouldSnapshot tests the snapshot interval logic.
func TestShouldSnapshot(t *testing.T) {
	acc, err := NewUniversalAccumulator(3) // Snapshot every 3 versions
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Initially should not need snapshot
	if acc.ShouldSnapshot() {
		t.Error("Should not need snapshot initially")
	}

	// Apply changesets to reach snapshot interval
	for i := 1; i <= 3; i++ {
		changeset := AccumulatorChangeset{
			Version: uint64(i),
			Entries: []AccumulatorKVPair{
				{Key: []byte(fmt.Sprintf("key_%d", i)), Value: []byte(fmt.Sprintf("value_%d", i)), Deleted: false},
			},
			Name: fmt.Sprintf("test_%d", i),
		}
		err = acc.ApplyChangeset(changeset)
		if err != nil {
			t.Fatalf("Failed to apply changeset %d: %v", i, err)
		}
	}

	// Now should need snapshot
	if !acc.ShouldSnapshot() {
		t.Error("Should need snapshot after interval")
	}

	// Create snapshot to reset interval
	_, err = acc.CreateCompleteSnapshot(3)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Should not need snapshot immediately after creating one
	if acc.ShouldSnapshot() {
		t.Error("Should not need snapshot immediately after creating one")
	}

	// Test accumulator with interval 0 (never snapshot)
	noSnapAcc, err := NewUniversalAccumulator(0)
	if err != nil {
		t.Fatalf("Failed to create no-snapshot accumulator: %v", err)
	}
	defer noSnapAcc.Close()

	changeset := AccumulatorChangeset{
		Version: 10,
		Entries: []AccumulatorKVPair{
			{Key: []byte("no_snap"), Value: []byte("value"), Deleted: false},
		},
		Name: "no_snap_test",
	}
	err = noSnapAcc.ApplyChangeset(changeset)
	if err != nil {
		t.Fatalf("Failed to apply changeset to no-snap accumulator: %v", err)
	}

	if noSnapAcc.ShouldSnapshot() {
		t.Error("Accumulator with interval 0 should never need snapshot")
	}
}

// TestLoadSnapshot tests the legacy LoadSnapshot method.
func TestLoadSnapshot(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add test data
	entries := []AccumulatorKVPair{
		{Key: []byte("load_key1"), Value: []byte("load_value1"), Deleted: false},
		{Key: []byte("load_key2"), Value: []byte("load_value2"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Create snapshot using SaveSnapshot (legacy method)
	snapshot, err := acc.SaveSnapshot(5)
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Create new accumulator and load snapshot
	newAcc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create new accumulator: %v", err)
	}
	defer newAcc.Close()

	err = newAcc.LoadSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	// Verify loaded state
	if newAcc.GetTotalElements() != 2 {
		t.Errorf("Expected 2 elements after load, got %d", newAcc.GetTotalElements())
	}

	version, err := newAcc.GetCurrentVersion()
	if err != nil {
		t.Fatalf("Failed to get version: %v", err)
	}
	if version != 5 {
		t.Errorf("Expected version 5 after load, got %d", version)
	}

	// Verify roots match
	originalRoot, _ := acc.CalculateRoot()
	loadedRoot, _ := newAcc.CalculateRoot()
	if string(originalRoot) != string(loadedRoot) {
		t.Error("Loaded accumulator should have same root as original")
	}
}

// TestFastStartup tests the fast startup functionality.
func TestFastStartup(t *testing.T) {
	acc, err := NewUniversalAccumulator(2) // Snapshot every 2 versions
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Create test data for multiple versions
	allChangesets := []AccumulatorChangeset{}
	snapshots := make(map[uint64]*AccumulatorSnapshot)

	// Build up state over several versions
	for version := uint64(1); version <= 6; version++ {
		changeset := AccumulatorChangeset{
			Version: version,
			Entries: []AccumulatorKVPair{
				{Key: []byte(fmt.Sprintf("startup_key_%d", version)),
					Value: []byte(fmt.Sprintf("startup_value_%d", version)), Deleted: false},
			},
			Name: fmt.Sprintf("startup_test_%d", version),
		}
		allChangesets = append(allChangesets, changeset)

		err = acc.ApplyChangeset(changeset)
		if err != nil {
			t.Fatalf("Failed to apply changeset for version %d: %v", version, err)
		}

		// Create snapshots at intervals
		if version%2 == 0 {
			snapshot, err := acc.CreateCompleteSnapshot(version)
			if err != nil {
				t.Fatalf("Failed to create snapshot at version %d: %v", version, err)
			}
			snapshots[version] = snapshot
		}
	}

	// Mock functions for FastStartup
	getSnapshotFunc := func(version uint64) (*AccumulatorSnapshot, error) {
		if snapshot, exists := snapshots[version]; exists {
			return snapshot, nil
		}
		return nil, fmt.Errorf("snapshot not found for version %d", version)
	}

	getChangesFunc := func(fromVersion, toVersion uint64) ([]AccumulatorChangeset, error) {
		var changes []AccumulatorChangeset
		for _, changeset := range allChangesets {
			if changeset.Version > fromVersion && changeset.Version <= toVersion {
				changes = append(changes, changeset)
			}
		}
		return changes, nil
	}

	// Test 1: Fast startup with recent snapshot available
	newAcc1, err := NewUniversalAccumulator(2)
	if err != nil {
		t.Fatalf("Failed to create new accumulator for fast startup: %v", err)
	}
	defer newAcc1.Close()

	err = newAcc1.FastStartup(6, getSnapshotFunc, getChangesFunc)
	if err != nil {
		t.Fatalf("Fast startup failed: %v", err)
	}

	if newAcc1.GetTotalElements() != 6 {
		t.Errorf("Expected 6 elements after fast startup, got %d", newAcc1.GetTotalElements())
	}

	// Verify final state matches original
	originalRoot, _ := acc.CalculateRoot()
	fastStartupRoot, _ := newAcc1.CalculateRoot()
	if string(originalRoot) != string(fastStartupRoot) {
		t.Error("Fast startup should produce same root as original accumulator")
	}

	// Test 2: Fast startup with no snapshots (fallback to genesis)
	getSnapshotFuncNoSnaps := func(version uint64) (*AccumulatorSnapshot, error) {
		return nil, errors.New("no snapshots available")
	}

	newAcc2, err := NewUniversalAccumulator(2)
	if err != nil {
		t.Fatalf("Failed to create new accumulator for genesis startup: %v", err)
	}
	defer newAcc2.Close()

	err = newAcc2.FastStartup(3, getSnapshotFuncNoSnaps, getChangesFunc)
	if err != nil {
		t.Fatalf("Genesis startup failed: %v", err)
	}

	if newAcc2.GetTotalElements() != 3 {
		t.Errorf("Expected 3 elements after genesis startup, got %d", newAcc2.GetTotalElements())
	}

	// Test 3: Fast startup with error in getChangesFunc (genesis path)
	getChangesFuncError := func(fromVersion, toVersion uint64) ([]AccumulatorChangeset, error) {
		return nil, errors.New("failed to get changes")
	}

	newAcc3, err := NewUniversalAccumulator(2)
	if err != nil {
		t.Fatalf("Failed to create new accumulator for error test: %v", err)
	}
	defer newAcc3.Close()

	err = newAcc3.FastStartup(3, getSnapshotFuncNoSnaps, getChangesFuncError)
	if err == nil {
		t.Error("Expected error when getChangesFunc fails in genesis path")
	}

	// Test 4: Fast startup to intermediate version (not latest)
	newAcc4, err := NewUniversalAccumulator(2)
	if err != nil {
		t.Fatalf("Failed to create new accumulator for intermediate startup: %v", err)
	}
	defer newAcc4.Close()

	err = newAcc4.FastStartup(4, getSnapshotFunc, getChangesFunc)
	if err != nil {
		t.Fatalf("Intermediate fast startup failed: %v", err)
	}

	if newAcc4.GetTotalElements() != 4 {
		t.Errorf("Expected 4 elements after intermediate startup, got %d", newAcc4.GetTotalElements())
	}

	version, err := newAcc4.GetCurrentVersion()
	if err != nil {
		t.Fatalf("Failed to get version: %v", err)
	}
	if version != 4 {
		t.Errorf("Expected version 4, got %d", version)
	}
}

// TestIncrementalUpdate tests the incremental update functionality.
func TestIncrementalUpdate(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add initial data
	initialEntries := []AccumulatorKVPair{
		{Key: []byte("initial_key1"), Value: []byte("initial_value1"), Deleted: false},
		{Key: []byte("initial_key2"), Value: []byte("initial_value2"), Deleted: false},
	}
	err = acc.AddEntries(initialEntries)
	if err != nil {
		t.Fatalf("Failed to add initial entries: %v", err)
	}

	if acc.GetTotalElements() != 2 {
		t.Errorf("Expected 2 elements initially, got %d", acc.GetTotalElements())
	}

	// Test incremental update with additions
	incrementalChangeset := AccumulatorChangeset{
		Version: 1,
		Entries: []AccumulatorKVPair{
			{Key: []byte("incremental_key1"), Value: []byte("incremental_value1"), Deleted: false},
			{Key: []byte("incremental_key2"), Value: []byte("incremental_value2"), Deleted: false},
		},
		Name: "incremental_test",
	}

	err = acc.IncrementalUpdate(incrementalChangeset)
	if err != nil {
		t.Fatalf("Failed to apply incremental update: %v", err)
	}

	if acc.GetTotalElements() != 4 {
		t.Errorf("Expected 4 elements after incremental update, got %d", acc.GetTotalElements())
	}

	// Test incremental update with deletions
	deletionChangeset := AccumulatorChangeset{
		Version: 2,
		Entries: []AccumulatorKVPair{
			{Key: []byte("initial_key1"), Value: []byte("initial_value1"), Deleted: true},
		},
		Name: "deletion_test",
	}

	err = acc.IncrementalUpdate(deletionChangeset)
	if err != nil {
		t.Fatalf("Failed to apply incremental deletion: %v", err)
	}

	if acc.GetTotalElements() != 3 {
		t.Errorf("Expected 3 elements after deletion, got %d", acc.GetTotalElements())
	}

	// Test incremental update with empty changeset
	emptyChangeset := AccumulatorChangeset{
		Version: 3,
		Entries: []AccumulatorKVPair{},
		Name:    "empty_test",
	}

	err = acc.IncrementalUpdate(emptyChangeset)
	if err != nil {
		t.Fatalf("Failed to apply empty incremental update: %v", err)
	}

	if acc.GetTotalElements() != 3 {
		t.Errorf("Expected 3 elements after empty update, got %d", acc.GetTotalElements())
	}

	// Test incremental update on closed accumulator
	closedAcc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator for close test: %v", err)
	}
	closedAcc.Close() // Close the accumulator to make engine nil

	err = closedAcc.IncrementalUpdate(incrementalChangeset)
	if err == nil {
		t.Error("Expected error when applying incremental update to closed accumulator")
	}
}

// TestAccumulatorEngineSnapshotMethods tests the engine-level snapshot methods.
func TestAccumulatorEngineSnapshotMethods(t *testing.T) {
	acc, err := NewUniversalAccumulator(3)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add some data
	entries := []AccumulatorKVPair{
		{Key: []byte("engine_key1"), Value: []byte("engine_value1"), Deleted: false},
		{Key: []byte("engine_key2"), Value: []byte("engine_value2"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Test engine-level Snapshot method
	engineSnapshot, err := acc.engine.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create engine snapshot: %v", err)
	}

	if engineSnapshot.TotalElements != 2 {
		t.Errorf("Expected 2 elements in engine snapshot, got %d", engineSnapshot.TotalElements)
	}
	if len(engineSnapshot.AccumulatorState) == 0 {
		t.Error("Engine snapshot accumulator state should not be empty")
	}

	// Test engine-level RestoreFromSnapshot method
	newAcc, err := NewUniversalAccumulator(3)
	if err != nil {
		t.Fatalf("Failed to create new accumulator: %v", err)
	}
	defer newAcc.Close()

	err = newAcc.engine.RestoreFromSnapshot(engineSnapshot)
	if err != nil {
		t.Fatalf("Failed to restore from engine snapshot: %v", err)
	}

	if newAcc.GetTotalElements() != 2 {
		t.Errorf("Expected 2 elements after engine restore, got %d", newAcc.GetTotalElements())
	}

	// Test engine-level ShouldSnapshot method
	// After restore, the lastSnapshotVersion is set, so we need to check the interval
	if newAcc.engine.ShouldSnapshot() {
		t.Log("Engine needs snapshot after restore (depends on interval)")
	} else {
		t.Log("Engine does not need snapshot after restore (interval not reached)")
	}

	// Apply more changesets to trigger snapshot
	for i := 1; i <= 3; i++ {
		changeset := AccumulatorChangeset{
			Version: uint64(i),
			Entries: []AccumulatorKVPair{
				{Key: []byte(fmt.Sprintf("new_key_%d", i)), Value: []byte(fmt.Sprintf("new_value_%d", i)), Deleted: false},
			},
			Name: fmt.Sprintf("test_%d", i),
		}
		err = newAcc.engine.ApplyChangeset(changeset)
		if err != nil {
			t.Fatalf("Failed to apply changeset %d: %v", i, err)
		}
	}

	// Should need snapshot now
	if !newAcc.engine.ShouldSnapshot() {
		t.Error("Engine should need snapshot after interval")
	}

	// Create snapshot to reset interval
	_, err = newAcc.engine.CreateCompleteSnapshot(3)
	if err != nil {
		t.Fatalf("Failed to create engine snapshot: %v", err)
	}

	// Should not need snapshot immediately after creating one
	if newAcc.engine.ShouldSnapshot() {
		t.Error("Engine should not need snapshot immediately after creating one")
	}
}

// TestSnapshotErrorCases tests various error scenarios in snapshot operations.
func TestSnapshotErrorCases(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Test snapshot on uninitialized engine
	uninitializedAcc := &UniversalAccumulator{engine: &AccumulatorEngine{initialized: false}}
	_, err = uninitializedAcc.CreateCompleteSnapshot(1)
	if err == nil {
		t.Error("Should error when creating snapshot on uninitialized accumulator")
	}

	// Test restore on uninitialized engine
	validSnapshot := &AccumulatorSnapshot{
		Version:          1,
		Hash:             []byte("test"),
		TotalElements:    1,
		AccumulatorState: []byte("test_state"),
		StateSize:        10,
	}
	err = uninitializedAcc.RestoreFromCompleteSnapshot(validSnapshot)
	if err == nil {
		t.Error("Should error when restoring to uninitialized accumulator")
	}

	// Test restore with empty state
	emptySnapshot := &AccumulatorSnapshot{
		Version:          1,
		Hash:             []byte("test"),
		TotalElements:    1,
		AccumulatorState: []byte{},
		StateSize:        0,
	}
	err = acc.RestoreFromCompleteSnapshot(emptySnapshot)
	if err == nil {
		t.Error("Should error when restoring from snapshot with empty state")
	}

	// Test restore with mismatched state size
	mismatchSnapshot := &AccumulatorSnapshot{
		Version:          1,
		Hash:             []byte("test"),
		TotalElements:    1,
		AccumulatorState: []byte("short"),
		StateSize:        100, // Mismatch with actual length
	}
	err = acc.RestoreFromCompleteSnapshot(mismatchSnapshot)
	if err == nil {
		t.Error("Should error when state size doesn't match actual state length")
	}
}

// TestProcessBlockWithSnapshots tests ProcessBlock with automatic snapshot creation.
func TestProcessBlockWithSnapshots(t *testing.T) {
	acc, err := NewUniversalAccumulator(2) // Snapshot every 2 versions
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Process blocks that should trigger snapshots
	for version := uint64(1); version <= 5; version++ {
		changeset := AccumulatorChangeset{
			Version: version,
			Entries: []AccumulatorKVPair{
				{Key: []byte(fmt.Sprintf("block_key_%d", version)),
					Value: []byte(fmt.Sprintf("block_value_%d", version)), Deleted: false},
			},
			Name: fmt.Sprintf("block_%d", version),
		}

		err = acc.ProcessBlock(version, changeset)
		if err != nil {
			t.Fatalf("Failed to process block %d: %v", version, err)
		}

		currentVersion, err := acc.GetCurrentVersion()
		if err != nil {
			t.Fatalf("Failed to get current version: %v", err)
		}
		if currentVersion != version {
			t.Errorf("Expected version %d, got %d", version, currentVersion)
		}
	}

	// Verify final state
	if acc.GetTotalElements() != 5 {
		t.Errorf("Expected 5 elements after processing all blocks, got %d", acc.GetTotalElements())
	}

	// Test ProcessBlock with deletions
	deletionChangeset := AccumulatorChangeset{
		Version: 6,
		Entries: []AccumulatorKVPair{
			{Key: []byte("block_key_1"), Value: []byte("block_value_1"), Deleted: true},
		},
		Name: "deletion_block",
	}

	err = acc.ProcessBlock(6, deletionChangeset)
	if err != nil {
		t.Fatalf("Failed to process deletion block: %v", err)
	}

	if acc.GetTotalElements() != 4 {
		t.Errorf("Expected 4 elements after deletion, got %d", acc.GetTotalElements())
	}
}

// TestSnapshotIntervalEdgeCases tests edge cases around snapshot intervals.
func TestSnapshotIntervalEdgeCases(t *testing.T) {
	// Test with interval 1 (snapshot every version)
	acc1, err := NewUniversalAccumulator(1)
	if err != nil {
		t.Fatalf("Failed to create accumulator with interval 1: %v", err)
	}
	defer acc1.Close()

	changeset1 := AccumulatorChangeset{
		Version: 1,
		Entries: []AccumulatorKVPair{
			{Key: []byte("test1"), Value: []byte("value1"), Deleted: false},
		},
		Name: "test1",
	}
	err = acc1.ApplyChangeset(changeset1)
	if err != nil {
		t.Fatalf("Failed to apply changeset: %v", err)
	}

	if !acc1.ShouldSnapshot() {
		t.Error("Should need snapshot with interval 1 after one changeset")
	}

	// Test with very large interval
	acc2, err := NewUniversalAccumulator(1000000)
	if err != nil {
		t.Fatalf("Failed to create accumulator with large interval: %v", err)
	}
	defer acc2.Close()

	changeset2 := AccumulatorChangeset{
		Version: 1,
		Entries: []AccumulatorKVPair{
			{Key: []byte("test2"), Value: []byte("value2"), Deleted: false},
		},
		Name: "test2",
	}
	err = acc2.ApplyChangeset(changeset2)
	if err != nil {
		t.Fatalf("Failed to apply changeset: %v", err)
	}

	if acc2.ShouldSnapshot() {
		t.Error("Should not need snapshot with large interval after one changeset")
	}
}

// TestSnapshotSizeCalculations tests snapshot size calculation functions.
func TestSnapshotSizeCalculations(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add some data to make the snapshot non-trivial
	entries := []AccumulatorKVPair{
		{Key: []byte("size_key1"), Value: []byte("size_value1"), Deleted: false},
		{Key: []byte("size_key2"), Value: []byte("size_value2"), Deleted: false},
		{Key: []byte("size_key3"), Value: []byte("size_value3"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Create snapshot and verify size calculations
	snapshot, err := acc.CreateCompleteSnapshot(1)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	if snapshot.StateSize != len(snapshot.AccumulatorState) {
		t.Errorf("StateSize %d should match actual state length %d", snapshot.StateSize, len(snapshot.AccumulatorState))
	}

	if snapshot.StateSize <= 0 {
		t.Error("StateSize should be positive")
	}

	if len(snapshot.Hash) == 0 {
		t.Error("Snapshot hash should not be empty")
	}

	if snapshot.Version != 1 {
		t.Errorf("Expected snapshot version 1, got %d", snapshot.Version)
	}

	if snapshot.TotalElements != 3 {
		t.Errorf("Expected 3 elements in snapshot, got %d", snapshot.TotalElements)
	}
}
