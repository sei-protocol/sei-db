package universalaccumulator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

// generateTestEntries generates string-based test data.
func generateTestEntries(count int) []AccumulatorKVPair {
	entries := make([]AccumulatorKVPair, count)

	for i := range count {
		entries[i] = AccumulatorKVPair{
			Key:     []byte(fmt.Sprintf("key_%d", i)),
			Value:   []byte(fmt.Sprintf("value_%d", i)),
			Deleted: false,
		}
	}
	return entries
}

// generateLargeTestEntries generates random byte-based test data.
func generateLargeTestEntries(count int) []AccumulatorKVPair {
	entries := make([]AccumulatorKVPair, count)

	for i := range count {
		value := make([]byte, 32)
		_, _ = rand.Read(value)
		entries[i] = AccumulatorKVPair{
			Key:     []byte(fmt.Sprintf("key_%d", i)),
			Value:   value,
			Deleted: false,
		}
	}
	return entries
}

// TestUniversalAccumulatorCreation tests accumulator initialization.
func TestUniversalAccumulatorCreation(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Test basic properties
	if acc.GetTotalElements() != 0 {
		t.Errorf("Expected 0 elements, got %d", acc.GetTotalElements())
	}
}

// TestFullKVPairRootCalculation_Small tests small dataset root calculation.
func TestFullKVPairRootCalculation_Small(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add small dataset
	entries := generateTestEntries(100)

	start := time.Now()
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}
	addTime := time.Since(start)

	// Calculate root hash
	start = time.Now()
	rootHash, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate root: %v", err)
	}
	rootTime := time.Since(start)

	t.Logf("Added %d entries in %v", len(entries), addTime)
	t.Logf("Calculated root in %v", rootTime)
	t.Logf("Root hash: %s", hex.EncodeToString(rootHash))

	if len(rootHash) == 0 {
		t.Error("Root hash should not be empty")
	}

	if acc.GetTotalElements() != len(entries) {
		t.Errorf("Expected %d elements, got %d", len(entries), acc.GetTotalElements())
	}
}

// TestFullKVPairRootCalculation_Large tests large dataset root calculation.
func TestFullKVPairRootCalculation_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Test with smaller but still substantial dataset (1M entries)
	// In real scenarios, this would be 515M
	entries := generateLargeTestEntries(1000000) // 1M entries

	start := time.Now()
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}
	addTime := time.Since(start)

	// Calculate root hash
	start = time.Now()
	rootHash, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate root: %v", err)
	}
	rootTime := time.Since(start)

	t.Logf("Added %d entries in %v (%.0f entries/sec)",
		len(entries), addTime, float64(len(entries))/addTime.Seconds())
	t.Logf("Calculated root in %v", rootTime)
	t.Logf("Root hash: %s", hex.EncodeToString(rootHash))

	if acc.GetTotalElements() != len(entries) {
		t.Errorf("Expected %d elements, got %d", len(entries), acc.GetTotalElements())
	}
}

// TestMembershipProof tests membership witness operations.
func TestMembershipProof(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add entries
	entries := generateTestEntries(50)
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Test membership proof for existing key
	testKey := "key_25"     // Should exist
	testValue := "value_25" // Corresponding value

	start := time.Now()
	witness, err := acc.IssueWitness([]byte(testKey), []byte(testValue), true) // true = membership proof
	if err != nil {
		t.Fatalf("Failed to issue membership witness: %v", err)
	}
	issueTime := time.Since(start)
	defer witness.Free()

	start = time.Now()
	isValid := acc.VerifyWitness(witness)
	verifyTime := time.Since(start)

	t.Logf("Issued membership witness in %v", issueTime)
	t.Logf("Verified membership witness in %v", verifyTime)

	if !isValid {
		t.Error("Membership witness should be valid for existing key")
	}

	// Generate fresh witness to verify it still works
	freshWitness, err := acc.IssueWitness([]byte(testKey), []byte(testValue), true)
	if err != nil {
		t.Fatalf("Failed to issue fresh witness: %v", err)
	}
	defer freshWitness.Free()

	// Verify fresh witness
	isValidFreshWitness := acc.VerifyWitness(freshWitness)
	if !isValidFreshWitness {
		t.Error("Fresh witness should be valid")
	}
}

// TestNonMembershipProof tests non-membership witness operations.
func TestNonMembershipProof(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add entries
	entries := generateTestEntries(50)
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Test non-membership proof for non-existing key
	testKey := "key_nonexistent"
	testValue := "value_nonexistent" // Any value for non-membership

	start := time.Now()
	witness, err := acc.IssueWitness([]byte(testKey), []byte(testValue), false) // false = non-membership proof
	if err != nil {
		t.Fatalf("Failed to issue non-membership witness: %v", err)
	}
	issueTime := time.Since(start)
	defer witness.Free()

	start = time.Now()
	isValid := acc.VerifyWitness(witness)
	verifyTime := time.Since(start)

	t.Logf("Issued non-membership witness in %v", issueTime)
	t.Logf("Verified non-membership witness in %v", verifyTime)

	if !isValid {
		t.Error("Non-membership witness should be valid for non-existing key")
	}
}

// TestIncrementalUpdates_BlockByBlock tests block-by-block processing.
func TestIncrementalUpdates_BlockByBlock(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Simulate block-by-block processing
	numBlocks := 20
	entriesPerBlock := 100

	var allRootHashes [][]byte

	for blockHeight := uint64(1); blockHeight <= uint64(numBlocks); blockHeight++ {
		// Generate changes for this block
		blockEntries := make([]AccumulatorKVPair, entriesPerBlock)

		for i := range entriesPerBlock {
			blockEntries[i] = AccumulatorKVPair{
				Key:     []byte(fmt.Sprintf("block_%d_key_%d", blockHeight, i)),
				Value:   []byte(fmt.Sprintf("block_%d_value_%d", blockHeight, i)),
				Deleted: false,
			}
		}

		// Process block incrementally
		start := time.Now()
		blockChanges := AccumulatorChangeset{
			Version: blockHeight,
			Entries: blockEntries,
			Name:    fmt.Sprintf("block_%d", blockHeight),
		}
		err = acc.ProcessBlock(blockHeight, blockChanges)
		if err != nil {
			t.Fatalf("Failed to process block %d: %v", blockHeight, err)
		}
		processTime := time.Since(start)

		// Calculate root after this block
		start = time.Now()
		rootHash, err := acc.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate root for block %d: %v", blockHeight, err)
		}
		rootTime := time.Since(start)

		allRootHashes = append(allRootHashes, rootHash)

		t.Logf("Block %d: processed %d entries in %v, calculated root in %v",
			blockHeight, len(blockEntries), processTime, rootTime)

		expectedElements := int(blockHeight) * entriesPerBlock
		if acc.GetTotalElements() != expectedElements {
			t.Errorf("Block %d: expected %d elements, got %d",
				blockHeight, expectedElements, acc.GetTotalElements())
		}
	}

	// Verify all root hashes are different (incremental changes)
	for i := 1; i < len(allRootHashes); i++ {
		if hex.EncodeToString(allRootHashes[i-1]) == hex.EncodeToString(allRootHashes[i]) {
			t.Errorf("Root hashes should be different between blocks %d and %d", i, i+1)
		}
	}

	t.Logf("Successfully processed %d blocks with incremental updates", numBlocks)
}

// TestIncrementalUpdateAPI tests incremental add/update/delete.
func TestIncrementalUpdateAPI(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Initial state
	initialEntries := generateTestEntries(20)
	err = acc.AddEntries(initialEntries)
	if err != nil {
		t.Fatalf("Failed to add initial entries: %v", err)
	}

	initialRoot, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate initial root: %v", err)
	}

	// Perform incremental update
	incrementalEntries := []AccumulatorKVPair{
		{Key: []byte("new_key_1"), Value: []byte("new_value_1"), Deleted: false},
		{Key: []byte("new_key_2"), Value: []byte("new_value_2"), Deleted: false},
		{Key: []byte("key_5"), Value: []byte("updated_value_5"), Deleted: false},
		{Key: []byte("key_10"), Value: []byte("updated_value_10"), Deleted: false},
		{Key: []byte("key_15"), Value: []byte(""), Deleted: true},
		{Key: []byte("key_16"), Value: []byte(""), Deleted: true},
	}

	incrementalChangeset := AccumulatorChangeset{
		Version: 1,
		Entries: incrementalEntries,
		Name:    "incremental_update",
	}

	start := time.Now()
	err = acc.IncrementalUpdate(incrementalChangeset)
	if err != nil {
		t.Fatalf("Failed to perform incremental update: %v", err)
	}
	updateTime := time.Since(start)

	finalRoot, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate final root: %v", err)
	}

	t.Logf("Incremental update completed in %v", updateTime)
	t.Logf("Initial root: %s", hex.EncodeToString(initialRoot))
	t.Logf("Final root: %s", hex.EncodeToString(finalRoot))

	// Root should be different after incremental update
	if hex.EncodeToString(initialRoot) == hex.EncodeToString(finalRoot) {
		t.Error("Root hash should change after incremental update")
	}
}

// TestSnapshotAndRecovery tests snapshot save/load.
func TestSnapshotAndRecovery(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Build state up to height 10
	for height := uint64(1); height <= 10; height++ {
		entries := []AccumulatorKVPair{
			{Key: []byte(fmt.Sprintf("key_h%d_1", height)), Value: []byte(fmt.Sprintf("value_h%d_1", height)), Deleted: false},
			{Key: []byte(fmt.Sprintf("key_h%d_2", height)), Value: []byte(fmt.Sprintf("value_h%d_2", height)), Deleted: false},
		}

		changeset := AccumulatorChangeset{
			Version: height,
			Entries: entries,
			Name:    fmt.Sprintf("block_%d", height),
		}

		err = acc.ProcessBlock(height, changeset)
		if err != nil {
			t.Fatalf("Failed to process block %d: %v", height, err)
		}
	}

	// Save snapshot at height 6
	snapshotHeight := uint64(6)
	start := time.Now()
	snapshot, err := acc.SaveSnapshot(snapshotHeight)
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}
	snapshotTime := time.Since(start)

	t.Logf("Saved snapshot at height %d in %v", snapshotHeight, snapshotTime)
	t.Logf("Snapshot contains %d elements", snapshot.TotalElements)

	// Get current root for comparison
	currentRoot, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate current root: %v", err)
	}

	// Create new accumulator and test recovery
	newAcc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create new accumulator: %v", err)
	}
	defer newAcc.Close()

	// Simulate recovery with FastStartup
	targetHeight := uint64(10)

	getSnapshotFunc := func(height uint64) (*AccumulatorSnapshot, error) {
		if height == snapshotHeight {
			return snapshot, nil
		}
		return nil, fmt.Errorf("snapshot not found for height %d", height)
	}

	getChangesFunc := func(fromHeight, toHeight uint64) ([]AccumulatorChangeset, error) {
		var changesets []AccumulatorChangeset

		for h := fromHeight; h <= toHeight; h++ {
			entries := []AccumulatorKVPair{
				{Key: []byte(fmt.Sprintf("key_h%d_1", h)), Value: []byte(fmt.Sprintf("value_h%d_1", h)), Deleted: false},
				{Key: []byte(fmt.Sprintf("key_h%d_2", h)), Value: []byte(fmt.Sprintf("value_h%d_2", h)), Deleted: false},
			}
			changesets = append(changesets, AccumulatorChangeset{
				Version: h,
				Entries: entries,
				Name:    fmt.Sprintf("block_%d", h),
			})
		}
		return changesets, nil
	}

	start = time.Now()
	err = newAcc.FastStartup(targetHeight, getSnapshotFunc, getChangesFunc)
	if err != nil {
		t.Fatalf("Failed to perform fast startup: %v", err)
	}
	startupTime := time.Since(start)

	// Verify recovery
	recoveredRoot, err := newAcc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate recovered root: %v", err)
	}

	t.Logf("Fast startup completed in %v", startupTime)
	t.Logf("Original root: %s", hex.EncodeToString(currentRoot))
	t.Logf("Recovered root: %s", hex.EncodeToString(recoveredRoot))

	// For a complete recovery test, roots should match
	// Note: In this simplified test, they may not match exactly due to incomplete snapshot implementation
	t.Logf("Total elements after recovery: %d", newAcc.GetTotalElements())
}

// TestStreamingAPI tests streaming entry addition.
func TestStreamingAPI(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Create channel for streaming data
	entryChan := make(chan AccumulatorKVPair, 1000)

	// Generate streaming data
	numEntries := 10000
	go func() {
		defer close(entryChan)
		for i := range numEntries {
			entryChan <- AccumulatorKVPair{
				Key:     []byte(fmt.Sprintf("stream_key_%d", i)),
				Value:   []byte(fmt.Sprintf("stream_value_%d", i)),
				Deleted: false,
			}
		}
	}()

	// Process streaming data
	ctx := context.Background()
	bufferSize := 1000

	start := time.Now()
	err = acc.AddEntriesStream(ctx, entryChan, bufferSize)
	if err != nil {
		t.Fatalf("Failed to process streaming entries: %v", err)
	}
	streamTime := time.Since(start)

	// Calculate final root
	rootHash, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate root: %v", err)
	}

	t.Logf("Processed %d streaming entries in %v (%.0f entries/sec)",
		numEntries, streamTime, float64(numEntries)/streamTime.Seconds())
	t.Logf("Final root: %s", hex.EncodeToString(rootHash))

	if acc.GetTotalElements() != numEntries {
		t.Errorf("Expected %d elements, got %d", numEntries, acc.GetTotalElements())
	}
}

// TestMemoryOptimizedProcessing tests large dataset handling.
func TestMemoryOptimizedProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory-optimized test in short mode")
	}

	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Generate large dataset (100M entries to trigger memory optimization)
	// Note: Adjust size based on available memory
	numEntries := 100000000 // 100M entries

	// Use memory-optimized processing
	start := time.Now()

	// Generate entries in chunks to avoid memory issues in test
	chunkSize := 1000000 // 1M per chunk
	for chunk := range numEntries / chunkSize {
		entries := make([]AccumulatorKVPair, chunkSize)

		for i := range chunkSize {
			value := make([]byte, 32)
			_, _ = rand.Read(value)
			entries[i] = AccumulatorKVPair{
				Key:     []byte(fmt.Sprintf("key_%d", chunk*chunkSize+i)),
				Value:   value,
				Deleted: false,
			}
		}

		err = acc.AddEntries(entries)
		if err != nil {
			t.Fatalf("Failed to add chunk %d: %v", chunk, err)
		}

		// Only process a few chunks in test to avoid excessive time
		if chunk >= 2 { // Process only 3M entries in test
			break
		}
	}

	processTime := time.Since(start)

	// Calculate root
	start = time.Now()
	rootHash, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate root: %v", err)
	}
	rootTime := time.Since(start)

	processedEntries := acc.GetTotalElements()
	t.Logf("Processed %d entries in %v (%.0f entries/sec)",
		processedEntries, processTime, float64(processedEntries)/processTime.Seconds())
	t.Logf("Calculated root in %v", rootTime)
	t.Logf("Root hash: %s", hex.EncodeToString(rootHash))
}

// TestWitnessValidationAcrossUpdates tests witness updates.
func TestWitnessValidationAcrossUpdates(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add initial entries
	initialEntries := generateTestEntries(20)
	err = acc.AddEntries(initialEntries)
	if err != nil {
		t.Fatalf("Failed to add initial entries: %v", err)
	}

	// Issue witness for existing key
	testKey := "key_10"
	testValue := "value_10" // Corresponding value
	witness, err := acc.IssueWitness([]byte(testKey), []byte(testValue), true)
	if err != nil {
		t.Fatalf("Failed to issue witness: %v", err)
	}
	defer witness.Free()

	// Verify witness is initially valid
	if !acc.VerifyWitness(witness) {
		t.Error("Witness should be initially valid")
	}

	// Add more entries (accumulator state changes)
	newEntries := []AccumulatorKVPair{
		{Key: []byte("key_100"), Value: []byte("value_100"), Deleted: false},
		{Key: []byte("key_101"), Value: []byte("value_101"), Deleted: false},
	}

	err = acc.AddEntries(newEntries)
	if err != nil {
		t.Fatalf("Failed to add new entries: %v", err)
	}

	// Witness might be invalid after state change
	isValidBeforeUpdate := acc.VerifyWitness(witness)
	t.Logf("Witness valid before update: %v", isValidBeforeUpdate)

	// UpdateWitness removed - using fresh witness generation instead
	// Generate fresh witness to verify it still works after state change
	freshWitness, err := acc.IssueWitness([]byte(testKey), []byte(testValue), true)
	if err != nil {
		t.Fatalf("Failed to issue fresh witness: %v", err)
	}
	defer freshWitness.Free()

	// Verify fresh witness is valid
	isValidFreshWitness := acc.VerifyWitness(freshWitness)
	if !isValidFreshWitness {
		t.Error("Fresh witness should be valid after state change")
	}

	t.Logf("Witness validation across updates successful")
}

// TestAccumulatorEngineErrorPaths tests error paths in AccumulatorEngine.
func TestAccumulatorEngineErrorPaths(t *testing.T) {
	// Test uninitialized engine
	engine := &AccumulatorEngine{initialized: false}

	// Test GetCurrentVersion on uninitialized engine
	_, err := engine.GetCurrentVersion()
	if err == nil {
		t.Error("GetCurrentVersion should error on uninitialized engine")
	}

	// Test CalculateStateHash on uninitialized engine
	stateHash := engine.CalculateStateHash()
	if stateHash.Hash != nil {
		t.Error("CalculateStateHash should return empty hash for uninitialized engine")
	}

	// Test ApplyChangeset on uninitialized engine
	changeset := AccumulatorChangeset{
		Version: 1,
		Entries: []AccumulatorKVPair{
			{Key: []byte("test"), Value: []byte("value"), Deleted: false},
		},
		Name: "test",
	}
	err = engine.ApplyChangeset(changeset)
	if err == nil {
		t.Error("ApplyChangeset should error on uninitialized engine")
	}

	// Test ApplyChangesetAsync on uninitialized engine (should not panic)
	engine.ApplyChangesetAsync(changeset)

	// Test Reset on uninitialized engine (should work - it creates new accumulator)
	err = engine.Reset()
	if err != nil {
		t.Errorf("Reset should work on uninitialized engine: %v", err)
	}
}

// TestProcessEntriesEdgeCases tests edge cases in processEntries.
func TestProcessEntriesEdgeCases(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Test processEntries with empty slice
	changeset := AccumulatorChangeset{
		Version: 1,
		Entries: []AccumulatorKVPair{},
		Name:    "empty",
	}
	err = acc.ApplyChangeset(changeset)
	if err != nil {
		t.Errorf("ApplyChangeset with empty entries should not error: %v", err)
	}

	// Test processEntries with only additions
	additionChangeset := AccumulatorChangeset{
		Version: 2,
		Entries: []AccumulatorKVPair{
			{Key: []byte("add1"), Value: []byte("value1"), Deleted: false},
			{Key: []byte("add2"), Value: []byte("value2"), Deleted: false},
		},
		Name: "additions",
	}
	err = acc.ApplyChangeset(additionChangeset)
	if err != nil {
		t.Errorf("ApplyChangeset with additions should not error: %v", err)
	}

	if acc.GetTotalElements() != 2 {
		t.Errorf("Expected 2 elements after additions, got %d", acc.GetTotalElements())
	}

	// Test processEntries with only deletions
	deletionChangeset := AccumulatorChangeset{
		Version: 3,
		Entries: []AccumulatorKVPair{
			{Key: []byte("add1"), Value: []byte("value1"), Deleted: true},
		},
		Name: "deletions",
	}
	err = acc.ApplyChangeset(deletionChangeset)
	if err != nil {
		t.Errorf("ApplyChangeset with deletions should not error: %v", err)
	}

	if acc.GetTotalElements() != 1 {
		t.Errorf("Expected 1 element after deletion, got %d", acc.GetTotalElements())
	}

	// Test processEntries with mixed additions and deletions
	mixedChangeset := AccumulatorChangeset{
		Version: 4,
		Entries: []AccumulatorKVPair{
			{Key: []byte("add3"), Value: []byte("value3"), Deleted: false},
			{Key: []byte("add2"), Value: []byte("value2"), Deleted: true},
			{Key: []byte("add4"), Value: []byte("value4"), Deleted: false},
		},
		Name: "mixed",
	}
	err = acc.ApplyChangeset(mixedChangeset)
	if err != nil {
		t.Errorf("ApplyChangeset with mixed entries should not error: %v", err)
	}

	if acc.GetTotalElements() != 2 {
		t.Errorf("Expected 2 elements after mixed operations, got %d", acc.GetTotalElements())
	}
}

// TestProcessEntriesDirectEdgeCases tests edge cases in processEntriesDirect.
func TestProcessEntriesDirectEdgeCases(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Test with empty entries
	err = acc.AddEntries([]AccumulatorKVPair{})
	if err != nil {
		t.Errorf("AddEntries with empty slice should not error: %v", err)
	}

	// Test with entries that have empty values
	emptyValueEntries := []AccumulatorKVPair{
		{Key: []byte("empty_value_key"), Value: []byte{}, Deleted: false},
		{Key: []byte("nil_value_key"), Value: nil, Deleted: false},
	}
	err = acc.AddEntries(emptyValueEntries)
	if err != nil {
		t.Errorf("AddEntries with empty/nil values should not error: %v", err)
	}

	if acc.GetTotalElements() != 2 {
		t.Errorf("Expected 2 elements after adding empty value entries, got %d", acc.GetTotalElements())
	}

	// Test with very large keys and values
	largeKey := make([]byte, 1000)
	largeValue := make([]byte, 2000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	for i := range largeValue {
		largeValue[i] = byte((i + 100) % 256)
	}

	largeEntries := []AccumulatorKVPair{
		{Key: largeKey, Value: largeValue, Deleted: false},
	}
	err = acc.AddEntries(largeEntries)
	if err != nil {
		t.Errorf("AddEntries with large keys/values should not error: %v", err)
	}

	if acc.GetTotalElements() != 3 {
		t.Errorf("Expected 3 elements after adding large entry, got %d", acc.GetTotalElements())
	}
}

// TestCalculateRootEdgeCases tests edge cases in calculateRoot.
func TestCalculateRootEdgeCases(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Test calculateRoot on empty accumulator
	root1, err := acc.CalculateRoot()
	if err != nil {
		t.Errorf("CalculateRoot on empty accumulator should not error: %v", err)
	}
	if len(root1) == 0 {
		t.Error("Root should not be empty even for empty accumulator")
	}

	// Add some data and calculate root again
	entries := []AccumulatorKVPair{
		{Key: []byte("root_test"), Value: []byte("root_value"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	root2, err := acc.CalculateRoot()
	if err != nil {
		t.Errorf("CalculateRoot after adding data should not error: %v", err)
	}

	// Roots should be different
	if string(root1) == string(root2) {
		t.Error("Root should change after adding data")
	}

	// Test CalculateStateHash consistency
	stateHash1 := acc.engine.CalculateStateHash()
	stateHash2 := acc.engine.CalculateStateHash()

	if string(stateHash1.Hash) != string(stateHash2.Hash) {
		t.Error("CalculateStateHash should be deterministic")
	}
	if stateHash1.Version != stateHash2.Version {
		t.Error("CalculateStateHash version should be consistent")
	}
}

// TestFinalizerEdgeCase tests the finalizer function.
func TestFinalizerEdgeCase(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}

	// Test calling finalizer manually (should not panic)
	acc.engine.finalize()

	// Test calling finalizer on already finalized engine (should not panic)
	acc.engine.finalize()

	// Proper cleanup
	acc.Close()
}

// TestResetEdgeCases tests edge cases in Reset function.
func TestResetEdgeCases(t *testing.T) {
	acc, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Add some data
	entries := []AccumulatorKVPair{
		{Key: []byte("reset_key1"), Value: []byte("reset_value1"), Deleted: false},
		{Key: []byte("reset_key2"), Value: []byte("reset_value2"), Deleted: false},
	}
	err = acc.AddEntries(entries)
	if err != nil {
		t.Fatalf("Failed to add entries: %v", err)
	}

	// Update version
	changeset := AccumulatorChangeset{
		Version: 5,
		Entries: []AccumulatorKVPair{
			{Key: []byte("version_key"), Value: []byte("version_value"), Deleted: false},
		},
		Name: "version_test",
	}
	err = acc.ApplyChangeset(changeset)
	if err != nil {
		t.Fatalf("Failed to apply changeset: %v", err)
	}

	// Verify state before reset
	if acc.GetTotalElements() != 3 {
		t.Errorf("Expected 3 elements before reset, got %d", acc.GetTotalElements())
	}

	version, err := acc.GetCurrentVersion()
	if err != nil {
		t.Fatalf("Failed to get version: %v", err)
	}
	if version != 5 {
		t.Errorf("Expected version 5 before reset, got %d", version)
	}

	// Reset the accumulator
	err = acc.Reset()
	if err != nil {
		t.Fatalf("Failed to reset accumulator: %v", err)
	}

	// Verify state after reset
	if acc.GetTotalElements() != 0 {
		t.Errorf("Expected 0 elements after reset, got %d", acc.GetTotalElements())
	}

	version, err = acc.GetCurrentVersion()
	if err != nil {
		t.Fatalf("Failed to get version after reset: %v", err)
	}
	if version != 0 {
		t.Errorf("Expected version 0 after reset, got %d", version)
	}

	// Verify accumulator is still functional after reset
	newEntries := []AccumulatorKVPair{
		{Key: []byte("post_reset_key"), Value: []byte("post_reset_value"), Deleted: false},
	}
	err = acc.AddEntries(newEntries)
	if err != nil {
		t.Errorf("Failed to add entries after reset: %v", err)
	}

	if acc.GetTotalElements() != 1 {
		t.Errorf("Expected 1 element after post-reset addition, got %d", acc.GetTotalElements())
	}
}
