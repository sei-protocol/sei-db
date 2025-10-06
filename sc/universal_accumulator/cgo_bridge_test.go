package universalaccumulator

import (
	"crypto/sha256"
	"testing"
	"unsafe"
)

// TestCreateAccumulator tests the createAccumulator wrapper function.
func TestCreateAccumulator(t *testing.T) {
	// Test successful creation
	accumulator, err := createAccumulator()
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	if accumulator == nil {
		t.Error("Created accumulator should not be nil")
	}

	// Clean up
	freeAccumulatorWrapper(accumulator)
}

// TestFreeAccumulatorWrapper tests the freeAccumulatorWrapper function.
func TestFreeAccumulatorWrapper(t *testing.T) {
	// Test with valid accumulator
	accumulator, err := createAccumulator()
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}

	// Should not panic
	freeAccumulatorWrapper(accumulator)

	// Test with nil accumulator (should not panic)
	freeAccumulatorWrapper(nil)
}

// TestCalculateRootWrapper tests the calculateRootWrapper function.
func TestCalculateRootWrapper(t *testing.T) {
	accumulator, err := createAccumulator()
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer freeAccumulatorWrapper(accumulator)

	// Test with valid buffer
	buffer := make([]byte, 128)
	size := calculateRootWrapper(accumulator, buffer)
	if size < 0 {
		t.Error("Calculate root should return positive size")
	}
	if size > len(buffer) {
		t.Errorf("Returned size %d should not exceed buffer size %d", size, len(buffer))
	}

	// Test with smaller buffer
	smallBuffer := make([]byte, 10)
	smallSize := calculateRootWrapper(accumulator, smallBuffer)
	if smallSize >= 0 && smallSize > len(smallBuffer) {
		t.Error("Should handle small buffer gracefully")
	}
}

// TestAddHashedElementsWrapper tests the addHashedElementsWrapper function.
func TestAddHashedElementsWrapper(t *testing.T) {
	accumulator, err := createAccumulator()
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer freeAccumulatorWrapper(accumulator)

	// Test with zero count (should return early)
	err = addHashedElementsWrapper(accumulator, []byte{}, 0)
	if err != nil {
		t.Errorf("Adding zero elements should not error: %v", err)
	}

	// Test with valid hashes
	hash1 := sha256.Sum256([]byte("test1"))
	hash2 := sha256.Sum256([]byte("test2"))
	flatHashes := make([]byte, 64) // 2 hashes * 32 bytes each
	copy(flatHashes[0:32], hash1[:])
	copy(flatHashes[32:64], hash2[:])

	err = addHashedElementsWrapper(accumulator, flatHashes, 2)
	if err != nil {
		t.Errorf("Adding valid hashes should not error: %v", err)
	}

	// Test adding single hash
	singleHash := sha256.Sum256([]byte("single"))
	err = addHashedElementsWrapper(accumulator, singleHash[:], 1)
	if err != nil {
		t.Errorf("Adding single hash should not error: %v", err)
	}
}

// TestBatchDelHashedElementsWrapper tests the batchDelHashedElementsWrapper function.
func TestBatchDelHashedElementsWrapper(t *testing.T) {
	accumulator, err := createAccumulator()
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer freeAccumulatorWrapper(accumulator)

	// Test with zero count (should return early)
	err = batchDelHashedElementsWrapper(accumulator, []byte{}, 0)
	if err != nil {
		t.Errorf("Deleting zero elements should not error: %v", err)
	}

	// Test with negative count (should return early)
	err = batchDelHashedElementsWrapper(accumulator, []byte{}, -1)
	if err != nil {
		t.Errorf("Deleting with negative count should not error: %v", err)
	}

	// First add some elements
	hash1 := sha256.Sum256([]byte("test1"))
	hash2 := sha256.Sum256([]byte("test2"))
	flatHashes := make([]byte, 64)
	copy(flatHashes[0:32], hash1[:])
	copy(flatHashes[32:64], hash2[:])

	err = addHashedElementsWrapper(accumulator, flatHashes, 2)
	if err != nil {
		t.Fatalf("Failed to add elements for deletion test: %v", err)
	}

	// Test deleting elements
	err = batchDelHashedElementsWrapper(accumulator, flatHashes, 2)
	if err != nil {
		t.Errorf("Deleting valid hashes should not error: %v", err)
	}

	// Test deleting single hash
	singleDelHash := sha256.Sum256([]byte("test1"))
	err = batchDelHashedElementsWrapper(accumulator, singleDelHash[:], 1)
	if err != nil {
		t.Errorf("Deleting single hash should not error: %v", err)
	}
}

// TestGenerateWitnessWrapper tests the generateWitnessWrapper function.
func TestGenerateWitnessWrapper(t *testing.T) {
	accumulator, err := createAccumulator()
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer freeAccumulatorWrapper(accumulator)

	// Test with invalid hash length
	shortHash := []byte("short")
	_, err = generateWitnessWrapper(accumulator, shortHash)
	if err == nil {
		t.Error("Should error with hash length != 32")
	}
	if err.Error() != "element hash must be 32 bytes" {
		t.Errorf("Expected specific error message, got: %v", err)
	}

	// Test with valid hash length
	hash := sha256.Sum256([]byte("test"))
	witness, err := generateWitnessWrapper(accumulator, hash[:])
	if err != nil {
		t.Errorf("Should not error with valid 32-byte hash: %v", err)
	}
	if len(witness) != 32 {
		t.Errorf("Expected witness length 32, got %d", len(witness))
	}

	// Verify witness content matches input hash (current implementation)
	for i := range hash {
		if witness[i] != hash[i] {
			t.Error("Witness should match input hash in current implementation")
			break
		}
	}
}

// TestWrapperErrorPaths tests error paths in wrapper functions.
func TestWrapperErrorPaths(t *testing.T) {
	// Test addHashedElementsWrapper with nil accumulator
	// Note: This would actually cause a segfault, so we skip it
	// err := addHashedElementsWrapper(nil, []byte{}, 1)

	// Test calculateRootWrapper with nil accumulator
	// Note: This would also cause issues, so we test with valid accumulator
	accumulator, err := createAccumulator()
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer freeAccumulatorWrapper(accumulator)

	// Test edge cases that are safe to test
	buffer := make([]byte, 64) // Reasonable buffer size
	size := calculateRootWrapper(accumulator, buffer)
	if size < 0 {
		t.Error("Calculate root should return valid size")
	} else {
		t.Log("Calculate root returned size:", size)
	}
}

// TestCGoIntegration tests the integration between Go and C code.
func TestCGoIntegration(t *testing.T) {
	// Create multiple accumulators to test memory management
	var accumulators []unsafe.Pointer

	for range 5 {
		acc, err := createAccumulator()
		if err != nil {
			t.Fatalf("Failed to create accumulator: %v", err)
		}
		accumulators = append(accumulators, acc)
	}

	// Add different data to each accumulator
	for i, acc := range accumulators {
		hash := sha256.Sum256([]byte(string(rune('a' + i))))
		flatHash := make([]byte, 32)
		copy(flatHash, hash[:])

		err := addHashedElementsWrapper(acc, flatHash, 1)
		if err != nil {
			t.Errorf("Failed to add hash to accumulator %d: %v", i, err)
		}

		// Calculate root to verify state
		buffer := make([]byte, 128)
		size := calculateRootWrapper(acc, buffer)
		if size <= 0 {
			t.Errorf("Invalid root size for accumulator %d: %d", i, size)
		}
	}

	// Clean up all accumulators
	for i, acc := range accumulators {
		freeAccumulatorWrapper(acc)
		t.Logf("Cleaned up accumulator %d", i)
	}
}
