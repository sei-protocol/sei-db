package universalaccumulator

import (
	"encoding/hex"
	"errors"
	"testing"
)

// MockPerHeightStorage implements PerHeightStorage for testing
type MockPerHeightStorage struct {
	factors map[uint64]Factor
	roots   map[uint64][]byte
	storage map[string]map[string][]byte // address -> key -> value
}

func NewMockPerHeightStorage() *MockPerHeightStorage {
	return &MockPerHeightStorage{
		factors: make(map[uint64]Factor),
		roots:   make(map[uint64][]byte),
		storage: make(map[string]map[string][]byte),
	}
}

func (m *MockPerHeightStorage) StoreFactor(height uint64, factor Factor) error {
	m.factors[height] = factor
	return nil
}

func (m *MockPerHeightStorage) GetFactor(height uint64) (Factor, error) {
	factor, exists := m.factors[height]
	if !exists {
		return nil, errors.New("factor not found")
	}
	return factor, nil
}

func (m *MockPerHeightStorage) StoreRoot(height uint64, root []byte) error {
	m.roots[height] = root
	return nil
}

func (m *MockPerHeightStorage) GetRoot(height uint64) ([]byte, error) {
	root, exists := m.roots[height]
	if !exists {
		return nil, errors.New("root not found")
	}
	return root, nil
}

func (m *MockPerHeightStorage) GetStorageValue(address, key []byte, height uint64) ([]byte, bool, error) {
	addrKey := hex.EncodeToString(address)
	storageKey := hex.EncodeToString(key)

	if addrStorage, exists := m.storage[addrKey]; exists {
		if value, exists := addrStorage[storageKey]; exists {
			return value, true, nil
		}
	}
	return nil, false, nil
}

func (m *MockPerHeightStorage) SetStorageValue(address, key, value []byte) {
	addrKey := hex.EncodeToString(address)
	storageKey := hex.EncodeToString(key)

	if m.storage[addrKey] == nil {
		m.storage[addrKey] = make(map[string][]byte)
	}
	m.storage[addrKey][storageKey] = value
}

func TestAccumulatorProofAPI_Basic(t *testing.T) {
	// Initialize accumulator
	acc, err := NewUniversalAccumulator(1000)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Initialize mock storage
	storage := NewMockPerHeightStorage()

	// Create proof API
	_ = NewAccumulatorProofAPI(acc, storage)

	// Test data
	address := []byte{0x12, 0x34, 0x56, 0x78}
	storageKey := []byte{0xab, 0xcd, 0xef, 0x00}
	storageValue := []byte{0x11, 0x22, 0x33, 0x44}

	// Set up mock storage
	storage.SetStorageValue(address, storageKey, storageValue)

	// Add some data to accumulator for height 100
	fullKey := append(append([]byte(nil), address...), storageKey...)
	changeset := AccumulatorChangeset{
		Version: 100,
		Entries: []AccumulatorKVPair{
			{Key: fullKey, Value: storageValue, Deleted: false},
		},
	}

	err = acc.ProcessBlock(100, changeset)
	if err != nil {
		t.Fatalf("Failed to process block: %v", err)
	}

	// Store the state for height 100
	factor, err := acc.Factor()
	if err != nil {
		t.Fatalf("Failed to get factor: %v", err)
	}

	stateHash, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate root: %v", err)
	}

	err = storage.StoreFactor(100, factor)
	if err != nil {
		t.Fatalf("Failed to store factor: %v", err)
	}

	err = storage.StoreRoot(100, stateHash)
	if err != nil {
		t.Fatalf("Failed to store root: %v", err)
	}

	t.Logf("Stored factor for height 100: %x", factor)
	t.Logf("Stored root for height 100: %x", stateHash)
}

func TestAccumulatorProofAPI_GetProof(t *testing.T) {
	// Initialize accumulator
	acc, err := NewUniversalAccumulator(1000)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Initialize mock storage
	storage := NewMockPerHeightStorage()

	// Create proof API
	api := NewAccumulatorProofAPI(acc, storage)

	// Test data
	address := []byte{0x12, 0x34, 0x56, 0x78}
	storageKey := []byte{0xab, 0xcd, 0xef, 0x00}
	storageValue := []byte{0x11, 0x22, 0x33, 0x44}

	// Set up mock storage
	storage.SetStorageValue(address, storageKey, storageValue)

	// Add data to accumulator
	fullKey := append(append([]byte(nil), address...), storageKey...)
	changeset := AccumulatorChangeset{
		Version: 200,
		Entries: []AccumulatorKVPair{
			{Key: fullKey, Value: storageValue, Deleted: false},
		},
	}

	err = acc.ProcessBlock(200, changeset)
	if err != nil {
		t.Fatalf("Failed to process block: %v", err)
	}

	// Store the state for height 200
	factor, err := acc.Factor()
	if err != nil {
		t.Fatalf("Failed to get factor: %v", err)
	}

	stateHash, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate root: %v", err)
	}

	err = storage.StoreFactor(200, factor)
	if err != nil {
		t.Fatalf("Failed to store factor: %v", err)
	}

	err = storage.StoreRoot(200, stateHash)
	if err != nil {
		t.Fatalf("Failed to store root: %v", err)
	}

	// Test GetProof
	addressHex := "0x" + hex.EncodeToString(address)
	storageKeyHex := "0x" + hex.EncodeToString(storageKey)

	proof, err := api.GetProof(
		addressHex,
		[]string{storageKeyHex},
		"0xc8", // 200 in hex
	)

	if err != nil {
		t.Fatalf("GetProof failed: %v", err)
	}

	// Verify proof structure
	if proof.Height != 200 {
		t.Errorf("Expected height 200, got %d", proof.Height)
	}

	if proof.Address != addressHex {
		t.Errorf("Expected address %s, got %s", addressHex, proof.Address)
	}

	if len(proof.StorageProof) != 1 {
		t.Fatalf("Expected 1 storage proof, got %d", len(proof.StorageProof))
	}

	storageProof := proof.StorageProof[0]
	if storageProof.Key != storageKeyHex {
		t.Errorf("Expected storage key %s, got %s", storageKeyHex, storageProof.Key)
	}

	expectedValueHex := "0x" + hex.EncodeToString(storageValue)
	if storageProof.Value != expectedValueHex {
		t.Errorf("Expected storage value %s, got %s", expectedValueHex, storageProof.Value)
	}

	if storageProof.Proof == nil {
		t.Error("Expected witness proof, got nil")
	}

	t.Logf("✓ GetProof test passed!")
	t.Logf("  Height: %d", proof.Height)
	t.Logf("  Root: %s", proof.Root)
	t.Logf("  Address: %s", proof.Address)
	t.Logf("  Storage Key: %s", storageProof.Key)
	t.Logf("  Storage Value: %s", storageProof.Value)
}

func TestAccumulatorProofAPI_VerifyProof(t *testing.T) {
	// Initialize accumulator
	acc, err := NewUniversalAccumulator(1000)
	if err != nil {
		t.Fatalf("Failed to create accumulator: %v", err)
	}
	defer acc.Close()

	// Initialize mock storage
	storage := NewMockPerHeightStorage()

	// Create proof API
	api := NewAccumulatorProofAPI(acc, storage)

	// Test data
	address := []byte{0xaa, 0xbb, 0xcc, 0xdd}
	storageKey := []byte{0x11, 0x22, 0x33, 0x44}
	storageValue := []byte{0xff, 0xee, 0xdd, 0xcc}

	// Set up mock storage
	storage.SetStorageValue(address, storageKey, storageValue)

	// Add data to accumulator
	fullKey := append(append([]byte(nil), address...), storageKey...)
	changeset := AccumulatorChangeset{
		Version: 300,
		Entries: []AccumulatorKVPair{
			{Key: fullKey, Value: storageValue, Deleted: false},
		},
	}

	err = acc.ProcessBlock(300, changeset)
	if err != nil {
		t.Fatalf("Failed to process block: %v", err)
	}

	// Store the state for height 300
	factor, err := acc.Factor()
	if err != nil {
		t.Fatalf("Failed to get factor: %v", err)
	}

	stateHash, err := acc.CalculateRoot()
	if err != nil {
		t.Fatalf("Failed to calculate root: %v", err)
	}

	err = storage.StoreFactor(300, factor)
	if err != nil {
		t.Fatalf("Failed to store factor: %v", err)
	}

	err = storage.StoreRoot(300, stateHash)
	if err != nil {
		t.Fatalf("Failed to store root: %v", err)
	}

	// Generate proof
	addressHex := "0x" + hex.EncodeToString(address)
	storageKeyHex := "0x" + hex.EncodeToString(storageKey)

	proof, err := api.GetProof(
		addressHex,
		[]string{storageKeyHex},
		"0x12c", // 300 in hex
	)

	if err != nil {
		t.Fatalf("GetProof failed: %v", err)
	}

	// Verify the proof
	consensusRoot := "0x" + hex.EncodeToString(stateHash)
	valid, err := api.VerifyProof(proof, consensusRoot)
	if err != nil {
		t.Fatalf("VerifyProof failed: %v", err)
	}

	if !valid {
		t.Error("Proof verification failed")
	}

	// Test with wrong root
	wrongRoot := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	valid, err = api.VerifyProof(proof, wrongRoot)
	if err != nil {
		t.Fatalf("VerifyProof failed: %v", err)
	}

	if valid {
		t.Error("Proof should not be valid with wrong root")
	}

	t.Logf("✓ VerifyProof test passed!")
	t.Logf("  Valid proof verified: true")
	t.Logf("  Invalid proof rejected: true")
}
