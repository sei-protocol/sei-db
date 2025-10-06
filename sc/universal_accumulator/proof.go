package universalaccumulator

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// AccumulatorProof represents a proof response similar to eth_getProof
type AccumulatorProof struct {
	Height       uint64         `json:"height"`
	Root         string         `json:"root"`    // Hex-encoded root hash
	Address      string         `json:"address"` // Hex-encoded address
	StorageProof []StorageProof `json:"storageProof"`
}

// StorageProof represents a single storage slot proof
type StorageProof struct {
	Key   string   `json:"key"`   // Hex-encoded storage key
	Value string   `json:"value"` // Hex-encoded storage value
	Proof *Witness `json:"proof"` // Universal accumulator witness
}

// PerHeightStorage interface for managing per-height state
type PerHeightStorage interface {
	StoreFactor(height uint64, factor Factor) error
	GetFactor(height uint64) (Factor, error)
	StoreRoot(height uint64, root []byte) error
	GetRoot(height uint64) ([]byte, error)
	GetStorageValue(address, key []byte, height uint64) ([]byte, bool, error)
}

// AccumulatorProofAPI provides eth_getProof-like functionality
type AccumulatorProofAPI struct {
	acc     *UniversalAccumulator
	storage PerHeightStorage
}

// NewAccumulatorProofAPI creates a new proof API instance
func NewAccumulatorProofAPI(acc *UniversalAccumulator, storage PerHeightStorage) *AccumulatorProofAPI {
	return &AccumulatorProofAPI{
		acc:     acc,
		storage: storage,
	}
}

// GetProof generates a proof for a key-value pair at a specific height
// Similar to eth_getProof RPC method
func (api *AccumulatorProofAPI) GetProof(
	address string, // Account address (hex)
	storageKeys []string, // Storage keys (hex array)
	blockHeight string, // Block height ("latest" or hex number)
) (*AccumulatorProof, error) {

	// Parse address
	addr, err := hex.DecodeString(strings.TrimPrefix(address, "0x"))
	if err != nil {
		return nil, fmt.Errorf("invalid address: %v", err)
	}

	// Parse block height
	height, err := api.parseBlockHeight(blockHeight)
	if err != nil {
		return nil, fmt.Errorf("invalid block height: %v", err)
	}

	// Restore accumulator to target height
	factor, err := api.storage.GetFactor(height)
	if err != nil {
		return nil, fmt.Errorf("failed to get factor for height %d: %v", height, err)
	}

	if err := api.acc.SetStateFromFactor(factor); err != nil {
		return nil, fmt.Errorf("failed to restore state: %v", err)
	}

	// Generate proofs for each storage key
	var storageProofs []StorageProof
	for _, keyHex := range storageKeys {
		key, err := hex.DecodeString(strings.TrimPrefix(keyHex, "0x"))
		if err != nil {
			return nil, fmt.Errorf("invalid storage key %s: %v", keyHex, err)
		}

		// Get the value from storage
		value, exists, err := api.storage.GetStorageValue(addr, key, height)
		if err != nil {
			return nil, fmt.Errorf("failed to get storage value: %v", err)
		}

		// Construct the full key: address + storage_key
		fullKey := append(append([]byte(nil), addr...), key...)

		// Generate witness
		witness, err := api.acc.IssueWitness(fullKey, value, exists)
		if err != nil {
			return nil, fmt.Errorf("failed to generate witness: %v", err)
		}

		storageProofs = append(storageProofs, StorageProof{
			Key:   "0x" + hex.EncodeToString(key),
			Value: "0x" + hex.EncodeToString(value),
			Proof: witness,
		})
	}

	// Get the root hash for this height
	root, err := api.storage.GetRoot(height)
	if err != nil {
		return nil, fmt.Errorf("failed to get root for height %d: %v", height, err)
	}

	return &AccumulatorProof{
		Height:       height,
		Root:         "0x" + hex.EncodeToString(root),
		Address:      "0x" + hex.EncodeToString(addr),
		StorageProof: storageProofs,
	}, nil
}

// VerifyProof verifies an accumulator proof against a known root
func (api *AccumulatorProofAPI) VerifyProof(proof *AccumulatorProof, consensusRoot string) (bool, error) {
	// Parse consensus root
	expectedRoot, err := hex.DecodeString(strings.TrimPrefix(consensusRoot, "0x"))
	if err != nil {
		return false, fmt.Errorf("invalid consensus root: %v", err)
	}

	// Parse proof root
	proofRoot, err := hex.DecodeString(strings.TrimPrefix(proof.Root, "0x"))
	if err != nil {
		return false, fmt.Errorf("invalid proof root: %v", err)
	}

	// Check root consistency
	if !bytes.Equal(proofRoot, expectedRoot) {
		return false, nil
	}

	// Parse address
	_, err = hex.DecodeString(strings.TrimPrefix(proof.Address, "0x"))
	if err != nil {
		return false, fmt.Errorf("invalid address in proof: %v", err)
	}

	// Restore accumulator state for verification
	factor, err := api.storage.GetFactor(proof.Height)
	if err != nil {
		return false, fmt.Errorf("failed to get factor: %v", err)
	}

	if err := api.acc.SetStateFromFactor(factor); err != nil {
		return false, fmt.Errorf("failed to restore state: %v", err)
	}

	// Verify each storage proof
	for _, sp := range proof.StorageProof {
		// Parse key and value
		_, err := hex.DecodeString(strings.TrimPrefix(sp.Key, "0x"))
		if err != nil {
			return false, fmt.Errorf("invalid key in storage proof: %v", err)
		}

		// Verify the witness
		if !api.acc.VerifyWitness(sp.Proof) {
			return false, nil
		}
	}

	return true, nil
}

// parseBlockHeight parses block height from string
func (api *AccumulatorProofAPI) parseBlockHeight(blockHeight string) (uint64, error) {
	if blockHeight == "latest" {
		// Return the latest height from storage
		// This would need to be implemented based on your storage system
		return 0, fmt.Errorf("latest block height resolution not implemented")
	}

	// Parse hex number
	heightStr := strings.TrimPrefix(blockHeight, "0x")
	height, err := strconv.ParseUint(heightStr, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid hex number: %v", err)
	}

	return height, nil
}
