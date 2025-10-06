package universalaccumulator

import "errors"

// Core accumulator errors
var (
	// ErrNotInitialized indicates the accumulator is not properly initialized
	ErrNotInitialized = errors.New("accumulator not initialized")

	// ErrInvalidFactor indicates an invalid factor was provided
	ErrInvalidFactor = errors.New("invalid factor")

	// ErrFactorSize indicates the factor size is incorrect
	ErrFactorSize = errors.New("empty factor")

	// ErrAlreadyInitialized indicates the accumulator is already initialized
	ErrAlreadyInitialized = errors.New("accumulator already initialized")
)

// Witness and proof errors
var (
	// ErrWitnessGeneration indicates failure to generate a witness
	ErrWitnessGeneration = errors.New("failed to generate witness")

	// ErrWitnessVerification indicates failure to verify a witness
	ErrWitnessVerification = errors.New("failed to verify witness")

	// ErrInvalidWitness indicates the witness is invalid or corrupted
	ErrInvalidWitness = errors.New("invalid witness")

	// ErrElementHashSize indicates incorrect element hash size
	ErrElementHashSize = errors.New("element hash must be 32 bytes")
)

// State management errors
var (
	// ErrStateCalculation indicates failure to calculate state
	ErrStateCalculation = errors.New("failed to calculate state")

	// ErrStateRestore indicates failure to restore state from factor
	ErrStateRestore = errors.New("failed to restore state from factor")

	// ErrSnapshotCreation indicates failure to create snapshot
	ErrSnapshotCreation = errors.New("failed to create snapshot")

	// ErrSnapshotRestore indicates failure to restore from snapshot
	ErrSnapshotRestore = errors.New("failed to restore from snapshot")
)

// Storage and persistence errors
var (
	// ErrFactorNotFound indicates the factor for given height was not found
	ErrFactorNotFound = errors.New("factor not found")

	// ErrRootNotFound indicates the root for given height was not found
	ErrRootNotFound = errors.New("root not found")

	// ErrStorageValueNotFound indicates the storage value was not found
	ErrStorageValueNotFound = errors.New("storage value not found")

	// ErrInvalidHeight indicates an invalid block height
	ErrInvalidHeight = errors.New("invalid block height")
)

// Proof API errors
var (
	// ErrInvalidAddress indicates an invalid address format
	ErrInvalidAddress = errors.New("invalid address")

	// ErrInvalidStorageKey indicates an invalid storage key format
	ErrInvalidStorageKey = errors.New("invalid storage key")

	// ErrProofGeneration indicates failure to generate proof
	ErrProofGeneration = errors.New("failed to generate proof")

	// ErrProofVerification indicates failure to verify proof
	ErrProofVerification = errors.New("failed to verify proof")

	// ErrRootMismatch indicates proof root doesn't match consensus root
	ErrRootMismatch = errors.New("proof root doesn't match consensus root")
)

// CGO and C library errors
var (
	// ErrCGOCall indicates a CGO function call failed
	ErrCGOCall = errors.New("CGO function call failed")

	// ErrRelicInit indicates RELIC library initialization failed
	ErrRelicInit = errors.New("RELIC library initialization failed")

	// ErrMemoryAllocation indicates memory allocation failed
	ErrMemoryAllocation = errors.New("memory allocation failed")
)
