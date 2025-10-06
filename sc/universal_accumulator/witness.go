package universalaccumulator

/*
#cgo linux CFLAGS: -I/usr/local/include -I/usr/include -fopenmp -DRELIC_THREAD
#cgo linux LDFLAGS: -L/usr/local/lib -L/usr/lib -L/lib/x86_64-linux-gnu
#cgo linux LDFLAGS: -L/usr/lib/x86_64-linux-gnu -lrelic_s -lssl -lcrypto -lgmp -fopenmp
#cgo darwin,arm64 CFLAGS: -I/opt/homebrew/include -I/opt/homebrew/opt/libomp/include
#cgo darwin,arm64 CFLAGS: -I/usr/local/include/relic -I/usr/local/include -DRELIC_THREAD
#cgo darwin,arm64 CFLAGS: -I/opt/homebrew/opt/openssl@3/include -I/opt/homebrew/opt/gmp/include
#cgo darwin,arm64 LDFLAGS: -L/opt/homebrew/lib -L/opt/homebrew/opt/libomp/lib
#cgo darwin,arm64 LDFLAGS: -L/opt/homebrew/opt/openssl@3/lib -L/opt/homebrew/opt/gmp/lib
#cgo darwin,arm64 LDFLAGS: -L/usr/local/lib -lrelic_s -lssl -lcrypto -lgmp -lomp
#cgo darwin,amd64 CFLAGS: -I/usr/local/include -I/opt/homebrew/include -DRELIC_THREAD
#cgo darwin,amd64 CFLAGS: -I/opt/homebrew/opt/libomp/include -I/usr/local/opt/openssl@3/include -I/usr/local/opt/gmp/include
#cgo darwin,amd64 LDFLAGS: -L/usr/local/lib -L/opt/homebrew/lib -L/usr/local/opt/openssl@3/lib -L/usr/local/opt/gmp/lib -lrelic_s -lssl -lcrypto -lgmp -lomp
#cgo !linux,!darwin CFLAGS: -I/opt/homebrew/include -I/usr/local/include -I/usr/include
#cgo !linux,!darwin LDFLAGS: -L/opt/homebrew/lib -L/usr/local/lib -L/usr/lib -lrelic_s -lssl -lcrypto -lgmp
#include "universal_accumulator.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/crypto/sha3"
)

// Witness represents a cryptographic witness for accumulator membership/non-membership.
type Witness struct {
	cWitness     *C.t_witness
	isMembership bool
}

// Free cleans up witness resources.
func (w *Witness) Free() {
	if w.cWitness != nil {
		C.destroy_witness(w.cWitness)
		w.cWitness = nil
	}
}

// IssueWitness creates a membership or non-membership witness for a given key-value pair.
func (acc *UniversalAccumulator) IssueWitness(key, value []byte, isMembership bool) (*Witness, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.engine == nil || !acc.engine.initialized {
		return nil, errors.New("accumulator not initialized")
	}

	// Combine key and value and hash directly (Keccak-256 to match add/delete)
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(key)
	if len(value) > 0 {
		hasher.Write(value)
	}
	hash := hasher.Sum(nil)

	// Use the C helper function to issue witness from hash
	cAcc := (*C.t_state)(acc.engine.accumulator)
	cWitness := C.issue_witness_from_hash(cAcc, (*C.uchar)(unsafe.Pointer(&hash[0])), C.bool(isMembership))
	if cWitness == nil {
		return nil, errors.New("failed to issue witness")
	}

	return &Witness{
		cWitness:     cWitness,
		isMembership: isMembership,
	}, nil
}

// VerifyWitness verifies a witness against the current accumulator state.
func (acc *UniversalAccumulator) VerifyWitness(witness *Witness) bool {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.engine == nil || !acc.engine.initialized || witness == nil || witness.cWitness == nil {
		return false
	}

	// Use the C verification function that returns a boolean result
	cAcc := (*C.t_state)(acc.engine.accumulator)
	result := C.verify_witness(cAcc, witness.cWitness)
	return bool(result)
}

// GenerateWitness generates a witness for a specific element (legacy compatibility).
func (acc *UniversalAccumulator) GenerateWitness(key, value []byte) (*Witness, error) {
	return acc.IssueWitness(key, value, true)
}

// BatchGenerateWitnesses generates witnesses for multiple elements efficiently.
func (acc *UniversalAccumulator) BatchGenerateWitnesses(entries []AccumulatorKVPair) ([]*Witness, error) {
	acc.mu.RLock()
	defer acc.mu.RUnlock()

	if acc.engine == nil || !acc.engine.initialized {
		return nil, errors.New("accumulator not initialized")
	}

	if len(entries) == 0 {
		return []*Witness{}, nil
	}

	witnesses := make([]*Witness, len(entries))

	for i, entry := range entries {
		witness, err := acc.IssueWitness(entry.Key, entry.Value, true)
		if err != nil {
			return nil, fmt.Errorf("failed to generate witness for entry %d: %w", i, err)
		}
		witnesses[i] = witness
	}

	return witnesses, nil
}
