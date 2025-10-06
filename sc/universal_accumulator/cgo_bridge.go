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
#cgo darwin,amd64 LDFLAGS: -L/opt/homebrew/opt/libomp/lib
#cgo !linux,!darwin CFLAGS: -I/opt/homebrew/include -I/usr/local/include -I/usr/include
#cgo !linux,!darwin CFLAGS: -I/opt/homebrew/opt/libomp/include
#cgo !linux,!darwin LDFLAGS: -L/opt/homebrew/lib -L/usr/local/lib -L/usr/lib -lrelic_s -lssl -lcrypto -lgmp -lomp

#include "universal_accumulator.h"
*/
import "C"

import (
	"errors"
	"unsafe"
)

// Go wrapper functions for C functions.

func addHashedElementsWrapper(accumulator unsafe.Pointer, flatHashes []byte, count int) error {
	if count == 0 {
		return nil
	}

	// Use C.add_hashed_elements directly
	ret := C.add_hashed_elements((*C.t_state)(accumulator), (*C.uchar)(unsafe.Pointer(&flatHashes[0])), C.int(count))
	if ret != 0 {
		return errors.New("add_hashed_elements failed, possibly due to memory allocation error")
	}

	return nil
}

func batchDelHashedElementsWrapper(accumulator unsafe.Pointer, flatHashes []byte, count int) error {
	if count <= 0 {
		return nil
	}

	// Use the existing batch_del_hashed_elements C function
	ret := C.batch_del_hashed_elements((*C.t_state)(accumulator), (*C.uchar)(unsafe.Pointer(&flatHashes[0])), C.int(count))
	if ret != 0 {
		return errors.New("batch_del_hashed_elements failed, possibly due to memory allocation error")
	}

	return nil
}

func calculateRootWrapper(accumulator unsafe.Pointer, buffer []byte) int {
	acc := (*C.t_state)(accumulator)
	return int(C.calculate_root(acc,
		(*C.uchar)(unsafe.Pointer(&buffer[0])),
		C.int(len(buffer))))
}

func freeAccumulatorWrapper(accumulator unsafe.Pointer) {
	if accumulator != nil {
		C.destroy_accumulator((*C.t_state)(accumulator))
	}
}

func createAccumulator() (unsafe.Pointer, error) {
	accumulator := C.malloc(C.size_t(unsafe.Sizeof(C.t_state{})))
	if accumulator == nil {
		return nil, errors.New("failed to allocate accumulator memory")
	}

	C.init((*C.t_state)(accumulator))
	return accumulator, nil
}

// getAccumulatorFactor retrieves the serialized big-endian bytes of fVa from C.
func getAccumulatorFactor(accumulator unsafe.Pointer) ([]byte, error) {
	cAcc := (*C.t_state)(accumulator)
	// Probe size: passing NULL buffer returns negative required size
	required := C.get_fva(cAcc, (*C.uchar)(nil), C.int(0))
	if required == 0 {
		return nil, errors.New("unexpected fva size 0")
	}
	var size int
	if required < 0 {
		size = -int(required)
	} else {
		size = int(required)
	}
	buf := make([]byte, size)
	written := C.get_fva(cAcc, (*C.uchar)(unsafe.Pointer(&buf[0])), C.int(size))
	if written < 0 {
		return nil, errors.New("failed to get fva")
	}
	return buf[:int(written)], nil
}

// setAccumulatorStateFromFactor sets fVa and recomputes V/eVPt in C.
func setAccumulatorStateFromFactor(accumulator unsafe.Pointer, factor []byte) error {
	if len(factor) == 0 {
		return errors.New("empty factor")
	}
	cAcc := (*C.t_state)(accumulator)
	ret := C.set_state_from_factor(cAcc, (*C.uchar)(unsafe.Pointer(&factor[0])), C.int(len(factor)))
	if ret != 0 {
		return errors.New("set_state_from_factor failed")
	}
	return nil
}

// generateWitnessWrapper generates a witness for a given element hash.
func generateWitnessWrapper(accumulator unsafe.Pointer, elementHash []byte) ([]byte, error) {
	cAccumulator := (*C.t_state)(accumulator)

	if len(elementHash) != 32 {
		return nil, errors.New("element hash must be 32 bytes")
	}

	// Generate witness using C function
	witness := C.issue_witness_from_hash(cAccumulator, (*C.uchar)(unsafe.Pointer(&elementHash[0])), C.bool(true))
	if witness == nil {
		return nil, errors.New("failed to generate witness")
	}
	defer C.destroy_witness(witness)

	// For simplicity, return the element hash as witness
	// In a full implementation, this would serialize the actual witness structure
	result := make([]byte, 32)
	copy(result, elementHash)
	return result, nil
}
