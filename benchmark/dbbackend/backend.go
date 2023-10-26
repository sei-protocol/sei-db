//go:build rocksdbBackend
// +build rocksdbBackend

package dbbackend

import (
	"fmt"
)

const RocksDBBackendName = "rocksDB"

type DBBackend interface {
	BenchmarkDBWrite(inputKVDir string, numVersions int, outputDBPath string, concurrency int, batchSize int)
	BenchmarkDBRead(inputKVDir string, numVersions int, outputDBPath string, concurrency int, maxOps int64)
	BenchmarkDBForwardIteration(inputKVDir string, numVersions int, outputDBPath string, concurrency int, maxOps int64, iterationSteps int)
	BenchmarkDBReverseIteration(inputKVDir string, numVersions int, outputDBPath string, concurrency int, maxOps int64, iterationSteps int)
}

func CreateDBBackend(dbBackend string) (DBBackend, error) {

	if dbBackend == RocksDBBackendName {
		return RocksDBBackend{}, nil
	}

	return nil, fmt.Errorf("db backend %s not supported", dbBackend)
}
