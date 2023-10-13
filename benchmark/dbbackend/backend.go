package dbbackend

type DBBackend interface {
	BenchmarkDBWrite(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int)
	BenchmarkDBRead(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int, maxOps int64)
	BenchmarkDBForwardIteration(inputKVDir string, outputDBPath string, concurrency int, maxOps int64, iterationSteps int)
	BenchmarkDBReverseIteration(inputKVDir string, outputDBPath string, concurrency int, maxOps int64, iterationSteps int)
}

type RocksDBBackend struct{}
