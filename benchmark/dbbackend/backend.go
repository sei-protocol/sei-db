package dbbackend

type DBBackend interface {
	BenchmarkDBWrite(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int)
	BenchmarkDBRead(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int)
	BenchmarkDBForwardIteration(inputKVDir string, outputDBPath string, concurrency int, maxIterationsPerFile int, iterationSteps int)
	BenchmarkDBReverseIteration(inputKVDir string, outputDBPath string, concurrency int, maxIterationsPerFile int, iterationSteps int)
}

type RocksDBBackend struct{}
