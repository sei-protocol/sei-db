package dbbackend

type DBBackend interface {
	BenchmarkDBWrite(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int)
	BenchmarkDBRead(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int)
	BenchmarkDBIteration(prefixes []string, outputDBPath string, concurrency int)
	BenchmarkDBReverse(prefixes []string, outputDBPath string, concurrency int)
}

type RocksDBBackend struct{}
