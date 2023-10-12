package dbbackend

type DBBackend interface {
	BenchmarkDBWrite(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int)
	BenchmarkDBRead(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int)
	BenchmarkDBForwardIteration(inputKVDir string, prefixes []string, outputDBPath string, concurrency int)
	BenchmarkDBReverseIteration(inputKVDir string, prefixes []string, outputDBPath string, concurrency int)
}

type RocksDBBackend struct{}
