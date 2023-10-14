package dbbackend

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/sei-protocol/sei-db/benchmark/utils"
)

func NewRocksDBOpts() *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	opts.SetTargetFileSizeMultiplier(2)
	opts.SetLevelCompactionDynamicLevelBytes(true)
	opts.EnableStatistics()

	// block based table options
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	// 1G block cache
	bbto.SetBlockSize(32 * 1024)
	bbto.SetBlockCache(grocksdb.NewLRUCache(1 << 30))

	bbto.SetFilterPolicy(grocksdb.NewRibbonHybridFilterPolicy(9.9, 1))
	bbto.SetIndexType(grocksdb.KBinarySearchWithFirstKey)
	bbto.SetOptimizeFiltersForMemory(true)
	opts.SetBlockBasedTableFactory(bbto)
	// improve sst file creation speed: compaction or sst file writer.
	opts.SetCompressionOptionsParallelThreads(4)

	return opts
}

func writeToRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, allKVs []utils.KeyValuePair, concurrency int, version string, batchSize int) []time.Duration {
	var allLatencies []time.Duration
	latencies := make(chan time.Duration, len(allKVs))

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	cfHandle, exists := cfHandles[version]
	if !exists {
		panic(fmt.Sprintf("column family for version %s does not exist\n", version))
	}

	kvsPerRoutine := len(allKVs) / concurrency
	remainder := len(allKVs) % concurrency

	wg := &sync.WaitGroup{}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start := i * kvsPerRoutine
			end := start + kvsPerRoutine

			if i == concurrency-1 {
				end += remainder
			}

			for j := start; j < end; j += batchSize {
				batch := grocksdb.NewWriteBatch()
				defer batch.Destroy()

				batchEnd := j + batchSize
				if batchEnd > end {
					batchEnd = end
				}

				// Add key-value pairs to the batch up to batchSize
				for k := j; k < batchEnd; k++ {
					kv := allKVs[k]
					batch.PutCF(cfHandle, kv.Key, kv.Value)
				}

				startTime := time.Now()
				err := db.Write(wo, batch)
				latency := time.Since(startTime)

				if err == nil {
					latencies <- latency
				} else {
					panic(err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(latencies)

	for l := range latencies {
		allLatencies = append(allLatencies, l)
	}

	return allLatencies
}

func (rocksDB RocksDBBackend) BenchmarkDBWrite(inputKVDir string, numVersions int, outputDBPath string, concurrency int, chunkSize int, batchSize int) {
	versions := utils.GenerateVersionNames(numVersions)
	kvData, err := utils.LoadAndShuffleKV(inputKVDir)
	if err != nil {
		panic(err)
	}

	// RocksDb options
	opts := NewRocksDBOpts()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir, numVersions)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	// Write each version sequentially
	totalTime := time.Duration(0)
	totalLatencies := []time.Duration{}
	for _, vd := range versions {
		// Write shuffled entries to RocksDB concurrently
		startTime := time.Now()
		latencies := writeToRocksDBConcurrently(db, cfHandleMap, kvData, concurrency, vd, batchSize)
		endTime := time.Now()
		totalTime = totalTime + endTime.Sub(startTime)
		totalLatencies = append(totalLatencies, latencies...)
	}

	// Log throughput
	fmt.Printf("Total Successfully Written %d\n", len(totalLatencies))
	fmt.Printf("Total Time taken: %v\n", totalTime)
	fmt.Printf("Throughput: %f writes/sec\n", float64(len(totalLatencies))/totalTime.Seconds())
	fmt.Printf("Total records written %d\n", len(totalLatencies))

	// Sort latencies for percentile calculations
	sort.Slice(totalLatencies, func(i, j int) bool { return totalLatencies[i] < totalLatencies[j] })

	// Calculate average latency
	var totalLatency time.Duration
	for _, l := range totalLatencies {
		totalLatency += l
	}
	avgLatency := totalLatency / time.Duration(len(totalLatencies))

	fmt.Printf("Average Latency: %v\n", avgLatency)
	fmt.Printf("P50 Latency: %v\n", utils.CalculatePercentile(totalLatencies, 50))
	fmt.Printf("P75 Latency: %v\n", utils.CalculatePercentile(totalLatencies, 75))
	fmt.Printf("P99 Latency: %v\n", utils.CalculatePercentile(totalLatencies, 99))
}

func readFromRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, allKVs []utils.KeyValuePair, concurrency int, maxOps int64) []time.Duration {
	var allLatencies []time.Duration
	latencies := make(chan time.Duration, maxOps)

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	var opCounter int64
	wg := &sync.WaitGroup{}

	// Creating a list of version keys for random selection
	versions := make([]string, 0, len(cfHandles))
	for v := range cfHandles {
		versions = append(versions, v)
	}

	// Each goroutine will handle reading a subset of kv pairs
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for atomic.LoadInt64(&opCounter) < maxOps {
				// Randomly pick a version and retrieve its column family handle
				version := versions[rand.Intn(len(versions))]
				cfHandle, exists := cfHandles[version]
				if !exists {
					log.Printf("No handle found for version: %s\n", version)
					continue
				}

				// Randomly pick a key-value pair to read
				kv := allKVs[rand.Intn(len(allKVs))]

				startTime := time.Now()
				_, err := db.GetCF(ro, cfHandle, kv.Key)
				latency := time.Since(startTime)

				if err == nil {
					latencies <- latency
				} else {
					log.Printf("Failed to read key: %v, Error: %v", kv.Key, err)
				}

				// Increment the global operation counter
				atomic.AddInt64(&opCounter, 1)
			}
		}()
	}

	wg.Wait()
	close(latencies)

	for l := range latencies {
		allLatencies = append(allLatencies, l)
	}

	return allLatencies
}

func (rocksDB RocksDBBackend) BenchmarkDBRead(inputKVDir string, numVersions int, outputDBPath string, concurrency int, chunkSize int, maxOps int64) {
	kvData, err := utils.LoadAndShuffleKV(inputKVDir)
	if err != nil {
		panic(err)
	}

	opts := NewRocksDBOpts()

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir, numVersions)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	startTime := time.Now()
	latencies := readFromRocksDBConcurrently(db, cfHandleMap, kvData, concurrency, maxOps)
	endTime := time.Now()

	totalTime := endTime.Sub(startTime)

	// Log throughput
	fmt.Printf("Total Successfully Read %d\n", len(latencies))
	fmt.Printf("Total Time taken: %v\n", totalTime)
	fmt.Printf("Throughput: %f reads/sec\n", float64(len(latencies))/totalTime.Seconds())
	fmt.Printf("Total records read %d\n", len(latencies))

	// Sort latencies for percentile calculations
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	// Calculate average latency
	var totalLatency time.Duration
	for _, l := range latencies {
		totalLatency += l
	}
	avgLatency := totalLatency / time.Duration(len(latencies))

	fmt.Printf("Average Latency: %v\n", avgLatency)
	fmt.Printf("P50 Latency: %v\n", utils.CalculatePercentile(latencies, 50))
	fmt.Printf("P75 Latency: %v\n", utils.CalculatePercentile(latencies, 75))
	fmt.Printf("P99 Latency: %v\n", utils.CalculatePercentile(latencies, 99))
}

func forwardIterateRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, allKVs []utils.KeyValuePair, concurrency int, numIterationSteps int, maxOps int64) ([]time.Duration, int) {
	var allLatencies []time.Duration
	var totalSteps int
	latencies := make(chan time.Duration, maxOps)
	steps := make(chan int, maxOps)

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	var opCounter int64
	wg := &sync.WaitGroup{}

	versions := make([]string, 0, len(cfHandles))
	for v := range cfHandles {
		versions = append(versions, v)
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for atomic.LoadInt64(&opCounter) < maxOps {
				// Randomly pick a version and retrieve its column family handle
				version := versions[rand.Intn(len(versions))]
				cfHandle, exists := cfHandles[version]
				if !exists {
					log.Printf("No handle found for version: %s\n", version)
					continue
				}

				// Randomly pick a key-value pair to seek to
				kv := allKVs[rand.Intn(len(allKVs))]

				it := db.NewIteratorCF(ro, cfHandle)
				defer it.Close()

				startTime := time.Now()
				it.Seek(kv.Key)
				step := 0
				for j := 0; j < numIterationSteps && it.Valid(); it.Next() {
					step++
				}
				latency := time.Since(startTime)

				latencies <- latency
				steps <- step

				// Increment the global operation counter
				atomic.AddInt64(&opCounter, 1)
			}
		}()
	}

	wg.Wait()
	close(latencies)
	close(steps)

	for l := range latencies {
		allLatencies = append(allLatencies, l)
	}

	for s := range steps {
		totalSteps += s
	}

	return allLatencies, totalSteps
}

// TODO: Add Random key iteration latency
func (rocksDB RocksDBBackend) BenchmarkDBForwardIteration(inputKVDir string, numVersions int, outputDBPath string, concurrency int, maxOps int64, iterationSteps int) {
	kvData, err := utils.LoadAndShuffleKV(inputKVDir)
	if err != nil {
		panic(err)
	}

	opts := NewRocksDBOpts()

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir, numVersions)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	startTime := time.Now()
	latencies, totalCountIteration := forwardIterateRocksDBConcurrently(db, cfHandleMap, kvData, concurrency, iterationSteps, maxOps)
	endTime := time.Now()

	totalTime := endTime.Sub(startTime)

	// Log throughput
	fmt.Printf("Total Prefixes Iterated: %d\n", totalCountIteration)
	fmt.Printf("Total Time taken: %v\n", totalTime)
	fmt.Printf("Throughput: %f iterations/sec\n", float64(totalCountIteration)/totalTime.Seconds())

	// Calculate average latency
	var totalLatency time.Duration
	for _, l := range latencies {
		totalLatency += l
	}
	avgLatency := time.Duration(int64(totalLatency) / int64(totalCountIteration))
	fmt.Printf("Average Per-Key Latency: %v\n", avgLatency)
}

func reverseIterateRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, allKVs []utils.KeyValuePair, concurrency int, numIterationSteps int, maxOps int64) ([]time.Duration, int) {
	var allLatencies []time.Duration
	var totalSteps int
	latencies := make(chan time.Duration, maxOps)
	steps := make(chan int, maxOps)

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	var opCounter int64
	wg := &sync.WaitGroup{}

	versions := make([]string, 0, len(cfHandles))
	for v := range cfHandles {
		versions = append(versions, v)
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for atomic.LoadInt64(&opCounter) < maxOps {
				// Randomly pick a version and retrieve its column family handle
				version := versions[rand.Intn(len(versions))]
				cfHandle, exists := cfHandles[version]
				if !exists {
					log.Printf("No handle found for version: %s\n", version)
					continue
				}

				// Randomly pick a key-value pair to seek to
				kv := allKVs[rand.Intn(len(allKVs))]

				it := db.NewIteratorCF(ro, cfHandle)
				defer it.Close()

				startTime := time.Now()
				it.Seek(kv.Key)
				step := 0
				for j := 0; j < numIterationSteps && it.Valid(); it.Prev() {
					step++
				}
				latency := time.Since(startTime)

				latencies <- latency
				steps <- step

				// Increment the global operation counter
				atomic.AddInt64(&opCounter, 1)
			}
		}()
	}

	wg.Wait()
	close(latencies)
	close(steps)

	for l := range latencies {
		allLatencies = append(allLatencies, l)
	}

	for s := range steps {
		totalSteps += s
	}

	return allLatencies, totalSteps
}

// TODO: Add Random key iteration latency
func (rocksDB RocksDBBackend) BenchmarkDBReverseIteration(inputKVDir string, numVersions int, outputDBPath string, concurrency int, maxOps int64, iterationSteps int) {
	kvData, err := utils.LoadAndShuffleKV(inputKVDir)
	if err != nil {
		panic(err)
	}

	opts := NewRocksDBOpts()

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir, numVersions)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	startTime := time.Now()
	latencies, totalCountIteration := reverseIterateRocksDBConcurrently(db, cfHandleMap, kvData, concurrency, iterationSteps, maxOps)
	endTime := time.Now()

	totalTime := endTime.Sub(startTime)

	// Log throughput
	fmt.Printf("Total Prefixes Revserse-Iterated: %d\n", totalCountIteration)
	fmt.Printf("Total Time taken: %v\n", totalTime)
	fmt.Printf("Throughput: %f iterations/sec\n", float64(totalCountIteration)/totalTime.Seconds())

	// Calculate average latency
	var totalLatency time.Duration
	for _, l := range latencies {
		totalLatency += l
	}
	avgLatency := time.Duration(int64(totalLatency) / int64(totalCountIteration))
	fmt.Printf("Average Per-Key Latency: %v\n", avgLatency)
}

// Helper to Open DB with all column families
func initializeDBWithColumnFamilies(opts *grocksdb.Options, outputDBPath string, inputKVDir string, numVersions int) (*grocksdb.DB, map[string]*grocksdb.ColumnFamilyHandle, error) {
	versions := utils.GenerateVersionNames(numVersions)

	allCFs := map[string]bool{"default": true} // Default CF should always exist
	for _, version := range versions {
		allCFs[version] = true
	}

	cfNames := make([]string, 0, len(allCFs))
	for name := range allCFs {
		cfNames = append(cfNames, name)
	}

	cfOptions := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOptions[i] = NewRocksDBOpts()
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, outputDBPath, cfNames, cfOptions)
	if err != nil {
		return nil, nil, err
	}

	cfHandleMap := make(map[string]*grocksdb.ColumnFamilyHandle)
	for i, cfName := range cfNames {
		cfHandleMap[cfName] = cfHandles[i]
	}

	return db, cfHandleMap, nil
}
