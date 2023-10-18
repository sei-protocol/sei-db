package sqlite

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sei-protocol/sei-db/benchmark/utils"
)

func writeToSqliteConcurrently(db *Database, allKVs []utils.KeyValuePair, concurrency int, version uint64, batchSize int) []time.Duration {
	var allLatencies []time.Duration
	latencies := make(chan time.Duration, len(allKVs))

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
				batch, err := NewBatch(db.storage, version)
				if err != nil {
					panic(err)
				}

				batchEnd := j + batchSize
				if batchEnd > end {
					batchEnd = end
				}

				// Add key-value pairs to the batch up to batchSize
				for k := j; k < batchEnd; k++ {
					kv := allKVs[k]
					batch.Set(kv.Key, kv.Value)
				}

				startTime := time.Now()
				err = batch.Write()
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

func (sqliteDB SqliteBackend) BenchmarkDBWrite(inputKVDir string, numVersions int, outputDBPath string, concurrency int, chunkSize int, batchSize int) {
	db, err := New(outputDBPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	startLoad := time.Now()
	kvData, err := utils.LoadAndShuffleKV(inputKVDir)
	if err != nil {
		panic(err)
	}
	endLoad := time.Now()
	fmt.Printf("Finishing loading %+v kv pairs into memory %+v\n", len(kvData), endLoad.Sub(startLoad).String())

	// Write each version sequentially
	totalTime := time.Duration(0)
	writeCount := 0
	v := uint64(0)
	for ; v < uint64(numVersions); v++ {
		// Write shuffled entries to RocksDB concurrently
		fmt.Printf("On Version %+v\n", v)
		totalLatencies := []time.Duration{}
		startTime := time.Now()
		latencies := writeToSqliteConcurrently(db, kvData, concurrency, v, batchSize)
		endTime := time.Now()
		totalTime = totalTime + endTime.Sub(startTime)
		totalLatencies = append(totalLatencies, latencies...)
		writeCount += len(latencies)

		sort.Slice(totalLatencies, func(i, j int) bool { return totalLatencies[i] < totalLatencies[j] })
		fmt.Printf("P50 Latency: %v\n", utils.CalculatePercentile(totalLatencies, 50))
		fmt.Printf("P75 Latency: %v\n", utils.CalculatePercentile(totalLatencies, 75))
		fmt.Printf("P99 Latency: %v\n", utils.CalculatePercentile(totalLatencies, 99))
		fmt.Printf("Total time: %v\n", totalTime)
		fmt.Printf("Total Successfully Written %d\n", writeCount)
		totalLatencies = nil
		runtime.GC()
	}

	// Log throughput
	fmt.Printf("Total Successfully Written %d\n", writeCount)
	fmt.Printf("Total Time taken: %v\n", totalTime)
	fmt.Printf("Throughput: %f writes/sec\n", float64(writeCount)/totalTime.Seconds())
	fmt.Printf("Total records written %d\n", writeCount)
}

func readFromSqliteDBConcurrently(db *Database, allKVs []utils.KeyValuePair, numVersions int, concurrency int, maxOps int64) []time.Duration {
	var allLatencies []time.Duration
	latencies := make(chan time.Duration, maxOps)

	var opCounter int64
	wg := &sync.WaitGroup{}

	// Each goroutine will handle reading a subset of kv pairs
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				currentOps := atomic.AddInt64(&opCounter, 1)
				if currentOps > maxOps {
					break
				}
				// Randomly pick a version and retrieve its column family handle
				version := uint64(rand.Intn(numVersions))

				// Randomly pick a key-value pair to read
				kv := allKVs[rand.Intn(len(allKVs))]

				startTime := time.Now()
				_, err := db.Get(version, kv.Key)
				latency := time.Since(startTime)
				if err == nil {
					latencies <- latency
				} else {
					fmt.Printf("Failed to read key: %v, Error: %v", kv.Key, err)
				}

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

func (sqliteDB SqliteBackend) BenchmarkDBRead(inputKVDir string, numVersions int, outputDBPath string, concurrency int, chunkSize int, maxOps int64) {
	kvData, err := utils.LoadAndShuffleKV(inputKVDir)
	if err != nil {
		panic(err)
	}

	// Initialize db
	db, err := New(outputDBPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	startTime := time.Now()
	latencies := readFromSqliteDBConcurrently(db, kvData, numVersions, concurrency, maxOps)
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

func forwardIterateSqliteDBConcurrently(db *Database, allKVs []utils.KeyValuePair, numVersions int, concurrency int, numIterationSteps int, maxOps int64) ([]time.Duration, int) {
	var allLatencies []time.Duration
	var totalSteps int
	latencies := make(chan time.Duration, maxOps)
	steps := make(chan int, maxOps)

	var opCounter int64
	wg := &sync.WaitGroup{}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				currentOps := atomic.AddInt64(&opCounter, 1)
				if currentOps > maxOps {
					break
				}
				// Randomly pick a version and retrieve its column family handle
				version := uint64(rand.Intn(numVersions))

				// Randomly pick a key-value pair to seek to
				kv := allKVs[rand.Intn(len(allKVs))]

				it, err := db.newIterator(version, kv.Key, false)
				if err != nil {
					panic(err)
				}
				defer it.Close()

				startTime := time.Now()
				step := 0
				for j := 0; j < numIterationSteps && it.Valid(); it.Next() {
					step++
				}
				latency := time.Since(startTime)

				latencies <- latency
				steps <- step
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

func (sqliteDB SqliteBackend) BenchmarkDBForwardIteration(inputKVDir string, numVersions int, outputDBPath string, concurrency int, maxOps int64, iterationSteps int) {
	kvData, err := utils.LoadAndShuffleKV(inputKVDir)
	if err != nil {
		panic(err)
	}

	// Initialize db
	db, err := New(outputDBPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	startTime := time.Now()
	latencies, totalCountIteration := forwardIterateSqliteDBConcurrently(db, kvData, numVersions, concurrency, iterationSteps, maxOps)
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

func reverseIterateRocksDBConcurrently(db *Database, allKVs []utils.KeyValuePair, numVersions int, concurrency int, numIterationSteps int, maxOps int64) ([]time.Duration, int) {
	var allLatencies []time.Duration
	var totalSteps int
	latencies := make(chan time.Duration, maxOps)
	steps := make(chan int, maxOps)

	var opCounter int64
	wg := &sync.WaitGroup{}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				currentOps := atomic.AddInt64(&opCounter, 1)
				if currentOps > maxOps {
					break
				}
				// Randomly pick a version and retrieve its column family handle
				version := uint64(rand.Intn(numVersions))

				// Randomly pick a key-value pair to seek to
				kv := allKVs[rand.Intn(len(allKVs))]

				it, err := db.newIterator(version, kv.Key, true)
				if err != nil {
					panic(err)
				}
				defer it.Close()

				startTime := time.Now()
				step := 0
				for j := 0; j < numIterationSteps && it.Valid(); it.Next() {
					step++
				}
				latency := time.Since(startTime)

				latencies <- latency
				steps <- step

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

func (sqliteDB SqliteBackend) BenchmarkDBReverseIteration(inputKVDir string, numVersions int, outputDBPath string, concurrency int, maxOps int64, iterationSteps int) {
	kvData, err := utils.LoadAndShuffleKV(inputKVDir)
	if err != nil {
		panic(err)
	}

	// Initialize db
	db, err := New(outputDBPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	startTime := time.Now()
	latencies, totalCountIteration := reverseIterateRocksDBConcurrently(db, kvData, numVersions, concurrency, iterationSteps, maxOps)
	endTime := time.Now()

	totalTime := endTime.Sub(startTime)

	// Log throughput
	fmt.Printf("Total Prefixes Reverse-Iterated: %d\n", totalCountIteration)
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
