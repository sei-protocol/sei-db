package pebbledb

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/sei-protocol/sei-db/benchmark/utils"
)

// writeToPebbleDBConcurrently generates random write load against the pebbleDB
// Given kv pairs (randomly shuffled), the version, batch size, it will spin up `concurrency` goroutines
// each of which is assigned to a portion of the kv data and writes to db in `batchSize` batches.
// It maintains a `latencies` channel which aggregates all the latencies
func writeToPebbleDBConcurrently(db *Database, allKVs []utils.KeyValuePair, concurrency int, version uint64, batchSize int) []time.Duration {
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

// BenchmarkDBWrite measures random write performance of pebbledb
// Given an input dir containing all the raw kv data, it writes to pebbledb one version after another
func (pebbleDB PebbleDBBackend) BenchmarkDBWrite(inputKVDir string, numVersions int, outputDBPath string, concurrency int, batchSize int) {
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
		// Write shuffled entries to pebbleDB concurrently
		fmt.Printf("On Version %+v\n", v)
		totalLatencies := []time.Duration{}
		startTime := time.Now()
		latencies := writeToPebbleDBConcurrently(db, kvData, concurrency, v, batchSize)
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

// readFromPebbleDBConcurrently generates random read load against the pebbleDb
// Given kv pairs (randomly shuffled), numVersions, it will spin up `concurrency` goroutines
// that randomly select a version, key and query the db.
// It only performs `maxOps“ random reads and maintains a `latencies` channel which aggregates all the latencies.
func readFromPebbleDBConcurrently(db *Database, allKVs []utils.KeyValuePair, numVersions int, concurrency int, maxOps int64) []time.Duration {
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
					panic(err)
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

// BenchmarkDBRead measures random read performance of pebbledb
// Given an input dir containing all the raw kv data, it generates random read load and measures performance.
func (pebbleDB PebbleDBBackend) BenchmarkDBRead(inputKVDir string, numVersions int, outputDBPath string, concurrency int, maxOps int64) {
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
	latencies := readFromPebbleDBConcurrently(db, kvData, numVersions, concurrency, maxOps)
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

// forwardIteratePebbleDBConcurrently generates forward iteration load against the pebbleDB
// Given kv pairs (randomly shuffled), numVersions, it will spin up `concurrency` goroutines
// that randomly select a version, key, seeks to that key and starts a forward iteration for at most `numIterationSteps` steps.
// It only performs `maxOps“ forward iterations and maintains a `latencies` channel which aggregates all the latencies.
func forwardIteratePebbleDBConcurrently(db *Database, allKVs []utils.KeyValuePair, numVersions int, concurrency int, numIterationSteps int, maxOps int64) ([]time.Duration, int) {
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

				lowerBound := kv.Key
				itr, err := db.storage.NewIter(&pebble.IterOptions{LowerBound: lowerBound})
				if err != nil {
					panic(err)
				}

				// Move start Time before instantiation where the seek to start occurs
				startTime := time.Now()
				it := NewPebbleDBIterator(itr, kv.Key, version)
				defer it.Close()

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

// BenchmarkDBForwardIteration measures forward iteration performance of pebbledb
// Given an input dir containing all the raw kv data, it selects a random key, forward iterates and measures performance.
func (pebbleDB PebbleDBBackend) BenchmarkDBForwardIteration(inputKVDir string, numVersions int, outputDBPath string, concurrency int, maxOps int64, iterationSteps int) {
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
	latencies, totalCountIteration := forwardIteratePebbleDBConcurrently(db, kvData, numVersions, concurrency, iterationSteps, maxOps)
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
