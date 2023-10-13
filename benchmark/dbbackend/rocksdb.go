package dbbackend

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
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

func writeToRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, inputDir string, concurrency int, maxRetries int, chunkSize int) []time.Duration {
	versionDirs, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic(err)
	}
	if len(versionDirs) == 0 {
		return []time.Duration{}
	}

	var allLatencies []time.Duration

	for _, versionDir := range versionDirs {
		fmt.Printf("Current Version: %+v\n", versionDir.Name())
		versionPath := filepath.Join(inputDir, versionDir.Name())
		allFiles, err := utils.ListAllFiles(versionPath)
		if err != nil {
			panic(err)
		}

		latencies := make(chan time.Duration, len(allFiles)*chunkSize)

		wg := &sync.WaitGroup{}
		processedFiles := &sync.Map{}

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				wo := grocksdb.NewDefaultWriteOptions()
				defer wo.Destroy()

				for {
					filename := utils.PickRandomItem(allFiles, processedFiles)
					if filename == "" {
						break
					}

					// Retrieve column family for the version
					// TODO: Refactor to helper
					version := versionDir.Name()
					cf, exists := cfHandles[version]
					if !exists {
						panic(fmt.Sprintf("No handle found for version: %s\n", version))
					}

					kvEntries, err := utils.ReadKVEntriesFromFile(filepath.Join(versionPath, filename))
					if err != nil {
						panic(err)
					}
					utils.RandomShuffle(kvEntries)

					// TODO: Randomly select any key and write vs iterating through single file each time
					for _, kv := range kvEntries {
						retries := 0
						for {
							startTime := time.Now()
							err := db.PutCF(wo, cf, kv.Key, kv.Value)
							latency := time.Since(startTime)
							if err == nil {
								latencies <- latency
								break
							}
							retries++
							if retries > maxRetries {
								break
							}
						}
					}
				}
			}()
		}
		wg.Wait()
		close(latencies)

		for l := range latencies {
			allLatencies = append(allLatencies, l)
		}
	}
	return allLatencies
}

func (rocksDB RocksDBBackend) BenchmarkDBWrite(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int) {
	opts := NewRocksDBOpts()
	// Configs taken from implementations
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	// Write shuffled entries to RocksDB concurrently
	startTime := time.Now()
	latencies := writeToRocksDBConcurrently(db, cfHandleMap, inputKVDir, concurrency, maxRetries, chunkSize)
	endTime := time.Now()

	totalTime := endTime.Sub(startTime)

	// Log throughput
	fmt.Printf("Total Successfully Written %d\n", len(latencies))
	fmt.Printf("Total Time taken: %v\n", totalTime)
	fmt.Printf("Throughput: %f writes/sec\n", float64(len(latencies))/totalTime.Seconds())
	fmt.Printf("Total records written %d\n", len(latencies))

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

func readFromRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, inputDir string, concurrency int, maxRetries int, maxOps int64) []time.Duration {
	versionDirs, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic(err)
	}
	if len(versionDirs) == 0 {
		return []time.Duration{}
	}

	// All files within each version are the same
	allFiles, err := utils.ListAllFiles(filepath.Join(inputDir, versionDirs[0].Name()))
	if err != nil {
		panic(err)
	}

	var allLatencies []time.Duration
	latencies := make(chan time.Duration, maxOps)

	var opCounter int64
	processedFiles := &sync.Map{}
	wg := &sync.WaitGroup{}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ro := grocksdb.NewDefaultReadOptions()
			defer ro.Destroy()

			for atomic.LoadInt64(&opCounter) < maxOps {
				// TODO: Refactor below into helper
				// Randomly select version
				versionDir := versionDirs[rand.Intn(len(versionDirs))]
				versionPath := filepath.Join(inputDir, versionDir.Name())

				// Randomly select file ensuring it's not being processed by another goroutine
				selectedFile := allFiles[rand.Intn(len(allFiles))]

				// Retrieve column family for the version
				cfHandle, exists := cfHandles[versionDir.Name()]
				if !exists {
					panic(fmt.Sprintf("No handle found for version: %s", versionDir.Name()))
				}

				kvEntries, err := utils.ReadKVEntriesFromFile(filepath.Join(versionPath, selectedFile))
				if err != nil {
					panic(err)
				}

				// Choose a random key to read from DB
				kv := kvEntries[rand.Intn(len(kvEntries))]

				_, inProgress := processedFiles.LoadOrStore(versionDir.Name()+"_"+selectedFile+"_"+string(kv.Key), struct{}{})

				if inProgress {
					continue
				}

				retries := 0
				for {
					startTime := time.Now()
					_, err := db.GetCF(ro, cfHandle, kv.Key)
					latency := time.Since(startTime)
					if err == nil {
						latencies <- latency
						break
					}
					retries++
					if retries > maxRetries {
						break
					}
				}
				// Increment the global iteration counter
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

func (rocksDB RocksDBBackend) BenchmarkDBRead(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int, maxOps int64) {
	opts := NewRocksDBOpts()

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	startTime := time.Now()
	latencies := readFromRocksDBConcurrently(db, cfHandleMap, inputKVDir, concurrency, maxRetries, maxOps)
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

func forwardIterateRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, inputDir string, concurrency int, maxOps int64, iterationSteps int) ([]time.Duration, int) {
	versionDirs, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic(err)
	}
	if len(versionDirs) == 0 {
		return []time.Duration{}, 0
	}

	// All files within each version are the same
	allFiles, err := utils.ListAllFiles(filepath.Join(inputDir, versionDirs[0].Name()))
	if err != nil {
		panic(err)
	}

	var allLatencies []time.Duration
	totalCount := 0
	latencies := make(chan time.Duration, maxOps)
	counts := make(chan int, maxOps*int64(iterationSteps))

	var opCounter int64
	processedFiles := &sync.Map{}
	wg := &sync.WaitGroup{}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ro := grocksdb.NewDefaultReadOptions()
			defer ro.Destroy()

			for atomic.LoadInt64(&opCounter) < maxOps {
				// TODO: Refactor below into helper
				// Randomly select version
				versionDir := versionDirs[rand.Intn(len(versionDirs))]
				versionPath := filepath.Join(inputDir, versionDir.Name())

				// Randomly select file ensuring it's not being processed by another goroutine
				selectedFile := allFiles[rand.Intn(len(allFiles))]

				// Retrieve column family for the version
				cfHandle, exists := cfHandles[versionDir.Name()]
				if !exists {
					panic(fmt.Sprintf("No handle found for version: %s", versionDir.Name()))
				}

				kvEntries, err := utils.ReadKVEntriesFromFile(filepath.Join(versionPath, selectedFile))
				if err != nil {
					panic(err)
				}

				// Choose a random key to start iteration from
				kv := kvEntries[rand.Intn(len(kvEntries))]

				_, inProgress := processedFiles.LoadOrStore(versionDir.Name()+"_"+selectedFile+"_"+string(kv.Key), struct{}{})

				if inProgress {
					continue
				}

				// Perform iteration operation and measure latency
				startTime := time.Now()
				iter := db.NewIteratorCF(ro, cfHandle)

				iter.Seek(kv.Key)
				steps := 0
				for ; iter.Valid() && steps < iterationSteps; iter.Next() {
					steps++
				}
				iter.Close()

				latency := time.Since(startTime)
				// TODO: Only add latency if iteration took at least iterationSteps
				latencies <- latency
				counts <- steps

				// Increment the global operation counter
				atomic.AddInt64(&opCounter, 1)
			}
		}()
	}
	wg.Wait()
	close(latencies)
	close(counts)

	for l := range latencies {
		allLatencies = append(allLatencies, l)
	}

	for innerCount := range counts {
		totalCount += innerCount
	}

	return allLatencies, totalCount
}

// TODO: Add Random key iteration latency
func (rocksDB RocksDBBackend) BenchmarkDBForwardIteration(inputKVDir string, outputDBPath string, concurrency int, maxOps int64, iterationSteps int) {
	// Open the DB with default options
	opts := NewRocksDBOpts()

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	startTime := time.Now()
	latencies, totalCountIteration := forwardIterateRocksDBConcurrently(db, cfHandleMap, inputKVDir, concurrency, maxOps, iterationSteps)
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

func reverseIterateRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, inputDir string, concurrency int, maxOps int64, iterationSteps int) ([]time.Duration, int) {
	versionDirs, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic(err)
	}
	if len(versionDirs) == 0 {
		return []time.Duration{}, 0
	}

	// All files within each version are the same
	allFiles, err := utils.ListAllFiles(filepath.Join(inputDir, versionDirs[0].Name()))
	if err != nil {
		panic(err)
	}

	var allLatencies []time.Duration
	totalCount := 0
	latencies := make(chan time.Duration, maxOps)
	counts := make(chan int, maxOps*int64(iterationSteps))

	var opCounter int64
	processedFiles := &sync.Map{}
	wg := &sync.WaitGroup{}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ro := grocksdb.NewDefaultReadOptions()
			defer ro.Destroy()

			for atomic.LoadInt64(&opCounter) < maxOps {
				// TODO: Refactor below into helper
				// Randomly select version
				versionDir := versionDirs[rand.Intn(len(versionDirs))]
				versionPath := filepath.Join(inputDir, versionDir.Name())

				// Randomly select file ensuring it's not being processed by another goroutine
				selectedFile := allFiles[rand.Intn(len(allFiles))]

				// Retrieve column family for the version
				cfHandle, exists := cfHandles[versionDir.Name()]
				if !exists {
					panic(fmt.Sprintf("No handle found for version: %s", versionDir.Name()))
				}

				kvEntries, err := utils.ReadKVEntriesFromFile(filepath.Join(versionPath, selectedFile))
				if err != nil {
					panic(err)
				}

				// Choose a random key to start iteration from
				kv := kvEntries[rand.Intn(len(kvEntries))]

				_, inProgress := processedFiles.LoadOrStore(versionDir.Name()+"_"+selectedFile+"_"+string(kv.Key), struct{}{})

				if inProgress {
					continue
				}

				// Perform iteration operation and measure latency
				startTime := time.Now()
				iter := db.NewIteratorCF(ro, cfHandle)

				iter.Seek(kv.Key)
				steps := 0
				for ; iter.Valid() && steps < iterationSteps; iter.Prev() {
					steps++
				}
				iter.Close()

				latency := time.Since(startTime)
				// TODO: Only add latency if iteration took at least iterationSteps
				latencies <- latency
				counts <- steps

				// Increment the global operation counter
				atomic.AddInt64(&opCounter, 1)
			}
		}()
	}
	wg.Wait()
	close(latencies)
	close(counts)

	for l := range latencies {
		allLatencies = append(allLatencies, l)
	}

	for innerCount := range counts {
		totalCount += innerCount
	}

	return allLatencies, totalCount
}

// TODO: Add Random key iteration latency
func (rocksDB RocksDBBackend) BenchmarkDBReverseIteration(inputKVDir string, outputDBPath string, concurrency int, maxOps int64, iterationSteps int) {
	// Open the DB with default options
	opts := NewRocksDBOpts()

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	startTime := time.Now()
	latencies, totalCountIteration := reverseIterateRocksDBConcurrently(db, cfHandleMap, inputKVDir, concurrency, maxOps, iterationSteps)
	endTime := time.Now()

	totalTime := endTime.Sub(startTime)

	// Log throughput
	fmt.Printf("Total Prefixes Reverse Iterated: %d\n", totalCountIteration)
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
func initializeDBWithColumnFamilies(opts *grocksdb.Options, outputDBPath string, inputKVDir string) (*grocksdb.DB, map[string]*grocksdb.ColumnFamilyHandle, error) {
	versionDirs, err := ioutil.ReadDir(inputKVDir)
	if err != nil {
		return nil, nil, err
	}

	allCFs := map[string]bool{"default": true} // Default CF should always exist
	for _, versionDir := range versionDirs {
		allCFs[versionDir.Name()] = true
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
