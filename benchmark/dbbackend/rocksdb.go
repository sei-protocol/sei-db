package dbbackend

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/sei-protocol/sei-db/benchmark/utils"
)

func writeToRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, inputDir string, concurrency int, maxRetries int, chunkSize int) []time.Duration {
	versionDirs, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic(err)
	}

	var allLatencies []time.Duration

	for _, versionDir := range versionDirs {
		if !versionDir.IsDir() {
			continue
		}

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

				for {
					filename := utils.PickRandomItem(allFiles, processedFiles)
					if filename == "" {
						break
					}

					// Retrieve column family for the version
					version := filepath.Base(filepath.Dir(filename))
					cf, exists := cfHandles[version]
					if !exists {
						panic(fmt.Sprintf("No handle found for version: %s", version))
					}

					kvEntries, _ := utils.ReadKVEntriesFromFile(filename)
					utils.RandomShuffle(kvEntries)

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
	opts := grocksdb.NewDefaultOptions()
	// Configs taken from implementations
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	opts.SetTargetFileSizeMultiplier(2)
	opts.SetOptimizeFiltersForHits(true)
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

func readFromRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, inputDir string, concurrency int, maxRetries int, chunkSize int) []time.Duration {
	versionDirs, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic(err)
	}

	var allLatencies []time.Duration

	for _, versionDir := range versionDirs {
		if !versionDir.IsDir() {
			continue
		}

		versionPath := filepath.Join(inputDir, versionDir.Name())
		allFiles, err := utils.ListAllFiles(versionPath)
		if err != nil {
			panic(err)
		}

		latencies := make(chan time.Duration, len(allFiles)*chunkSize)
		processedFiles := &sync.Map{}
		wg := &sync.WaitGroup{}

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ro := grocksdb.NewDefaultReadOptions()
				for {
					filename := utils.PickRandomItem(allFiles, processedFiles)
					if filename == "" {
						break
					}

					// Retrieve column family for the version
					version := filepath.Base(filepath.Dir(filename))
					cfHandle, exists := cfHandles[version]
					if !exists {
						panic(fmt.Sprintf("No handle found for version: %s", version))
					}

					kvEntries, _ := utils.ReadKVEntriesFromFile(filename)
					utils.RandomShuffle(kvEntries)

					for _, kv := range kvEntries {
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

func (rocksDB RocksDBBackend) BenchmarkDBRead(inputKVDir string, outputDBPath string, concurrency int, maxRetries int, chunkSize int) {
	opts := grocksdb.NewDefaultOptions()
	// Configs taken from implementations
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	opts.SetTargetFileSizeMultiplier(2)
	opts.SetOptimizeFiltersForHits(true)

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	startTime := time.Now()
	latencies := readFromRocksDBConcurrently(db, cfHandleMap, inputKVDir, concurrency, maxRetries, chunkSize)
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

func forwardIterateRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, inputDir string, prefixes []string, concurrency int) ([]time.Duration, int) {
	// Fetch version directories
	versionDirs, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic(err)
	}

	var allLatencies []time.Duration
	totalCount := 0

	for _, versionDir := range versionDirs {
		if !versionDir.IsDir() {
			continue
		}

		// Get the cfHandle for the current version
		version := versionDir.Name()
		cfHandle, exists := cfHandles[version]
		if !exists {
			panic(fmt.Sprintf("No handle found for version: %s", version))
		}

		latencies := make(chan time.Duration, len(versionDirs)*len(prefixes))
		counts := make(chan int, len(versionDirs)*len(prefixes))

		processedPrefixes := &sync.Map{}
		wg := &sync.WaitGroup{}

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ro := grocksdb.NewDefaultReadOptions()

				for {
					selectedPrefix := utils.PickRandomItem(prefixes, processedPrefixes)
					if selectedPrefix == "" {
						break
					}

					startTime := time.Now()
					iter := db.NewIteratorCF(ro, cfHandle)
					defer iter.Close()

					count := 0
					for iter.Seek([]byte(selectedPrefix)); iter.ValidForPrefix([]byte(selectedPrefix)); iter.Next() {
						count++
					}

					latency := time.Since(startTime)
					latencies <- latency
					counts <- count
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
	}
	return allLatencies, totalCount
}

// TODO: Add Random key iteration latency
func (rocksDB RocksDBBackend) BenchmarkDBForwardIteration(inputKVDir string, prefixes []string, outputDBPath string, concurrency int) {
	// Open the DB with default options
	opts := grocksdb.NewDefaultOptions()
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	opts.SetTargetFileSizeMultiplier(2)
	opts.SetOptimizeFiltersForHits(true)

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	startTime := time.Now()
	latencies, totalCountIteration := forwardIterateRocksDBConcurrently(db, cfHandleMap, inputKVDir, prefixes, concurrency)
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

func reverseIterateRocksDBConcurrently(db *grocksdb.DB, cfHandles map[string]*grocksdb.ColumnFamilyHandle, inputDir string, prefixes []string, concurrency int) ([]time.Duration, int) {
	// Fetch version directories
	versionDirs, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic(err)
	}

	var allLatencies []time.Duration
	totalCount := 0

	for _, versionDir := range versionDirs {
		if !versionDir.IsDir() {
			continue
		}

		// Get the cfHandle for the current version
		version := versionDir.Name()
		cfHandle, exists := cfHandles[version]
		if !exists {
			panic(fmt.Sprintf("No handle found for version: %s", version))
		}

		latencies := make(chan time.Duration, len(versionDirs)*len(prefixes))
		counts := make(chan int, len(versionDirs)*len(prefixes))

		processedPrefixes := &sync.Map{}
		wg := &sync.WaitGroup{}

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ro := grocksdb.NewDefaultReadOptions()

				for {
					selectedPrefix := utils.PickRandomItem(prefixes, processedPrefixes)
					if selectedPrefix == "" {
						break
					}

					startTime := time.Now()
					iter := db.NewIteratorCF(ro, cfHandle)
					defer iter.Close()

					count := 0
					for iter.SeekForPrev([]byte(selectedPrefix)); iter.ValidForPrefix([]byte(selectedPrefix)); iter.Prev() {
						count++
					}

					latency := time.Since(startTime)
					latencies <- latency
					counts <- count
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
	}
	return allLatencies, totalCount
}

// TODO: Add Random key iteration latency
func (rocksDB RocksDBBackend) BenchmarkDBReverseIteration(inputKVDir string, prefixes []string, outputDBPath string, concurrency int) {
	// Open the DB with default options
	opts := grocksdb.NewDefaultOptions()
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	opts.SetTargetFileSizeMultiplier(2)
	opts.SetOptimizeFiltersForHits(true)

	// Initialize db
	db, cfHandleMap, err := initializeDBWithColumnFamilies(opts, outputDBPath, inputKVDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	defer db.Close()

	startTime := time.Now()
	latencies, totalCountIteration := reverseIterateRocksDBConcurrently(db, cfHandleMap, inputKVDir, prefixes, concurrency)
	endTime := time.Now()

	totalTime := endTime.Sub(startTime)

	// Log throughput
	fmt.Printf("Total Prefixes Reverse-Iterated: %d\n", totalCountIteration)
	fmt.Printf("Total Time taken: %v\n", totalTime)
	fmt.Printf("Throughput: %f reverse-iterations/sec\n", float64(totalCountIteration)/totalTime.Seconds())

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
	// List existing column families
	existingCFs, err := grocksdb.ListColumnFamilies(opts, outputDBPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}

	versionDirs, err := ioutil.ReadDir(inputKVDir)
	if err != nil {
		return nil, nil, err
	}

	allCFs := map[string]bool{"default": true} // Default CF should always exist
	for _, name := range existingCFs {
		allCFs[name] = true
	}
	for _, versionDir := range versionDirs {
		if versionDir.IsDir() {
			allCFs[versionDir.Name()] = true
		}
	}

	cfNames := make([]string, 0, len(allCFs))
	for name := range allCFs {
		cfNames = append(cfNames, name)
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, outputDBPath, cfNames, make([]*grocksdb.Options, len(cfNames)))
	if err != nil {
		return nil, nil, err
	}

	cfHandleMap := make(map[string]*grocksdb.ColumnFamilyHandle)
	for i, cfName := range cfNames {
		cfHandleMap[cfName] = cfHandles[i]
	}

	return db, cfHandleMap, nil
}
