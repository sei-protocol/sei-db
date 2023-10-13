package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/sei-protocol/sei-db/benchmark/dbbackend"
	"github.com/sei-protocol/sei-db/benchmark/utils"
)

const rocksDBBackend = "rocksDB"

var (
	levelDBDir           string
	modules              string
	outputDir            string
	dbBackend            string
	rawKVInputDir        string
	version              int
	concurrency          int
	maxRetries           int
	chunkSize            int
	numOutputVersions    int
	maxIterationsPerFile int
	iterationSteps       int
	exportModules        = []string{
		"dex", "wasm", "accesscontrol", "oracle", "epoch", "mint", "acc", "bank", "crisis", "feegrant", "staking", "distribution", "slashing", "gov", "params", "ibc", "upgrade", "evidence", "transfer", "tokenfactory",
	}
	validDBBackends = map[string]bool{
		rocksDBBackend: true,
	}

	rootCmd = &cobra.Command{
		Use:   "dumpkv",
		Short: "A tool to generate raw key value data from a node as well as benchmark different backends",
	}

	generateCmd = &cobra.Command{
		Use:   "generate",
		Short: "Generate uses the iavl viewer logic to write out the raw keys and values from the kb for each module",
		Run:   generate,
	}

	benchmarkWriteCmd = &cobra.Command{
		Use:   "benchmark-write",
		Short: "Benchmark write is designed to measure write performance of different db backends",
		Run:   benchmarkWrite,
	}

	benchmarkReadCmd = &cobra.Command{
		Use:   "benchmark-read",
		Short: "Benchmark read is designed to measure read performance of different db backends",
		Run:   benchmarkRead,
	}

	benchmarkForwardIterationCmd = &cobra.Command{
		Use:   "benchmark-iteration",
		Short: "Benchmark iteration is designed to measure forward iteration performance of different db backends",
		Run:   benchmarkForwardIteration,
	}

	benchmarkReverseIterationCmd = &cobra.Command{
		Use:   "benchmark-reverse-iteration",
		Short: "Benchmark reverse iteration is designed to measure reverse iteration performance of different db backends",
		Run:   benchmarkReverseIteration,
	}
)

func init() {
	rootCmd.AddCommand(generateCmd, benchmarkWriteCmd, benchmarkReadCmd, benchmarkForwardIterationCmd, benchmarkReverseIterationCmd)

	generateCmd.Flags().StringVar(&levelDBDir, "leveldb-dir", "/root/.sei/data/application.db", "level db dir")
	generateCmd.Flags().StringVar(&outputDir, "output-dir", "", "Output Directory")
	generateCmd.Flags().StringVar(&modules, "modules", "", "Comma separated modules to export")
	generateCmd.Flags().IntVar(&version, "version", 0, "db version")
	generateCmd.Flags().IntVar(&chunkSize, "chunk-size", 100, "chunk size for each kv file")
	generateCmd.Flags().IntVar(&numOutputVersions, "num-output-versions", 1, "Number of output versions generated")

	benchmarkWriteCmd.Flags().StringVar(&dbBackend, "db-backend", "", "DB Backend")
	benchmarkWriteCmd.Flags().StringVar(&rawKVInputDir, "raw-kv-input-dir", "", "Input Directory for benchmark which contains the raw kv data")
	benchmarkWriteCmd.Flags().StringVar(&outputDir, "output-dir", "", "Output Directory")
	benchmarkWriteCmd.Flags().IntVar(&concurrency, "concurrency", 1, "Concurrency while writing to db")
	benchmarkWriteCmd.Flags().IntVar(&maxRetries, "max-retries", 0, "Max Retries while writing to db")
	benchmarkWriteCmd.Flags().IntVar(&chunkSize, "chunk-size", 100, "chunk size for each kv file")

	benchmarkReadCmd.Flags().StringVar(&dbBackend, "db-backend", "", "DB Backend")
	benchmarkReadCmd.Flags().StringVar(&rawKVInputDir, "raw-kv-input-dir", "", "Input Directory for benchmark which contains the raw kv data")
	benchmarkReadCmd.Flags().StringVar(&outputDir, "output-dir", "", "Output Directory which contains db")
	benchmarkReadCmd.Flags().IntVar(&concurrency, "concurrency", 1, "Concurrency while reading from db")
	benchmarkReadCmd.Flags().IntVar(&maxRetries, "max-retries", 0, "Max Retries while reading from db")
	benchmarkReadCmd.Flags().IntVar(&chunkSize, "chunk-size", 100, "chunk size for each kv file")

	benchmarkForwardIterationCmd.Flags().StringVar(&dbBackend, "db-backend", "", "DB Backend")
	benchmarkForwardIterationCmd.Flags().StringVar(&rawKVInputDir, "raw-kv-input-dir", "", "Input Directory for benchmark which contains the raw kv data")
	benchmarkForwardIterationCmd.Flags().StringVar(&outputDir, "output-dir", "", "Output Directory which contains db")
	benchmarkForwardIterationCmd.Flags().IntVar(&concurrency, "concurrency", 1, "Concurrency while reading from db")
	benchmarkForwardIterationCmd.Flags().IntVar(&maxIterationsPerFile, "max-iterations-per-file", 1, "Max iterations to run")
	benchmarkForwardIterationCmd.Flags().IntVar(&iterationSteps, "iteration-steps", 10, "Number of steps to run per iteration")

	benchmarkReverseIterationCmd.Flags().StringVar(&dbBackend, "db-backend", "", "DB Backend")
	benchmarkReverseIterationCmd.Flags().StringVar(&rawKVInputDir, "raw-kv-input-dir", "", "Input Directory for benchmark which contains the raw kv data")
	benchmarkReverseIterationCmd.Flags().StringVar(&outputDir, "output-dir", "", "Output Directory which contains db")
	benchmarkReverseIterationCmd.Flags().IntVar(&concurrency, "concurrency", 1, "Concurrency while reading from db")
	benchmarkReverseIterationCmd.Flags().IntVar(&maxIterationsPerFile, "max-iterations-per-file", 1, "Max iterations to run")
	benchmarkReverseIterationCmd.Flags().IntVar(&iterationSteps, "iteration-steps", 10, "Number of steps to run per iteration")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}

func generate(cmd *cobra.Command, args []string) {
	if outputDir == "" {
		panic("Must provide output dir when generating raw kv data")
	}

	if modules != "" {
		exportModules = strings.Split(modules, ",")
	}
	GenerateData(levelDBDir, exportModules, outputDir, version, chunkSize, numOutputVersions)
}

func benchmarkWrite(cmd *cobra.Command, args []string) {
	if dbBackend == "" {
		panic("Must provide db backend when benchmarking")
	}

	if rawKVInputDir == "" {
		panic("Must provide raw kv input dir when benchmarking")
	}

	if outputDir == "" {
		panic("Must provide output dir")
	}

	_, isAcceptedBackend := validDBBackends[dbBackend]
	if !isAcceptedBackend {
		panic(fmt.Sprintf("Unsupported db backend: %s\n", dbBackend))
	}

	BenchmarkWrite(rawKVInputDir, outputDir, dbBackend, concurrency, maxRetries, chunkSize)
}

func benchmarkRead(cmd *cobra.Command, args []string) {
	if dbBackend == "" {
		panic("Must provide db backend when benchmarking")
	}

	if rawKVInputDir == "" {
		panic("Must provide raw kv input dir when benchmarking")
	}

	if outputDir == "" {
		panic("Must provide output dir")
	}

	_, isAcceptedBackend := validDBBackends[dbBackend]
	if !isAcceptedBackend {
		panic(fmt.Sprintf("Unsupported db backend: %s\n", dbBackend))
	}

	BenchmarkRead(rawKVInputDir, outputDir, dbBackend, concurrency, maxRetries, chunkSize)
}

func benchmarkForwardIteration(cmd *cobra.Command, args []string) {
	if dbBackend == "" {
		panic("Must provide db backend when benchmarking")
	}

	if rawKVInputDir == "" {
		panic("Must provide raw kv input dir when benchmarking")
	}

	if outputDir == "" {
		panic("Must provide output dir")
	}

	_, isAcceptedBackend := validDBBackends[dbBackend]
	if !isAcceptedBackend {
		panic(fmt.Sprintf("Unsupported db backend: %s\n", dbBackend))
	}

	BenchmarkDBIteration(rawKVInputDir, outputDir, dbBackend, concurrency, maxIterationsPerFile, iterationSteps)
}

func benchmarkReverseIteration(cmd *cobra.Command, args []string) {
	if dbBackend == "" {
		panic("Must provide db backend when benchmarking")
	}

	if rawKVInputDir == "" {
		panic("Must provide raw kv input dir when benchmarking")
	}

	if outputDir == "" {
		panic("Must provide output dir")
	}

	_, isAcceptedBackend := validDBBackends[dbBackend]
	if !isAcceptedBackend {
		panic(fmt.Sprintf("Unsupported db backend: %s\n", dbBackend))
	}

	BenchmarkDBReverseIteration(rawKVInputDir, outputDir, dbBackend, concurrency, maxIterationsPerFile, iterationSteps)
}

// Outputs the raw keys and values for all modules at a height to a file
func GenerateData(dbDir string, modules []string, outputDir string, version int, chunkSize int, numOutputVersions int) {
	// Create output directory
	baseOutputDir := filepath.Join(outputDir, "version_0")
	err := os.MkdirAll(baseOutputDir, fs.ModePerm)
	if err != nil {
		panic(err)
	}

	// Generate raw kv data for each module
	db, err := utils.OpenDB(dbDir)
	if err != nil {
		panic(err)
	}
	for _, module := range modules {
		fmt.Printf("Generating Raw Keys and Values for %s module at version %d\n", module, version)

		modulePrefix := fmt.Sprintf("s/k:%s/", module)
		tree, err := utils.ReadTree(db, version, []byte(modulePrefix))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading data: %s\n", err)
			return
		}
		treeHash, err := tree.Hash()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error hashing tree: %s\n", err)
			return
		}

		fmt.Printf("Tree hash is %X, tree size is %d\n", treeHash, tree.ImmutableTree().Size())

		outputFileNamePattern := filepath.Join(baseOutputDir, module)
		utils.WriteTreeDataToFile(tree, outputFileNamePattern, chunkSize)
	}

	// Symlink versions of the base dir
	err = utils.CreateVersions(baseOutputDir, outputDir, numOutputVersions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating versions: %s\n", err)
		return
	}
}

// Benchmark write latencies and throughput of db backend
func BenchmarkWrite(inputKVDir string, outputDir string, dbBackend string, concurrency int, maxRetries int, chunkSize int) {
	// Create output directory
	err := os.MkdirAll(outputDir, fs.ModePerm)
	if err != nil {
		panic(err)
	}
	// Iterate over files in directory
	fmt.Printf("Reading Raw Keys and Values from %s\n", inputKVDir)

	if dbBackend == rocksDBBackend {
		backend := dbbackend.RocksDBBackend{}
		backend.BenchmarkDBWrite(inputKVDir, outputDir, concurrency, maxRetries, chunkSize)
	}

	return
}

// Benchmark read latencies and throughput of db backend
func BenchmarkRead(inputKVDir string, outputDir string, dbBackend string, concurrency int, maxRetries int, chunkSize int) {
	// Create output directory
	err := os.MkdirAll(outputDir, fs.ModePerm)
	if err != nil {
		panic(err)
	}
	// Iterate over files in directory
	fmt.Printf("Reading Raw Keys and Values from %s\n", inputKVDir)

	if dbBackend == rocksDBBackend {
		backend := dbbackend.RocksDBBackend{}
		backend.BenchmarkDBRead(inputKVDir, outputDir, concurrency, maxRetries, chunkSize)
	}

	return
}

// Benchmark forward iteration performance of db backend
func BenchmarkDBIteration(inputKVDir string, outputDir string, dbBackend string, concurrency int, maxIterations int, iterationSteps int) {
	// Iterate over db at directory
	fmt.Printf("Iterating Over DB at  %s\n", outputDir)

	if dbBackend == rocksDBBackend {
		backend := dbbackend.RocksDBBackend{}
		backend.BenchmarkDBForwardIteration(inputKVDir, outputDir, concurrency, maxIterations, iterationSteps)
	}

	return
}

// Benchmark reverse iteration performance of db backend
func BenchmarkDBReverseIteration(inputKVDir string, outputDir string, dbBackend string, concurrency int, maxIterations int, iterationSteps int) {
	// Reverse Iterate over db at directory
	fmt.Printf("Iterating Over DB at  %s\n", outputDir)

	if dbBackend == rocksDBBackend {
		backend := dbbackend.RocksDBBackend{}
		backend.BenchmarkDBReverseIteration(inputKVDir, outputDir, concurrency, maxIterations, iterationSteps)
	}

	return
}
