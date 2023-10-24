package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func BenchmarkDBReverseIterationCmd() *cobra.Command {
	benchmarkReverseIterationCmd := &cobra.Command{
		Use:   "benchmark-reverse-iteration",
		Short: "Benchmark reverse iteration is designed to measure reverse iteration performance of different db backends",
		Run:   benchmarkReverseIteration,
	}

	benchmarkReverseIterationCmd.PersistentFlags().StringP("db-backend", "d", "", "DB Backend")
	benchmarkReverseIterationCmd.PersistentFlags().StringP("raw-kv-input-dir", "r", "", "Input Directory for benchmark which contains the raw kv data")
	benchmarkReverseIterationCmd.PersistentFlags().StringP("output-dir", "o", "", "Output Directory")
	benchmarkReverseIterationCmd.PersistentFlags().IntP("concurrency", "c", 1, "Concurrency while writing to db")
	benchmarkReverseIterationCmd.PersistentFlags().Int64P("max-operations", "p", 1000, "Max operations to run")
	benchmarkReverseIterationCmd.PersistentFlags().IntP("num-versions", "v", 1, "number of versions in db")
	benchmarkReverseIterationCmd.PersistentFlags().IntP("iteration-steps", "i", 10, "Number of steps to run per iteration")

	return benchmarkReverseIterationCmd
}

func benchmarkReverseIteration(cmd *cobra.Command, args []string) {
	dbBackend, _ := cmd.Flags().GetString("db-backend")
	rawKVInputDir, _ := cmd.Flags().GetString("raw-kv-input-dir")
	outputDir, _ := cmd.Flags().GetString("output-dir")
	numVersions, _ := cmd.Flags().GetInt("num-versions")
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	maxOps, _ := cmd.Flags().GetInt64("max-operations")
	iterationSteps, _ := cmd.Flags().GetInt("iteration-steps")

	if dbBackend == "" {
		panic("Must provide db backend when benchmarking")
	}

	if rawKVInputDir == "" {
		panic("Must provide raw kv input dir when benchmarking")
	}

	if outputDir == "" {
		panic("Must provide output dir")
	}

	_, isAcceptedBackend := ValidDBBackends[dbBackend]
	if !isAcceptedBackend {
		panic(fmt.Sprintf("Unsupported db backend: %s\n", dbBackend))
	}

	BenchmarkDBReverseIteration(rawKVInputDir, numVersions, outputDir, dbBackend, concurrency, maxOps, iterationSteps)
}

// Benchmark reverse iteration performance of db backend
func BenchmarkDBReverseIteration(inputKVDir string, numVersions int, outputDir string, dbBackend string, concurrency int, maxOps int64, iterationSteps int) {
	panic("Not Implemented")
}