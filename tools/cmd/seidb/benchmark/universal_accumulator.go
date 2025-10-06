package benchmark

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"time"

	ua "github.com/sei-protocol/sei-db/sc/universal_accumulator"
	"github.com/sei-protocol/sei-db/tools/utils"
	"github.com/spf13/cobra"
)

func UniversalAccumulatorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "benchmark-ua",
		Short: "Benchmark universal accumulator by applying KV entries in blocks",
		Run:   executeUABenchmark,
	}

	cmd.PersistentFlags().StringP("input-dir", "i", "", "Optional: input directory containing *.kv chunks from generate command. If empty, generate data on the fly.")
	// Hardcode snapshot interval in benchmark
	cmd.PersistentFlags().IntP("entries-per-block", "b", 1000, "Number of entries per block")
	cmd.PersistentFlags().IntP("concurrency", "c", 4, "Concurrency for loading input files")
	cmd.PersistentFlags().IntP("max-entries", "m", 0, "Max entries to process (0 = all)")
	cmd.PersistentFlags().Bool("calc-root", false, "Calculate root each block (slower, for verification)")
	// Synthetic data options (default when input-dir is empty)
	cmd.PersistentFlags().Int("num-entries", 100000, "Number of synthetic entries to generate")
	cmd.PersistentFlags().Int("key-size", 32, "Key size in bytes for synthetic data")
	cmd.PersistentFlags().Int("value-size", 128, "Value size in bytes for synthetic data")
	cmd.PersistentFlags().Int64("seed", 0, "Deterministic seed for synthetic data (0 = random)")

	return cmd
}

func executeUABenchmark(cmd *cobra.Command, _ []string) {
	inputDir, _ := cmd.Flags().GetString("input-dir")

	snapshotInterval := uint64(10000)
	entriesPerBlock, _ := cmd.Flags().GetInt("entries-per-block")
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	maxEntries, _ := cmd.Flags().GetInt("max-entries")
	calcRoot, _ := cmd.Flags().GetBool("calc-root")
	numEntries, _ := cmd.Flags().GetInt("num-entries")
	keySize, _ := cmd.Flags().GetInt("key-size")
	valueSize, _ := cmd.Flags().GetInt("value-size")
	seed, _ := cmd.Flags().GetInt64("seed")

	startLoad := time.Now()
	var kvs []utils.KeyValuePair
	if inputDir == "" {
		if numEntries <= 0 {
			panic("--num-entries must be > 0")
		}
		if keySize <= 0 || valueSize < 0 {
			panic("--key-size must be > 0 and --value-size must be >= 0")
		}
		kvs = generateSyntheticKVs(numEntries, keySize, valueSize, seed)
	} else {
		var err error
		kvs, err = utils.LoadAndShuffleKV(inputDir, concurrency)
		if err != nil {
			panic(err)
		}
	}
	if maxEntries > 0 && maxEntries < len(kvs) {
		kvs = kvs[:maxEntries]
	}
	loadDur := time.Since(startLoad)

	acc, err := ua.NewUniversalAccumulator(snapshotInterval)
	if err != nil {
		panic(err)
	}
	defer acc.Close()

	if entriesPerBlock <= 0 {
		entriesPerBlock = 1000
	}

	totalEntries := len(kvs)
	blocks := (totalEntries + entriesPerBlock - 1) / entriesPerBlock

	fmt.Printf("Loaded %d entries from %s in %s; processing %d blocks (block size %d)\n", totalEntries, inputDir, loadDur, blocks, entriesPerBlock)

	startApply := time.Now()
	var rootTime time.Duration
	for b := 0; b < blocks; b++ {
		startIdx := b * entriesPerBlock
		endIdx := startIdx + entriesPerBlock
		if endIdx > totalEntries {
			endIdx = totalEntries
		}

		entries := make([]ua.AccumulatorKVPair, endIdx-startIdx)
		for i := range entries {
			entries[i] = ua.AccumulatorKVPair{Key: kvs[startIdx+i].Key, Value: kvs[startIdx+i].Value}
		}

		changeset := ua.AccumulatorChangeset{
			Version: uint64(b + 1),
			Entries: entries,
			Name:    "benchmark-block",
		}
		if err := acc.ProcessBlock(uint64(b+1), changeset); err != nil {
			panic(err)
		}

		if calcRoot {
			rootStart := time.Now()
			if _, err := acc.CalculateRoot(); err != nil {
				panic(err)
			}
			rootTime += time.Since(rootStart)
		}
	}
	applyDur := time.Since(startApply)

	fmt.Printf("Applied %d entries in %s (%.2f K entries/s) across %d blocks (%.2f blocks/s). Root time: %s\n",
		totalEntries,
		applyDur,
		float64(totalEntries)/applyDur.Seconds()/1000.0,
		blocks,
		float64(blocks)/applyDur.Seconds(),
		rootTime,
	)
}

// generateSyntheticKVs creates random KV pairs efficiently.
func generateSyntheticKVs(n int, keySize int, valueSize int, seed int64) []utils.KeyValuePair {
	out := make([]utils.KeyValuePair, n)
	// Use math/rand for speed; seed optionally deterministic
	r := mrand.New(mrand.NewSource(time.Now().UnixNano()))
	if seed != 0 {
		r = mrand.New(mrand.NewSource(seed))
	}

	// Use a crypto/rand salt to ensure uniqueness across runs when seed=0
	var salt [8]byte
	if seed == 0 {
		_, _ = rand.Read(salt[:])
	}

	for i := 0; i < n; i++ {
		k := make([]byte, keySize)
		v := make([]byte, valueSize)
		// Fill with fast PRNG
		for j := 0; j < keySize; j++ {
			k[j] = byte(r.Intn(256))
		}
		for j := 0; j < valueSize; j++ {
			v[j] = byte(r.Intn(256))
		}
		// Encode index and salt into key tail to reduce collisions
		if keySize >= 12 {
			idx := uint32(i)
			k[keySize-12] = salt[0]
			k[keySize-11] = salt[1]
			k[keySize-10] = salt[2]
			k[keySize-9] = salt[3]
			k[keySize-8] = byte(idx >> 24)
			k[keySize-7] = byte(idx >> 16)
			k[keySize-6] = byte(idx >> 8)
			k[keySize-5] = byte(idx)
		}
		out[i] = utils.KeyValuePair{Key: k, Value: v}
	}
	return out
}
