package operations

import (
	"fmt"
	"time"

	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/sc/memiavl"
	"github.com/spf13/cobra"
)

const defaultProgressInterval uint64 = 10000

func StateSizeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "state-size",
		Short: "Delete zero-value state entries",
		Run:   executeStateSize,
	}

	cmd.PersistentFlags().StringP("db-dir", "d", "", "Database Directory")
	cmd.PersistentFlags().Int64("height", 0, "Block Height")
	cmd.PersistentFlags().StringP("module", "m", "", "Module to process. Defaults to all modules")
	cmd.PersistentFlags().Uint64("progress-interval", defaultProgressInterval, "Deletions between progress logs")

	return cmd
}

func executeStateSize(cmd *cobra.Command, _ []string) {
	module, _ := cmd.Flags().GetString("module")
	dbDir, _ := cmd.Flags().GetString("db-dir")
	height, _ := cmd.Flags().GetInt64("height")
	progressInterval, _ := cmd.Flags().GetUint64("progress-interval")

	if dbDir == "" {
		panic("Must provide database dir")
	}

	if progressInterval == 0 {
		progressInterval = defaultProgressInterval
	}

	opts := memiavl.Options{
		Dir:             dbDir,
		ZeroCopy:        true,
		CreateIfMissing: false,
	}
	db, err := memiavl.OpenDB(logger.NewNopLogger(), height, opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	actualHeight := db.Version()
	fmt.Printf("%s Opened db at height %d (requested: %d). Target module: %s\n", timestamp(), actualHeight, height, targetModuleLabel(module))

	modules := modulesToProcess(module)
	overallStart := time.Now()
	var totalDeleted uint64

	for idx, moduleName := range modules {
		tree := db.TreeByName(moduleName)
		position := fmt.Sprintf("%d/%d", idx+1, len(modules))

		if tree == nil {
			fmt.Printf("%s [%s] Tree does not exist for module %s. Skipping.\n", timestamp(), position, moduleName)
			continue
		}

		fmt.Printf("%s [%s] Starting zero-value deletion for module %s\n", timestamp(), position, moduleName)

		deleted, duration := deleteZeroValueEntries(tree, moduleName, progressInterval)
		totalDeleted += deleted

		if deleted == 0 {
			fmt.Printf("%s [%s] No zero-value entries deleted for module %s (elapsed %s)\n", timestamp(), position, moduleName, duration)
		} else {
			fmt.Printf("%s [%s] Finished module %s: deleted %d zero-value entries in %s\n", timestamp(), position, moduleName, deleted, duration)
		}
	}

	fmt.Printf("%s Completed zero-value deletion across %d modules. Total deletions: %d. Total time: %s\n", timestamp(), len(modules), totalDeleted, time.Since(overallStart))
}

func deleteZeroValueEntries(tree *memiavl.Tree, moduleName string, progressInterval uint64) (uint64, time.Duration) {
	if progressInterval == 0 {
		progressInterval = defaultProgressInterval
	}

	var (
		deleted   uint64
		scanStart time.Time
		scanEnd   time.Time
	)

	tree.ScanPostOrder(func(node memiavl.Node) bool {
		fmt.Println("Scanning node %X", node.Hash())
		if !node.IsLeaf() {
			return true
		}
		fmt.Printf("Node is a leaf\n")
		fmt.Printf("moduleName: %s\n", moduleName)

		if moduleName != "evm" {
			return true
		}

		now := time.Now()
		if scanStart.IsZero() {
			scanStart = now
		}
		scanEnd = now

		key := node.Key()

		value := node.Value()
		if len(key) == 0 || key[0] != 0x03 {
			fmt.Printf("Key is not a zero-value\n")
			return true
		}
		fmt.Printf("0x3 Key: %X\n", key)
		fmt.Printf("0x3 Value: %X\n", value)
		if !isZeroValue(value) {
			scanEnd = time.Now()
			return true
		}
		fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		fmt.Printf("Value is a zero-value\n")
		fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

		deleted++
		tree.Remove(key)
		scanEnd = time.Now()
		fmt.Printf("Deleted %d zero-value entries in module %s (elapsed %s)\n", deleted, moduleName, time.Since(scanStart))

		if deleted%progressInterval == 0 {
			elapsed := time.Duration(0)
			if !scanStart.IsZero() {
				elapsed = time.Since(scanStart)
			}
			fmt.Printf("%s Deleted %d zero-value entries in module %s (elapsed %s)\n", timestamp(), deleted, moduleName, elapsed)
		}

		return true
	})

	if scanStart.IsZero() {
		return deleted, 0
	}

	if scanEnd.IsZero() {
		scanEnd = time.Now()
	}

	return deleted, scanEnd.Sub(scanStart)
}

func modulesToProcess(module string) []string {
	if module == "" {
		modules := make([]string, 0, len(AllModules))
		modules = append(modules, AllModules...)
		return modules
	}
	return []string{module}
}

func targetModuleLabel(module string) string {
	if module == "" {
		return "all modules"
	}
	return module
}

func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func isZeroValue(value []byte) bool {
	if len(value) == 0 {
		return true
	}
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}
