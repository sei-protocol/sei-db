package operations

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/sc/memiavl"
	"github.com/spf13/cobra"
)

func StateSizeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "state-size",
		Short: "Print analytical results for state size",
		Run:   executeStateSize,
	}

	cmd.PersistentFlags().StringP("db-dir", "d", "", "Database Directory")
	cmd.PersistentFlags().Int64("height", 0, "Block Height")
	cmd.PersistentFlags().StringP("module", "m", "", "Module to export. Default to export all")
	return cmd
}

func executeStateSize(cmd *cobra.Command, _ []string) {
	module, _ := cmd.Flags().GetString("module")
	dbDir, _ := cmd.Flags().GetString("db-dir")
	height, _ := cmd.Flags().GetInt64("height")
	if dbDir == "" {
		panic("Must provide database dir")
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
	err = PrintStateSize(module, db)
	if err != nil {
		panic(err)
	}
}

// ContractSizeInfo stores size information for a contract
type ContractSizeInfo struct {
	TotalSize int
	Count     int
}

func PrintStateSize(module string, db *memiavl.DB) error {
	modules := []string{}
	if module == "" {
		modules = AllModules
	} else {
		modules = append(modules, module)
	}

	for _, moduleName := range modules {
		tree := db.TreeByName(moduleName)
		totalNumKeys := 0
		totalKeySize := 0
		totalValueSize := 0
		totalSize := 0

		if tree == nil {
			fmt.Printf("Tree does not exist for module %s \n", moduleName)
		} else {
			fmt.Printf("Calculating for module: %s \n", moduleName)
			sizeByPrefix := map[string]int{}
			// Track contract sizes for 0x03 prefix
			contractSizes := make(map[string]*ContractSizeInfo)

			tree.ScanPostOrder(func(node memiavl.Node) bool {
				if node.IsLeaf() {
					key := node.Key()
					value := node.Value()
					totalNumKeys++
					totalKeySize += len(key)
					totalValueSize += len(value)
					totalSize += len(key) + len(value)

					prefix := fmt.Sprintf("%X", key[:1])
					sizeByPrefix[prefix] += len(value)

					// Special handling for 0x03 prefix
					if prefix == "03" && len(key) >= 21 {
						// Extract contract address (next 20 bytes after prefix)
						contractAddr := fmt.Sprintf("%X", key[1:21])
						if _, exists := contractSizes[contractAddr]; !exists {
							contractSizes[contractAddr] = &ContractSizeInfo{}
						}
						contractSizes[contractAddr].TotalSize += len(value)
						contractSizes[contractAddr].Count++
					}
				}
				return true
			})

			// Print overall stats
			fmt.Printf("Module %s total numKeys:%d, total keySize:%d, total valueSize:%d, totalSize: %d \n",
				moduleName, totalNumKeys, totalKeySize, totalValueSize, totalSize)

			// Print general prefix breakdown
			result, _ := json.MarshalIndent(sizeByPrefix, "", "  ")
			fmt.Printf("Module %s prefix breakdown: %s \n", moduleName, result)

			// Sort and print contract sizes for 0x03 prefix
			if sizeByPrefix["03"] > 0 {
				type contractSizeEntry struct {
					Address  string
					Size     int
					KeyCount int
				}

				var sortedContracts []contractSizeEntry
				for addr, info := range contractSizes {
					sortedContracts = append(sortedContracts, contractSizeEntry{
						Address:  addr,
						Size:     info.TotalSize,
						KeyCount: info.Count,
					})
				}

				sort.Slice(sortedContracts, func(i, j int) bool {
					return sortedContracts[i].Size > sortedContracts[j].Size
				})

				fmt.Printf("\nDetailed breakdown for 0x03 prefix (top 20 contracts by size):\n")
				fmt.Printf("%-42s %15s %10s\n", "Contract Address", "Total Size", "Key Count")
				fmt.Printf("%s\n", strings.Repeat("-", 70))

				for i := 0; i < min(20, len(sortedContracts)); i++ {
					contract := sortedContracts[i]
					fmt.Printf("0x%-40s %15d %10d\n",
						contract.Address,
						contract.Size,
						contract.KeyCount)
				}
			}
		}
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
