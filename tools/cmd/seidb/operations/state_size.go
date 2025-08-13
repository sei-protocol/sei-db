package operations

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/sc/memiavl"
	"github.com/spf13/cobra"
)

// ProjectContracts Fill this in with the contract addresses to get their size.
var ProjectContracts = map[string][]string{}

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

type contractSizeEntry struct {
	Address   string
	TotalSize int64
	KeyCount  int
}

type projectSizeEntry struct {
	Project   string
	TotalSize int64
	KeyCount  int
}

type moduleStats struct {
	keySizeByPrefix   map[string]int64
	valueSizeByPrefix map[string]int64
	numKeysByPrefix   map[string]int64
	contractSizes     map[string]*contractSizeEntry
	totalNumKeys      int
	totalKeySize      int
	totalValueSize    int
	totalSize         int
}

func newModuleStats() *moduleStats {
	return &moduleStats{
		keySizeByPrefix:   make(map[string]int64),
		valueSizeByPrefix: make(map[string]int64),
		numKeysByPrefix:   make(map[string]int64),
		contractSizes:     make(map[string]*contractSizeEntry),
	}
}

// evmPrefixLabels maps the first-byte hex prefix to its EVM key space name
var evmPrefixLabels = map[string]string{
	"01": "EVMAddressToSeiAddressKeyPrefix",
	"02": "SeiAddressToEVMAddressKeyPrefix",
	"03": "StateKeyPrefix",
	"04": "TransientStateKeyPrefix (deprecated)",
	"05": "AccountTransientStateKeyPrefix (deprecated)",
	"06": "TransientModuleStateKeyPrefix (deprecated)",
	"07": "CodeKeyPrefix",
	"08": "CodeHashKeyPrefix",
	"09": "CodeSizeKeyPrefix",
	"0A": "NonceKeyPrefix",
	"0B": "ReceiptKeyPrefix",
	"0C": "WhitelistedCodeHashesForBankSendPrefix",
	"0D": "BlockBloomPrefix",
	"0E": "TxHashesPrefix (deprecated)",
	"0F": "WhitelistedCodeHashesForDelegateCallPrefix",
	"15": "PointerRegistryPrefix",
	"16": "PointerCWCodePrefix",
	"17": "PointerReverseRegistryPrefix",
	"18": "AnteSurplusPrefix (transient)",
	"19": "DeferredInfoPrefix (transient)",
	"1A": "LegacyBlockBloomCutoffHeightKey",
	"1B": "BaseFeePerGasPrefix",
	"1C": "NextBaseFeePerGasPrefix",
}

func prefixDisplayName(moduleName, hexPrefix string) string {
	if moduleName == "evm" {
		if name, ok := evmPrefixLabels[strings.ToUpper(hexPrefix)]; ok {
			return name
		}
	}
	return hexPrefix
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
	fmt.Printf("Finished opening db, calculating state size for module: %s\n", module)
	err = PrintStateSize(module, db)
	if err != nil {
		panic(err)
	}
}

// PrintStateSize print the raw keys and values for given module at given height for memIAVL tree
func PrintStateSize(module string, db *memiavl.DB) error {
	modules := []string{}
	numToShow := 20
	if module == "" {
		modules = AllModules
	} else {
		modules = append(modules, module)
	}
	for _, moduleName := range modules {
		tree := db.TreeByName(moduleName)
		if tree == nil {
			fmt.Printf("Tree does not exist for module %s \n", moduleName)
			continue
		}
		fmt.Printf("Calculating for module: %s \n", moduleName)
		stats := newModuleStats()
		// Scan and accumulate statistics
		tree.ScanPostOrder(func(node memiavl.Node) bool {
			processNode(moduleName, module, node, stats)
			if stats.totalNumKeys%1000000 == 0 && stats.totalNumKeys > 0 {
				fmt.Printf("Scanned %d keys for module %s\n", stats.totalNumKeys, moduleName)
			}
			return true
		})

		printModuleTotals(moduleName, stats)
		printPrefixBreakdown(moduleName, stats)

		if module == "evm" {
			printEvmContractBreakdown(stats, numToShow)
			printEvmProjectTotals(stats)
			printEvmContractSizeDistribution(stats)
		}
	}
	return nil
}

func processNode(moduleName, requestedModule string, node memiavl.Node, stats *moduleStats) {
	if !node.IsLeaf() {
		return
	}
	stats.totalNumKeys++
	keySize := len(node.Key())
	valueSize := len(node.Value())
	stats.totalKeySize += keySize
	stats.totalValueSize += valueSize
	stats.totalSize += keySize + valueSize

	prefixKey := fmt.Sprintf("%X", node.Key())
	prefix := prefixKey[:2]
	stats.keySizeByPrefix[prefix] += int64(keySize)
	stats.valueSizeByPrefix[prefix] += int64(valueSize)
	stats.numKeysByPrefix[prefix]++

	if requestedModule == "evm" && prefix == "03" {
		addrHexNoPrefix := strings.ToLower(prefixKey[2:42]) // 40 hex chars
		if _, exists := stats.contractSizes[addrHexNoPrefix]; !exists {
			stats.contractSizes[addrHexNoPrefix] = &contractSizeEntry{Address: addrHexNoPrefix}
		}
		entry := stats.contractSizes[addrHexNoPrefix]
		entry.TotalSize += int64(keySize) + int64(valueSize)
		entry.KeyCount++
	}
}

func printModuleTotals(moduleName string, stats *moduleStats) {
	fmt.Printf(
		"Module %s total numKeys:%d, total keySize MB:%d, total valueSize MB:%d, totalSize MB: %d \n",
		moduleName,
		stats.totalNumKeys,
		stats.totalKeySize/1024/1024,
		stats.totalValueSize/1024/1024,
		stats.totalSize/1024/1024,
	)
}

func printPrefixBreakdown(moduleName string, stats *moduleStats) {
	keys := make([]string, 0, len(stats.keySizeByPrefix))
	for k := range stats.keySizeByPrefix {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ki, kj := keys[i], keys[j]
		return stats.keySizeByPrefix[ki]+stats.valueSizeByPrefix[ki] > stats.keySizeByPrefix[kj]+stats.valueSizeByPrefix[kj]
	})

	labels := make(map[string]string, len(keys))
	labelWidth := len("Prefix")
	for _, k := range keys {
		l := prefixDisplayName(moduleName, k)
		labels[k] = l
		if len(l) > labelWidth {
			labelWidth = len(l)
		}
	}
	if labelWidth < 24 {
		labelWidth = 24
	}
	fmt.Printf("%-*s  %15s  %10s\n", labelWidth, "Prefix", "Total Size (MB)", "Percentage")
	for _, k := range keys {
		keySize := stats.keySizeByPrefix[k]
		valueSize := stats.valueSizeByPrefix[k]
		kvTotalSize := keySize + valueSize
		percentage := 0.0
		if stats.totalSize > 0 {
			percentage = float64(kvTotalSize) / float64(stats.totalSize) * 100
		}
		label := labels[k]
		fmt.Printf("%-*s  %15d  %9.2f\n", labelWidth, label, kvTotalSize/1024/1024, percentage)
	}
}

func printEvmContractBreakdown(stats *moduleStats, numToShow int) {
	var sortedContracts []contractSizeEntry
	for _, entry := range stats.contractSizes {
		sortedContracts = append(sortedContracts, *entry)
	}
	sort.Slice(sortedContracts, func(i, j int) bool { return sortedContracts[i].TotalSize > sortedContracts[j].TotalSize })

	fmt.Printf("\nDetailed breakdown for 0x03 prefix (top %d contracts by total size):\n", numToShow)
	fmt.Printf("%s,%s,%s\n", "Contract Address", "Total Size", "Key Count")
	if len(sortedContracts) < numToShow {
		numToShow = len(sortedContracts)
	}
	for i := 0; i < numToShow; i++ {
		contract := sortedContracts[i]
		fmt.Printf("0x%s,%d,%d\n", contract.Address, contract.TotalSize/1024/1024, contract.KeyCount)
	}
	fmt.Printf("Total unique contracts: %d\n", len(stats.contractSizes))
}

func printEvmProjectTotals(stats *moduleStats) {
	projectSizes := make(map[string]*projectSizeEntry)
	addrToProjects := buildAddrToProjects()
	for _, c := range stats.contractSizes {
		addr0x := "0x" + strings.ToLower(c.Address)
		if projs, ok := addrToProjects[addr0x]; ok {
			for _, p := range projs {
				ps := projectSizes[p]
				if ps == nil {
					ps = &projectSizeEntry{Project: p}
					projectSizes[p] = ps
				}
				ps.TotalSize += c.TotalSize
				ps.KeyCount += c.KeyCount
			}
		}
	}
	var projList []projectSizeEntry
	for _, ps := range projectSizes {
		projList = append(projList, *ps)
	}
	sort.Slice(projList, func(i, j int) bool { return projList[i].TotalSize > projList[j].TotalSize })
	fmt.Printf("\nProject totals (sum over listed contract addresses in 0x03 keys):\n")
	fmt.Printf("%s,%s,%s\n", "Project", "Total Size (MB)", "Key Count")
	for _, ps := range projList {
		fmt.Printf("%s,%d,%d\n", ps.Project, ps.TotalSize/1024/1024, ps.KeyCount)
	}
}

func printEvmContractSizeDistribution(stats *moduleStats) {
	const (
		KB = int64(1024)
		MB = int64(1024 * 1024)
	)
	total := len(stats.contractSizes)
	if total == 0 {
		fmt.Printf("\nNo EVM contracts found for size distribution.\n")
		return
	}

	var (
		lt1KBCount      int
		_1KBto1MBCount  int
		_1MBto10MBCount int
		_10MBto100Count int
		gt100MBCount    int

		lt1KBSizeBytes      int64
		_1KBto1MBSizeBytes  int64
		_1MBto10MBSizeBytes int64
		_10MBto100SizeBytes int64
		gt100MBSizeBytes    int64
	)

	for _, c := range stats.contractSizes {
		size := c.TotalSize
		switch {
		case size < 1*KB:
			lt1KBCount++
			lt1KBSizeBytes += size
		case size >= 1*KB && size < 1*MB:
			_1KBto1MBCount++
			_1KBto1MBSizeBytes += size
		case size >= 1*MB && size < 10*MB:
			_1MBto10MBCount++
			_1MBto10MBSizeBytes += size
		case size >= 10*MB && size <= 100*MB:
			_10MBto100Count++
			_10MBto100SizeBytes += size
		default: // > 100MB
			gt100MBCount++
			gt100MBSizeBytes += size
		}
	}

	// Sum sizes for size-based percentages
	sumSizeBytes := lt1KBSizeBytes + _1KBto1MBSizeBytes + _1MBto10MBSizeBytes + _10MBto100SizeBytes + gt100MBSizeBytes

	fmt.Printf("\nContract size distribution (StateKeyPrefix 0x03 only):\n")
	fmt.Printf("%-20s  %10s  %9s  %15s  %8s\n", "Bucket", "Count", "Count %", "Total Size (MB)", "Size %")
	printBucket := func(name string, count int, sizeBytes int64) {
		countPct := float64(count) / float64(total) * 100
		sizePct := 0.0
		if sumSizeBytes > 0 {
			sizePct = float64(sizeBytes) / float64(sumSizeBytes) * 100
		}
		fmt.Printf("%-20s  %10d  %8.2f  %15d  %7.2f\n", name, count, countPct, sizeBytes/MB, sizePct)
	}
	printBucket("<1KB", lt1KBCount, lt1KBSizeBytes)
	printBucket("1KB - <1MB", _1KBto1MBCount, _1KBto1MBSizeBytes)
	printBucket("1MB - <10MB", _1MBto10MBCount, _1MBto10MBSizeBytes)
	printBucket("10MB - 100MB", _10MBto100Count, _10MBto100SizeBytes)
	printBucket(">100MB", gt100MBCount, gt100MBSizeBytes)

	// Totals row and module share
	fmt.Printf("%-20s  %10d  %8.2f  %15d  %7.2f\n", "Total", total, 100.00, sumSizeBytes/MB, 100.00)
	if stats.totalSize > 0 {
		share := float64(sumSizeBytes) / float64(stats.totalSize) * 100
		fmt.Printf("Share of module total size: %.2f%%\n", share)
	}
}

func buildAddrToProjects() map[string][]string {
	addrToProjects := make(map[string][]string)
	for proj, addrs := range ProjectContracts {
		for _, a := range addrs {
			key := strings.ToLower(a) // keep "0x" prefix
			addrToProjects[key] = append(addrToProjects[key], proj)
		}
	}
	return addrToProjects
}
