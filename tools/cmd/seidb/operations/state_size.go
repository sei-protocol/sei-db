package operations

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/sc/memiavl"
	"github.com/sei-protocol/sei-db/tools/utils"
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

	// DynamoDB export flags
	cmd.PersistentFlags().Bool("export-dynamodb", false, "Export results to DynamoDB instead of printing")
	cmd.PersistentFlags().String("dynamodb-table", "state_size_analysis", "DynamoDB table name")
	cmd.PersistentFlags().String("aws-region", "us-east-2", "AWS region for DynamoDB")

	return cmd
}

const (
	deletionBatchSize   = 5000
	deletionLogInterval = 5000
)

func executeStateSize(cmd *cobra.Command, _ []string) {
	module, _ := cmd.Flags().GetString("module")
	dbDir, _ := cmd.Flags().GetString("db-dir")
	height, _ := cmd.Flags().GetInt64("height")
	exportDynamoDB, _ := cmd.Flags().GetBool("export-dynamodb")
	dynamoDBTable, _ := cmd.Flags().GetString("dynamodb-table")
	awsRegion, _ := cmd.Flags().GetString("aws-region")

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

	// Get the actual height of the opened database
	actualHeight := db.Version()
	fmt.Printf("Finished opening db at height %d (requested: %d), calculating state size for module: %s\n", actualHeight, height, module)

	// First, collect all the data by scanning the trees
	moduleResults, err := collectAllModuleData(module, db)
	if err != nil {
		panic(err)
	}

	// Then process the results based on the flag
	if exportDynamoDB {
		fmt.Printf("Exporting results to DynamoDB table: %s\n", dynamoDBTable)
		err = exportResultsToDynamoDB(moduleResults, actualHeight, dynamoDBTable, awsRegion)
		if err != nil {
			panic(err)
		}
		fmt.Println("Successfully exported to DynamoDB!")
	} else {
		printResultsToConsole(moduleResults)
	}
}

// collectModuleStats collects all the statistics for a module and records zeroed entries for deletion.
func collectModuleStats(tree *memiavl.Tree, moduleName string, deletions *moduleDeletionCollector) (*ModuleResult, error) {
	result := &ModuleResult{
		ModuleName:    moduleName,
		PrefixSizes:   make(map[string]*utils.PrefixSize),
		ContractSizes: make(map[string]*utils.ContractSizeEntry),
	}

	deletedCount := 0

	var scanErr error

	// Scan the tree to collect statistics
	tree.ScanPostOrder(func(node memiavl.Node) bool {
		if node.IsLeaf() {
			result.TotalNumKeys++
			keySize := len(node.Key())
			valueSize := len(node.Value())
			result.TotalKeySize += uint64(keySize)
			result.TotalValueSize += uint64(valueSize)
			result.TotalSize += uint64(keySize + valueSize)

			prefixKey := fmt.Sprintf("%X", node.Key())
			prefix := prefixKey[:2]
			if _, exists := result.PrefixSizes[moduleName]; !exists {
				result.PrefixSizes[moduleName] = &utils.PrefixSize{}
			}
			result.PrefixSizes[moduleName].KeySize += uint64(keySize)
			result.PrefixSizes[moduleName].ValueSize += uint64(valueSize)
			result.PrefixSizes[moduleName].TotalSize += uint64(keySize + valueSize)
			result.PrefixSizes[moduleName].KeyCount++

			// Handle EVM contract analysis
			if moduleName == "evm" && prefix == "03" {
				result.TotalEVM03Entries++
				if isAllZero(node.Value()) {
					result.ZeroedEVM03Entries++
					result.ZeroedEVM03KeyBytes += uint64(keySize)
					result.ZeroedEVM03ValueBytes += uint64(valueSize)
					deletedCount++
					currentCount := deletedCount
					keyCopy := append([]byte(nil), node.Key()...)
					if currentCount%deletionLogInterval == 0 {
						fmt.Printf("Found zeroed EVM 0x03 entry #%d; preparing deletion for key %X\n", currentCount, keyCopy)
						fmt.Printf("Deleting zeroed EVM 0x03 entry #%d with key %X\n", currentCount, keyCopy)
					}
					if err := deletions.AddKey(keyCopy); err != nil {
						scanErr = err
						return false
					}
				}
				addr := prefixKey[2:42]
				if _, exists := result.ContractSizes[addr]; !exists {
					result.ContractSizes[addr] = &utils.ContractSizeEntry{Address: addr}
				}
				entry := result.ContractSizes[addr]
				entry.TotalSize += uint64(len(node.Key()) + len(node.Value()))
				entry.KeyCount++
			}

			if result.TotalNumKeys%1000000 == 0 {
				fmt.Printf("Scanned %d keys for module %s\n", result.TotalNumKeys, moduleName)
			}
		}
		return true
	})
	if scanErr != nil {
		return nil, scanErr
	}

	// Limit to top 100 contracts by total size
	result.ContractSizes = limitToTopContracts(result.ContractSizes, 100)

	return result, nil
}

// limitToTopContracts keeps only the top N contracts by total size
func limitToTopContracts(contracts map[string]*utils.ContractSizeEntry, limit int) map[string]*utils.ContractSizeEntry {
	if len(contracts) <= limit {
		return contracts
	}

	// Convert to slice for sorting
	var contractSlice []utils.ContractSizeEntry
	for _, contract := range contracts {
		contractSlice = append(contractSlice, *contract)
	}

	// Sort by total size in descending order
	sort.Slice(contractSlice, func(i, j int) bool {
		return contractSlice[i].TotalSize > contractSlice[j].TotalSize
	})

	// Keep only top N
	result := make(map[string]*utils.ContractSizeEntry)
	for i := 0; i < limit; i++ {
		contract := contractSlice[i]
		result[contract.Address] = &contract
	}

	return result
}

// ModuleResult holds the complete analysis results for a single module
type ModuleResult struct {
	ModuleName     string
	TotalNumKeys   uint64
	TotalKeySize   uint64
	TotalValueSize uint64
	TotalSize      uint64
	PrefixSizes    map[string]*utils.PrefixSize
	ContractSizes  map[string]*utils.ContractSizeEntry
	// EVM-specific statistics for 0x03 storage prefix
	TotalEVM03Entries     uint64
	ZeroedEVM03Entries    uint64
	ZeroedEVM03KeyBytes   uint64
	ZeroedEVM03ValueBytes uint64
}

// collectAllModuleData scans all modules, collects statistics, and persists deletions.
func collectAllModuleData(module string, db *memiavl.DB) (map[string]*ModuleResult, error) {
	modules := []string{}
	if module == "" {
		modules = AllModules
	} else {
		modules = append(modules, module)
	}

	moduleResults := make(map[string]*ModuleResult)

	for _, moduleName := range modules {
		tree := db.TreeByName(moduleName)
		if tree == nil {
			fmt.Printf("Tree does not exist for module %s, skipping...\n", moduleName)
			continue
		}

		fmt.Printf("Analyzing module: %s\n", moduleName)

		collector, err := newModuleDeletionCollector(moduleName)
		if err != nil {
			return nil, fmt.Errorf("create deletion collector: %w", err)
		}
		defer collector.Cleanup()

		// Collect statistics directly into ModuleResult
		result, err := collectModuleStats(tree, moduleName, collector)
		if err != nil {
			return nil, fmt.Errorf("collect module stats for %s: %w", moduleName, err)
		}

		// Store in memory (result is already a ModuleResult)
		moduleResults[moduleName] = result

		fmt.Printf("Collected stats for module %s: %d keys, %d total size\n",
			moduleName, result.TotalNumKeys, result.TotalSize)

		if err := collector.CloseWriter(); err != nil {
			return nil, fmt.Errorf("finalize deletion collector for %s: %w", moduleName, err)
		}

		if err := applyDeletionBatches(db, moduleName, collector); err != nil {
			return nil, fmt.Errorf("apply deletions for %s: %w", moduleName, err)
		}

		collector.Cleanup()
	}

	return moduleResults, nil
}

type moduleDeletionCollector struct {
	moduleName string
	path       string
	file       *os.File
	writer     *bufio.Writer
	total      int
}

func newModuleDeletionCollector(moduleName string) (*moduleDeletionCollector, error) {
	file, err := os.CreateTemp("", fmt.Sprintf("state-size-%s-deletions-*.bin", moduleName))
	if err != nil {
		return nil, err
	}

	return &moduleDeletionCollector{
		moduleName: moduleName,
		path:       file.Name(),
		file:       file,
		writer:     bufio.NewWriter(file),
	}, nil
}

func (m *moduleDeletionCollector) AddKey(key []byte) error {
	if m.writer == nil {
		return fmt.Errorf("deletion collector writer closed for module %s", m.moduleName)
	}
	if err := binary.Write(m.writer, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := m.writer.Write(key); err != nil {
		return err
	}
	m.total++
	return nil
}

func (m *moduleDeletionCollector) CloseWriter() error {
	if m.writer != nil {
		if err := m.writer.Flush(); err != nil {
			return err
		}
		m.writer = nil
	}
	if m.file != nil {
		if err := m.file.Close(); err != nil {
			return err
		}
		m.file = nil
	}
	return nil
}

func (m *moduleDeletionCollector) Cleanup() {
	if m.file != nil {
		_ = m.file.Close()
		m.file = nil
	}
	if m.path != "" {
		_ = os.Remove(m.path)
		m.path = ""
	}
}

func applyDeletionBatches(db *memiavl.DB, moduleName string, collector *moduleDeletionCollector) error {
	if collector == nil || collector.total == 0 {
		return nil
	}

	file, err := os.Open(collector.path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	batch := make([]*iavl.KVPair, 0, deletionBatchSize)
	processed := 0

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		sort.Slice(batch, func(i, j int) bool {
			return bytes.Compare(batch[i].Key, batch[j].Key) < 0
		})
		changeSet := iavl.ChangeSet{Pairs: batch}
		if err := db.ApplyChangeSets([]*proto.NamedChangeSet{{
			Name:      moduleName,
			Changeset: changeSet,
		}}); err != nil {
			return err
		}
		if _, err := db.Commit(); err != nil {
			return err
		}
		for _, pair := range batch {
			processed++
			if processed%deletionLogInterval == 0 {
				fmt.Printf("Deleted zeroed EVM 0x03 entry #%d with key %X\n", processed, pair.Key)
			}
		}
		// reuse underlying array to keep allocations small
		for i := range batch {
			batch[i] = nil
		}
		batch = batch[:0]
		return nil
	}

	for {
		var keyLen uint32
		err := binary.Read(reader, binary.BigEndian, &keyLen)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return err
		}
		batch = append(batch, &iavl.KVPair{Key: key, Delete: true})
		if len(batch) >= deletionBatchSize {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}

	if err := flushBatch(); err != nil {
		return err
	}

	fmt.Printf("Committed deletion of %d zeroed EVM 0x03 entries for module %s\n", collector.total, moduleName)
	return nil
}

// exportResultsToDynamoDB exports the collected results to DynamoDB
func exportResultsToDynamoDB(moduleResults map[string]*ModuleResult, height int64, tableName, awsRegion string) error {
	// Initialize DynamoDB client
	dynamoClient, err := utils.NewDynamoDBClient(tableName, awsRegion)
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB client: %w", err)
	}

	var analyses []*utils.StateSizeAnalysis

	for _, result := range moduleResults {
		// Create analysis object directly from raw data
		analysis := createStateSizeAnalysis(height, result.ModuleName, result)
		analyses = append(analyses, analysis)
	}

	// Export all analyses to DynamoDB
	if err := dynamoClient.ExportMultipleAnalyses(analyses); err != nil {
		return fmt.Errorf("failed to export analyses to DynamoDB: %w", err)
	}

	metadataTableName := tableName + "_metadata"
	_, err = dynamoClient.UpdateLatestHeightIfGreater(metadataTableName, height)
	return err
}

// printResultsToConsole prints the collected results to console
func printResultsToConsole(moduleResults map[string]*ModuleResult) {

	for moduleName, result := range moduleResults {
		fmt.Printf("Module %s total numKeys:%d, total keySize:%d, total valueSize:%d, totalSize: %d \n",
			result.ModuleName, result.TotalNumKeys, result.TotalKeySize, result.TotalValueSize, result.TotalSize)

		fmt.Println("prefix sizes: ", result.PrefixSizes)
		fmt.Println("module name: ", moduleName)
		fmt.Println("Prefix Sizes[moduleName]: ", result.PrefixSizes[moduleName])

		prefixKeyResult, _ := json.MarshalIndent(result.PrefixSizes[moduleName].KeySize, "", "  ")
		fmt.Printf("Module %s prefix key size breakdown (bytes): %s \n", result.ModuleName, prefixKeyResult)

		prefixValueResult, _ := json.MarshalIndent(result.PrefixSizes[moduleName].ValueSize, "", "  ")
		fmt.Printf("Module %s prefix value size breakdown (bytes): %s \n", result.ModuleName, prefixValueResult)

		totalSizeResult, _ := json.MarshalIndent(result.PrefixSizes[moduleName].TotalSize, "", "  ")
		fmt.Printf("Module %s prefix total size breakdown (bytes): %s \n", result.ModuleName, totalSizeResult)

		numKeysResult, _ := json.MarshalIndent(result.PrefixSizes[moduleName].KeyCount, "", "  ")
		fmt.Printf("Module %s prefix num of keys breakdown: %s \n", result.ModuleName, numKeysResult)

		// EVM-only: zeroed-entry statistics for 0x03 storage
		if moduleName == "evm" {
			var pct float64
			if result.TotalEVM03Entries > 0 {
				pct = float64(result.ZeroedEVM03Entries) / float64(result.TotalEVM03Entries) * 100
			}
			fmt.Printf("EVM 0x03 entries: total=%d, zeroed=%d (%.2f%%), zeroed_key_bytes=%d, zeroed_value_bytes=%d\n",
				result.TotalEVM03Entries,
				result.ZeroedEVM03Entries,
				pct,
				result.ZeroedEVM03KeyBytes,
				result.ZeroedEVM03ValueBytes,
			)
		}

		// Display top contracts (already limited to top 100)
		fmt.Printf("\nDetailed breakdown for 0x03 prefix (top %d contracts by total size):\n", len(result.ContractSizes))
		fmt.Printf("%-42s %15s %10s\n", "Contract Address", "Total Size", "Key Count")
		fmt.Printf("%s\n", strings.Repeat("-", 70))

		// Convert to slice for display
		var contractSlice []utils.ContractSizeEntry
		for _, entry := range result.ContractSizes {
			contractSlice = append(contractSlice, *entry)
		}

		// Sort by total size in descending order for display
		sort.Slice(contractSlice, func(i, j int) bool {
			return contractSlice[i].TotalSize > contractSlice[j].TotalSize
		})

		for _, contract := range contractSlice {
			fmt.Printf("0x%-40s %15d %10d\n",
				contract.Address,
				contract.TotalSize,
				contract.KeyCount)
		}
	}
}

// createStateSizeAnalysis creates a new StateSizeAnalysis from ModuleResult
func createStateSizeAnalysis(blockHeight int64, moduleName string, result *ModuleResult) *utils.StateSizeAnalysis {
	// Convert raw data to JSON strings for DynamoDB storage

	prefixJSON, _ := json.Marshal(result.PrefixSizes)

	var contractSlice []utils.ContractSizeEntry
	for _, contract := range result.ContractSizes {
		contractSlice = append(contractSlice, *contract)
	}
	contractJSON, _ := json.Marshal(contractSlice)

	return &utils.StateSizeAnalysis{
		BlockHeight:       blockHeight,
		ModuleName:        moduleName,
		TotalNumKeys:      result.TotalNumKeys,
		TotalKeySize:      result.TotalKeySize,
		TotalValueSize:    result.TotalValueSize,
		TotalSize:         result.TotalSize,
		PrefixBreakdown:   string(prefixJSON),
		ContractBreakdown: string(contractJSON),
	}
}

// isAllZero returns true if the provided byte slice is empty or consists entirely of zero bytes.
func isAllZero(b []byte) bool {
	for _, by := range b {
		if by != 0x00 {
			return false
		}
	}
	return true
}
