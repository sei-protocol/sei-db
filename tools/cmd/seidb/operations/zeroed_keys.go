package operations

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/sc/memiavl"
	"github.com/spf13/cobra"
)

const (
	defaultZeroedKeyLimit   = 1000
	defaultDeletionChunkCap = 1000
)

type keyValuePair struct {
	Key   []byte
	Value []byte
}

// DumpZeroedKeysCmd writes up to N zeroed EVM 0x03 keys to a file for later processing.
func DumpZeroedKeysCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump-zeroed-keys",
		Short: "Export zero-valued EVM 0x03 keys to a file",
		RunE:  runDumpZeroedKeys,
	}

	cmd.PersistentFlags().StringP("db-dir", "d", "", "Database directory")
	cmd.PersistentFlags().Int64("height", 0, "Block height to open")
	cmd.PersistentFlags().Int("limit", defaultZeroedKeyLimit, "Maximum number of keys to export")
	cmd.PersistentFlags().StringP("output", "o", "zeroed_keys.txt", "Output file for exported keys")

	return cmd
}

func runDumpZeroedKeys(cmd *cobra.Command, _ []string) error {
	dbDir, _ := cmd.Flags().GetString("db-dir")
	height, _ := cmd.Flags().GetInt64("height")
	limit, _ := cmd.Flags().GetInt("limit")
	outputPath, _ := cmd.Flags().GetString("output")

	if dbDir == "" {
		return fmt.Errorf("must provide --db-dir")
	}
	if limit <= 0 {
		return fmt.Errorf("limit must be positive")
	}

	opts := memiavl.Options{Dir: dbDir, ZeroCopy: true, CreateIfMissing: false}
	db, err := memiavl.OpenDB(logger.NewNopLogger(), height, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	tree := db.TreeByName("evm")
	if tree == nil {
		return fmt.Errorf("evm tree does not exist")
	}

	entries := collectZeroedEntries(tree, limit)
	if len(entries) == 0 {
		fmt.Println("No zeroed EVM 0x03 keys found")
		return nil
	}

	if err := writeHexEntries(outputPath, entries); err != nil {
		return err
	}

	fmt.Printf("Wrote %d zeroed EVM 0x03 key/value pairs to %s\n", len(entries), outputPath)
	return nil
}

func collectZeroedEntries(tree *memiavl.Tree, limit int) []keyValuePair {
	var entries []keyValuePair

	tree.ScanPostOrder(func(node memiavl.Node) bool {
		if len(entries) >= limit {
			return false
		}
		if !node.IsLeaf() {
			return true
		}

		if len(node.Key()) == 0 {
			return true
		}

		rawKey := node.Key()
		rawValue := node.Value()
		if rawKey[0] == 0x03 && isAllZero(rawValue) {
			keyCopy := append([]byte(nil), rawKey...)
			valueCopy := append([]byte(nil), rawValue...)
			entries = append(entries, keyValuePair{Key: keyCopy, Value: valueCopy})
		}
		return true
	})

	return entries
}

func writeHexEntries(path string, entries []keyValuePair) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, entry := range entries {
		if _, err := writer.WriteString(fmt.Sprintf("%s,%s\n", hex.EncodeToString(entry.Key), hex.EncodeToString(entry.Value))); err != nil {
			return err
		}
	}
	return writer.Flush()
}

// ApplyZeroedKeyDeletesCmd deletes keys listed in a file, committing in configurable chunks.
func ApplyZeroedKeyDeletesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "apply-zeroed-key-deletes",
		Short: "Delete keys listed in a file using chunked commits",
		RunE:  runApplyZeroedKeyDeletes,
	}

	cmd.PersistentFlags().StringP("db-dir", "d", "", "Database directory")
	cmd.PersistentFlags().Int64("height", 0, "Block height to open")
	cmd.PersistentFlags().StringP("input", "i", "zeroed_keys.txt", "Input file containing hex-encoded keys")
	cmd.PersistentFlags().Int("chunk-size", defaultDeletionChunkCap, "Number of keys to delete per commit")

	return cmd
}

func runApplyZeroedKeyDeletes(cmd *cobra.Command, _ []string) error {
	dbDir, _ := cmd.Flags().GetString("db-dir")
	height, _ := cmd.Flags().GetInt64("height")
	inputPath, _ := cmd.Flags().GetString("input")
	chunkSize, _ := cmd.Flags().GetInt("chunk-size")

	if dbDir == "" {
		return fmt.Errorf("must provide --db-dir")
	}
	if chunkSize <= 0 {
		return fmt.Errorf("chunk-size must be positive")
	}

	entries, err := readHexEntries(inputPath)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		fmt.Println("No keys loaded from input file")
		return nil
	}

	opts := memiavl.Options{Dir: dbDir, ZeroCopy: true, CreateIfMissing: false}
	db, err := memiavl.OpenDB(logger.NewNopLogger(), height, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	chunkPairs := make([]*iavl.KVPair, 0, chunkSize)
	processed := 0
	for idx, entry := range entries {
		chunkPairs = append(chunkPairs, &iavl.KVPair{Key: entry.Key, Value: entry.Value, Delete: true})
		if len(chunkPairs) == chunkSize || idx == len(entries)-1 {
			sort.Slice(chunkPairs, func(i, j int) bool {
				return bytes.Compare(chunkPairs[i].Key, chunkPairs[j].Key) < 0
			})
			if err := db.ApplyChangeSet("evm", iavl.ChangeSet{Pairs: chunkPairs}); err != nil {
				return fmt.Errorf("apply change set chunk: %w", err)
			}
			if _, err := db.Commit(); err != nil {
				panic(err)
			}
			processed += len(chunkPairs)
			fmt.Printf("Committed deletion chunk of %d keys (%d/%d total)\n", len(chunkPairs), processed, len(entries))
			chunkPairs = chunkPairs[:0]
		}
	}

	fmt.Printf("Finished deleting %d keys from %s\n", len(entries), inputPath)
	return nil
}

func readHexEntries(path string) ([]keyValuePair, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var entries []keyValuePair
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid line %q: expected key,value", line)
		}
		keyBytes, err := hex.DecodeString(parts[0])
		if err != nil {
			return nil, fmt.Errorf("decode key %q: %w", parts[0], err)
		}
		valueBytes, err := hex.DecodeString(parts[1])
		if err != nil {
			return nil, fmt.Errorf("decode value %q: %w", parts[1], err)
		}
		entries = append(entries, keyValuePair{Key: keyBytes, Value: valueBytes})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}
