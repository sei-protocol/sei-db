package operations

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"sort"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/sc/memiavl"
	"github.com/spf13/cobra"
)

const (
	defaultZeroedKeyLimit   = 1000
	defaultDeletionChunkCap = 1000
)

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

	keys := collectZeroedKeys(tree, limit)
	if len(keys) == 0 {
		fmt.Println("No zeroed EVM 0x03 keys found")
		return nil
	}

	if err := writeHexKeys(outputPath, keys); err != nil {
		return err
	}

	fmt.Printf("Wrote %d zeroed EVM 0x03 keys to %s\n", len(keys), outputPath)
	return nil
}

func collectZeroedKeys(tree *memiavl.Tree, limit int) [][]byte {
	var keys [][]byte

	tree.ScanPostOrder(func(node memiavl.Node) bool {
		if len(keys) >= limit {
			return false
		}
		if !node.IsLeaf() {
			return true
		}

		rawKey := node.Key()
		if len(rawKey) < 1 {
			return true
		}

		if rawKey[0] == 0x03 && isAllZero(node.Value()) {
			keyCopy := append([]byte(nil), rawKey...)
			keys = append(keys, keyCopy)
		}
		return true
	})

	return keys
}

func writeHexKeys(path string, keys [][]byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, key := range keys {
		if _, err := writer.WriteString(fmt.Sprintf("%s\n", hex.EncodeToString(key))); err != nil {
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

	keys, err := readHexKeys(inputPath)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		fmt.Println("No keys loaded from input file")
		return nil
	}

	opts := memiavl.Options{Dir: dbDir, ZeroCopy: true, CreateIfMissing: false}
	db, err := memiavl.OpenDB(logger.NewNopLogger(), height, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	chunkKeys := make([]*iavl.KVPair, 0, chunkSize)
	processed := 0
	for idx, key := range keys {
		chunkKeys = append(chunkKeys, &iavl.KVPair{Key: key, Delete: true})
		if len(chunkKeys) == chunkSize || idx == len(keys)-1 {
			sort.Slice(chunkKeys, func(i, j int) bool {
				return bytes.Compare(chunkKeys[i].Key, chunkKeys[j].Key) < 0
			})
			if err := db.ApplyChangeSet("evm", iavl.ChangeSet{Pairs: chunkKeys}); err != nil {
				return fmt.Errorf("apply change set chunk: %w", err)
			}
			if _, err := db.Commit(); err != nil {
				return fmt.Errorf("commit chunk: %w", err)
			}
			processed += len(chunkKeys)
			fmt.Printf("Committed deletion chunk of %d keys (%d/%d total)\n", len(chunkKeys), processed, len(keys))
			chunkKeys = chunkKeys[:0]
		}
	}

	fmt.Printf("Finished deleting %d keys from %s\n", len(keys), inputPath)
	return nil
}

func readHexKeys(path string) ([][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var keys [][]byte
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		decoded, err := hex.DecodeString(line)
		if err != nil {
			return nil, fmt.Errorf("decode key %q: %w", line, err)
		}
		keys = append(keys, decoded)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}
