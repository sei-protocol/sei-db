package operations

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/sc/memiavl"
	"github.com/spf13/cobra"
)

// PoCCmd runs a minimal add-and-delete flow against a single module.
func PoCCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "poc-mutate",
		Short: "Add a single key/value, commit, delete it, and commit again",
		RunE:  runPoC,
	}

	cmd.PersistentFlags().StringP("module", "m", "evm", "Module name to mutate")
	cmd.PersistentFlags().String("key", "POC_KEY", "Key to insert (hex or raw string)")
	cmd.PersistentFlags().String("value", "POC_VALUE", "Value to insert (hex or raw string)")
	cmd.PersistentFlags().Bool("hex", false, "Interpret key/value flags as hex-encoded bytes")

	return cmd
}

func runPoC(cmd *cobra.Command, _ []string) error {
	moduleName, _ := cmd.Flags().GetString("module")
	keyFlag, _ := cmd.Flags().GetString("key")
	valueFlag, _ := cmd.Flags().GetString("value")
	hexMode, _ := cmd.Flags().GetBool("hex")

	keyBytes, valueBytes, err := decodeKeyValue(keyFlag, valueFlag, hexMode)
	if err != nil {
		return err
	}

	tempDir, err := os.MkdirTemp("", "poc-memiavl-")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	opts := memiavl.Options{
		Dir:             tempDir,
		ZeroCopy:        true,
		CreateIfMissing: true,
		InitialStores:   []string{moduleName},
	}
	db, err := memiavl.OpenDB(logger.NewNopLogger(), 0, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := applyAndCommit(db, moduleName, []*iavl.KVPair{{Key: keyBytes, Value: valueBytes}}); err != nil {
		return fmt.Errorf("insert step failed: %w", err)
	}
	fmt.Printf("Inserted key %X with value %X into module %s\n", keyBytes, valueBytes, moduleName)

	if err := applyAndCommit(db, moduleName, []*iavl.KVPair{{Key: keyBytes, Value: valueBytes, Delete: true}}); err != nil {
		return fmt.Errorf("delete step failed: %w", err)
	}
	fmt.Printf("Deleted key %X from module %s\n", keyBytes, moduleName)

	return nil
}

func decodeKeyValue(keyFlag, valueFlag string, hexMode bool) ([]byte, []byte, error) {
	if !hexMode {
		return []byte(keyFlag), []byte(valueFlag), nil
	}
	keyBytes, err := hex.DecodeString(keyFlag)
	if err != nil {
		return nil, nil, fmt.Errorf("decode key: %w", err)
	}
	valueBytes, err := hex.DecodeString(valueFlag)
	if err != nil {
		return nil, nil, fmt.Errorf("decode value: %w", err)
	}
	return keyBytes, valueBytes, nil
}

func applyAndCommit(db *memiavl.DB, module string, pairs []*iavl.KVPair) error {
	if err := db.ApplyChangeSet(module, iavl.ChangeSet{Pairs: pairs}); err != nil {
		return err
	}
	_, err := db.Commit()
	return err
}
