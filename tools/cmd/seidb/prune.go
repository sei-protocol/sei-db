package main

import (
	"fmt"

	"github.com/sei-protocol/sei-db/ss"
	"github.com/spf13/cobra"
)

func PruneCmd() *cobra.Command {
	pruneDbCmd := &cobra.Command{
		Use:   "prune",
		Short: "Prune a db at a given height",
		Run:   prune,
	}

	pruneDbCmd.PersistentFlags().StringP("db-dir", "d", "", "Database Directory")
	pruneDbCmd.PersistentFlags().StringP("db-backend", "b", "", "DB Backend")
	pruneDbCmd.PersistentFlags().Int64P("version", "v", 0, "Version to prune at")

	return pruneDbCmd
}

func prune(cmd *cobra.Command, _ []string) {
	dbDir, _ := cmd.Flags().GetString("db-dir")
	dbBackend, _ := cmd.Flags().GetString("db-backend")
	version, _ := cmd.Flags().GetInt64("version")

	if dbDir == "" {
		panic("Must provide database dir")
	}

	if dbBackend == "" {
		panic("Must provide db backend")
	}

	_, isAcceptedBackend := ValidDBBackends[dbBackend]
	if !isAcceptedBackend {
		panic(fmt.Sprintf("Unsupported db backend: %s\n", dbBackend))
	}

	if version == 0 {
		panic("Must provide prune version")
	}

	PruneDB(dbBackend, dbDir, version)
}

// Prunes DB at given height
func PruneDB(dbBackend string, dbDir string, version int64) {
	// TODO: Defer Close Db
	backend, err := ss.NewStateStoreDB(dbDir, dbBackend)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Pruning %s db at path %s at height %d...\n", dbBackend, dbDir, version)

	// Callback to write db entries to file
	err = backend.Prune(version)
	if err != nil {
		panic(err)
	}
}
