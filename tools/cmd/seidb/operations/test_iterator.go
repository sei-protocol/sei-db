package operations

import (
	"fmt"

	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/config"
	"github.com/sei-protocol/sei-db/ss"
	"github.com/spf13/cobra"
)

func TestIteratorCmd() *cobra.Command {
	iteratorCmd := &cobra.Command{
		Use:   "test-iterator",
		Short: "Test forward or reverse iterator",
		Run:   executeIterator,
	}

	iteratorCmd.PersistentFlags().StringP("home-dir", "d", "/root/.sei", "Database Directory")
	return iteratorCmd
}

func executeIterator(cmd *cobra.Command, _ []string) {
	homeDir, _ := cmd.Flags().GetString("home-dir")
	IterateDbData(homeDir)
}

func IterateDbData(homeDir string) {
	ssConfig := config.DefaultStateStoreConfig()
	ssConfig.KeepRecent = 0
	ssStore, err := ss.NewStateStore(logger.NewNopLogger(), homeDir, ssConfig)
	defer ssStore.Close()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Start iteration")
	iter, err := ssStore.ReverseIterator("oracle", 98350313, []byte("07"), []byte("08"))
	if err != nil {
		panic(err)
	}
	for ; iter.Valid(); iter.Next() {
		fmt.Printf("key: %X, value %X\n", iter.Key(), iter.Value())
	}
	fmt.Printf("Complete iteration")
}
