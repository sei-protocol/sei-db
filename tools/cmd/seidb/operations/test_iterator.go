package operations

import (
	"encoding/hex"
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
	iteratorCmd.PersistentFlags().StringP("start", "s", "07", "Start key")
	iteratorCmd.PersistentFlags().StringP("end", "e", "08", "End key")

	return iteratorCmd
}

func executeIterator(cmd *cobra.Command, _ []string) {
	homeDir, _ := cmd.Flags().GetString("home-dir")
	start, _ := cmd.Flags().GetString("start")
	end, _ := cmd.Flags().GetString("end")
	IterateDbData(homeDir, start, end)
}

func IterateDbData(homeDir string, start string, end string) {
	ssConfig := config.DefaultStateStoreConfig()
	ssConfig.KeepRecent = 0
	ssStore, err := ss.NewStateStore(logger.NewNopLogger(), homeDir, ssConfig)
	defer ssStore.Close()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Start forward iteration\n")
	forwardIter, err := ssStore.Iterator("oracle", 98350313, nil, nil)
	for ; forwardIter.Valid(); forwardIter.Next() {
		fmt.Printf("key: %X, value %X\n", forwardIter.Key(), forwardIter.Value())
	}
	forwardIter.Close()
	fmt.Printf("Finished forward iteration\n")
	fmt.Printf("Start reverse iteration\n")
	startPos, _ := hex.DecodeString(start)
	endPos, _ := hex.DecodeString(end)
	iter, err := ssStore.ReverseIterator("oracle", 98350313, startPos, endPos)
	if err != nil {
		panic(err)
	}
	for ; iter.Valid(); iter.Next() {
		fmt.Printf("key: %X, value %X\n", iter.Key(), iter.Value())
	}
	iter.Close()
	fmt.Printf("Complete reverse iteration\n")
}
