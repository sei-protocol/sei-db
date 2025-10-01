package parquet_test

import (
	"fmt"
	"log"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/config"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/ss/parquet"
)

func Example_debugTraceQuery() {
	// Create a new Parquet database
	db, err := parquet.New("/tmp/parquet-db", config.StateStoreConfig{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	// Simulate writing data for different accounts at different heights
	accounts := []string{"account1", "account2", "account3"}
	
	for height := int64(100); height <= 105; height++ {
		for _, account := range accounts {
			// Create sample data
			key := []byte(fmt.Sprintf("%s_balance", account))
			value := []byte(fmt.Sprintf("%d", height*1000))
			
			cs := &proto.NamedChangeSet{
				Name: "bank",
				Changeset: iavl.ChangeSet{
					Pairs: []*iavl.KVPair{
						{
							Key:   key,
							Value: value,
							Delete: false,
						},
					},
				},
			}
			
			if err := db.ApplyChangeset(height, cs); err != nil {
				log.Fatal(err)
			}
		}
	}
	
	// Flush to ensure data is written
	db.Close()
	db, _ = parquet.New("/tmp/parquet-db", config.StateStoreConfig{})
	
	// Example 1: Debug trace query for specific account at specific height
	// This is optimized to read only the specific partition
	records, err := db.OptimizedQuery("bank", 103, "account1", parquet.QueryOptions{
		MaxWorkers: 4,
	})
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("Account1 data at height 103:\n")
	for _, record := range records {
		fmt.Printf("  Key: %s, Value: %s\n", record.Key, record.Value)
	}
	
	// Example 2: Get account history across multiple heights
	// This efficiently reads from multiple version partitions for a single owner
	history, err := db.GetAccountHistory("bank", "account2", 101, 104)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("\nAccount2 history from height 101-104:\n")
	for _, record := range history {
		fmt.Printf("  Height %d: Key: %s, Value: %s\n", record.Version, record.Key, record.Value)
	}
	
	// Example 3: Bulk query for multiple accounts at different heights
	// This demonstrates parallel query execution
	queries := []struct {
		StoreKey string
		Version  int64
		Owner    string
		Options  parquet.QueryOptions
	}{
		{StoreKey: "bank", Version: 102, Owner: "account1", Options: parquet.QueryOptions{}},
		{StoreKey: "bank", Version: 104, Owner: "account3", Options: parquet.QueryOptions{}},
	}
	
	results, err := db.BulkQuery(queries)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("\nBulk query results:\n")
	for key, records := range results {
		fmt.Printf("  Query %s: %d records\n", key, len(records))
	}
	
	// Output:
	// Account1 data at height 103:
	//   Key: account1_balance, Value: 103000
	//
	// Account2 history from height 101-104:
	//   Height 101: Key: account2_balance, Value: 101000
	//   Height 102: Key: account2_balance, Value: 102000
	//   Height 103: Key: account2_balance, Value: 103000
	//   Height 104: Key: account2_balance, Value: 104000
	//
	// Bulk query results:
	//   Query bank:102:account1: 1 records
	//   Query bank:104:account3: 1 records
}