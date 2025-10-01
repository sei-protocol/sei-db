package parquet

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/config"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/stretchr/testify/require"
)

func TestOptimizedQuery(t *testing.T) {
	// Create test database
	db, err := New(t.TempDir(), config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()

	// Prepare test data
	storeKey := "testStore"
	version := int64(100)
	
	// Insert test data with different owners
	testData := []struct {
		key   []byte
		value []byte
		owner string
	}{
		{[]byte("account1_key1"), []byte("value1"), "account1"},
		{[]byte("account1_key2"), []byte("value2"), "account1"},
		{[]byte("account2_key1"), []byte("value3"), "account2"},
		{[]byte("account2_key2"), []byte("value4"), "account2"},
	}
	
	for _, td := range testData {
		cs := &proto.NamedChangeSet{
			Name: storeKey,
			Changeset: iavl.ChangeSet{
				Pairs: []*iavl.KVPair{
					{
						Key:    td.key,
						Value:  td.value,
						Delete: false,
					},
				},
			},
		}
		err := db.ApplyChangeset(version, cs)
		require.NoError(t, err)
	}
	
	// Flush buffer to ensure data is written
	err = db.flushWriteBuffer()
	require.NoError(t, err)
	
	// Test 1: Query by owner
	records, err := db.OptimizedQuery(storeKey, version, "account1", QueryOptions{
		MaxWorkers: 2,
	})
	require.NoError(t, err)
	require.Len(t, records, 2)
	
	// Test 2: Query with key prefix
	records, err = db.OptimizedQuery(storeKey, version, "account1", QueryOptions{
		KeyPrefix: []byte("account1_key1"),
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.True(t, bytes.Equal(records[0].Key, []byte("account1_key1")))
	
	// Test 3: Query with value filter
	records, err = db.OptimizedQuery(storeKey, version, "account2", QueryOptions{
		ValueFilter: func(val []byte) bool {
			return bytes.Contains(val, []byte("3"))
		},
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.True(t, bytes.Equal(records[0].Value, []byte("value3")))
}

func TestGetAccountHistory(t *testing.T) {
	// Create test database
	db, err := New(t.TempDir(), config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()
	
	storeKey := "testStore"
	owner := "account1"
	key := []byte(fmt.Sprintf("%s_key", owner))
	
	// Insert data at multiple versions
	for version := int64(1); version <= 5; version++ {
		value := []byte(fmt.Sprintf("value_v%d", version))
		
		cs := &proto.NamedChangeSet{
			Name: storeKey,
			Changeset: iavl.ChangeSet{
				Pairs: []*iavl.KVPair{
					{
						Key:    key,
						Value:  value,
						Delete: false,
					},
				},
			},
		}
		err := db.ApplyChangeset(version, cs)
		require.NoError(t, err)
	}
	
	// Flush buffer
	err = db.flushWriteBuffer()
	require.NoError(t, err)
	
	// Get account history
	history, err := db.GetAccountHistory(storeKey, owner, 2, 4)
	require.NoError(t, err)
	require.Len(t, history, 3) // versions 2, 3, 4
	
	// Verify history is ordered by version
	for i := 0; i < len(history)-1; i++ {
		require.True(t, history[i].Version <= history[i+1].Version)
	}
}

func TestBulkQuery(t *testing.T) {
	// Create test database
	db, err := New(t.TempDir(), config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()
	
	// Insert test data
	stores := []string{"store1", "store2"}
	owners := []string{"owner1", "owner2"}
	versions := []int64{1, 2, 3}
	
	for _, store := range stores {
		for _, owner := range owners {
			for _, version := range versions {
				key := []byte(fmt.Sprintf("%s_%s_key", store, owner))
				value := []byte(fmt.Sprintf("%s_%s_v%d", store, owner, version))
				
				cs := &proto.NamedChangeSet{
					Name: store,
					Changeset: iavl.ChangeSet{
						Pairs: []*iavl.KVPair{
							{
								Key:    key,
								Value:  value,
								Delete: false,
							},
						},
					},
				}
				err := db.ApplyChangeset(version, cs)
				require.NoError(t, err)
			}
		}
	}
	
	// Flush buffer
	err = db.flushWriteBuffer()
	require.NoError(t, err)
	
	// Perform bulk query
	queries := []struct {
		StoreKey string
		Version  int64
		Owner    string
		Options  QueryOptions
	}{
		{StoreKey: "store1", Version: 2, Owner: "owner1", Options: QueryOptions{}},
		{StoreKey: "store2", Version: 3, Owner: "owner2", Options: QueryOptions{}},
	}
	
	results, err := db.BulkQuery(queries)
	require.NoError(t, err)
	require.Len(t, results, 2)
	
	// Verify results
	key1 := fmt.Sprintf("%s:%d:%s", "store1", 2, "owner1")
	require.Contains(t, results, key1)
	require.Len(t, results[key1], 1)
	
	key2 := fmt.Sprintf("%s:%d:%s", "store2", 3, "owner2")
	require.Contains(t, results, key2)
	require.Len(t, results[key2], 1)
}