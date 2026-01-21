package pebbledb

import (
	"fmt"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/sei-protocol/sei-db/config"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	db, err := New(t.TempDir(), config.DefaultStateStoreConfig())
	require.NoError(t, err)
	db.ApplyChangeset(1, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/a"),
					Value: []byte("a1"),
				},
			},
		},
	})
	db.ApplyChangeset(2, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/a"),
					Value: []byte("a2"),
				},
				{
					Key:   []byte("s/k:evm/b"),
					Value: []byte("b2"),
				},
			},
		},
	})
	db.ApplyChangeset(3, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Delete: true,
					Key:    []byte("s/k:evm/a"),
				},
				{
					Key:   []byte("s/k:evm/b"),
					Value: []byte("b3"),
				},
			},
		},
	})
	db.ApplyChangeset(4, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/a"),
					Value: []byte("a4"),
				},
				{
					Delete: true,
					Key:    []byte("s/k:evm/b"),
				},
			},
		},
	})
	iter, err := db.Iterator("evm", 1, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("a1"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())

	iter, err = db.Iterator("evm", 2, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("a2"), iter.Value())
	iter.Next()
	require.True(t, iter.Valid())
	require.Equal(t, []byte("b2"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())

	iter, err = db.Iterator("evm", 3, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("b3"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())

	iter, err = db.Iterator("evm", 4, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("a4"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
}

func TestReverseIterator(t *testing.T) {
	db, err := New(t.TempDir(), config.DefaultStateStoreConfig())
	require.NoError(t, err)
	db.ApplyChangeset(1, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/a"),
					Value: []byte("a1"),
				},
			},
		},
	})
	db.ApplyChangeset(2, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/a"),
					Value: []byte("a2"),
				},
				{
					Key:   []byte("s/k:evm/b"),
					Value: []byte("b2"),
				},
			},
		},
	})
	db.ApplyChangeset(3, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Delete: true,
					Key:    []byte("s/k:evm/a"),
				},
				{
					Key:   []byte("s/k:evm/b"),
					Value: []byte("b3"),
				},
			},
		},
	})
	db.ApplyChangeset(4, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/a"),
					Value: []byte("a4"),
				},
				{
					Delete: true,
					Key:    []byte("s/k:evm/b"),
				},
			},
		},
	})
	iter, err := db.ReverseIterator("evm", 1, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("a1"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())

	iter, err = db.ReverseIterator("evm", 2, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("b2"), iter.Value())
	iter.Next()
	require.True(t, iter.Valid())
	require.Equal(t, []byte("a2"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())

	iter, err = db.ReverseIterator("evm", 3, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("b3"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())

	iter, err = db.ReverseIterator("evm", 4, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("a4"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
}

func TestIteratorEdgeCases(t *testing.T) {
	db, err := New(t.TempDir(), config.DefaultStateStoreConfig())
	require.NoError(t, err)

	// Test case 1: Empty database
	iter, err := db.Iterator("evm", 1, nil, nil)
	require.NoError(t, err)
	require.False(t, iter.Valid())
	iter.Close()

	// Test case 2: Single key with multiple versions
	db.ApplyChangeset(1, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/key1"),
					Value: []byte("v1"),
				},
			},
		},
	})
	db.ApplyChangeset(2, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/key1"),
					Value: []byte("v2"),
				},
			},
		},
	})

	// Test forward iteration
	iter, err = db.Iterator("evm", 2, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("v2"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
	iter.Close()

	// Test reverse iteration
	iter, err = db.ReverseIterator("evm", 2, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("v2"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
	iter.Close()
}

func TestIteratorWithDeletes(t *testing.T) {
	db, err := New(t.TempDir(), config.DefaultStateStoreConfig())
	require.NoError(t, err)

	// Test case: Keys with deletes and reinserts
	db.ApplyChangeset(1, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/key1"),
					Value: []byte("v1"),
				},
				{
					Key:   []byte("s/k:evm/key2"),
					Value: []byte("v2"),
				},
			},
		},
	})

	// Delete key1 and update key2
	db.ApplyChangeset(2, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Delete: true,
					Key:    []byte("s/k:evm/key1"),
				},
				{
					Key:   []byte("s/k:evm/key2"),
					Value: []byte("v2-updated"),
				},
			},
		},
	})

	// Reinsert key1 and delete key2
	db.ApplyChangeset(3, &proto.NamedChangeSet{
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{
					Key:   []byte("s/k:evm/key1"),
					Value: []byte("v1-new"),
				},
				{
					Delete: true,
					Key:    []byte("s/k:evm/key2"),
				},
			},
		},
	})

	// Test forward iteration at version 2
	iter, err := db.Iterator("evm", 2, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("v2-updated"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
	iter.Close()

	// Test reverse iteration at version 2
	iter, err = db.ReverseIterator("evm", 2, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("v2-updated"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
	iter.Close()

	// Test forward iteration at version 3
	iter, err = db.Iterator("evm", 3, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("v1-new"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
	iter.Close()

	// Test reverse iteration at version 3
	iter, err = db.ReverseIterator("evm", 3, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("v1-new"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
	iter.Close()
}

func TestIteratorWithRange(t *testing.T) {
	db, err := New(t.TempDir(), config.DefaultStateStoreConfig())
	require.NoError(t, err)

	// Insert keys in a specific order
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for i, key := range keys {
		db.ApplyChangeset(int64(i+1), &proto.NamedChangeSet{
			Changeset: iavl.ChangeSet{
				Pairs: []*iavl.KVPair{
					{
						Key:   []byte("s/k:evm/" + key),
						Value: []byte("v" + key),
					},
				},
			},
		})
	}

	// Test forward iteration with range
	iter, err := db.Iterator("evm", 5, []byte("key2"), []byte("key4"))
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("vkey2"), iter.Value())
	iter.Next()
	require.True(t, iter.Valid())
	require.Equal(t, []byte("vkey3"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
	iter.Close()

	// Test reverse iteration with range
	iter, err = db.ReverseIterator("evm", 5, []byte("key2"), []byte("key4"))
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("vkey3"), iter.Value())
	iter.Next()
	require.True(t, iter.Valid())
	require.Equal(t, []byte("vkey2"), iter.Value())
	iter.Next()
	require.False(t, iter.Valid())
	iter.Close()
}

func TestIteratorWithPruning(t *testing.T) {
	db, err := New(t.TempDir(), config.DefaultStateStoreConfig())
	require.NoError(t, err)

	// Insert keys with versions
	for i := 1; i <= 10; i++ {
		db.ApplyChangeset(int64(i), &proto.NamedChangeSet{
			Changeset: iavl.ChangeSet{
				Pairs: []*iavl.KVPair{
					{
						Key:   []byte("s/k:evm/key1"),
						Value: []byte(fmt.Sprintf("v%d", i)),
					},
				},
			},
		})
	}

	// Prune versions 1-5
	err = db.Prune(5)
	require.NoError(t, err)

	// Test iteration at version 3 (should be invalid as it's pruned)
	iter, err := db.Iterator("evm", 3, nil, nil)
	require.NoError(t, err)
	require.False(t, iter.Valid())
	iter.Close()

	// Test iteration at version 7 (should be valid)
	iter, err = db.Iterator("evm", 7, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("v7"), iter.Value())
	iter.Close()

	// Test reverse iteration at version 7
	iter, err = db.ReverseIterator("evm", 7, nil, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, []byte("v7"), iter.Value())
	iter.Close()
}
