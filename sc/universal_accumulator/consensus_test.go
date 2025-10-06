package universalaccumulator

import (
	"encoding/hex"
	"testing"
)

// TestConsensusRootHash tests that different nodes produce the same root hash.
func TestConsensusRootHash(t *testing.T) {
	t.Log("Testing consensus: multiple nodes should produce identical root hashes")

	// Create two separate accumulator instances (simulating different nodes)
	node1, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create node1 accumulator: %v", err)
	}
	defer node1.Close()

	node2, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create node2 accumulator: %v", err)
	}
	defer node2.Close()

	// Test 1: Empty state should produce same root
	t.Run("EmptyState", func(t *testing.T) {
		root1, err := node1.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate root for node1: %v", err)
		}

		root2, err := node2.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate root for node2: %v", err)
		}

		if hex.EncodeToString(root1) != hex.EncodeToString(root2) {
			t.Errorf("Empty state roots differ:\n  Node1: %s\n  Node2: %s",
				hex.EncodeToString(root1), hex.EncodeToString(root2))
		} else {
			t.Logf("Empty state root matches: %s", hex.EncodeToString(root1))
		}
	})

	// Test 2: Same data should produce same root
	t.Run("SameData", func(t *testing.T) {
		// Add identical data to both nodes
		testData := []AccumulatorKVPair{
			{Key: []byte("account:alice"), Value: []byte("balance:1000"), Deleted: false},
			{Key: []byte("account:bob"), Value: []byte("balance:2000"), Deleted: false},
			{Key: []byte("contract:token"), Value: []byte("supply:10000"), Deleted: false},
		}

		// Add to node1
		err = node1.AddEntries(testData)
		if err != nil {
			t.Fatalf("Failed to add entries to node1: %v", err)
		}

		// Add to node2
		err = node2.AddEntries(testData)
		if err != nil {
			t.Fatalf("Failed to add entries to node2: %v", err)
		}

		// Calculate roots
		root1, err := node1.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate root for node1: %v", err)
		}

		root2, err := node2.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate root for node2: %v", err)
		}

		if hex.EncodeToString(root1) != hex.EncodeToString(root2) {
			t.Errorf("Same data roots differ:\n  Node1: %s\n  Node2: %s",
				hex.EncodeToString(root1), hex.EncodeToString(root2))
		} else {
			t.Logf("Same data root matches: %s", hex.EncodeToString(root1))
		}
	})

	// Test 3: Different order should produce same root (order independence)
	t.Run("DifferentOrder", func(t *testing.T) {
		// Create fresh nodes for this test
		nodeA, err := NewUniversalAccumulator(10)
		if err != nil {
			t.Fatalf("Failed to create nodeA: %v", err)
		}
		defer nodeA.Close()

		nodeB, err := NewUniversalAccumulator(10)
		if err != nil {
			t.Fatalf("Failed to create nodeB: %v", err)
		}
		defer nodeB.Close()

		// Add data in different orders
		dataSet1 := []AccumulatorKVPair{
			{Key: []byte("key1"), Value: []byte("value1"), Deleted: false},
			{Key: []byte("key2"), Value: []byte("value2"), Deleted: false},
			{Key: []byte("key3"), Value: []byte("value3"), Deleted: false},
		}

		dataSet2 := []AccumulatorKVPair{
			{Key: []byte("key3"), Value: []byte("value3"), Deleted: false},
			{Key: []byte("key1"), Value: []byte("value1"), Deleted: false},
			{Key: []byte("key2"), Value: []byte("value2"), Deleted: false},
		}

		err = nodeA.AddEntries(dataSet1)
		if err != nil {
			t.Fatalf("Failed to add entries to nodeA: %v", err)
		}

		err = nodeB.AddEntries(dataSet2)
		if err != nil {
			t.Fatalf("Failed to add entries to nodeB: %v", err)
		}

		rootA, err := nodeA.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate root for nodeA: %v", err)
		}

		rootB, err := nodeB.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate root for nodeB: %v", err)
		}

		if hex.EncodeToString(rootA) != hex.EncodeToString(rootB) {
			t.Errorf("Different order roots differ:\n  NodeA: %s\n  NodeB: %s",
				hex.EncodeToString(rootA), hex.EncodeToString(rootB))
		} else {
			t.Logf("Different order root matches: %s", hex.EncodeToString(rootA))
		}
	})

	// Test 4: Block-by-block processing should produce same result
	t.Run("BlockByBlock", func(t *testing.T) {
		// Create fresh nodes for this test
		nodeX, err := NewUniversalAccumulator(10)
		if err != nil {
			t.Fatalf("Failed to create nodeX: %v", err)
		}
		defer nodeX.Close()

		nodeY, err := NewUniversalAccumulator(10)
		if err != nil {
			t.Fatalf("Failed to create nodeY: %v", err)
		}
		defer nodeY.Close()

		// Process 5 blocks on both nodes
		for blockHeight := uint64(1); blockHeight <= 5; blockHeight++ {
			blockData := []AccumulatorKVPair{
				{
					Key:     []byte("block_" + string(rune(blockHeight))),
					Value:   []byte("data_" + string(rune(blockHeight))),
					Deleted: false,
				},
			}

			// Process on nodeX
			changesetX := AccumulatorChangeset{
				Version: blockHeight,
				Entries: blockData,
				Name:    "block_" + string(rune(blockHeight)),
			}
			err = nodeX.ApplyChangeset(changesetX)
			if err != nil {
				t.Fatalf("Failed to apply changeset to nodeX at height %d: %v", blockHeight, err)
			}

			// Process on nodeY
			changesetY := AccumulatorChangeset{
				Version: blockHeight,
				Entries: blockData,
				Name:    "block_" + string(rune(blockHeight)),
			}
			err = nodeY.ApplyChangeset(changesetY)
			if err != nil {
				t.Fatalf("Failed to apply changeset to nodeY at height %d: %v", blockHeight, err)
			}

			// Check roots match at each height
			rootX, err := nodeX.CalculateRoot()
			if err != nil {
				t.Fatalf("Failed to calculate root for nodeX at height %d: %v", blockHeight, err)
			}

			rootY, err := nodeY.CalculateRoot()
			if err != nil {
				t.Fatalf("Failed to calculate root for nodeY at height %d: %v", blockHeight, err)
			}

			if hex.EncodeToString(rootX) != hex.EncodeToString(rootY) {
				t.Errorf("Block %d roots differ:\n  NodeX: %s\n  NodeY: %s",
					blockHeight, hex.EncodeToString(rootX), hex.EncodeToString(rootY))
			} else {
				t.Logf("Block %d root matches: %s", blockHeight, hex.EncodeToString(rootX))
			}
		}
	})
}

// TestDeterministicInitialization tests that the fixed seed produces deterministic results.
func TestDeterministicInitialization(t *testing.T) {
	t.Log("Testing deterministic initialization with fixed seed")

	// Create multiple accumulator instances
	accumulators := make([]*UniversalAccumulator, 3)
	for i := range 3 {
		acc, err := NewUniversalAccumulator(10)
		if err != nil {
			t.Fatalf("Failed to create accumulator %d: %v", i, err)
		}
		defer acc.Close()
		accumulators[i] = acc
	}

	// All should have the same initial state
	var initialRoots [][]byte
	for i, acc := range accumulators {
		root, err := acc.CalculateRoot()
		if err != nil {
			t.Fatalf("Failed to calculate initial root for accumulator %d: %v", i, err)
		}
		initialRoots = append(initialRoots, root)
	}

	// Check all initial roots are identical
	for i := 1; i < len(initialRoots); i++ {
		if hex.EncodeToString(initialRoots[0]) != hex.EncodeToString(initialRoots[i]) {
			t.Errorf("Initial roots differ between accumulator 0 and %d:\n  Acc0: %s\n  Acc%d: %s",
				i, hex.EncodeToString(initialRoots[0]), i, hex.EncodeToString(initialRoots[i]))
		}
	}

	t.Logf("All %d accumulators have identical initial root: %s",
		len(accumulators), hex.EncodeToString(initialRoots[0]))
}

// TestWitnessCompatibility tests that witnesses generated by one node can be verified by another.
func TestWitnessCompatibility(t *testing.T) {
	t.Log("Testing witness compatibility between nodes")

	// Create two nodes
	producer, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create producer node: %v", err)
	}
	defer producer.Close()

	verifier, err := NewUniversalAccumulator(10)
	if err != nil {
		t.Fatalf("Failed to create verifier node: %v", err)
	}
	defer verifier.Close()

	// Add same data to both nodes
	testData := []AccumulatorKVPair{
		{Key: []byte("account:alice"), Value: []byte("balance:1000"), Deleted: false},
		{Key: []byte("account:bob"), Value: []byte("balance:2000"), Deleted: false},
	}

	err = producer.AddEntries(testData)
	if err != nil {
		t.Fatalf("Failed to add entries to producer: %v", err)
	}

	err = verifier.AddEntries(testData)
	if err != nil {
		t.Fatalf("Failed to add entries to verifier: %v", err)
	}

	// Producer generates witness
	witness, err := producer.IssueWitness([]byte("account:alice"), []byte("balance:1000"), true)
	if err != nil {
		t.Fatalf("Failed to generate witness: %v", err)
	}
	defer witness.Free()

	// Verifier should be able to verify the witness
	isValid := verifier.VerifyWitness(witness)
	if !isValid {
		t.Error("Witness generated by producer should be valid on verifier")
	} else {
		t.Log("Witness generated by producer is valid on verifier")
	}

	// Check that both nodes have the same root
	producerRoot, _ := producer.CalculateRoot()
	verifierRoot, _ := verifier.CalculateRoot()

	if hex.EncodeToString(producerRoot) != hex.EncodeToString(verifierRoot) {
		t.Errorf("Producer and verifier have different roots:\n  Producer: %s\n  Verifier: %s",
			hex.EncodeToString(producerRoot), hex.EncodeToString(verifierRoot))
	} else {
		t.Logf("Producer and verifier have same root: %s", hex.EncodeToString(producerRoot))
	}
}
