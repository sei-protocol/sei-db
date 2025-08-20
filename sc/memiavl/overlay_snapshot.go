package memiavl

import (
	"bytes"
	"fmt"

	"github.com/sei-protocol/sei-db/sc/types"
)

// OverlaySnapshot implements fast restart by overlaying incremental changes
// on top of a memory-mapped base snapshot
type OverlaySnapshot struct {
	baseSnapshot  *Snapshot
	modifications map[string]*types.SnapshotNode
	version       uint32
	rootHash      []byte
}

// NewOverlaySnapshot creates a new overlay snapshot from a base snapshot and incremental snapshots
func NewOverlaySnapshot(baseSnapshot *Snapshot, incrementalSnapshots []*Snapshot) (*OverlaySnapshot, error) {
	if len(incrementalSnapshots) == 0 {
		return nil, fmt.Errorf("no incremental snapshots provided")
	}

	latestSnapshot := incrementalSnapshots[len(incrementalSnapshots)-1]
	version := latestSnapshot.Version()

	// Create a tree from the latest incremental snapshot
	latestTree := NewFromSnapshot(latestSnapshot, true, 0)

	// Extract modifications by comparing with base snapshot
	modifications := make(map[string]*types.SnapshotNode)

	if !baseSnapshot.IsEmpty() {
		baseTree := NewFromSnapshot(baseSnapshot, true, 0)

		// Export all nodes from latest tree and check if they differ from base
		exporter := latestTree.Export()
		for {
			snapshotNode, err := exporter.Next()
			if err != nil {
				break // End of export
			}

			// Only process leaf nodes (those with values)
			if snapshotNode.Value == nil {
				continue
			}

			// Check if this key exists in base tree and if value is different
			baseValue := baseTree.Get(snapshotNode.Key)
			key := string(snapshotNode.Key)

			if baseValue == nil {
				// This is a new key
				modifications[key] = snapshotNode
			} else if !bytes.Equal(baseValue, snapshotNode.Value) {
				// This is an updated value
				modifications[key] = snapshotNode
			}
		}
		exporter.Close()

		// Check for deletions (keys in base but not in latest)
		baseExporter := baseTree.Export()
		for {
			baseNode, err := baseExporter.Next()
			if err != nil {
				break
			}

			latestValue := latestTree.Get(baseNode.Key)
			if latestValue == nil {
				// This key was deleted (only if it's not already marked as a new key)
				key := string(baseNode.Key)
				if _, exists := modifications[key]; !exists {
					modifications[key] = &types.SnapshotNode{
						Key:   baseNode.Key,
						Value: nil, // nil indicates deletion
					}
				}
			}
		}
		baseExporter.Close()
	} else {
		// Base is empty, all nodes in latest are modifications
		exporter := latestTree.Export()
		for {
			snapshotNode, err := exporter.Next()
			if err != nil {
				break
			}
			// Only process leaf nodes (those with values)
			if snapshotNode.Value == nil {
				continue
			}
			key := string(snapshotNode.Key)
			modifications[key] = snapshotNode
		}
		exporter.Close()
	}

	// Calculate root hash
	rootHash := latestTree.RootHash()

	return &OverlaySnapshot{
		baseSnapshot:  baseSnapshot,
		modifications: modifications,
		version:       version,
		rootHash:      rootHash,
	}, nil
}

// Implement SnapshotInterface methods

func (os *OverlaySnapshot) Close() error {
	// Base snapshot will be closed by its owner
	// We don't own the base snapshot, so we don't close it
	return nil
}

func (os *OverlaySnapshot) IsEmpty() bool {
	return os.baseSnapshot.IsEmpty() && len(os.modifications) == 0
}

func (os *OverlaySnapshot) Version() uint32 {
	return os.version
}

func (os *OverlaySnapshot) RootHash() []byte {
	return os.rootHash
}

// Get retrieves a value by key, checking overlay first then base snapshot
func (os *OverlaySnapshot) Get(key []byte) ([]byte, error) {
	// Check modifications first (most recent changes)
	if node, exists := os.modifications[string(key)]; exists {
		if node.Value == nil {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return node.Value, nil
	}

	// Fall back to base snapshot
	if os.baseSnapshot.IsEmpty() {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	return os.getFromBaseSnapshot(key)
}

// getFromBaseSnapshot retrieves a value from the base snapshot
func (os *OverlaySnapshot) getFromBaseSnapshot(key []byte) ([]byte, error) {
	if os.baseSnapshot.IsEmpty() {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Create a temporary tree from base snapshot for lookup
	baseTree := NewFromSnapshot(os.baseSnapshot, true, 0)
	value := baseTree.Get(key)
	if value == nil {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return value, nil
}

// Key retrieves a key by offset (for compatibility with SnapshotInterface)
func (os *OverlaySnapshot) Key(offset uint64) []byte {
	// Not implemented for overlay approach - mainly used for iteration
	return nil
}

// KeyValue retrieves a key-value pair by offset (for compatibility with SnapshotInterface)
func (os *OverlaySnapshot) KeyValue(offset uint64) ([]byte, []byte) {
	// Not implemented for overlay approach - mainly used for iteration
	return nil, nil
}

// LeafKey retrieves a leaf key by index (for compatibility with SnapshotInterface)
func (os *OverlaySnapshot) LeafKey(index uint32) []byte {
	// Not implemented for overlay approach - mainly used for iteration
	return nil
}

// LeafKeyValue retrieves a leaf key-value pair by index (for compatibility with SnapshotInterface)
func (os *OverlaySnapshot) LeafKeyValue(index uint32) ([]byte, []byte) {
	// Not implemented for overlay approach - mainly used for iteration
	return nil, nil
}

// Export creates an exporter for the overlay snapshot
func (os *OverlaySnapshot) Export() *Exporter {
	return newExporter(os.export)
}

// export is the internal export function
func (os *OverlaySnapshot) export(callback func(*types.SnapshotNode) bool) {
	// Create a complete tree by applying modifications to base
	if os.baseSnapshot.IsEmpty() {
		// Export only modifications
		for _, node := range os.modifications {
			if !callback(node) {
				return
			}
		}
		return
	}

	// Create a tree from base snapshot and apply modifications
	baseTree := NewFromSnapshot(os.baseSnapshot, true, 0)

	// Apply all modifications
	for _, node := range os.modifications {
		if node.Value == nil {
			baseTree.Remove(node.Key)
		} else {
			baseTree.Set(node.Key, node.Value)
		}
	}

	// Export the modified tree using the tree's export method
	exporter := baseTree.Export()
	defer exporter.Close()

	for {
		node, err := exporter.Next()
		if err != nil {
			break
		}
		if !callback(node) {
			return
		}
	}
}

// Internal methods needed by PersistedNode (simplified implementations)

func (os *OverlaySnapshot) nodesLen() int {
	// Not implemented for overlay approach - mainly used for internal node access
	return 0
}

func (os *OverlaySnapshot) leavesLen() int {
	// Not implemented for overlay approach - mainly used for internal node access
	return 0
}

func (os *OverlaySnapshot) getNodesLayout() Nodes {
	// Not implemented for overlay approach - mainly used for internal node access
	return Nodes{}
}

func (os *OverlaySnapshot) getLeavesLayout() Leaves {
	// Not implemented for overlay approach - mainly used for internal node access
	return Leaves{}
}

func (os *OverlaySnapshot) getNodes() []byte {
	// Not implemented for overlay approach - mainly used for internal node access
	return nil
}

func (os *OverlaySnapshot) getLeaves() []byte {
	// Not implemented for overlay approach - mainly used for internal node access
	return nil
}

func (os *OverlaySnapshot) getKvs() []byte {
	// Not implemented for overlay approach - mainly used for internal node access
	return nil
}

// GetModificationCount returns the number of modifications in the overlay
func (os *OverlaySnapshot) GetModificationCount() int {
	return len(os.modifications)
}

// HasModification checks if a key has been modified
func (os *OverlaySnapshot) HasModification(key []byte) bool {
	_, exists := os.modifications[string(key)]
	return exists
}

// IsDeleted checks if a key has been deleted
func (os *OverlaySnapshot) IsDeleted(key []byte) bool {
	if node, exists := os.modifications[string(key)]; exists {
		return node.Value == nil
	}
	return false
}
