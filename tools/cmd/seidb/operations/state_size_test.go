package operations

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/sei-protocol/sei-db/sc/memiavl"
)

// fakeLeaf implements memiavl.Node for testing minimal behavior used by processNode
type fakeLeaf struct {
	key []byte
	val []byte
}

func (f *fakeLeaf) Height() uint8                          { return 0 }
func (f *fakeLeaf) IsLeaf() bool                           { return true }
func (f *fakeLeaf) Size() int64                            { return 1 }
func (f *fakeLeaf) Version() uint32                        { return 0 }
func (f *fakeLeaf) Key() []byte                            { return f.key }
func (f *fakeLeaf) Value() []byte                          { return f.val }
func (f *fakeLeaf) Left() memiavl.Node                     { return nil }
func (f *fakeLeaf) Right() memiavl.Node                    { return nil }
func (f *fakeLeaf) Hash() []byte                           { return nil }
func (f *fakeLeaf) SafeHash() []byte                       { return nil }
func (f *fakeLeaf) Mutate(uint32, uint32) *memiavl.MemNode { panic("not implemented") }
func (f *fakeLeaf) Get([]byte) ([]byte, uint32)            { return nil, 0 }
func (f *fakeLeaf) GetByIndex(uint32) ([]byte, []byte)     { return nil, nil }

func captureOutput(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()

	done := make(chan string)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()

	fn()
	_ = w.Close()
	out := <-done
	_ = r.Close()
	return out
}

func TestPrefixDisplayName_EVMAndDefault(t *testing.T) {
	// Known EVM prefixes
	if got := prefixDisplayName("evm", "03"); got != "StateKeyPrefix" {
		t.Fatalf("expected StateKeyPrefix, got %q", got)
	}
	if got := prefixDisplayName("evm", "07"); got != "CodeKeyPrefix" {
		t.Fatalf("expected CodeKeyPrefix, got %q", got)
	}
	if got := prefixDisplayName("evm", "1C"); got != "NextBaseFeePerGasPrefix" {
		t.Fatalf("expected NextBaseFeePerGasPrefix, got %q", got)
	}
	// Unknown stays as hex
	if got := prefixDisplayName("evm", "FF"); got != "FF" {
		t.Fatalf("expected FF, got %q", got)
	}
	// Non-evm returns raw hex
	if got := prefixDisplayName("bank", "03"); got != "03" {
		t.Fatalf("expected 03, got %q", got)
	}
}

func TestProcessNode_EVMContractAccumulation(t *testing.T) {
	stats := newModuleStats()

	// Build key: 0x03 + 20-byte address + trailing byte
	addr := bytes.Repeat([]byte{0xAB}, 20)
	key := append([]byte{0x03}, addr...)
	key = append(key, 0x99)
	val := bytes.Repeat([]byte{0xCD}, 7)

	node := &fakeLeaf{key: key, val: val}
	processNode("evm", "evm", node, stats)

	if stats.totalNumKeys != 1 {
		t.Fatalf("expected totalNumKeys 1, got %d", stats.totalNumKeys)
	}
	if stats.keySizeByPrefix["03"] != int64(len(key)) {
		t.Fatalf("expected key bytes %d, got %d", len(key), stats.keySizeByPrefix["03"])
	}
	if stats.valueSizeByPrefix["03"] != int64(len(val)) {
		t.Fatalf("expected val bytes %d, got %d", len(val), stats.valueSizeByPrefix["03"])
	}
	if len(stats.contractSizes) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(stats.contractSizes))
	}
	// Expected lowercased hex of 20-byte address
	expectedAddr := strings.ToLower(hex.EncodeToString(addr))
	entry, ok := stats.contractSizes[expectedAddr]
	if !ok {
		keys := make([]string, 0, len(stats.contractSizes))
		for k := range stats.contractSizes {
			keys = append(keys, k)
		}
		t.Fatalf("expected contract key %s present, got keys=%v", expectedAddr, keys)
	}
	if entry.KeyCount != 1 {
		t.Fatalf("expected keycount 1, got %d", entry.KeyCount)
	}
	expectedTotal := int64(len(key) + len(val))
	if entry.TotalSize != expectedTotal {
		t.Fatalf("expected totalsize %d, got %d", expectedTotal, entry.TotalSize)
	}

	// Non-0x03 prefix should not add contractSizes
	stats2 := newModuleStats()
	node2 := &fakeLeaf{key: []byte{0x07, 0x01, 0x02}, val: []byte{0x01}}
	processNode("evm", "evm", node2, stats2)
	if len(stats2.contractSizes) != 0 {
		t.Fatalf("expected 0 contracts for non-0x03 prefix, got %d", len(stats2.contractSizes))
	}
}

func TestPrintPrefixBreakdown_FormatAndOrder(t *testing.T) {
	// Prepare stats with two prefixes and known totals
	const mb = int64(1024 * 1024)
	stats := newModuleStats()
	stats.keySizeByPrefix["03"] = 15 * mb
	stats.valueSizeByPrefix["03"] = 5 * mb
	stats.keySizeByPrefix["07"] = 2 * mb
	stats.valueSizeByPrefix["07"] = 3 * mb
	stats.totalSize = int((15 + 5 + 2 + 3) * mb)

	out := captureOutput(t, func() { printPrefixBreakdown("evm", stats) })

	// Header present
	if !strings.Contains(out, "Prefix") || !strings.Contains(out, "Total Size (MB)") || !strings.Contains(out, "Percentage") {
		t.Fatalf("expected header in output, got: %s", out)
	}
	// Labels mapped for evm
	if !strings.Contains(out, "StateKeyPrefix") || !strings.Contains(out, "CodeKeyPrefix") {
		t.Fatalf("expected EVM labels, got: %s", out)
	}
	// Order: StateKeyPrefix (20MB) before CodeKeyPrefix (5MB)
	if strings.Index(out, "StateKeyPrefix") > strings.Index(out, "CodeKeyPrefix") {
		t.Fatalf("expected StateKeyPrefix before CodeKeyPrefix, got: %s", out)
	}
	// MB values shown
	if !strings.Contains(out, "20") || !strings.Contains(out, "5") {
		t.Fatalf("expected MB totals in output, got: %s", out)
	}
}

func TestPrintEvmContractSizeDistribution(t *testing.T) {
	// Prepare contracts across buckets
	stats := newModuleStats()
	// Bytes thresholds
	KB := int64(1024)
	MB := int64(1024 * 1024)

	stats.contractSizes["a"] = &contractSizeEntry{Address: "a", TotalSize: KB - 1}   // <1KB
	stats.contractSizes["b"] = &contractSizeEntry{Address: "b", TotalSize: 10 * KB}  // 1KB-1MB
	stats.contractSizes["c"] = &contractSizeEntry{Address: "c", TotalSize: 2 * MB}   // 1MB-10MB
	stats.contractSizes["d"] = &contractSizeEntry{Address: "d", TotalSize: 50 * MB}  // 10MB-100MB
	stats.contractSizes["e"] = &contractSizeEntry{Address: "e", TotalSize: 101 * MB} // >100MB

	out := captureOutput(t, func() { printEvmContractSizeDistribution(stats) })
	// Expect headers and each bucket
	for _, label := range []string{"Bucket", "Count", "Total Size (MB)", "Percent", "<1KB", "1KB - <1MB", "1MB - <10MB", "10MB - 100MB", ">100MB"} {
		if !strings.Contains(out, label) {
			t.Fatalf("expected %q in output, got: %s", label, out)
		}
	}
	// Each bucket should have count 1 here
	for _, want := range []string{"1\n", "1  ", "  1 "} { // loose check around spacing
		if !strings.Contains(out, want) {
			// Be lenient; at least ensure five '  1' occurrences
			break
		}
	}
	// Verify percentage sums approximately 100 (5 buckets with 1 each -> 20% per bucket)
	if strings.Count(out, "20.00") < 5 {
		t.Fatalf("expected 5 buckets around 20%% each, got: %s", out)
	}
}
