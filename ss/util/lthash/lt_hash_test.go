package lthash

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLtHashBasic(t *testing.T) {
	lth1 := NewEmptyLtHash()
	assert.True(t, lth1.IsIdentity())

	data1 := []byte("hello")
	lth1 = FromBytes(data1)
	assert.False(t, lth1.IsIdentity())

	lth2 := FromBytes(data1)
	assert.Equal(t, lth1.limbs, lth2.limbs)
	assert.Equal(t, lth1.Checksum(), lth2.Checksum())

	data2 := []byte("world")
	lth2 = FromBytes(data2)
	assert.NotEqual(t, lth1.limbs, lth2.limbs)

	// Test MixIn
	lthSum := NewEmptyLtHash()
	lthSum.MixIn(lth1)
	lthSum.MixIn(lth2)

	lthSum2 := NewEmptyLtHash()
	lthSum2.MixIn(lth2)
	lthSum2.MixIn(lth1)
	assert.Equal(t, lthSum.limbs, lthSum2.limbs) // Commutative

	// Test MixOut
	lthSum.MixOut(lth1)
	assert.Equal(t, lth2.limbs, lthSum.limbs)
	lthSum.MixOut(lth2)
	assert.True(t, lthSum.IsIdentity())
}

func TestLtHashSerialization(t *testing.T) {
	lth1 := FromBytes([]byte("test data"))
	bz := lth1.Bytes()
	assert.Equal(t, LtHashBytes, len(bz))

	lth2, err := FromRaw(bz)
	require.NoError(t, err)
	assert.Equal(t, lth1.limbs, lth2.limbs)
}

func TestLtHashParallel(t *testing.T) {
	dbName := "bank"
	kvs := []KVPairWithOldValue{
		{Key: []byte("key1"), Value: []byte("val1"), LastFlushValue: nil},
		{Key: []byte("key2"), Value: []byte("val2"), LastFlushValue: nil},
		{Key: []byte("key3"), Value: []byte("val3"), LastFlushValue: []byte("old3")},
		{Key: []byte("key4"), Value: nil, LastFlushValue: []byte("old4"), Deleted: true},
	}

	delta, _ := ComputeLtHashDeltaParallel(dbName, kvs, 2)

	// Manual computation
	expected := NewEmptyLtHash()
	// key1: add new
	expected.MixIn(FromBytes(SerializeForLtHash(dbName, []byte("key1"), []byte("val1"))))
	// key2: add new
	expected.MixIn(FromBytes(SerializeForLtHash(dbName, []byte("key2"), []byte("val2"))))
	// key3: sub old, add new
	expected.MixOut(FromBytes(SerializeForLtHash(dbName, []byte("key3"), []byte("old3"))))
	expected.MixIn(FromBytes(SerializeForLtHash(dbName, []byte("key3"), []byte("val3"))))
	// key4: sub old
	expected.MixOut(FromBytes(SerializeForLtHash(dbName, []byte("key4"), []byte("old4"))))

	assert.Equal(t, expected.limbs, delta.limbs)
}

func TestModuleSerialization(t *testing.T) {
	addr := make([]byte, 20)
	addr[0] = 0xAA
	
	// Balance
	balance := []byte{0x01, 0x02}
	bz := SerializeBalanceForLtHash(addr, balance)
	assert.Equal(t, byte(TypePrefixBalance), bz[0])
	assert.Equal(t, addr, bz[1:21])
	assert.Equal(t, byte(0x01), bz[51])
	assert.Equal(t, byte(0x02), bz[52])

	// Storage
	fullKey := make([]byte, 52)
	copy(fullKey, addr)
	fullKey[20] = 0xBB // storage key
	val := make([]byte, 32)
	val[31] = 0xCC
	bz = SerializeStorageForLtHash(fullKey, val)
	assert.Equal(t, byte(TypePrefixStorage), bz[0])
	assert.Equal(t, addr, bz[1:21])
	assert.Equal(t, byte(0xBB), bz[21])
	assert.Equal(t, byte(0xCC), bz[84])

	// Code
	code := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	bz = SerializeCodeForLtHash(addr, code)
	assert.Equal(t, byte(TypePrefixCode), bz[0])
	assert.Equal(t, uint32(4), uint32(bz[21]) | uint32(bz[22])<<8)
	assert.Equal(t, code, bz[25:])

	// Nonce
	nonce := []byte{0, 0, 0, 0, 0, 0, 0, 0x05}
	bz = SerializeNonceForLtHash(addr, nonce)
	assert.Equal(t, byte(TypePrefixNonce), bz[0])
	assert.Equal(t, uint64(5), uint64(bz[21]) | uint64(bz[22])<<8)
}

func BenchmarkLtHashDeltaParallel(b *testing.B) {
	dbName := "storage"
	numKVs := 1000
	kvs := make([]KVPairWithOldValue, numKVs)
	for i := 0; i < numKVs; i++ {
		key := make([]byte, 52)
		key[0] = byte(i % 256)
		val := make([]byte, 32)
		val[0] = byte(i % 256)
		kvs[i] = KVPairWithOldValue{
			Key:            key,
			Value:          val,
			LastFlushValue: val, // Just for benchmark
			Deleted:        false,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ComputeLtHashDeltaParallel(dbName, kvs, runtime.NumCPU())
	}
}

