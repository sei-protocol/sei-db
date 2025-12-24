package lthash

import (
	"encoding/binary"
)

const (
	TypePrefixGeneric = 0x00 // Generic store prefix (includes dbName for domain isolation)
	TypePrefixBalance = 0x01
	TypePrefixStorage = 0x02
	TypePrefixCode    = 0x03
	TypePrefixNonce   = 0x04
)

// SerializeForLtHash serializes a KV for LtHash. Returns nil for empty/zero values.
func SerializeForLtHash(dbName string, key, value []byte) []byte {
	if len(key) == 0 || len(value) == 0 {
		return nil
	}

	switch dbName {
	case "balance", "balance.db":
		return SerializeBalanceForLtHash(key, value)
	case "storage", "storage.db":
		return SerializeStorageForLtHash(key, value)
	case "code", "code.db":
		return SerializeCodeForLtHash(key, value)
	case "nonce", "nonce.db":
		return SerializeNonceForLtHash(key, value)
	default:
		// Include dbName to isolate hash input domain across stores,
		// preventing cross-store collision attacks where different stores
		// might have the same (key, value) pair.
		// Format: 0x00 (TypePrefixGeneric) || dbNameLen[2] || dbName || key || value
		dbNameBytes := []byte(dbName)
		buf := make([]byte, 1+2+len(dbNameBytes)+len(key)+len(value))
		buf[0] = TypePrefixGeneric
		binary.LittleEndian.PutUint16(buf[1:3], uint16(len(dbNameBytes)))
		copy(buf[3:3+len(dbNameBytes)], dbNameBytes)
		copy(buf[3+len(dbNameBytes):], key)
		copy(buf[3+len(dbNameBytes)+len(key):], value)
		return buf
	}
}

// SerializeBalanceForLtHash serializes balance for LtHash.
// Format: 0x01 || address[20] || balance_be[32]
// Returns nil for zero balance (skip).
func SerializeBalanceForLtHash(addrBytes, balanceBytes []byte) []byte {
	if len(addrBytes) != 20 {
		return nil
	}
	if len(balanceBytes) == 0 {
		return nil // Zero balance, skip
	}

	// Check if balance is zero
	allZero := true
	for _, b := range balanceBytes {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return nil // Skip zero balance
	}

	buf := make([]byte, 53)
	buf[0] = TypePrefixBalance
	copy(buf[1:21], addrBytes)

	// Right-align balance as 32-byte big-endian.
	if len(balanceBytes) <= 32 {
		copy(buf[53-len(balanceBytes):53], balanceBytes)
	} else {
		// Truncate to 32 bytes.
		copy(buf[21:53], balanceBytes[len(balanceBytes)-32:])
	}
	return buf
}

// SerializeStorageForLtHash serializes storage for LtHash.
// Format: 0x02 || address[20] || key[32] || value[32]
// Returns nil for zero value (skip).
func SerializeStorageForLtHash(fullKeyBytes, valueBytes []byte) []byte {
	if len(fullKeyBytes) != 52 { // 20 (address) + 32 (key)
		return nil
	}
	if len(valueBytes) != 32 {
		return nil
	}

	// Skip zero value.
	isZero := true
	for _, b := range valueBytes {
		if b != 0 {
			isZero = false
			break
		}
	}
	if isZero {
		return nil // Skip zero storage value
	}

	buf := make([]byte, 85)
	buf[0] = TypePrefixStorage
	copy(buf[1:21], fullKeyBytes[:20])  // address
	copy(buf[21:53], fullKeyBytes[20:]) // storage key
	copy(buf[53:85], valueBytes)        // storage value
	return buf
}

// SerializeCodeForLtHash serializes code for LtHash.
// Format: 0x03 || address[20] || len[4] || bytecode
// Returns nil for empty code (skip).
func SerializeCodeForLtHash(addrBytes, codeBytes []byte) []byte {
	if len(addrBytes) != 20 {
		return nil
	}
	if len(codeBytes) == 0 {
		return nil // Skip empty code
	}

	buf := make([]byte, 25+len(codeBytes))
	buf[0] = TypePrefixCode
	copy(buf[1:21], addrBytes)
	binary.LittleEndian.PutUint32(buf[21:25], uint32(len(codeBytes)))
	copy(buf[25:], codeBytes)
	return buf
}

// SerializeNonceForLtHash serializes nonce. Nonce=0 is included.
func SerializeNonceForLtHash(addrBytes, nonceBytes []byte) []byte {
	if len(addrBytes) != 20 {
		return nil
	}
	if len(nonceBytes) != 8 {
		return nil
	}

	buf := make([]byte, 29)
	buf[0] = TypePrefixNonce
	copy(buf[1:21], addrBytes)
	// DB stores big-endian; LtHash uses little-endian.
	nonce := binary.BigEndian.Uint64(nonceBytes)
	binary.LittleEndian.PutUint64(buf[21:29], nonce)
	return buf
}
