#include "universal_accumulator.h"
#include <string.h>
#include <stdint.h>
#include <arpa/inet.h>  // for htonl/ntohl

// Snapshot format constants
#define SNAPSHOT_MAGIC 0xACC01234
#define SNAPSHOT_VERSION 1
#define SNAPSHOT_FLAG_COMPRESSED 0x01

// Snapshot header for data integrity
typedef struct {
    uint32_t magic;      // Magic number for format validation
    uint16_t version;    // Version number
    uint16_t flags;      // Feature flags
    uint32_t data_size;  // Size of serialized data (excluding header)
    uint32_t checksum;   // Simple checksum for basic integrity
} snapshot_header_t;

// Forward declarations
int get_serialized_state_size(t_state *acc);

// Simple checksum calculation
static uint32_t calculate_checksum(const unsigned char *data, int size) {
    uint32_t checksum = 0;
    for (int i = 0; i < size; i++) {
        checksum = (checksum << 1) ^ data[i];
    }
    return checksum;
}

// Serialization helpers for accumulator state
int serialize_accumulator_state(t_state *acc, unsigned char *buffer, int buffer_size) {
	if (!acc || !buffer) {
		return -1;
	}

	// Check minimum buffer size for header
	if (buffer_size < sizeof(snapshot_header_t)) {
		return -1;
	}

	// Calculate data size needed (excluding header)
	int data_size = get_serialized_state_size(acc);
	if (data_size < 0) {
		return -1;
	}

	// Check total buffer size
	int total_size = sizeof(snapshot_header_t) + data_size;
	if (total_size > buffer_size) {
		return -1;
	}

	// Start serializing data after header
	unsigned char *data_buffer = buffer + sizeof(snapshot_header_t);
	int offset = 0;

    // Serialize G1 elements (P, V)
    int g1_size = g1_size_bin(acc->P, 1); // compressed form
    g1_write_bin(data_buffer + offset, g1_size, acc->P, 1); offset += g1_size;
    g1_write_bin(data_buffer + offset, g1_size, acc->V, 1); offset += g1_size;

	// Serialize G2 elements (Pt, Qt)
	int g2_size = g2_size_bin(acc->Pt, 1); // compressed form
	g2_write_bin(data_buffer + offset, g2_size, acc->Pt, 1); offset += g2_size;
	g2_write_bin(data_buffer + offset, g2_size, acc->Qt, 1); offset += g2_size;

    // Serialize GT elements (cached pairings)
    int gt_size = gt_size_bin(acc->ePPt, 1); // compressed form
    gt_write_bin(data_buffer + offset, gt_size, acc->ePPt, 1); offset += gt_size;
    gt_write_bin(data_buffer + offset, gt_size, acc->eVPt, 1); offset += gt_size;

	// Serialize BN elements (n, a, fVa)
	int bn_size = bn_size_bin(acc->n);
	bn_write_bin(data_buffer + offset, bn_size, acc->n); offset += bn_size;
	bn_write_bin(data_buffer + offset, bn_size, acc->a); offset += bn_size;
	bn_write_bin(data_buffer + offset, bn_size, acc->fVa); offset += bn_size;

	// Calculate checksum of serialized data
	uint32_t checksum = calculate_checksum(data_buffer, data_size);

	// Write header with network byte order
	snapshot_header_t *header = (snapshot_header_t *)buffer;
	header->magic = htonl(SNAPSHOT_MAGIC);
	header->version = htons(SNAPSHOT_VERSION);
	header->flags = htons(SNAPSHOT_FLAG_COMPRESSED);
	header->data_size = htonl(data_size);
	header->checksum = htonl(checksum);

	return total_size; // Return total bytes written
}

int deserialize_accumulator_state(t_state *acc, unsigned char *buffer, int buffer_size) {
	if (!acc || !buffer) {
		return -1;
	}

	// Check minimum buffer size for header
	if (buffer_size < sizeof(snapshot_header_t)) {
		return -1;
	}

	// Read and validate header
	snapshot_header_t *header = (snapshot_header_t *)buffer;
	uint32_t magic = ntohl(header->magic);
	uint16_t version = ntohs(header->version);
	uint16_t flags = ntohs(header->flags);
	uint32_t data_size = ntohl(header->data_size);
	uint32_t expected_checksum = ntohl(header->checksum);

	// Validate magic number
	if (magic != SNAPSHOT_MAGIC) {
		return -1;
	}

	// Validate version
	if (version != SNAPSHOT_VERSION) {
		return -1;
	}

	// Check data size
	if (sizeof(snapshot_header_t) + data_size > buffer_size) {
		return -1;
	}

	// Validate checksum
	unsigned char *data_buffer = buffer + sizeof(snapshot_header_t);
	uint32_t actual_checksum = calculate_checksum(data_buffer, data_size);
	if (actual_checksum != expected_checksum) {
		return -1;
	}

	int offset = 0;

    // Deserialize G1 elements (P, V)
    int g1_size = g1_size_bin(acc->P, 1); // compressed form
    g1_read_bin(acc->P, data_buffer + offset, g1_size); offset += g1_size;
    g1_read_bin(acc->V, data_buffer + offset, g1_size); offset += g1_size;

	// Deserialize G2 elements (Pt, Qt)
	int g2_size = g2_size_bin(acc->Pt, 1); // compressed form
	g2_read_bin(acc->Pt, data_buffer + offset, g2_size); offset += g2_size;
	g2_read_bin(acc->Qt, data_buffer + offset, g2_size); offset += g2_size;

    // Deserialize GT elements (cached pairings)
    int gt_size = gt_size_bin(acc->ePPt, 1); // compressed form
    gt_read_bin(acc->ePPt, data_buffer + offset, gt_size); offset += gt_size;
    gt_read_bin(acc->eVPt, data_buffer + offset, gt_size); offset += gt_size;

	// Deserialize BN elements (n, a, fVa)
	int bn_size = bn_size_bin(acc->n);
	bn_read_bin(acc->n, data_buffer + offset, bn_size); offset += bn_size;
	bn_read_bin(acc->a, data_buffer + offset, bn_size); offset += bn_size;
	bn_read_bin(acc->fVa, data_buffer + offset, bn_size); offset += bn_size;

	return sizeof(snapshot_header_t) + data_size; // Return total bytes read
}

int get_serialized_state_size(t_state *acc) {
	if (!acc) return -1;

	int g1_size = g1_size_bin(acc->P, 1);
	int g2_size = g2_size_bin(acc->Pt, 1);
	int gt_size = gt_size_bin(acc->ePPt, 1);
	int bn_size = bn_size_bin(acc->n);

    return 2 * g1_size + 2 * g2_size + 2 * gt_size + 3 * bn_size;
}

// Get total size including header
int get_total_snapshot_size(t_state *acc) {
	int data_size = get_serialized_state_size(acc);
	if (data_size < 0) return -1;
	return sizeof(snapshot_header_t) + data_size;
}

// Basic validation of accumulator state (to be called after deserialization)
int validate_accumulator_state(t_state *acc) {
	if (!acc) return 0;
	
	// Basic checks - ensure all group elements are valid
	// This is a simplified validation - in practice, you might want more thorough checks
	if (g1_is_infty(acc->P) || g1_is_infty(acc->V)) return 0;
	if (g2_is_infty(acc->Pt) || g2_is_infty(acc->Qt)) return 0;
	if (bn_is_zero(acc->n) || bn_is_zero(acc->a)) return 0;
	
	return 1; // Valid
} 