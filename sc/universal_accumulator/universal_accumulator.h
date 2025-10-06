#ifndef UNIVERSAL_ACCUMULATOR_H
#define UNIVERSAL_ACCUMULATOR_H

#include <stdbool.h>
#include <relic/relic.h>

// Forward declarations
typedef struct t_state t_state;
typedef struct t_witness t_witness;

// Struct definitions
struct t_state {
    g1_t P;  // Generator of G1
    g1_t V;  // Current accumulator value
    g2_t Pt; // Generator of G2
    g2_t Qt; // a*Pt
    gt_t ePPt; // e(P,Pt)
    gt_t eVPt; // e(V,Pt), make proof verification fast
    bn_t n;  // Order of the groups
    bn_t a;  // Secret key
    bn_t fVa;// fV(a) - for non-membership witnesses
};

struct t_witness {
    bn_t y;   // Element
    g1_t C;   // Witness value
    bn_t d;   // Additional value for non-membership
    gt_t eCPt;// e(C,Pt) - cached pairing
};

// Function declarations
void init(t_state* accumulator);
int calculate_root(t_state *acc, unsigned char *buf, int buf_size);
void hash_to_field_element(unsigned char* hash, bn_t result, bn_t modulus);
int add_hashed_elements(t_state *acc, unsigned char *flat_hashes, int count);
int batch_del_hashed_elements(t_state *acc, unsigned char *flat_hashes, int count);
void batch_add(t_state* accumulator, bn_t* elements, int batch_size);
int batch_del_with_elements(t_state* accumulator, bn_t* elements, int batch_size);
void destroy_accumulator(t_state *accumulator);
t_witness* issue_witness(t_state* accumulator, bn_t y, bool is_membership);
bool verify_witness(t_state* accumulator, t_witness* wit);
void destroy_witness(t_witness *witness);
t_witness* issue_witness_from_hash(t_state* accumulator, unsigned char* hash, bool is_membership);

// Snapshot functions
int serialize_accumulator_state(t_state *acc, unsigned char *buffer, int buffer_size);
int deserialize_accumulator_state(t_state *acc, unsigned char *buffer, int buffer_size);
int get_serialized_state_size(t_state *acc);
int get_total_snapshot_size(t_state *acc);
int validate_accumulator_state(t_state *acc);

int init_relic_core(void);
void cleanup_relic_core(void);
int set_pairing_params(void);

// Expose factor (fVa) helpers for efficient per-height reconstruction
int get_fva(t_state *acc, unsigned char *buffer, int buffer_size);
int set_state_from_factor(t_state *acc, unsigned char *factor, int factor_size);

#endif // UNIVERSAL_ACCUMULATOR_H
