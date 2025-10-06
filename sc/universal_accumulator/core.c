#include "universal_accumulator.h"
#include <openssl/evp.h>
#include <omp.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h> // For bool type and true/false
#include <assert.h>

// Static flags to ensure initialization routines are idempotent (safe to call multiple times).
static bool relic_core_initialized = false;
static bool relic_params_set = false;

// =================================================================================
// Static Inline Helper Functions
// =================================================================================

// Converts a 32-byte hash to a field element bn_t.
static inline void _hash_to_field_element(bn_t out, const unsigned char *hash, bn_t n) {
    bn_read_bin(out, hash, 32);
    bn_mod(out, out, n);
}

// Computes (in + a) mod n.
static inline void _element_add_a(bn_t out, const bn_t in, const bn_t a, const bn_t n) {
    bn_add(out, in, a);
    bn_mod(out, out, n);
}


// RELIC core initialization.
int init_relic_core() {
    if (relic_core_initialized) {
        return RLC_OK;
    }
    if (core_init() != RLC_OK) {
        return RLC_ERR;
    }
    relic_core_initialized = true;
    return RLC_OK;
}

// RELIC pairing parameter setup.
int set_pairing_params() {
    if (relic_params_set) {
        return RLC_OK;
    }
    if (pc_param_set_any() != RLC_OK) {
        return RLC_ERR;
    }
    relic_params_set = true;
    return RLC_OK;
}

void cleanup_relic_core() {
    core_clean();
    relic_params_set = false;
    relic_core_initialized = false;
}

// Accumulator initialization
void init(t_state * accumulator) {
    // Use the idempotent initialization helpers.
    if (!relic_core_initialized) {
        if (init_relic_core() != RLC_OK) {
            printf("RELIC core initialization failed\n");
            return;
        }
    }
    if (!relic_params_set) {
        if (set_pairing_params() != RLC_OK) {
            printf("Pairing parameters setup failed\n");
            core_clean();
            return;
        }
    }

    bn_t n2;

    g1_null(accumulator->P); g1_null(accumulator->V);
    g2_null(accumulator->Pt); g2_null(accumulator->Qt);
    gt_null(accumulator->ePPt); gt_null(accumulator->eVPt);
    bn_null(accumulator->n); bn_null(n2); bn_null(accumulator->a);

    g1_new(accumulator->P); g1_new(accumulator->V);
    g2_new(accumulator->Pt); g2_new(accumulator->Qt);
    gt_new(accumulator->ePPt); gt_new(accumulator->eVPt);
    bn_new(accumulator->n); bn_new(n2); bn_new(accumulator->a);

    g1_get_ord(accumulator->n);
    g2_get_ord(n2);
    assert(bn_cmp(accumulator->n, n2) == RLC_EQ);

    // Using a fixed seed is for now for consensus.
    unsigned char fixed_seed[32] = {
        0x73, 0x65, 0x69, 0x2d, 0x76, 0x33, 0x2d, 0x61,
        0x63, 0x63, 0x75, 0x6d, 0x75, 0x6c, 0x61, 0x74,
        0x6f, 0x72, 0x2d, 0x73, 0x65, 0x65, 0x64, 0x2d,
        0x76, 0x31, 0x2e, 0x30, 0x2e, 0x30, 0x2d, 0x78
    };

    bn_read_bin(accumulator->a, fixed_seed, 32);
    bn_mod(accumulator->a, accumulator->a, accumulator->n);

    g1_get_gen(accumulator->P);
    g2_get_gen(accumulator->Pt);
    g2_mul(accumulator->Qt, accumulator->Pt, accumulator->a);

    g1_get_gen(accumulator->V);

    // Precompute pairings as fixed GT bases to avoid repeated pairing ops.
    // Later: e(P, a·Pt) = ePPt^a, e(V, a·Pt) = eVPt^a
    pc_map(accumulator->ePPt, accumulator->P, accumulator->Pt);
    pc_map(accumulator->eVPt, accumulator->V, accumulator->Pt);

    bn_null(accumulator->fVa);
    bn_new(accumulator->fVa);
    bn_set_dig(accumulator->fVa, 1);

    bn_free(n2);

    return;
}


// Helper functions for CGO wrapper.
int calculate_root(t_state *acc, unsigned char *buf, int buf_size) {
    if (!acc || !buf) {
        return -1;
    }
    int required_size = g1_size_bin(acc->V, 1);
    if (buf_size < required_size) {
        return -1;
    }
    g1_write_bin(buf, required_size, acc->V, 1);
    return required_size;
}

// add_hashed_elements adds a batch of hashed elements to the accumulator
int add_hashed_elements(t_state *acc, unsigned char *flat_hashes, int count) {
    if (count <= 0) return 0;

    bn_t product_of_additions;
    bn_null(product_of_additions);
    bn_new(product_of_additions);
    bn_set_dig(product_of_additions, 1);

    int max_threads = omp_get_max_threads();
    bn_t* partial_products = (bn_t*)malloc(sizeof(bn_t) * max_threads);
    for (int i = 0; i < max_threads; i++) {
        bn_null(partial_products[i]);
        bn_new(partial_products[i]);
        bn_set_dig(partial_products[i], 1);
    }

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        // Use a small, per-thread scratchpad of bn_t variables to avoid malloc churn in the loop.
        bn_t scratch_temp, scratch_add;
        bn_null(scratch_temp); bn_new(scratch_temp);
        bn_null(scratch_add); bn_new(scratch_add);

        #pragma omp for schedule(static, 4096) nowait
        for (int i = 0; i < count; i++) {
            const unsigned char* p = flat_hashes + (i << 5); // i * 32
            _hash_to_field_element(scratch_temp, p, acc->n);
            _element_add_a(scratch_add, scratch_temp, acc->a, acc->n);
            bn_mul(partial_products[thread_id], partial_products[thread_id], scratch_add);
            bn_mod(partial_products[thread_id], partial_products[thread_id], acc->n);
        }

        bn_free(scratch_temp);
        bn_free(scratch_add);
    }

    for (int i = 0; i < max_threads; i++) {
        bn_mul(product_of_additions, product_of_additions, partial_products[i]);
        bn_mod(product_of_additions, product_of_additions, acc->n);
        bn_free(partial_products[i]);
    }
    free(partial_products);

    bn_mul(acc->fVa, acc->fVa, product_of_additions);
    bn_mod(acc->fVa, acc->fVa, acc->n);
    g1_mul(acc->V, acc->V, product_of_additions);
    gt_exp(acc->eVPt, acc->eVPt, product_of_additions);

    bn_free(product_of_additions);
    return 0;
}

// Forward declaration needed for batch_del_hashed_elements
int batch_del_with_elements(t_state * accumulator, bn_t* elements, int batch_size);

// batch_del_hashed_elements removes a batch of hashed elements from the accumulator
int batch_del_hashed_elements(t_state *acc, unsigned char *flat_hashes, int count) {
    if (count <= 0) return 0;

    bn_t* elements = (bn_t*)malloc(sizeof(bn_t) * count);
    if (!elements) return RLC_ERR;

    for (int i = 0; i < count; i++) {
        bn_null(elements[i]);
        bn_new(elements[i]);
        const unsigned char* p = flat_hashes + (i << 5); // i * 32
        _hash_to_field_element(elements[i], p, acc->n);
    }

    int result = batch_del_with_elements(acc, elements, count);

    for (int i = 0; i < count; i++) {
        bn_free(elements[i]);
    }
    free(elements);

    return result;
}

// Batch deletion with provided elements, optimized with Montgomery Batch Inversion.
// This algorithm is significantly faster than computing modular inverses individually.
//
// The core idea is:
// 1. Compute the prefix product of all elements to be inverted (P[i] = e[0]*...*e[i]).
// 2. Compute a single modular inverse of the total product (inv(P[n-1])). This is the only expensive step.
// 3. Work backwards to find the inverse of each element e[i] using the formula:
//    inv(e[i]) = inv(P[i]) * P[i-1].
//
// This function is also robust against non-invertible elements by checking each one
// before starting the batch inversion process.
int batch_del_with_elements(t_state * accumulator, bn_t* elements, int batch_size) {
    if (batch_size <= 0) return 0;

    bn_t *yplus_a_vals = (bn_t *)malloc(batch_size * sizeof(bn_t));
    bn_t *inverses = (bn_t *)malloc(batch_size * sizeof(bn_t));
    if (!yplus_a_vals || !inverses) {
        if(yplus_a_vals) free(yplus_a_vals);
        if(inverses) free(inverses);
        return RLC_ERR;
    }

    bn_t gcd, tmp;
    bn_null(gcd); bn_new(gcd);
    bn_null(tmp); bn_new(tmp);

    // Step 1: Compute all (y_i + a) and check for non-invertible elements.
    for (int i = 0; i < batch_size; i++) {
        bn_null(yplus_a_vals[i]); bn_new(yplus_a_vals[i]);
        _element_add_a(yplus_a_vals[i], elements[i], accumulator->a, accumulator->n);
        
        // Robustness Check: Ensure (y+a) is invertible before proceeding.
        // This prevents a DoS attack via a crafted non-invertible element.
        bn_gcd_ext_lehme(gcd, tmp, NULL, yplus_a_vals[i], accumulator->n);
        if (bn_cmp_dig(gcd, 1) != RLC_EQ) {
            // Found a non-invertible element. Abort the whole batch.
            for(int j = 0; j <= i; j++) bn_free(yplus_a_vals[j]);
            free(yplus_a_vals);
            free(inverses);
            bn_free(gcd);
            bn_free(tmp);
            return RLC_ERR; // Return error
        }
    }
    bn_free(gcd);
    bn_free(tmp);


    // Step 2: Montgomery Batch Inversion
    bn_t last_prod;
    bn_null(last_prod); bn_new(last_prod);

    // Create prefix products: yplus_a_vals[i] = yplus_a_vals[0] * ... * yplus_a_vals[i]
    bn_copy(inverses[0], yplus_a_vals[0]);
    for (int i = 1; i < batch_size; i++) {
        bn_null(inverses[i]); bn_new(inverses[i]);
        bn_mul(inverses[i], inverses[i-1], yplus_a_vals[i]);
        bn_mod(inverses[i], inverses[i], accumulator->n);
    }

    // Invert the total product (one expensive operation)
    bn_mod_inv(last_prod, inverses[batch_size - 1], accumulator->n);

    // Go backwards to find individual inverses
    // Use a single scratch variable for the temporary inverse to avoid malloc churn.
    bn_t temp_inv;
    bn_null(temp_inv); bn_new(temp_inv);
    for (int i = batch_size - 1; i > 0; --i) {
        // inv[i] = inv[i-1] * total_inv
        bn_mul(temp_inv, inverses[i-1], last_prod);
        bn_mod(temp_inv, temp_inv, accumulator->n);
        // total_inv = total_inv * val[i]
        bn_mul(last_prod, last_prod, yplus_a_vals[i]);
        bn_mod(last_prod, last_prod, accumulator->n);
        bn_copy(inverses[i], temp_inv);
    }
    bn_copy(inverses[0], last_prod);
    bn_free(last_prod);
    bn_free(temp_inv);

    // Step 3: Multiply all inverses together
    bn_t product_of_inverses;
    bn_null(product_of_inverses); bn_new(product_of_inverses);
    bn_set_dig(product_of_inverses, 1);
    for (int i = 0; i < batch_size; i++) {
        bn_mul(product_of_inverses, product_of_inverses, inverses[i]);
        bn_mod(product_of_inverses, product_of_inverses, accumulator->n);
    }

    // Step 4: Apply the single update to the accumulator state
    g1_mul(accumulator->V, accumulator->V, product_of_inverses);
    bn_mul(accumulator->fVa, accumulator->fVa, product_of_inverses);
    bn_mod(accumulator->fVa, accumulator->fVa, accumulator->n);
    // Note: Updating eVPt for deletion requires a modular inverse exponentiation,
    // which is also slow. If deletions are frequent, re-computing it might be better.
    // For now, we re-compute it from scratch after deletion.
    pc_map(accumulator->eVPt, accumulator->V, accumulator->Pt);


    // Cleanup
    for (int i = 0; i < batch_size; i++) {
        bn_free(yplus_a_vals[i]);
        bn_free(inverses[i]);
    }
    free(yplus_a_vals);
    free(inverses);
    bn_free(product_of_inverses);
    
    return 0; // Success
}

// Renamed for API safety. This frees internal members but not the struct pointer itself.
void destroy_accumulator(t_state *accumulator) {
    if (!accumulator) return;
    g1_free(accumulator->P); g1_free(accumulator->V);
    g2_free(accumulator->Pt); g2_free(accumulator->Qt);
    gt_free(accumulator->ePPt); gt_free(accumulator->eVPt);
    bn_free(accumulator->n); bn_free(accumulator->a); bn_free(accumulator->fVa);
    // The caller is responsible for freeing the 'accumulator' struct itself if it was heap-allocated.
}

// Issue (non-)membership witnesses (no ZK fields required)
t_witness * issue_witness(t_state * accumulator, bn_t y, bool is_membership) {
    t_witness * w_y = (t_witness *)malloc(sizeof(t_witness));
    if (!w_y) return NULL;
    
    bn_t tmp, one, yplus_a_inv, c;
    
    g1_null(w_y->C); bn_null(w_y->y); bn_null(w_y->d); gt_null(w_y->eCPt);
    g1_new(w_y->C); bn_new(w_y->y); bn_new(w_y->d); gt_new(w_y->eCPt);
    
    bn_null(tmp); bn_new(tmp);
    bn_null(one); bn_new(one);
    bn_null(yplus_a_inv); bn_new(yplus_a_inv);
    
    _element_add_a(tmp, y, accumulator->a, accumulator->n);
    bn_gcd_ext_lehme(one, yplus_a_inv, NULL, tmp, accumulator->n);

    // Robustness: check if inverse exists.
    if (bn_cmp_dig(one,1) != RLC_EQ) {
        destroy_witness(w_y);
        free(w_y); // Free the container since we are aborting
        bn_free(tmp); bn_free(one); bn_free(yplus_a_inv);
        return NULL;
    }
    bn_mod(yplus_a_inv, yplus_a_inv, accumulator->n);

    if (is_membership == true) {
        g1_mul(w_y->C, accumulator->V, yplus_a_inv);
        bn_set_dig(w_y->d, 0);
    } else {
        bn_null(c); bn_new(c);
        bn_sub_dig(c, accumulator->fVa, 1);
        bn_mul(c, c, yplus_a_inv);
        bn_mod(c, c, accumulator->n);
        g1_mul(w_y->C, accumulator->P, c);
        bn_set_dig(w_y->d, 1);
        bn_free(c);
    }

    bn_copy(w_y->y, y);
    pc_map(w_y->eCPt, w_y->C, accumulator->Pt);

    bn_free(tmp); bn_free(one); bn_free(yplus_a_inv);

    return w_y;
}

//Witness Verification
bool verify_witness(t_state * accumulator, t_witness * wit) {
    gt_t e1, e2, tmp;
    bn_t yplus_a;
    
    gt_null(e1); gt_new(e1);
    gt_null(e2); gt_new(e2);
    gt_null(tmp); gt_new(tmp);
    bn_null(yplus_a); bn_new(yplus_a);
    
    _element_add_a(yplus_a, wit->y, accumulator->a, accumulator->n);

    if (bn_is_zero(wit->d)) {
        gt_exp(e1, wit->eCPt, yplus_a);
        gt_copy(e2, accumulator->eVPt);
    } else {
        gt_exp(e1, wit->eCPt, yplus_a);
        gt_exp(tmp, accumulator->ePPt, wit->d);
        gt_mul(e1, e1, tmp);
        gt_copy(e2, accumulator->eVPt);
    }

    bool result = (gt_cmp(e1, e2) == RLC_EQ);

    gt_free(e1); gt_free(e2); gt_free(tmp); bn_free(yplus_a);

    return result;
}

// ----------------------------------------------------------------------------
// Factor helpers: expose/get fVa and rebuild state from factor
// ----------------------------------------------------------------------------
// get_fva returns the serialized bn of fVa. If buffer is NULL or too small,
// it returns -required_size, where required_size is the number of bytes needed.
int get_fva(t_state *acc, unsigned char *buffer, int buffer_size) {
    if (!acc) return -1;
    int required_size = bn_size_bin(acc->fVa);
    if (buffer == NULL || buffer_size < required_size) {
        return -required_size;
    }
    bn_write_bin(buffer, required_size, acc->fVa);
    return required_size;
}

// set_state_from_factor sets acc->fVa from provided big-endian bytes, and
// recomputes V and eVPt as:
//   V = fVa * P
//   eVPt = e(P,Pt)^{fVa}
int set_state_from_factor(t_state *acc, unsigned char *factor, int factor_size) {
    if (!acc || !factor || factor_size <= 0) return RLC_ERR;
    bn_read_bin(acc->fVa, factor, factor_size);
    bn_mod(acc->fVa, acc->fVa, acc->n);
    g1_mul(acc->V, acc->P, acc->fVa);
    gt_exp(acc->eVPt, acc->ePPt, acc->fVa);
    return RLC_OK;
}

// Renamed for API safety. Frees internal members, not the struct pointer.
void destroy_witness(t_witness *witness) {
    if (!witness) return;
    bn_free(witness->y);
    g1_free(witness->C);
    bn_free(witness->d);
    gt_free(witness->eCPt);
    // The caller is responsible for freeing the 'witness' struct itself.
}

// Helper function to issue witness from hash
t_witness* issue_witness_from_hash(t_state* accumulator, unsigned char* hash, bool is_membership) {
    bn_t y;
    bn_null(y); bn_new(y);
    _hash_to_field_element(y, hash, accumulator->n);
    t_witness* witness = issue_witness(accumulator, y, is_membership);
    bn_free(y);
    return witness;
}