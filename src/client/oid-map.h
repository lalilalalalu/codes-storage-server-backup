/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <stdint.h>

// forward decl
struct oid_map;

// types of mappings supported
enum oid_map_type {
    // dummy mapper - maps to server 0
    OID_MAP_ZERO,
    // maps to servers using a simple modulo of the oid
    OID_MAP_MOD,
    // maps to servers by binning OIDs (server = oid / (MAX_OID / num_servers))
    OID_MAP_BIN,
};

// returns server ID given oid, map. Return -1 if an invalid mapping
int oid_map_lookup(uint64_t oid, struct oid_map const * map);

// creates a set of random OIDs, from start_server (inclusive) to end_server
// (exclusive). If there are more possible servers than strips, picks a random
// start server such that there's no wraparound
// returns the number of RNG calls made, or -1 if a failure
int oid_map_generate_striped_random(
        int stripe_factor,
        int start_server_incl,
        int end_server_excl,
        uint64_t * oids,
        uint64_t (*rng_fn)(void *),
        void * rng_arg,
        struct oid_map const * map);

void oid_map_generate_striped_random_rc(
        int n_rng_calls,
        void (*rng_rc_fn)(void *),
        void * rng_arg);

// deterministic version of ^ - generates arbitary OIDs you can't assume to be
// unique across the range [start_server, end_server]
int oid_map_generate_striped(
        int start_server_incl,
        int end_server_excl,
        uint64_t * oids,
        struct oid_map const * map);

struct oid_map * oid_map_create_zero(void);
struct oid_map * oid_map_create_mod(int num_servers);
struct oid_map * oid_map_create_bin(int num_servers);

void oid_map_destroy(struct oid_map * map);

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 *  indent-tabs-mode: nil
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
