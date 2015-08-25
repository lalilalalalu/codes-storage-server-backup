/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <stdint.h>
#include <limits.h>
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>

#include "oid-map.h"

struct oid_map
{
    enum oid_map_type t;
    uint64_t num_servers;
};

int oid_map_lookup(uint64_t oid, struct oid_map const * map)
{
    switch(map->t) {
        case OID_MAP_ZERO:
            return 0;
        case OID_MAP_MOD:
            return (int) (oid % map->num_servers);
        case OID_MAP_BIN:
            return (int) (oid / (UINT64_MAX / map->num_servers));
        default:
            assert(0);
    }
    return -1;
}

int oid_map_generate_striped_random(
        int stripe_factor,
        int start_server_incl,
        int end_server_excl,
        uint64_t * oids,
        uint64_t (*rng_fn)(void *),
        void * rng_arg,
        struct oid_map const * map)
{
    if (end_server_excl <= start_server_incl)
        return -1;

    int num_servers = end_server_excl - start_server_incl;
    if (num_servers < stripe_factor || num_servers > map->num_servers)
        return -1;

    if (map->t == OID_MAP_ZERO) {
        if (num_servers > 1)
            return -1;
        else {
            oids[0] = 0;
            return 0;
        }
    }

    // pick a random start server
    int start;
    if (rng_fn)
        start = ((int) (rng_fn(rng_arg) % INT_MAX)) % num_servers +
            start_server_incl;
    else
        start = start_server_incl;

    for (int i = 0; i < stripe_factor; i++) {
        int target_svr = start + i;

        uint64_t r;
        if (rng_fn)
            r = rng_fn(rng_arg);
        else if (map->t == OID_MAP_MOD)
            r = target_svr;
        else if (map->t == OID_MAP_BIN)
            r = (uint64_t) target_svr * (UINT64_MAX / map->num_servers);
        else
            assert(0);

        if (map->t == OID_MAP_MOD) {
            // prevent bad wraparound
            uint64_t oid_max = UINT64_MAX - (uint64_t) end_server_excl;
            if (r > oid_max)
                r = oid_max;
            oids[i] =
                r - (r % map->num_servers) + (target_svr % map->num_servers);
        }
        else if (map->t == OID_MAP_BIN) {
            uint64_t oids_per_bin = UINT64_MAX / map->num_servers;
            uint64_t oid_lb = oids_per_bin * start_server_incl;
            uint64_t oid_ub = (end_server_excl == map->num_servers)
                ? UINT64_MAX
                : oids_per_bin * end_server_excl;
            r = r % (oid_ub - oid_lb);
            r += oid_lb;
            oids[i] = r;
        }
        else
            assert(0);
    }
    return (rng_fn) ? 1 + num_servers : 0;
}

void oid_map_generate_striped_random_rc(
        int n_rng_calls,
        void (*rng_rc_fn)(void *),
        void * rng_arg)
{
    for (int i = 0; i < n_rng_calls; i++)
        rng_rc_fn(rng_arg);
}

int oid_map_generate_striped(
        int start_server_incl,
        int end_server_excl,
        uint64_t * oids,
        struct oid_map const * map)
{
    return oid_map_generate_striped_random(end_server_excl - start_server_incl,
            start_server_incl, end_server_excl, oids, NULL, NULL, map);
}

static struct oid_map * oid_map_create(enum oid_map_type type, int num_servers)
{
    struct oid_map *rtn = malloc(sizeof(*rtn));
    rtn->t = type;
    rtn->num_servers = num_servers;
    return rtn;
}
struct oid_map * oid_map_create_zero(void)
{
    return oid_map_create(OID_MAP_ZERO, 0);
}

struct oid_map * oid_map_create_mod(int num_servers)
{
    return oid_map_create(OID_MAP_MOD, num_servers);
}

struct oid_map * oid_map_create_bin(int num_servers)
{
    return oid_map_create(OID_MAP_BIN, num_servers);
}

void oid_map_destroy(struct oid_map *map)
{
    free(map);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 *  indent-tabs-mode: nil
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
