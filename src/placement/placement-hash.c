/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <assert.h>
#include "placement-hash.h"
#include "placement.h"
#include "codes/jenkins-hash.h"
#include "objects.h"

static uint64_t placement_distance_hash(uint64_t a, uint64_t b, 
    unsigned int num_servers);
static void placement_find_closest_hash(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int num_servers);

static struct placement_mod mod_hash = 
{
    "hash",
    placement_distance_hash,
    placement_find_closest_hash,
    placement_create_striped_random
};

struct placement_mod* placement_mod_hash(void)
{
    return(&mod_hash);
}

static uint64_t placement_distance_hash(uint64_t a, uint64_t b, 
    unsigned int num_servers)
{
    uint64_t higher;
    uint64_t lower;
    uint64_t dist;
    uint32_t h1, h2;

    /* figure out wich number is higher */
    /* we are just doing this to make the operation commutative, so
     * dist(a,b) == dist(b,a)
     */
    if(a>b)
    {
        higher = a;
        lower = b;
    }
    else
    {
        higher = b;
        lower = a;
    }

    h1 = higher & 0xFFFFFFFF;
    h2 = (higher >> 32) & 0xFFFFFFFF;

    bj_hashlittle2(&lower, sizeof(lower), &h1, &h2);

    dist  = h1 + (((uint64_t)h2)<<32);

    return(dist);
}


static void placement_find_closest_hash(uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs, unsigned int num_servers)
{
    unsigned int i, j;
    uint64_t svr, tmp_svr;
    uint64_t servers[MAX_REPLICATION];

    for(i=0; i<replication; i++)
        servers[i] = UINT64_MAX;

    for(i=0; i<num_servers; i++)
    {
        svr = placement_index_to_id(i);
        for(j=0; j<replication; j++)
        {
            if(servers[j] == UINT64_MAX || placement_distance_hash(obj, svr, num_servers) < placement_distance_hash(obj, servers[j], num_servers))
            {
                tmp_svr = servers[j];
                servers[j] = svr;
                svr = tmp_svr;
            }
        }
    }

    for(i=0; i<replication; i++)
    {
        server_idxs[i] = placement_id_to_index(servers[i]);
    }

    return;
}



/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
