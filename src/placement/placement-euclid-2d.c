/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <assert.h>
#include <math.h>

#include "placement-euclid-2d.h"
#include "placement.h"
#include "objects.h"

static uint64_t placement_distance_2d(uint64_t a, uint64_t b, 
    unsigned int num_servers);
static void placement_find_closest_2d(uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs, unsigned int num_servers);

struct placement_mod mod_2d = 
{
    "2d",
    placement_distance_2d,
    placement_find_closest_2d,
    placement_create_striped_random
};

struct placement_mod* placement_mod_euclid_2d(void)
{
    return(&mod_2d);
}

static uint64_t placement_distance_2d(uint64_t a, uint64_t b, 
    unsigned int num_servers)
{
    double x1, x2, y1, y2, dist;

    x1 = a & 0xFFFFFFFF;
    x2 = b & 0xFFFFFFFF;
    y1 = (a >> 32) & 0xFFFFFFFF;
    y2 = (b >> 32) & 0xFFFFFFFF;

    dist = sqrt(pow(x1-x2, 2.0) + pow(y1-y2, 2.0));

    return(dist);
}

static void placement_find_closest_2d(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int num_servers)
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
            if(servers[j] == UINT64_MAX || placement_distance_2d(obj, svr, num_servers) < placement_distance_2d(obj, servers[j], num_servers))
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
