/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <assert.h>

#include "placement-euclid-1d.h"
#include "placement.h"
#include "objects.h"

static uint64_t placement_distance_1d(uint64_t a, uint64_t b, 
    unsigned int num_servers);
static void placement_find_closest_1d(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int num_servers);

struct placement_mod mod_1d = 
{
    "1d",
    placement_distance_1d,
    placement_find_closest_1d,
    placement_create_striped_random
};

struct placement_mod* placement_mod_euclid_1d(void)
{
    return(&mod_1d);
}

static uint64_t placement_distance_1d(uint64_t a, uint64_t b, 
    unsigned int num_servers)
{
    uint64_t higher;
    uint64_t lower;
    uint64_t diff1;
    uint64_t diff2;

    /* figure out wich number is higher */
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

    /* normal case */
    diff1 = higher-lower;

    /* wrap-around 0 case */
    diff2 = (UINT64_MAX-higher) + lower;

    if(diff1 < diff2)
        return(diff1);
    else
        return(diff2);

    return(0);
}

static unsigned int svr_offset(int offset, unsigned int server_idx, unsigned int num_servers)
{
    unsigned int svr;

    if(offset < 0)
        offset = num_servers + offset;

    svr = (offset + server_idx) % num_servers;

    return(svr);
}

static void placement_find_closest_1d(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int num_servers)
{
    unsigned int svr_idx;
    int over = 0;
    int search_offset = 1;
    int i;
#if 0
    uint64_t dist = 0;
#endif
    uint64_t server_increment = UINT64_MAX / num_servers;
    
    svr_idx = obj/server_increment;
    if(placement_distance_1d(placement_index_to_id(svr_idx), obj, num_servers) >
        placement_distance_1d(placement_index_to_id(svr_idx), obj, num_servers))
    {
        svr_idx = svr_offset(1, svr_idx, num_servers);
        over = 1;
    }
    server_idxs[0] = svr_idx;

    for(i=1; i<replication; i++)
    {
        if(over)
            search_offset = -((i-1)/2 + 1);
        else
            search_offset = (i-1)/2 + 1;
        server_idxs[i] = svr_offset(search_offset, svr_idx, num_servers);
        over = !over;
    }

    /* safety check */
#if 0
    for(i=0; i<replication; i++)
    {
        assert(distance(obj, servers[i]) >= dist);
        dist = distance(obj, servers[i]);
    }
#endif
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
