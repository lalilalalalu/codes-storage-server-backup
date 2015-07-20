/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#include "placement-virt.h"
#include "placement.h"
#include "objects.h"
#include "codes/jenkins-hash.h"

static uint64_t placement_distance_virt(uint64_t a, uint64_t b, 
    unsigned int num_servers);
static void placement_find_closest_virt(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int num_servers);
static int virtual_cmp(const void* a, const void *b);
static int nearest_cmp(const void* a, const void *b);

static int partition_factor = 0;
static unsigned int num_servers = 0;

struct server_id
{
    uint64_t virtual_id;
    unsigned long idx;
    struct server_id* first; /* pointer to first element in array */
    unsigned long array_idx;
};

static struct server_id* ring = NULL;

struct placement_mod mod_virt = 
{
    "virt",
    placement_distance_virt,
    placement_find_closest_virt,
    placement_create_striped_random
};

void placement_virt_set_factor(int factor, unsigned int set_num_servers)
{
    unsigned int i, j;
    uint64_t server_increment;
    uint64_t server_id;
    uint32_t h1, h2;

    num_servers = set_num_servers;
    partition_factor = factor;
    assert(partition_factor > 0);
    assert(num_servers > 0);
    
    server_increment = UINT64_MAX / num_servers;

    ring = malloc(num_servers*partition_factor*sizeof(*ring));
    assert(ring);

    for(i=0; i<num_servers; i++)
    {
        for(j=0; j<partition_factor; j++)
        {
            ring[j+i*partition_factor].idx = i;
            server_id = 1+(server_increment*i);
            h1 = j;
            h2 = 0;
            bj_hashlittle2(&server_id, sizeof(server_id), &h1, &h2);
            ring[j+i*partition_factor].virtual_id = 
                h1 + (((uint64_t)h2)<<32);
        }
    }

    qsort(ring, num_servers*partition_factor, sizeof(*ring), virtual_cmp);

    
    for(i=0; i<num_servers*partition_factor; i++)
    {
        ring[i].array_idx = i;
        ring[i].first = ring;
        /* printf("FOO: server_idx:%lu, virtual_id:%lu\n", (long unsigned)ring[i].idx, (long unsigned)ring[i].virtual_id);  */
    }

    return;
}

struct placement_mod* placement_mod_virt(void)
{
    return(&mod_virt);
}

static uint64_t placement_distance_virt(uint64_t a, uint64_t b, 
    unsigned int num_servers)
{
    /* not actually needed for this algorithm */
    assert(0);

    return(0);
}

static void placement_find_closest_virt(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int arg_num_servers)
{
    int i, j;
    struct server_id *svr;
    int current_index;
    int dup;

    assert(num_servers == arg_num_servers);

    /* binary search through ring to find the server with the greatest node ID
     * less than the OID
     */
    svr = bsearch(&obj, ring, num_servers*partition_factor, sizeof(*ring), nearest_cmp);

    /* if bsearch didn't find a match, then the object belongs to the last
     * server partition
     */
    if(!svr)
        svr = &ring[(num_servers*partition_factor)-1];

    current_index = svr->array_idx;

    for(i=0; i<replication; i++)
    {
        if(current_index == num_servers*partition_factor)
            current_index = 0;
        /* we have to skip duplicates */
        do
        {
            dup = 0;
            for(j=0; j<i; j++)
            {
                if(ring[current_index].idx == server_idxs[j])
                {
                    dup = 1;
                    current_index++;
                    if(current_index == num_servers*partition_factor)
                        current_index = 0;
                    break;
                }
            }
        }while(dup);

        server_idxs[i] = ring[current_index].idx;
        current_index++;
    }
}


static int nearest_cmp(const void* key, const void *member)
{
    const uint64_t* obj = key; 
    const struct server_id *svr = member;

    if(*obj < svr->virtual_id)
        return(-1);
    if(*obj > svr->virtual_id)
    {
        /* TODO: fix this.  Logic is wrong; need to stop at last entry in
         * ring, which is num_servers * factor.
         */
        assert(0);
        /* are we on the last server already? */
        if(svr->array_idx == num_servers-1)
            return(0);
        /* is the oid also larger than the next server's id? */
        if(svr->first[svr->array_idx+1].virtual_id < *obj)
            return(1);
    }

    /* otherwise it is in our range */
    return(0);
}

static int virtual_cmp(const void* a, const void *b)
{
    const struct server_id *s_a = a;
    const struct server_id *s_b = b;

    if(s_a->virtual_id < s_b->virtual_id)
        return(-1);
    else if(s_a->virtual_id > s_b->virtual_id)
        return(1);
    else
        return(0);

}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
