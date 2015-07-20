/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#include "placement-multiring.h"
#include "placement.h"
#include "objects.h"
#include "codes/jenkins-hash.h"

static uint64_t placement_distance_multiring(uint64_t a, uint64_t b, 
    unsigned int num_servers);
static void placement_find_closest_multiring(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int num_servers);
static int virtual_cmp(const void* a, const void *b);
static int nearest_cmp(const void* a, const void *b);
static void placement_create_striped_multiring(unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes);

static int num_rings = 0;
static unsigned int num_servers = 0;

struct server_id
{
    uint64_t virtual_id;
    unsigned long idx;
    struct server_id* first; /* pointer to first element in array */
    unsigned long array_idx;
};

struct ring
{
    int num_servers;
    struct server_id* servers;
};

static struct ring* multirings = NULL;

struct placement_mod mod_multiring = 
{
    "multiring",
    placement_distance_multiring,
    placement_find_closest_multiring,
    placement_create_striped_multiring
};

void placement_multiring_set_rings(int rings, unsigned int set_num_servers)
{
    unsigned int i, j;
    uint64_t server_increment;
    uint64_t server_id;
    uint32_t h1, h2;

    num_servers = set_num_servers;
    num_rings = rings;
    assert(num_rings > 0);
    assert(num_servers > 0);
    
    server_increment = UINT64_MAX / num_servers;

    multirings = malloc(num_rings*sizeof(*multirings));
    assert(multirings);

    for(i=0; i<num_rings; i++)
    {
        multirings[i].num_servers = num_servers;
        multirings[i].servers = malloc(num_servers*sizeof(*multirings[i].servers));
        assert(multirings[i].servers);

        for(j=0; j<num_servers; j++)
        {
            multirings[i].servers[j].idx = j;
            server_id = 1+(server_increment*j);
            h1 = i;
            h2 = 0;
            bj_hashlittle2(&server_id, sizeof(server_id), &h1, &h2);
            multirings[i].servers[j].virtual_id = 
                h1 + (((uint64_t)h2)<<32);
            /* printf("FOO: ring:%u, server_id:%lu, server_idx:%lu, virtual_id:%lu\n", i, (long unsigned)server_id, (long unsigned)j, (long unsigned)multirings[i].servers[j].virtual_id); */
        }
        qsort(multirings[i].servers, num_servers, sizeof(*multirings[i].servers), virtual_cmp);

        for(j=0; j<num_servers; j++)
        {
            multirings[i].servers[j].array_idx = j;
            multirings[i].servers[j].first = multirings[i].servers;
            /* printf("FOO: ring:%u, server_idx:%lu, virtual_id:%lu\n", i, (long unsigned)multirings[i].servers[j].idx, (long unsigned)multirings[i].servers[j].virtual_id); */
        }

    }

    return;
}

struct placement_mod* placement_mod_multiring(void)
{
    return(&mod_multiring);
}

static uint64_t placement_distance_multiring(uint64_t a, uint64_t b, 
    unsigned int num_servers)
{
    /* not actually needed for this algorithm */
    assert(0);

    return(0);
}

static void placement_find_closest_multiring(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int arg_num_servers)
{
    struct server_id *svr;
    int current_index;
    int i;
    /* NOTE: there are other methods of partitioning objects across rings;
     * for now we assuming object IDs are randomly distributed and modulo
     * will work just fine.
     */
    int ring = obj % num_rings;

    assert(num_servers == arg_num_servers);

    /* binary search through ring to find the server with the greatest node ID
     * less than the OID
     */
    svr = bsearch(&obj, multirings[ring].servers, num_servers, sizeof(*multirings[ring].servers), nearest_cmp);

    /* if bsearch didn't find a match, then the object belongs to the last
     * server
     */
    if(!svr)
        svr = &multirings[ring].servers[num_servers-1];

    current_index = svr->array_idx;

    for(i=0; i<replication; i++)
    {
        if(current_index == num_servers)
            current_index = 0;

        server_idxs[i] = multirings[ring].servers[current_index].idx;
        current_index++;
    }

    return;
}


static int nearest_cmp(const void* key, const void *member)
{
    const uint64_t* obj = key; 
    const struct server_id *svr = member;

    if(*obj < svr->virtual_id)
        return(-1);
    if(*obj > svr->virtual_id)
    {
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

static void placement_create_striped_multiring(unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes)
{
    int ring = random() % num_rings;
    int ring_idx = random() % num_servers;
    unsigned int stripe_width;
    int i;
    unsigned long size_left = file_size;
    unsigned long full_stripes;
    uint64_t range, oid_offset;
    unsigned long check_size = 0;

    assert(replication*max_stripe_width <= num_servers);

    /* how many objects to use */
    stripe_width = file_size / strip_size + 1;
    if(file_size % strip_size == 0)
        stripe_width--;
    if(stripe_width > max_stripe_width)
        stripe_width = max_stripe_width;
    *num_objects = stripe_width;

    /* size of each object */
    full_stripes = size_left/(stripe_width*strip_size);
    size_left -= full_stripes * stripe_width * strip_size;
    for(i=0; i<stripe_width; i++)
    {
        /* handle full stripes */
        sizes[i] = full_stripes*strip_size;
        /* handle remainder */
        if(size_left > 0)
        {
            if(size_left > strip_size)
            {
                sizes[i] += strip_size;
                size_left -= strip_size;
            }
            else
            {
                sizes[i] += size_left;
                size_left = 0;
            }
        }
        check_size += sizes[i];
    }
    assert(check_size == file_size);
    assert(size_left == 0);

    /* oid of each object */
    for(i=0; i<stripe_width; i++)
    {
        /* figure out size of object interval for this server on this ring */
        if(ring_idx < (num_servers-1))
            range = multirings[ring].servers[ring_idx+1].virtual_id - multirings[ring].servers[ring_idx].virtual_id;
        else
            range = UINT64_MAX - multirings[ring].servers[ring_idx].virtual_id
                + multirings[ring].servers[0].virtual_id;

        /* divide by num_rings to account for the fact that objects are
         * partitioned over each ring
         */
        range /= num_rings;
        /* conservatively reduce range to account for math skew below */
        range -= 3;

        /* pick oid offset within range as random number within range */
        oid_offset = random_u64() % range;
        /* calculate true oid based on offset */
        oids[i] = (multirings[ring].servers[ring_idx].virtual_id + (oid_offset+1)*num_rings);
        /* round down to an oid that falls in this ring */
        oids[i] -= oids[i]%num_rings;
        oids[i] += ring;

        ring_idx = (ring_idx + replication) % num_servers;
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
