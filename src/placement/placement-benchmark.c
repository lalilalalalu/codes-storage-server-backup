/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/time.h>

#include "../placement/objects.h"
#include "../placement/placement.h"

struct options
{
    unsigned int num_servers;
    unsigned int num_objs;
    unsigned int replication;
    char* placement;
};

static int usage (char *exename);
static struct options *parse_args(int argc, char *argv[]);

#define OBJ_ARRAY_SIZE 5000000UL

static double Wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)(t.tv_usec) / 1000000);
}

int main(
    int argc,
    char **argv)
{
    struct options *ig_opts = NULL;
    unsigned long total_byte_count = 0;
    unsigned long total_obj_count = 0;
    struct obj* total_objs = NULL;
    unsigned int i;
    double t1, t2;

    ig_opts = parse_args(argc, argv);
    if(!ig_opts)
    {
        usage(argv[0]);
        return(-1);
    }

    placement_set(ig_opts->placement, ig_opts->num_servers);

    /* generate random set of objects for testing */
    printf("# WARNING: remember to compile this with -O2 at least.\n");
    printf("# Generating random object IDs...\n");
    objects_generate("random", OBJ_ARRAY_SIZE, ULONG_MAX,
        8675309, ig_opts->replication, ig_opts->num_servers,
        NULL,
        &total_byte_count, &total_obj_count, &total_objs);
    printf("# Done.\n");

    assert(total_obj_count == OBJ_ARRAY_SIZE);

    sleep(1);

    printf("# Running placement benchmark...\n");
    /* run placement benchmark */
    t1 = Wtime();
#pragma omp parallel for
    for(i=0; i<ig_opts->num_objs; i++)
    {
        placement_find_closest(total_objs[i%OBJ_ARRAY_SIZE].oid, ig_opts->replication, total_objs[i%OBJ_ARRAY_SIZE].server_idxs);
    }
    t2 = Wtime();
    printf("# Done.\n");

    printf("# <objects>\t<replication>\t<servers>\t<algorithm>\t<time (s)>\n");
    printf("%u\t%d\t%u\t%s\t%f\n",
        ig_opts->num_objs,
        ig_opts->replication,
        ig_opts->num_servers,
        ig_opts->placement,
        t2-t1);

    /* we don't need the global list any more */
    free(total_objs);
    total_obj_count = 0;
    total_byte_count = 0;

#if 0
    char* gen_name = NULL;
    struct server_data* server_array;
    unsigned int i;
    int j;
    unsigned int* server_array_cnt;
    int fd;
    char header[1024];
    int header_index = 0;
    int ret;
    struct server_idx_infile* server_idx_array;
    unsigned int server_idx;
    unsigned long obj_idx;


    /* set up array to track per-server object data */
    server_array = malloc(ig_opts->num_servers*sizeof(*server_array));
    assert(server_array);
    memset(server_array, 0, ig_opts->num_servers*sizeof(*server_array));
    server_array_cnt = malloc(ig_opts->num_servers*sizeof(*server_array_cnt));
    assert(server_array_cnt);
    memset(server_array_cnt, 0, ig_opts->num_servers*sizeof(*server_array_cnt));
    server_idx_array = malloc(ig_opts->num_servers*sizeof(*server_idx_array));
    assert(server_idx_array);
    memset(server_idx_array, 0, ig_opts->num_servers*sizeof(*server_idx_array));

    for(i=0; i<ig_opts->num_servers; i++)
    {
        server_array[i].server_index = i;
        server_array[i].server_id = placement_index_to_id(i);
    }

    printf("Potential object count distribution:\n");
    for(i=0; i<ig_opts->num_servers; i++)
    {
        printf("svr %u: %u\n", i, server_array[i].potential_obj_count);
        /* allocate array to hold potential objects on each server */
        server_array[i].potential_obj_array = malloc(server_array[i].potential_obj_count * sizeof(*server_array[i].potential_obj_array));
        assert(server_array[i].potential_obj_array);

        /* set up indexes that will be stored in file */
        server_idx_array[i].server_index = i;
        server_idx_array[i].obj_count = server_array[i].potential_obj_count;
        if(i==0)
            server_idx_array[i].offset = 1024 + ig_opts->num_servers*sizeof(*server_idx_array);
        else
            server_idx_array[i].offset = server_idx_array[i-1].offset + server_idx_array[i-1].obj_count*sizeof(*total_objs);

    }

    /* second pass through objects to assign them to per-server arrays */
    for(i=0; i<total_obj_count; i++)
    {
        for(j=0; j<MAX_REPLICATION  && j<(total_objs[i].replication*2-1); j++)
        {
            server_idx = total_objs[i].server_idxs[j];
            assert(server_idx < ig_opts->num_servers);

            obj_idx = server_array_cnt[server_idx];
            assert(obj_idx < server_array[server_idx].potential_obj_count);

            server_array[server_idx].potential_obj_array[obj_idx] = total_objs[i];
            server_array_cnt[server_idx]++;
        }
    }

    /* write header to file */
    memset(header, 0, 1024);
    ret = sprintf(&header[header_index], "v:0.1 ");
    header_index += ret;
    for(j=0; j<argc; j++)
    {
        ret = sprintf(&header[header_index], "%s ", argv[j]);
        header_index += ret;
    }

    ret = write(fd, header, 1024);
    assert(ret == 1024);

    /* write indexes indicating where to find object list for each server */
    ret = write(fd, server_idx_array, ig_opts->num_servers*sizeof(*server_idx_array));
    assert(ret == ig_opts->num_servers*sizeof(*server_idx_array));

    /* write object list for each server */
    for(i=0; i<ig_opts->num_servers; i++)
    {
#if 0
        for(j=0; j<server_array[i].potential_obj_count; j++)
        {
            printf("FOO: object id: %lu\n", server_array[i].potential_obj_array[j].oid);
        }
#endif
        ret = write(fd, server_array[i].potential_obj_array, server_array[i].potential_obj_count*sizeof(*server_array[i].potential_obj_array));
        assert(ret == server_array[i].potential_obj_count*sizeof(*server_array[i].potential_obj_array));
    }

    close(fd);

#endif

    return(0);
}

static int usage (char *exename)
{
    fprintf(stderr, "Usage: %s [options]\n", exename);
    fprintf(stderr, "    -s <number of servers>\n");
    fprintf(stderr, "    -o <number of objects>\n");
    fprintf(stderr, "    -r <replication factor>\n");
    fprintf(stderr, "    -p <placement algorithm>\n");

    exit(1);
}

static struct options *parse_args(int argc, char *argv[])
{
    struct options *opts = NULL;
    int ret = -1;
    int one_opt = 0;

    opts = (struct options*)malloc(sizeof(*opts));
    if(!opts)
        return(NULL);
    memset(opts, 0, sizeof(*opts));

    while((one_opt = getopt(argc, argv, "s:o:r:hp:")) != EOF)
    {
        switch(one_opt)
        {
            case 's':
                ret = sscanf(optarg, "%u", &opts->num_servers);
                if(ret != 1)
                    return(NULL);
                break;
            case 'o':
                ret = sscanf(optarg, "%u", &opts->num_objs);
                if(ret != 1)
                    return(NULL);
                break;
            case 'r':
                ret = sscanf(optarg, "%u", &opts->replication);
                if(ret != 1)
                    return(NULL);
                break;
            case 'p':
                opts->placement = strdup(optarg);
                if(!opts->placement)
                    return(NULL);
                break;
            case '?':
                usage(argv[0]);
                exit(1);
        }
    }

    if(opts->replication < 2)
        return(NULL);
    if(opts->num_servers < (opts->replication+1))
        return(NULL);
    if(opts->num_objs < 1)
        return(NULL);
    if(!opts->placement)
        return(NULL);

    assert(opts->replication <= MAX_REPLICATION);

    return(opts);
}


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
