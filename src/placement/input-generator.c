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

#include "input-generator.h"
#include "objects.h"
#include "placement.h"
#include "comb.h"

#define PRINT_PROGRESS 1

#define LOW_MEM 0

/* TODO: make max replication factor (size of array in each obj struct)
 * configurable
 */
/* TODO: make file format portable across architectures */

struct options
{
    unsigned int num_servers;
    unsigned int max_objs;
    long unsigned int max_bytes;
    unsigned int replication;
    int print_objs;
    char* type;
    char* placement;
    char* filename;
    char* comb_name;
    char* params;
};

struct server_data
{
    unsigned int server_index;
    uint64_t server_id;
    /* objects that might reside on this server during the course of a simulation */
    unsigned int potential_obj_count; 
    struct obj* potential_obj_array;
};

struct comb_stats {
    unsigned long count;
    unsigned long bytes;
};

static int comb_cmp (const void *a, const void *b);

static int usage (char *exename);
static struct options *parse_args(int argc, char *argv[]);
static void write_all(
        int fd, 
        unsigned long total_obj_count, 
        struct obj *total_objs,
        struct server_data *server_array,
        struct server_idx_infile* server_idx_array,
        unsigned int *server_array_cnt,
        struct options* ig_opts);

int main(
    int argc,
    char **argv)
{
    struct options *ig_opts = NULL;
    unsigned long total_byte_count = 0;
    unsigned long total_obj_count = 0;
    struct obj* total_objs = NULL;
    struct server_data* server_array;
    unsigned long i;
    int j;
    unsigned int* server_array_cnt;
    int fd, fd_comb;
    char header[1024];
    int header_index = 0;
    int ret;
    struct server_idx_infile* server_idx_array;
    unsigned int server_idx;
    unsigned long obj_idx;

    /* counts of combinations of servers that objects reside on*/
    int do_comb = 0;
    struct comb_stats *cs;
    uint64_t num_combs;
    /* temporary to hold sorted combinations - we don't want to influence object order which may affect results */
    unsigned long *comb_tmp;

#if PRINT_PROGRESS
    unsigned long progress = 0;
    unsigned long prog_cnt;
    int percentage;
#endif

    ig_opts = parse_args(argc, argv);
    if(!ig_opts)
    {
        usage(argv[0]);
        return(-1);
    }

    placement_set(ig_opts->placement, ig_opts->num_servers);

    /* open output files */
    if (ig_opts->print_objs){
        fd = open(ig_opts->filename, O_WRONLY|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR|S_IRGRP);
        if(fd < 0)
        {
            perror("open");
            return(-1);
        }
    }

    if (ig_opts->comb_name){
        fd_comb = open(ig_opts->comb_name, O_WRONLY|O_CREAT|O_EXCL, 
                S_IRUSR|S_IWUSR|S_IRGRP);
        if(fd_comb < 0) {
            perror("open");
            return(-1);
        }
        else{
            do_comb = 1;
        }
    }

    /* generate global object list (no replication or placement, just list
     * of object IDs that will populate the storage system)
     */
    /* TODO: make random seed configurable */
    printf("Generating up to %u objects...\n", ig_opts->max_objs);
    objects_generate(ig_opts->type, ig_opts->max_objs, ig_opts->max_bytes,
        8675309, ig_opts->replication, ig_opts->num_servers, ig_opts->params,
        &total_byte_count, &total_obj_count, &total_objs);

    printf("Produced %lu objects with %lu total bytes (not counting replication).\n", total_obj_count, total_byte_count);

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

    /* set up per-server-combination counters */
    if (do_comb){
        num_combs = choose(ig_opts->num_servers, ig_opts->replication);
        cs = calloc(num_combs, sizeof(*cs));
        comb_tmp = malloc(ig_opts->replication*sizeof(*comb_tmp));
        printf("Combinations for %u servers and %u replication: %lu\n",
                ig_opts->num_servers, ig_opts->replication, num_combs);
    }

    for(i=0; i<ig_opts->num_servers; i++)
    {
        server_array[i].server_index = i;
        server_array[i].server_id = placement_index_to_id(i);
    }

    /* Iterate through global object list and calculate placement for each
     * object
     */
    printf("Calculating server placements\n");
#if PRINT_PROGRESS
    printf("Progress: 0%%\n");
    fflush(stdout);
    progress = 0;
    percentage = 0;
    prog_cnt = total_obj_count / 10ul + (total_obj_count%10ul > 0);
#endif
    for(i=0; i<total_obj_count; i++)
    {
#if PRINT_PROGRESS
        if (i - progress >= prog_cnt){
            progress += prog_cnt;
            percentage += 10;
            printf("...%d%%\n", percentage);
            fflush(stdout);
        }
#endif
        placement_find_closest(total_objs[i].oid, MAX_REPLICATION, total_objs[i].server_idxs);
        /* compute the index corresponding to this combination of servers */
        if (do_comb){
            memcpy(comb_tmp, total_objs[i].server_idxs, 
                    ig_opts->replication*sizeof(*comb_tmp));
            rev_ins_sort(ig_opts->replication, comb_tmp);
            uint64_t idx = comb_index(ig_opts->replication, comb_tmp);
            cs[idx].count++;
            cs[idx].bytes += total_objs[i].size;
        }

        for(j=0; j<MAX_REPLICATION && j<(total_objs[i].replication*2-1); j++)
        {
            /* for each server placement, increment potential_obj_count for
             * that server
             */
            server_array[total_objs[i].server_idxs[j]].potential_obj_count++;
        }
    }
#if PRINT_PROGRESS
    printf("...100%%\n");
    fflush(stdout);
#endif

    printf("Potential object count distribution:\n");
    for(i=0; i<ig_opts->num_servers; i++)
    {
        printf("svr %lu: %u\n", i, server_array[i].potential_obj_count);

        /* set up indexes that will be stored in file */
        server_idx_array[i].server_index = i;
        server_idx_array[i].obj_count = server_array[i].potential_obj_count;
        if(i==0)
            server_idx_array[i].offset = 1024 + ig_opts->num_servers*sizeof(*server_idx_array);
        else
            server_idx_array[i].offset = server_idx_array[i-1].offset + server_idx_array[i-1].obj_count*sizeof(*total_objs);

    }

    /* LOW MEMORY VERSION OF WRITING SERVER-OBJECT LISTS TO FILE */
#if LOW_MEM
    if (ig_opts->print_objs){
        unsigned long idx_size = ig_opts->num_servers*sizeof(*server_idx_array);
        /* skip the header (INCLUDING server indexes) and drop into 
         * the write */
        lseek(fd, 1024+idx_size, SEEK_SET);
        write_all(fd, total_obj_count, total_objs, server_array, 
                server_idx_array, server_array_cnt, ig_opts);
        /* go back and write the header */
        lseek(fd, 0, SEEK_SET);
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
        ret = write(fd, server_idx_array, idx_size);
        assert(ret == idx_size);
        close(fd);
    }

    /* ORIGINAL VERSION OF WRITING SERVER-OBJECT LISTS TO FILE */
#else
    /* second pass not necessary if not printing to file */
    if (ig_opts->print_objs){
        /* second pass through objects to assign them to per-server arrays */
        printf("Reorganizing objects/replicas by server\n");
#if PRINT_PROGRESS
        printf("Progress: 0%%");
        fflush(stdout);
        progress = 0;
        percentage = 0;
#endif
        for (i=0; i<ig_opts->num_servers; i++){
            /* allocate array to hold potential objects on each server */
            server_array[i].potential_obj_array = malloc(server_array[i].potential_obj_count * sizeof(*server_array[i].potential_obj_array));
            assert(server_array[i].potential_obj_array);
        }
        for(i=0; i<total_obj_count; i++)
        {
#if PRINT_PROGRESS
            if (i - progress >= prog_cnt){            
                progress += prog_cnt;
                percentage += 10;
                printf("...%d%%", percentage);
                fflush(stdout);
            }
#endif
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
#if PRINT_PROGRESS
        printf("...100%%\n");
        fflush(stdout);
#endif 

        /* we don't need the global list any more */
        total_obj_count = 0;
        total_byte_count = 0;

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
        printf("Writing per-server object lists\n");
#if PRINT_PROGRESS
        printf("Progress: 0%%");
        fflush(stdout);
        progress = 0;
        prog_cnt = ig_opts->num_servers / 10ul + (ig_opts->num_servers%10ul > 0);
        percentage = 0;
#endif

        for(i=0; i<ig_opts->num_servers; i++)
        {
#if PRINT_PROGRESS
            if (i - progress >= prog_cnt){            
                progress += prog_cnt;
                percentage += 10;
                printf("...%d%%", percentage);
                fflush(stdout);
            }
#endif
#if 0
            for(j=0; j<server_array[i].potential_obj_count; j++)
            {
                printf("FOO: object id: %lu\n", server_array[i].potential_obj_array[j].oid);
            }
#endif
            ret = write(fd, server_array[i].potential_obj_array, server_array[i].potential_obj_count*sizeof(*server_array[i].potential_obj_array));
            assert(ret == server_array[i].potential_obj_count*sizeof(*server_array[i].potential_obj_array));
        }
#if PRINT_PROGRESS
        printf("...100%%\n");
        fflush(stdout);
#endif

        close(fd);
    }
#endif
    free(total_objs);

    /* print out the counts of used combinations */
    if (do_comb){
        int sz = 1<<20;
        char *buf = malloc(sz);
        int written = 0;
        uint64_t total = 0;
        uint64_t num_zeros;

        printf("Sorting/writing server combinations\n");
        qsort(cs, num_combs, sizeof(*cs), comb_cmp);

        /* find the number of 0 entries - we aren't printing them */
        for (num_zeros = 0;
             cs[num_zeros].count == 0 && num_zeros < num_combs;
             num_zeros++);

        /* print the header - the number of possible combinations and the
         * number of non-zero entries */
        written = snprintf(buf+written, sz, "%lu %lu\n", num_combs,
                num_combs-num_zeros);
        assert(written < sz);

        /* start the counter where we left off */
        total = num_zeros;
        while (total < num_combs){
            int w = snprintf(buf+written, sz-written, "%lu %lu\n", 
                    cs[total].count, cs[total].bytes);
            if (w >= sz-written){
                ret = write(fd_comb, buf, written);
                assert(ret == written);
                written=0;
            }
            else{
                written += w;
                total++;
            }
        }
        if (written > 0){
            ret = write(fd_comb, buf, written);
            assert(ret == written);
        }
    }

    close(fd_comb);

    return(0);
}

static void write_all(
        int fd, 
        unsigned long total_obj_count, 
        struct obj *total_objs,
        struct server_data *server_array,
        struct server_idx_infile* server_idx_array,
        unsigned int *server_array_cnt,
        struct options* ig_opts){
    unsigned long i,s;
    int j;
    unsigned long progress, prog_cnt;
    int ret;
    int percentage;
    unsigned long obj_idx;

    printf("# Writing objects/replicas by server\n");
#if PRINT_PROGRESS
        printf("# Progress: 0%%");
        fflush(stdout);
        progress = 0;
        percentage = 0;
        prog_cnt = ig_opts->num_servers / 10ul + (ig_opts->num_servers%10ul > 0);
#endif
    for (s = 0; s < ig_opts->num_servers; s++){
#if PRINT_PROGRESS
        if (s - progress >= prog_cnt){            
            progress += prog_cnt;
            percentage += 10;
            printf("...%d%%", percentage);
            if (percentage==100) { printf("\n"); }
            fflush(stdout);
        }
#endif
        server_array[s].potential_obj_array = malloc(server_array[s].potential_obj_count * sizeof(*server_array[s].potential_obj_array));
        assert(server_array[s].potential_obj_array);
        /* set up indexes that will be stored in file */
        server_idx_array[s].server_index = s;
        server_idx_array[s].obj_count = server_array[s].potential_obj_count;
        if(s==0)
            server_idx_array[s].offset = 1024 + ig_opts->num_servers*sizeof(*server_idx_array);
        else
            server_idx_array[s].offset = server_idx_array[s-1].offset + server_idx_array[s-1].obj_count*sizeof(*total_objs);


        for(i=0; i<total_obj_count; i++)
        {
            for(j=0; j<MAX_REPLICATION  && j<(total_objs[i].replication*2-1); j++)
            {
                assert(total_objs[i].server_idxs[j] < ig_opts->num_servers);
                if (s != total_objs[i].server_idxs[j]) continue;

                obj_idx = server_array_cnt[s];
                assert(obj_idx < server_array[s].potential_obj_count);

                server_array[s].potential_obj_array[obj_idx] = total_objs[i];
                server_array_cnt[s]++;
            }
        }

        /* now have all of this servers objects, print out 
         * NOTE: assumes fd past the header */
        ret = write(fd, server_array[s].potential_obj_array, server_array[s].potential_obj_count*sizeof(*server_array[s].potential_obj_array));
        assert(ret == server_array[s].potential_obj_count*sizeof(*server_array[s].potential_obj_array));

        /* now free the used memory */
        free(server_array[s].potential_obj_array);
    }
}

int comb_cmp (const void *a, const void *b){
    unsigned long au = ((struct comb_stats*)a)->count;
    unsigned long bu = ((struct comb_stats*)b)->count; 
    int rtn;
    if (au < bu)       rtn = -1;
    else if (au == bu) rtn = 0;
    else               rtn = 1;
    return rtn;
}

static int usage (char *exename)
{
    fprintf(stderr, "Usage: %s [options] <output-filename>\n", exename);
    fprintf(stderr, "    -s <number of servers>\n");
    fprintf(stderr, "    -o <max number of objects>\n");
    fprintf(stderr, "    -b <max number of bytes>\n");
    fprintf(stderr, "    -r <replication factor>\n");
    fprintf(stderr, "    -t <object generator: basic/random/hist_stripe/hist_hadoop>\n");
    fprintf(stderr, "    -p <placement algorithm>\n");
    fprintf(stderr, "    -x <parameters to data set generator, optional>\n");
    fprintf(stderr, "    -c <filename for server combination counts, optional>\n");
    fprintf(stderr, "    -i disables printing the objects themselves,\n"
                    "       if only the combinatorial results are wanted\n");

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
    opts->print_objs=1;

    while((one_opt = getopt(argc, argv, "s:o:b:r:t:hp:x:c:i")) != EOF)
    {
        switch(one_opt)
        {
            case 's':
                ret = sscanf(optarg, "%u", &opts->num_servers);
                if(ret != 1)
                    return(NULL);
                break;
            case 'o':
                ret = sscanf(optarg, "%u", &opts->max_objs);
                if(ret != 1)
                    return(NULL);
                break;
            case 'b':
                ret = sscanf(optarg, "%lu", &opts->max_bytes);
                if(ret != 1)
                    return(NULL);
                break;
            case 'r':
                ret = sscanf(optarg, "%u", &opts->replication);
                if(ret != 1)
                    return(NULL);
                break;
            case 't':
                opts->type = strdup(optarg);
                if(!opts->type)
                    return(NULL);
                break;
            case 'p':
                opts->placement = strdup(optarg);
                if(!opts->placement)
                    return(NULL);
                break;
            case 'x':
                opts->params = strdup(optarg);
                if(!opts->params)
                    return(NULL);
                break;
            case 'c':
                opts->comb_name = strdup(optarg);
                if (!opts->comb_name)
                    return(NULL);
                break;
            case 'i':
                opts->print_objs = 0;
                break;
            case '?':
                usage(argv[0]);
                exit(1);
        }
    }

    if(optind != argc-1)
    {
        return(NULL);
    }
    opts->filename = strdup(argv[optind]);

    if(opts->replication < 2)
        return(NULL);
    if(opts->num_servers < (opts->replication+1))
        return(NULL);
    if(opts->max_objs < 1)
        return(NULL);
    if(!opts->placement)
        return(NULL);
    if(!opts->type)
        return(NULL);

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
