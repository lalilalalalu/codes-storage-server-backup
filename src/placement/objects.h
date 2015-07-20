/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef OBJECTS_H
#define OBJECTS_H

#include <stdint.h>

/* maximum replication factor */
#define MAX_REPLICATION 4

/* describes an object */
struct obj
{
    uint64_t oid;          /* identifier */
    unsigned int replication;  /* replication factor */
    uint64_t size;         /* size of object */
    unsigned long server_idxs[MAX_REPLICATION]; /* cached placement data */
};

void objects_sort(struct obj* objs, unsigned int objs_count);

void objects_randomize(struct obj* objs, unsigned int objs_count, unsigned int seed);

/* generates an array of objects to populate the system */
/* should be called once per MPI process; data can be shared among all LPs
 * on that process.
 */
void objects_generate(char* gen_name, 
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_obj_count,
    struct obj** total_objs);

/* generates random 64 bit numbers */
uint64_t random_u64(void);

#endif /* OBJECTS_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
