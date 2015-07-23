/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CODES_STORE_PIPELINE_H
#define CODES_STORE_PIPELINE_H

#include <stdint.h>

typedef struct cs_pipelined_req cs_pipelined_req;
typedef struct cs_pipelined_thread cs_pipelined_thread;

// threaded pipelining of request buffers
struct cs_pipelined_req {
    // remaining bytes of request, decremented when thread inits client rdma
    // so we can properly determine some boundary conditions
    uint64_t rem;
    // actual bytes received of request (recv-based protocols check against this)
    uint64_t received;
    // bytes locally committed to disk (commit-based protocols check against this)
    uint64_t committed;
    // bytes confirmed forwarded to replica servers / clients (write +
    // all_commit checks against as well as committed. read also checks against
    // this, updating when a chunk is confirmed sent)
    uint64_t forwarded;
    // total # of threads, initialized threads, threads waiting on mem, idle
    // threads (for op completion)
    int nthreads, nthreads_init, nthreads_alloc_waiting, nthreads_fin;
    // chunk id to read from next
    int thread_chunk_id_curr;
    // max buffer size per thread
    uint64_t punit_size;
    // thread-specific info
    cs_pipelined_thread *threads;
};

struct cs_pipelined_thread {
    // which chunk the thread is currently processing (-1 -> unset)
    int chunk_id;
    // chunk and pipeline size
    // NOTE: there is probably a more elegant way of tracking these rather than
    // explicitly tracking them
    uint64_t chunk_size;
    uint64_t punit_size;
};

cs_pipelined_req* cs_pipeline_init(
        int nthreads,
        uint64_t punit_size_max,
        uint64_t req_size);

void cs_pipeline_destroy(cs_pipelined_req *req);

#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
