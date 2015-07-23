/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include "codes-store-pipeline.h"
#include <stdlib.h>
#include <assert.h>

cs_pipelined_req* cs_pipeline_init(
        int nthreads,
        uint64_t punit_size_max,
        uint64_t req_size)
{
    cs_pipelined_req *r;
    if (nthreads <= 0){
        return NULL;
    }

    r = malloc(sizeof(cs_pipelined_req));
    assert(r != NULL);

    r->nthreads = nthreads;
    r->nthreads_init = 0;
    r->nthreads_alloc_waiting = 0;
    r->nthreads_fin = 0;
    r->thread_chunk_id_curr = 0;

    r->thread_chunk_id_curr = 0;

    r->punit_size = punit_size_max;

    r->rem = req_size;
    r->received = 0;
    r->committed = 0;
    r->forwarded = 0;

    r->threads = calloc(nthreads, sizeof(cs_pipelined_thread));
    assert(r->threads != NULL);

    // 0-initialize is OK for all except the chunk ids, which need to be -1
    for (int i = 0; i < nthreads; i++){
        r->threads[i].chunk_id = -1;
    }

    return r;
}

void cs_pipeline_destroy(cs_pipelined_req *req){
    free(req->threads);
    free(req);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
