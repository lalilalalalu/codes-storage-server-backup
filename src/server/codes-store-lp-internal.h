/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CODES_STORE_LP_INTERNAL_H
#define CODES_STORE_LP_INTERNAL_H

#include <ross.h>
#include <codes/codes-store-lp.h>
#include <codes/codes-callback.h>
#include "codes/lp-io.h"
#include "codes/quicklist.h"
#include "codes/lp-msg.h"
#include "codes/resource-lp.h"
#include "codes-store-pipeline.h"

#define CS_REQ_CONTROL_SZ 128

extern int cs_magic;

#define CS_EVENTS \
    X(CS_RECV_CLI_REQ,            recv_cli_req, = 20) \
    X(CS_PIPELINE_ALLOC_CALLBACK, palloc_callback, ) \
    X(CS_RECV_CHUNK,              recv_chunk, ) \
    X(CS_COMPLETE_DISK_OP,        complete_disk_op, ) \
    X(CS_COMPLETE_CHUNK_SEND,     complete_chunk_send, )

#define X(a,b,c) a,
enum cs_event_type {
    CS_EVENTS
};
#undef X

// a wrapper for a pipelined op id and a thread id
typedef struct cs_callback_id {
    int op_id; // pipelined op id
    int tid;   // thread id for particular event
} cs_callback_id;

// event structures
struct ev_recv_cli_req {
    struct codes_store_request req;
    struct codes_cb_params callback;
    struct {
        int op_id; // needed to map to created op id
    } rc;
};

struct ev_palloc_callback{
    resource_callback cb;
    cs_callback_id id;
    struct {
        // see handle_pipeline_alloc_callback for usage
        int nthreads_init;
        int nthreads_fin;
    } rc;
};

struct ev_recv_chunk {
    cs_callback_id id;
};

struct ev_complete_disk_op {
    cs_callback_id id;
    // data vs. metadata req - controls which queue will be searched
    int is_data_op;
    struct {
        int chunk_id;
        uint64_t chunk_size;
    } rc;
};

struct ev_complete_chunk_send{
    cs_callback_id id;
    struct {
        int chunk_id;
        uint64_t chunk_size;
    } rc;
};

#define X(a,b,c) struct ev_##b b;
struct cs_msg {
    msg_header h;

    union {
        CS_EVENTS
    } u;
};
#undef X

typedef struct cs_msg cs_msg;

// sugar for extracting a pointer to a specific msg type
#define GETEV(_result_var, _evar_ptr, _etype) \
    struct ev_##_etype *_result_var = &(_evar_ptr)->u._etype

#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
