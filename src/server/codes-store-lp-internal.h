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

extern int cs_magic; // needed by msg-v2.c
extern int replication_factor; // needed by client (for computing placement)

typedef struct cs_msg cs_msg;

enum cs_event_type {
    // receive an initial request from a client 
    // - primary-server only
    RECV_CLI_REQ = 20,
    // allocation succeeded for a unit of the pipeline
    // - primary-server only
    PIPELINE_ALLOC_CALLBACK,
    // pipeline unit filled with data from client/other server
    RECV_CHUNK,
    // a server made a request to write a pipeline unit's worth of data
    // - non-primary-server only
    RECV_SRV_REQ,
    // local storage operation completed
    // - all servers
    COMPLETE_DISK_OP,
    // data read - data can be considered sent to client for the purposes of
    // reusing the message buffer
    // - all servers
    COMPLETE_CHUNK_SEND
};

// a wrapper for a pipelined op id and a thread id
typedef struct cs_callback_id {
    int op_id; // pipelined op id
    int tid;   // thread id for particular event
} cs_callback_id;

struct cs_msg {
    msg_header h;

    union {
        struct {
            struct codes_store_request req;
            struct codes_cb_params callback;
            struct {
                int op_id; // needed to map to created op id
            } rc;
        } creq; // RECV_CLI_REQ
        struct {
            resource_callback cb;
            cs_callback_id id;
            struct {
                // see handle_pipeline_alloc_callback for usage
                int nthreads_init;
                int nthreads_fin;
            } rc;
        } palloc_callback; // PIPELINE_ALLOC_CALLBACK
        struct {
            cs_callback_id id;
        } recv_chunk; // RECV_CHUNK
        struct {
            cs_callback_id id;
            // data vs. metadata req - controls which queue will be searched
            int is_data_op;
            struct {
                int chunk_id;
                uint64_t chunk_size;
            } rc;
        } complete_sto; // COMPLETE_DISK_OP
        struct {
            cs_callback_id id;
            struct {
                int chunk_id;
                uint64_t chunk_size;
            } rc;
        } complete_chunk_send; // COMPLETE_CHUNK_SEND
    } u;
};

/// helper structures for overall message struct

struct cs_qitem {
    // my op id
    int op_id;
    struct codes_store_request req;
    struct codes_cb_params cli_cb;
    cs_pipelined_req *preq;
    struct qlist_head ql;
};
typedef struct cs_qitem cs_qitem;

#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
