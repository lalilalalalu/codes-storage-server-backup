/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef ROSD_H
#define ROSD_H

#include <ross.h>    
#include "codes/lp-io.h"
#include "codes/quicklist.h"
#include "../util/msg.h"
#include "codes/lp-msg.h"
#include "codes/resource-lp.h"
#include "rosd-creq.h"

#define ROSD_REQ_CONTROL_SZ 128

/* DEBUG flags:
 * 1 - LP and msg states printed to per-lp logs
 * 2 - 1, as well as event specific logs to stdout */
#define ROSD_DEBUG 0

extern int rosd_magic; // needed by msg-v2.c
extern int replication_factor; // needed by client (for computing placement)
extern int do_rosd_lp_io; // needed by configuration in main (TODO: fix)
extern lp_io_handle rosd_io_handle; // needed by config in main (TODO: fix)

typedef struct triton_rosd_msg triton_rosd_msg;
typedef struct triton_rosd_state triton_rosd_state;

enum triton_rosd_event_type {
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

typedef struct rosd_meta_qitem rosd_meta_qitem;
typedef struct rosd_pipeline_qitem rosd_pipeline_qitem;
// a wrapper for a pipelined op id and a thread id
typedef struct rosd_callback_id {
    int op_id; // pipelined op id
    int tid;   // thread id for particular event
} rosd_callback_id;

struct triton_rosd_msg {
    msg_header h;

    union {
        struct {
            request_params req;
            triton_cli_callback callback;
            struct {
                int op_id; // needed to map to created op id
            } rc;
        } creq; // RECV_CLI_REQ
        struct {
            resource_callback cb;
            rosd_callback_id id;
            struct {
                // see handle_pipeline_alloc_callback for usage
                int nthreads_init;
                int nthreads_fin;
            } rc;
        } palloc_callback; // PIPELINE_ALLOC_CALLBACK
        struct {
            rosd_callback_id id;
        } recv_chunk; // RECV_CHUNK
        struct {
            rosd_callback_id id;
            // data vs. metadata req - controls which queue will be searched
            int is_data_op;
            struct {
                int chunk_id;
                uint64_t chunk_size;
            } rc;
        } complete_sto; // COMPLETE_DISK_OP
        struct {
            rosd_callback_id id;
            struct {
                int chunk_id;
                uint64_t chunk_size;
            } rc;
        } complete_chunk_send; // COMPLETE_CHUNK_SEND
    } u;
};

/// helper structures for overall message struct

// queue structure for pipeline operations
struct rosd_pipeline_qitem {
    // my op id
    int op_id;
    // request type (either read or write for the time being)
    enum request_type type;
    // must store both the client and the actual source, for the case when we
    // send an ack to the client from a non-primary server (all_recv+chain)
    tw_lpid cli_lp;
    triton_cli_callback cli_cb;  // for client-server reqs
    rosd_pipelined_req *req;
    // i'm part of a linked list
    struct qlist_head ql;
};

struct rosd_meta_qitem {
    // my op id
    int op_id;
    request_params req;
    // must store both the client and the actual source, for the case when we
    // send an ack to the client from a non-primary server (all_recv+chain)
    tw_lpid cli_lp;
    triton_cli_callback cli_cb;  // for client-server reqs
    struct qlist_head ql;
};

// NOTE: this should not be used by general users, it is only defined here so
// that the forwarding protocol has access to it
struct triton_rosd_state {
    // my logical (not lp) id
    int server_index;

    // list of pending pipeline operations (indexed by operation id)
    struct qlist_head pending_pipeline_ops;
    struct rc_stack * finished_pipeline_ops;
    // non-primary servers: list of full request sizes and chunks processed
    // for all_recv protocol (chain - client ack'ing, fan - server acking)
    // indexed by pair of operation id and src LP
    struct qlist_head pending_chunk_ops;
    struct rc_stack * finished_chunk_ops;

    // all servers: list of open / metadata-only calls (ie those that don't
    // need to go through the buffer allocation / threading process)
    struct qlist_head pending_meta_ops;
    struct rc_stack * finished_meta_ops;

    // unique (not reused on reverse-comp) identifiers for
    // requests
    int op_idx_pl;

    // stats
    // stats: bytes 
    // - written locally 
    // - read locally 
    // - sent to other servers (writes)
    // - sent to clients (read) 
    // - per server forwards
    // TODO: include data on acks?
    unsigned long bytes_written_local;
    unsigned long bytes_read_local;
    unsigned long bytes_forwarded;
    unsigned long bytes_returned;
    unsigned long *bytes_fwd_svrs;

    // scratch space for placement calculations
    unsigned long *oid_srv_map;

    // number of errors we have encountered (for self-suspend)
    int error_ct;

    // scratch output buffer (for lpio)
    char output_buf[256];
};

// q item for pending_chunk_ops
typedef struct rosd_chunk_req_info {
    tw_lpid cli_src; // client lp, used for identification
    int cli_op_id;   // client op id, used for identification
    uint64_t total;     // total data requested
    uint64_t received;  // total data received (for recv-based protos)
    struct qlist_head ql;
} rosd_chunk_req_info;

// registers the lp type with ross
void rosd_register();
// configures the lp given the global config object
void rosd_configure();

/// Protocol options
// X macro for forwarding modes
// format: enum, id string
#define FWD_MODES \
    X(FWD_CHAIN, "chain")\
    X(FWD_FAN,   "fan")\
    X(NUM_FWD_MODES, NULL)
    

// X macro for acking modes
// format: enum, id string
#define ACK_MODES \
    X(ACK_PRIMARY_RECV,   "primary_recv")\
    X(ACK_PRIMARY_COMMIT, "primary_commit")\
    X(ACK_ALL_RECV,       "all_recv")\
    X(ACK_ALL_COMMIT,     "all_commit")\
    X(NUM_ACK_MODES,     NULL)

#define X(a,b) a,
typedef enum {
    FWD_MODES
} rosd_fwd_mode;

typedef enum {
    ACK_MODES
} rosd_ack_mode;
#undef X

// forwarding utilities
int rosd_get_fwd_mode(char const * mode, rosd_fwd_mode *f);
int rosd_get_ack_mode(char const * mode, rosd_ack_mode *a);
int is_rosd_valid_fwd_config(rosd_fwd_mode f, rosd_ack_mode a);

#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
