/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CODES_STORE_LP_H
#define CODES_STORE_LP_H

#include <stdint.h>
#include <stdbool.h>
#include <ross.h>

#include <codes/lp-msg.h>
#include <codes/codes-callback.h>
#include <codes/codes-mapping-context.h>

/*** LP name ***/

extern char const * const CODES_STORE_LP_NAME;

// LP API parameters

enum codes_store_req_type {
    CSREQ_OPEN,
    CSREQ_CREATE,
    CSREQ_READ,
    CSREQ_WRITE
};

struct codes_store_request {
    enum codes_store_req_type type;
    int prio;
    uint64_t oid;
    uint64_t xfer_offset;
    uint64_t xfer_size;
};

enum codes_store_ret {
    CODES_STORE_OK = 0,
    CODES_STORE_ERR_OTHER
};

typedef enum codes_store_ret codes_store_ret_t;

// param init sugar
void codes_store_init_req(
        enum codes_store_req_type type,
        int priority,
        uint64_t oid,
        uint64_t xfer_offset,
        uint64_t xfer_size,
        struct codes_store_request *req);

// LP API

void codes_store_send_req(
        struct codes_store_request const * r,
        int dest_id, // LP relative ID
        tw_lp * sender,
        int model_net_id,
        struct codes_mctx const * cli_mctx,
        int tag,
        msg_header const * h,
        struct codes_cb_info const * cb);

void codes_store_send_req_rc(int model_net_id, tw_lp * sender);

tw_lpid codes_store_get_store_lpid(
        int rel_id,
        char const * annotation,
        bool ignore_annotations);

tw_lpid codes_store_get_local_store_lpid(
        tw_lp const * lp,
        bool ignore_annotations);


// LP initialization

void codes_store_register();

void codes_store_configure(int model_net_id);

void codes_store_set_scnd_net(int second_net_id);
#endif /* end of include guard: CODES_STORE_LP_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 *  indent-tabs-mode: nil
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
