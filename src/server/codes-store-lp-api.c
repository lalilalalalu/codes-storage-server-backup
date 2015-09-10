/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <ross.h>
#include <codes/codes-callback.h>
#include <codes/lp-msg.h>
#include <codes/codes_mapping.h>
#include <codes/model-net.h>
#include <codes/model-net-sched.h>

#include <codes/codes-store-lp.h>

#include "codes-store-lp-internal.h"

char const * const CODES_STORE_LP_NAME = "codes-store";

void codes_store_init_req(
        enum codes_store_req_type type,
        int priority,
        uint64_t oid,
        uint64_t xfer_offset,
        uint64_t xfer_size,
        struct codes_store_request *req)
{
    req->type = type;
    req->prio = priority;
    req->oid = oid;
    req->xfer_offset = xfer_offset;
    req->xfer_size = xfer_size;
}

void codes_store_send_req(
        struct codes_store_request const * r,
        int dest_id, // LP relative ID
        tw_lp * sender,
        int model_net_id,
        struct codes_mctx const * cli_mctx,
        int tag,
        msg_header const * h,
        struct codes_cb_info const * cb)
{
    SANITY_CHECK_CB(cb, codes_store_ret_t);

    /* get the LP */
    tw_lpid store_lpid = codes_store_get_store_lpid(dest_id, NULL, 0);

    cs_msg m;
    msg_set_header(cs_magic, CS_RECV_CLI_REQ, sender->gid, &m.h);
    GETEV(creq, &m, recv_cli_req);

    creq->req = *r;
    creq->callback.info = *cb;
    creq->callback.h = *h;
    creq->callback.tag = tag;
    creq->cli_mctx = *cli_mctx;

    int prio = 0;
    model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
            (void*) &prio);
    model_net_event_mctx(model_net_id, cli_mctx, CODES_STORE_LP_MCTX,
            CODES_STORE_LP_NAME, store_lpid, CS_REQ_CONTROL_SZ, 0.0,
            sizeof(cs_msg), &m, 0, NULL, sender);
}

void codes_store_send_req_rc(int model_net_id, tw_lp * sender)
{
    model_net_event_rc(model_net_id, sender, CS_REQ_CONTROL_SZ);
}

tw_lpid codes_store_get_store_lpid(
        int rel_id,
        char const * annotation,
        bool ignore_annotations)
{
    return codes_mapping_get_lpid_from_relative(rel_id, NULL,
            CODES_STORE_LP_NAME, annotation, ignore_annotations);
}

tw_lpid codes_store_get_local_store_lpid(
        tw_lp const * lp,
        bool ignore_annotations)
{
    char group_name[MAX_NAME_LENGTH];
    char anno[MAX_NAME_LENGTH];
    int rep_id, dummy;
    tw_lpid rtn;

    codes_mapping_get_lp_info(lp->gid, group_name, NULL, NULL,
            NULL, ignore_annotations ? NULL : anno, &rep_id, &dummy);

    codes_mapping_get_lp_id(group_name, CODES_STORE_LP_NAME, anno,
            ignore_annotations, rep_id, 0, &rtn);

    return rtn;
}

/* register/configure are defined in the lp impl file */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 *  indent-tabs-mode: nil
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
