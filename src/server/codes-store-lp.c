/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <ross.h>
#include <codes/codes-store-lp.h>
#include <codes/codes-callback.h>
#include <codes/codes-store-common.h>
#include <codes/lp-msg.h>
#include <codes/model-net.h>
#include <codes/model-net-sched.h>

#include "rosd.h"

void codes_store_send_req(
        struct codes_store_request const * r,
        int dest_id, // LP relative ID
        tw_lp * sender,
        int model_net_id,
        int tag,
        msg_header const * h,
        struct codes_cb_info const * cb)
{
    SANITY_CHECK_CB(cb, int);

    /* get the LP */
    tw_lpid store_lpid = codes_store_get_store_lpid(dest_id, NULL, 0);

    triton_rosd_msg m;
    msg_set_header(rosd_magic, RECV_CLI_REQ, sender->gid, &m.h);

    m.u.creq.req = *r;
    m.u.creq.callback.info = *cb;
    m.u.creq.callback.h = *h;
    m.u.creq.callback.tag = tag;

    int prio = 0;
    model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
            (void*) &prio);
    model_net_event(model_net_id, "rosd", store_lpid,
            ROSD_REQ_CONTROL_SZ, 0.0, sizeof(triton_rosd_msg), &m, 0,
            NULL, sender);
}

void codes_store_send_req_rc(int model_net_id, tw_lp * sender)
{
    model_net_event_rc(model_net_id, sender, ROSD_REQ_CONTROL_SZ);
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
