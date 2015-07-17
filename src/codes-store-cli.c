/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include "ross.h"
#include "codes/codes-store-cli.h"
#include "codes/codes-callback.h"

void codes_store_send_req(
        struct codes_store_request const * r,
        int model_net_id,
        tw_lp * sender,
        int tag,
        struct codes_cb_info const * cb)
{
    SANITY_CHECK_CB(cb, int);

    /* TODO */
}

void codes_store_send_req_rc(tw_lp * sender)
{
    /* TODO */
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 *  indent-tabs-mode: nil
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
