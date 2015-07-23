/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CODES_STORE_CLI_H
#define CODES_STORE_CLI_H

#include <codes/lp-msg.h>
#include <codes/codes-callback.h>

#include "codes-store-common.h"

void codes_store_send_req(
        struct codes_store_request const * r,
        int dest_id,
        tw_lp * sender,
        int model_net_id,
        int tag,
        msg_header const * h,
        struct codes_cb_info const * cb);

void codes_store_send_req_rc(int model_net_id, tw_lp * sender);

void codes_store_register();
void codes_store_configure(int model_net_id);

#endif /* end of include guard: CODES_STORE_CLI_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 *  indent-tabs-mode: nil
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
