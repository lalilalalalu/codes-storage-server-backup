 /*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CODES_EXTERNAL_STORE_H
#define CODES_EXTERNAL_STORE_H

#include <stdint.h>
#include <stdbool.h>
#include <ross.h>

#include <codes/lp-msg.h>
#include <codes/codes-callback.h>
#include <codes/codes-mapping-context.h>

/**** LP name ****/
extern char const * const CODES_EX_STORE_LP_NAME;
extern int ces_magic;
extern int es_mn_id;
// LP API parameters

enum codes_ex_store_req_type {
    CES_WRITE,
    CES_WRITE_COMPLETE
};

typedef struct es_msg
{
    msg_header h;
    struct codes_mctx sender_mctx;
    int xfer_size;
} es_msg;

// API functions

void codes_ex_store_send_req(
		int model_net_id,
		uint64_t msg_size,
        struct codes_mctx const * sender_mctx,
		tw_lp * sender);

void codes_ex_store_send_req_rc(int model_net_id, tw_lp * sender);

tw_lpid codes_ex_store_get_lpid(
        int rel_id,
        char const * annotation,
        bool ignore_annotations);

tw_lpid codes_ex_store_get_local_lpid(
        tw_lp const * lp,
        bool ignore_annotations);

void codes_ex_store_register();
#endif

