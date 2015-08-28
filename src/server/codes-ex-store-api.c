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
#include <codes/codes-external-store.h>

char const * const CODES_EX_STORE_LP_NAME = "codes-external-store";

void codes_ex_store_send_req_rc(int model_net_id, tw_lp * sender)
{
    model_net_event_rc(model_net_id, sender, 0);
}

void codes_ex_store_send_req(
		int model_net_id,
		uint64_t msg_size,
        struct codes_mctx const * sender_mctx,
		tw_lp * sender)
{
    es_mn_id = model_net_id;

    /* get the external store LP */
    tw_lpid ex_store_lpid = codes_ex_store_get_lpid(0, NULL, 0);

    es_msg m_out;
    msg_set_header(ces_magic, CES_WRITE, sender->gid, &m_out.h);

    m_out.xfer_size = msg_size;
    m_out.sender_mctx = *sender_mctx;

    model_net_event_mctx(model_net_id, sender_mctx, CODES_MCTX_DEFAULT,
	CODES_EX_STORE_LP_NAME, ex_store_lpid, 
	msg_size, 0.0,
	sizeof(es_msg), &m_out, 0.0, NULL, sender);
}

tw_lpid codes_ex_store_get_lpid(
        int rel_id,
        char const * annotation,
        bool ignore_annotations)
{
   return codes_mapping_get_lpid_from_relative(rel_id, NULL,
		CODES_EX_STORE_LP_NAME, annotation, ignore_annotations); 
}

tw_lpid codes_ex_store_get_local_lpid(
        tw_lp const * lp,
        bool ignore_annotations)
{
    char group_name[MAX_NAME_LENGTH];
    char anno[MAX_NAME_LENGTH];
    int rep_id, dummy;
    tw_lpid rtn;

    codes_mapping_get_lp_info(lp->gid, group_name, NULL, NULL,
            NULL, ignore_annotations ? NULL : anno, &rep_id, &dummy);

    codes_mapping_get_lp_id(group_name, CODES_EX_STORE_LP_NAME, anno,
            ignore_annotations, rep_id, 0, &rtn);

    return rtn;
}

