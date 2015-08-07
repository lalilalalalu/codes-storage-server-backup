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

void codes_ex_store_send_req(
		int type,
		uint64_t xfer_size,
		void const * self_event,
		int self_event_size,
		tw_lp * sender)
{
    /* get the external store LP */
    tw_lpid ex_store_lpid = codes_ex_store_get_lpid(0, NULL, 0);

    es_msg * m_out;
    tw_event * e_local;
    e_local = codes_event_new(ex_store_lpid, codes_local_latency(sender), sender);
    m_out = tw_event_data(e_local); 
    msg_set_header(ces_magic, CES_WRITE, sender->gid, &m_out->h);

    m_out->num_bytes = xfer_size;
    m_out->self_event_size = self_event_size;

    void * m_pt = m_out + 1;
    if(self_event_size > 0)
       memcpy(m_pt, self_event, self_event_size);
       
    tw_event_send(e_local);
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

