/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <assert.h>
#include <limits.h>
#include <stdlib.h>
#include "msg.h"
#include "codes/model-net.h"
#include "codes/lp-msg.h"
#include "codes/model-net-sched.h"
#include "../server/rosd.h"
#include "../placement/placement.h"
#include "map_util.h"

void triton_send_request(triton_io_greq *r, tw_lp *sender, int model_net_id){
    triton_rosd_msg m;
    msg_set_header(rosd_magic, RECV_CLI_REQ, r->callback.header.src, 
            &m.h);
    m.u.creq.callback = r->callback;
    m.u.creq.req = r->req;

    /* sanity check - ack size is large enough for response */
    assert(m.u.creq.callback.event_size >= sizeof(triton_io_gresp)+sizeof(msg_header));

    unsigned long rosd_ul;
    /* find the primary ROSD server lp corresponding to this request */
    placement_find_closest(m.u.creq.req.oid, 1, &rosd_ul);
    tw_lpid rosd_lp = get_rosd_lpid((int)rosd_ul);
    //char * cat = "cli-srv-req";
    // need this to be a high-priority message
    int prio = 0;
    model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
            (void*) &prio);
    model_net_event(model_net_id, "rosd", rosd_lp, 
            ROSD_REQ_CONTROL_SZ, 0.0, sizeof(triton_rosd_msg), &m, 0,
            NULL, sender);
}

void triton_send_request_rev(uint64_t req_size, tw_lp *sender, int model_net_id){
    model_net_event_rc(model_net_id, sender, req_size);
}

void triton_send_response(
        triton_cli_callback *c, /* provided by client */
        request_params *req,    /* provided by client */
        tw_lp *sender, 
        int model_net_id, 
        int resp_size,
        int ack_status){ /* 0 on success, non-zero otherwise */
    void *msg = malloc(c->event_size);
    msg_header *h = (msg_header*)((char*)msg + c->header_offset);
    msg_set_header(c->header.magic, c->header.event_type, sender->gid, h);
    triton_io_gresp *r = (triton_io_gresp*)((char*)msg + c->resp_offset);
    r->ack_status = ack_status;
    r->resp_size = resp_size;
    r->op_index = c->op_index;
    r->req = *req;
    //char * cat = "srv-cli-ack";
    // set acks at highest priority
    int prio = 0;
    model_net_set_msg_param(MN_MSG_PARAM_SCHED, MN_SCHED_PARAM_PRIO,
            (void*) &prio);
    model_net_event(model_net_id, "rosd", c->header.src, resp_size, 
            0.0, c->event_size, msg, 0, NULL, sender);
    free(msg);
}

void triton_send_response_rev(tw_lp *sender, int model_net_id, int payload_size){
    model_net_event_rc(model_net_id, sender, payload_size);
}

/* data structure utilities (setters) */
void msg_set_cli_callback(unsigned long op_index, int event_size, 
        int header_offset, int resp_offset, triton_cli_callback *c){
    c->op_index = op_index;
    c->event_size = event_size;
    c->header_offset = header_offset;
    c->resp_offset = resp_offset;
}
void msg_set_request(enum request_type req_type, int create, uint64_t oid,
        uint64_t xfer_offset, uint64_t xfer_size, request_params *r){
    r->req_type = req_type;
    r->create = create;
    r->oid = oid;
    r->xfer_offset = xfer_offset;
    r->xfer_size = xfer_size;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
